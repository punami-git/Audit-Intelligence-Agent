import os
import sqlite3
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from streamlit.errors import StreamlitSecretNotFoundError

try:
    from src.audit_pattern_agent import AuditPatternDetectionAgent
    from src.audit_pattern_data import seed_audit_pattern_db
except ModuleNotFoundError:
    from audit_pattern_agent import AuditPatternDetectionAgent
    from audit_pattern_data import seed_audit_pattern_db

DB_PATH = BASE_DIR / "audit_patterns_mock.db"
TABLES = ["control_failures", "audit_findings", "risky_transactions"]


def load_table(table_name: str) -> pd.DataFrame:
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql_query(f"SELECT * FROM {table_name}", conn)


def get_pattern_agent() -> AuditPatternDetectionAgent:
    return AuditPatternDetectionAgent(db_path=str(DB_PATH))


def apply_styles() -> None:
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Manrope:wght@400;600;700;800&display=swap');

        :root {
            --ink: #0f172a;
            --sky: #0ea5e9;
            --blue: #1d4ed8;
            --card: rgba(255, 255, 255, 0.86);
            --border: rgba(148, 163, 184, 0.22);
        }

        html, body, [class*="css"] {
            font-family: 'Manrope', sans-serif;
        }

        .stApp {
            background:
                radial-gradient(circle at top left, rgba(14, 165, 233, 0.14), transparent 28%),
                radial-gradient(circle at 85% 15%, rgba(29, 78, 216, 0.10), transparent 22%),
                linear-gradient(180deg, #f8fbff 0%, #f8fafc 45%, #eef4ff 100%);
        }

        .hero {
            background: linear-gradient(135deg, #0f172a 0%, #1d4ed8 48%, #0891b2 100%);
            color: white;
            border-radius: 22px;
            padding: 34px 38px;
            margin-bottom: 24px;
            box-shadow: 0 18px 50px rgba(15, 23, 42, 0.18);
        }

        .pill {
            display: inline-block;
            padding: 9px 14px;
            margin-right: 8px;
            margin-bottom: 8px;
            border-radius: 999px;
            background: rgba(255, 255, 255, 0.14);
            border: 1px solid rgba(255, 255, 255, 0.18);
            font-size: 13px;
            font-weight: 700;
            letter-spacing: 0.25px;
        }

        .hero h1 {
            font-size: 3rem;
            line-height: 1.05;
            margin: 14px 0 16px 0;
        }

        .hero-copy {
            font-size: 1.1rem;
            line-height: 1.7;
            max-width: 980px;
        }

        .hero-list {
            margin: 18px 0 0 0;
            padding-left: 22px;
        }

        .hero-list li {
            margin-bottom: 12px;
            font-size: 1.05rem;
        }

        .pattern-card {
            background: rgba(255, 255, 255, 0.92);
            border-left: 4px solid var(--sky);
            border-radius: 12px;
            padding: 18px 20px;
            margin-bottom: 14px;
            font-size: 1.02rem;
        }

        .pattern-title {
            font-weight: 800;
            color: var(--ink);
            margin-bottom: 8px;
            font-size: 1.12rem;
        }

        div[data-testid="stMetric"] {
            background: rgba(255, 255, 255, 0.8);
            border: 1px solid var(--border);
            border-radius: 16px;
            padding: 18px 18px 14px 18px;
        }

        div[data-testid="stMetric"] label,
        div[data-testid="stMetric"] div {
            font-size: 1.02rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def load_mistral_key_from_secrets() -> None:
    try:
        secret_key = st.secrets.get("MISTRAL_API_KEY")
    except StreamlitSecretNotFoundError:
        secret_key = None

    if secret_key and not os.environ.get("MISTRAL_API_KEY"):
        os.environ["MISTRAL_API_KEY"] = secret_key


def render_pattern_cards(patterns: list[dict[str, str]]) -> None:
    if not patterns:
        st.info("No strong recurring pattern was detected from the supporting evidence tables.")
        return

    for pattern in patterns:
        st.markdown(
            f"""
            <div class="pattern-card">
              <div class="pattern-title">{pattern['pattern']}</div>
              <div>{pattern['detail']}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )


load_dotenv()
st.set_page_config(page_title="Audit Intelligence Agent", layout="wide")
apply_styles()
load_mistral_key_from_secrets()
seed_audit_pattern_db(str(DB_PATH))

if not os.environ.get("MISTRAL_API_KEY"):
    st.error("MISTRAL_API_KEY is not set. Add it as an environment variable or Streamlit secret.")
    st.stop()

control_df = load_table("control_failures")
findings_df = load_table("audit_findings")
risky_df = load_table("risky_transactions")

st.markdown(
    """
    <div class="hero">
      
      <h2>Audit Intelligence Agent</h2>
      <div class="hero-copy">
        <p style="margin:0 0 16px 0;">
          This tool analyzes control failures and past audit findings to identify recurring issues and flag weak controls.
        </p>
        <p style="margin:0 0 16px 0;">
          It is a multi-agent AI system built with LangGraph and LangChain, powered by a Large Language Model (Mistral). The pipeline orchestrates two specialized agents:
        </p>
        <ul class="hero-list">
          <li><strong>SQL Agent</strong> - translates natural language queries into SQL using tool calling, executes them against a live database, and returns structured results</li>
          <li><strong>Pattern Detection Agent</strong> - analyzes query outputs to identify recurring patterns control failures, weaknesses, and emerging risk patterns across audit findings. It summarizes and reports them.</li>
        </ul>
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

st.subheader("Mock Datasets")

table_tabs = st.tabs(["Control Failures", "Audit Findings", "Risky Transactions"])
with table_tabs[0]:
    st.dataframe(control_df, width="stretch", height=320)
with table_tabs[1]:
    st.dataframe(findings_df, width="stretch", height=320)
with table_tabs[2]:
    st.dataframe(risky_df, width="stretch", height=320)

st.subheader("Ask a Question in Natural Language")
example_questions = [
    "Analyze these control failures and tell me what recurring problems you see.",
    "Looking across finding summaries and remediation notes, what broader patterns stand out?",
    "What recurring access review or access removal problems appear in the data?",
]
st.write("Try one of these sample questions")
for example in example_questions:
    st.code(example)

question = st.text_area(
    "Ask about recurring patterns",
    height=110,
    placeholder="e.g., analyze these open failures and tell me what recurring hidden problems you see",
)
run_pattern = st.button("Run Prompt", type="primary")

if run_pattern:
    if not question.strip():
        st.warning("Please enter a question.")
    else:
        with st.spinner("Running the pattern detection workflow..."):
            agent = get_pattern_agent()
            try:
                result = agent.ask(question)
            except Exception as exc:
                st.exception(exc)
            else:
                st.markdown("### Summary of Findings")
                st.success(result.summary)

                st.markdown("### Findings")
                render_pattern_cards(result.patterns)

                st.markdown("### Question Specific SQL Query")
                st.code(result.primary_sql or "No SQL generated.", language="sql")

                st.markdown("### Question-Specific Result Table")
                primary_df = pd.DataFrame(result.primary_rows)
                if primary_df.empty:
                    st.info("The SQL query ran but returned no rows.")
                else:
                    st.dataframe(primary_df, width="stretch", height=280)

                st.markdown("### Supporting Evidence Tables")
                evidence_tabs = st.tabs(
                    [
                        "Failures by Team",
                        "Findings by System",
                        "Transactions by Region",
                        "Status Mix",
                    ]
                )
                evidence_keys = [
                    "failures_by_team",
                    "findings_by_system",
                    "flagged_transactions_by_region",
                    "status_mix",
                ]
                for tab, evidence_key in zip(evidence_tabs, evidence_keys):
                    with tab:
                        st.dataframe(
                            pd.DataFrame(result.evidence_tables[evidence_key]),
                            width="stretch",
                            height=260,
                        )

                if result.candidate_focus_area:
                    st.markdown("### Candidate Focus Area")
                    st.info(result.candidate_focus_area)
