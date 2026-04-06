import sqlite3
from collections import Counter
from dataclasses import dataclass
from typing import Any

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_mistralai import ChatMistralAI
from langgraph.graph import END, START, StateGraph
from typing_extensions import TypedDict

from src.sql_langgraph_agent import LangGraphSQLAgent


THEME_RULES = {
    "Approval workflow weakness": {
        "keywords": [
            "approval",
            "sign-off",
            "signoff",
            "manager review",
            "review step",
            "unsigned",
            "release gate",
            "approval trail",
        ],
        "label": "missing approval, skipped review, or incomplete sign-off steps",
    },
    "Manual workflow dependency": {
        "keywords": [
            "email",
            "offline",
            "outside system",
            "outside the system",
            "spreadsheet",
            "manual",
            "follow-up",
            "follow up",
        ],
        "label": "work that is still being handled outside the system or through manual follow-up",
    },
    "Access governance weakness": {
        "keywords": [
            "access",
            "privileged",
            "role update",
            "role change",
            "transfer",
            "deprovision",
            "review certification",
            "review file",
            "access removal",
        ],
        "label": "delayed access removal or incomplete access review controls",
    },
    "Workflow automation gap": {
        "keywords": [
            "automate",
            "workflow block",
            "trigger",
            "tracked system workflow",
            "centralize task tracking",
            "checkpoint",
            "certification step",
            "reminder",
        ],
        "label": "repeated requests for stronger workflow enforcement and automation",
    },
}


class AuditPatternState(TypedDict, total=False):
    question: str
    filters: dict[str, list[str]]
    primary_sql: str
    primary_rows: list[dict[str, Any]]
    evidence_tables: dict[str, list[dict[str, Any]]]
    pattern_source_rows: list[dict[str, Any]]
    pattern_basis: str
    patterns: list[dict[str, str]]
    summary: str
    candidate_focus_area: str


@dataclass
class AuditPatternResult:
    primary_sql: str
    primary_rows: list[dict[str, Any]]
    evidence_tables: dict[str, list[dict[str, Any]]]
    patterns: list[dict[str, str]]
    summary: str
    candidate_focus_area: str


class AuditPatternDetectionAgent:
    def __init__(self, db_path: str, model: str = "mistral-large-latest") -> None:
        prompt = (
            "You are an analytics assistant for issue and audit tables. "
            "Use SQLite SQL. When the user asks about recurring, hidden, semantic, or broader patterns, "
            "prefer retrieving the raw rows with the narrative text columns that explain the issue rather than only counts. "
            "Use aggregate queries only when they directly help answer the question."
        )
        self.db_path = db_path
        self.query_agent = LangGraphSQLAgent(db_path=db_path, model=model, system_prompt=prompt)
        self.summary_llm = ChatMistralAI(model=model, temperature=0, max_retries=2)
        self.graph = self._build_graph()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _run_query(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
            return [dict(row) for row in rows]

    def _build_where_clause(
        self,
        field_map: dict[str, str],
        filters: dict[str, list[str]],
        base_clauses: list[str] | None = None,
    ) -> tuple[str, tuple[Any, ...]]:
        clauses: list[str] = list(base_clauses or [])
        params: list[Any] = []

        for filter_key, column_name in field_map.items():
            values = filters.get(filter_key, [])
            if not values:
                continue
            placeholders = ", ".join(["?"] * len(values))
            clauses.append(f"{column_name} IN ({placeholders})")
            params.extend(values)

        if not clauses:
            return "", ()
        return "WHERE " + " AND ".join(clauses), tuple(params)

    def _filters_to_prompt_text(self, filters: dict[str, list[str]]) -> str:
        filter_labels = {
            "team_name": "team",
            "system_name": "system",
            "severity": "severity",
            "status": "status",
            "resolution_status": "resolution status",
            "region": "region",
        }

        parts: list[str] = []
        for key, label in filter_labels.items():
            values = filters.get(key, [])
            if values:
                parts.append(f"{label} in {', '.join(values)}")

        if not parts:
            return ""
        return " Only analyze rows where " + "; ".join(parts) + "."

    def _retrieve_primary_rows(self, state: AuditPatternState) -> AuditPatternState:
        filters = state.get("filters", {})
        scoped_question = (
            state["question"]
            + self._filters_to_prompt_text(filters)
            + " If the question is about hidden or recurring themes, return the relevant raw rows with the text columns needed for semantic review."
        )
        result = self.query_agent.ask(scoped_question)
        return {
            "primary_sql": result.sql,
            "primary_rows": result.rows,
        }

    def _retrieve_supporting_evidence(self, state: AuditPatternState) -> AuditPatternState:
        filters = state.get("filters", {})

        control_fields = {
            "team_name": "team_name",
            "system_name": "system_name",
            "severity": "severity",
            "status": "status",
        }
        findings_fields = {
            "team_name": "team_name",
            "system_name": "system_name",
            "severity": "severity",
            "status": "status",
        }
        risky_fields = {
            "region": "region",
            "resolution_status": "resolution_status",
        }

        control_base = [] if filters.get("status") else ["status != 'Closed'"]
        findings_base = [] if filters.get("status") else ["status != 'Closed'"]
        risky_base = [] if filters.get("resolution_status") else ["resolution_status != 'Resolved'"]

        control_where, control_params = self._build_where_clause(control_fields, filters, control_base)
        findings_where, findings_params = self._build_where_clause(findings_fields, filters, findings_base)
        risky_where, risky_params = self._build_where_clause(risky_fields, filters, risky_base)

        evidence_tables = {
            "failures_by_team": self._run_query(
                f"""
                SELECT team_name,
                       COUNT(*) AS failure_count,
                       SUM(CASE WHEN severity = 'High' THEN 1 ELSE 0 END) AS high_severity_count
                FROM control_failures
                {control_where}
                GROUP BY team_name
                ORDER BY failure_count DESC, high_severity_count DESC, team_name
                """,
                control_params,
            ),
            "findings_by_system": self._run_query(
                f"""
                SELECT system_name,
                       COUNT(*) AS finding_count,
                       SUM(CASE WHEN severity = 'High' THEN 1 ELSE 0 END) AS high_severity_count
                FROM audit_findings
                {findings_where}
                GROUP BY system_name
                ORDER BY finding_count DESC, high_severity_count DESC, system_name
                """,
                findings_params,
            ),
            "flagged_transactions_by_region": self._run_query(
                f"""
                SELECT region,
                       COUNT(*) AS flagged_count,
                       ROUND(AVG(amount), 2) AS average_amount,
                       SUM(CASE WHEN amount >= 10000 THEN 1 ELSE 0 END) AS over_10000_count
                FROM risky_transactions
                {risky_where}
                GROUP BY region
                ORDER BY flagged_count DESC, average_amount DESC, region
                """,
                risky_params,
            ),
            "status_mix": self._run_query(
                f"""
                SELECT source_table,
                       status,
                       COUNT(*) AS row_count
                FROM (
                    SELECT 'control_failures' AS source_table, status
                    FROM control_failures
                    {control_where}
                    UNION ALL
                    SELECT 'audit_findings' AS source_table, status
                    FROM audit_findings
                    {findings_where}
                    UNION ALL
                    SELECT 'risky_transactions' AS source_table, resolution_status AS status
                    FROM risky_transactions
                    {risky_where}
                )
                GROUP BY source_table, status
                ORDER BY source_table, row_count DESC, status
                """,
                control_params + findings_params + risky_params,
            ),
        }

        pattern_source_rows = self._run_query(
            f"""
            SELECT 'control_failures' AS source_table,
                   failure_id AS record_id,
                   team_name,
                   system_name,
                   severity,
                   status,
                   team_name || ' / ' || system_name AS context_label,
                   issue_note AS primary_text,
                   reviewer_comment AS secondary_text,
                   NULL AS tertiary_text
            FROM control_failures
            {control_where}
            UNION ALL
            SELECT 'audit_findings' AS source_table,
                   finding_id AS record_id,
                   team_name,
                   system_name,
                   severity,
                   status,
                   team_name || ' / ' || system_name AS context_label,
                   finding_summary AS primary_text,
                   remediation_note AS secondary_text,
                   NULL AS tertiary_text
            FROM audit_findings
            {findings_where}
            UNION ALL
            SELECT 'risky_transactions' AS source_table,
                   CAST(transaction_id AS TEXT) AS record_id,
                   NULL AS team_name,
                   NULL AS system_name,
                   NULL AS severity,
                   resolution_status AS status,
                   region AS context_label,
                   flag_reason AS primary_text,
                   analyst_note AS secondary_text,
                   NULL AS tertiary_text
            FROM risky_transactions
            {risky_where}
            """,
            control_params + findings_params + risky_params,
        )

        return {
            "evidence_tables": evidence_tables,
            "pattern_source_rows": pattern_source_rows,
        }

    def _build_pattern_detail(self, matches: list[dict[str, Any]], label: str) -> tuple[str, str]:
        team_counts = Counter(row["team_name"] for row in matches if row.get("team_name"))
        system_counts = Counter(row["system_name"] for row in matches if row.get("system_name"))
        context_counts = Counter(row["context_label"] for row in matches if row.get("context_label"))

        samples: list[str] = []
        for row in matches:
            snippet = row.get("primary_text") or row.get("secondary_text") or ""
            if snippet and snippet not in samples:
                samples.append(snippet)
            if len(samples) == 2:
                break

        focus_bits: list[str] = []
        if team_counts:
            focus_bits.append(f"Most often tied to {team_counts.most_common(1)[0][0]}")
        if system_counts:
            focus_bits.append(f"with concentration in {system_counts.most_common(1)[0][0]}")
        if not focus_bits and context_counts:
            focus_bits.append(f"Most visible in {context_counts.most_common(1)[0][0]}")

        detail = f"{len(matches)} in-scope records point to {label}."
        if samples:
            detail += f" Example language includes '{samples[0]}'"
            if len(samples) > 1:
                detail += f" and '{samples[1]}'."
            else:
                detail += "."
        if focus_bits:
            detail += " " + " ".join(focus_bits) + "."

        focus_area = ""
        if system_counts:
            focus_area = system_counts.most_common(1)[0][0]
        elif team_counts:
            focus_area = team_counts.most_common(1)[0][0]
        elif context_counts:
            focus_area = context_counts.most_common(1)[0][0]

        return detail, focus_area

    def _normalize_primary_rows_for_patterns(self, primary_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        text_keys = [
            "issue_note",
            "reviewer_comment",
            "finding_summary",
            "remediation_note",
            "flag_reason",
            "analyst_note",
        ]
        context_keys = [
            "control_name",
            "issue_title",
            "team_name",
            "system_name",
            "region",
            "transaction_id",
            "failure_id",
            "finding_id",
        ]

        normalized: list[dict[str, Any]] = []
        for row in primary_rows:
            available_text = {key: row.get(key) for key in text_keys if row.get(key)}
            if not available_text:
                continue

            context_parts = [str(row.get(key)) for key in context_keys if row.get(key)]
            normalized.append(
                {
                    "source_table": row.get("source_table", "primary_query"),
                    "record_id": str(
                        row.get("failure_id")
                        or row.get("finding_id")
                        or row.get("transaction_id")
                        or len(normalized) + 1
                    ),
                    "team_name": row.get("team_name"),
                    "system_name": row.get("system_name"),
                    "severity": row.get("severity"),
                    "status": row.get("status") or row.get("resolution_status"),
                    "context_label": " / ".join(context_parts[:2]) if context_parts else "question-specific rows",
                    "primary_text": available_text.get("issue_note")
                    or available_text.get("finding_summary")
                    or available_text.get("flag_reason"),
                    "secondary_text": available_text.get("reviewer_comment")
                    or available_text.get("remediation_note")
                    or available_text.get("analyst_note"),
                    "tertiary_text": None,
                }
            )

        return normalized

    def _detect_patterns(self, state: AuditPatternState) -> AuditPatternState:
        primary_rows = state.get("primary_rows", [])
        primary_pattern_rows = self._normalize_primary_rows_for_patterns(primary_rows)
        if primary_pattern_rows:
            source_rows = primary_pattern_rows
            pattern_basis = "question-specific rows"
        else:
            source_rows = state.get("pattern_source_rows", [])
            pattern_basis = "broader supporting evidence"

        patterns: list[dict[str, str]] = []
        theme_counts: list[tuple[str, int, str]] = []

        for theme_name, config in THEME_RULES.items():
            matches: list[dict[str, Any]] = []
            for row in source_rows:
                text_blob = " ".join(
                    str(row.get(field, "")).lower()
                    for field in ("context_label", "primary_text", "secondary_text", "tertiary_text")
                    if row.get(field)
                )
                if any(keyword in text_blob for keyword in config["keywords"]):
                    matches.append(row)

            if not matches:
                continue

            detail, focus_area = self._build_pattern_detail(matches, config["label"])
            patterns.append(
                {
                    "pattern": theme_name,
                    "detail": detail,
                }
            )
            theme_counts.append((theme_name, len(matches), focus_area))

        patterns = sorted(
            patterns,
            key=lambda item: next(
                (count for name, count, _ in theme_counts if name == item["pattern"]),
                0,
            ),
            reverse=True,
        )[:4]

        candidate_focus_area = "Recurring workflow issues"
        if theme_counts:
            top_theme, _, focus_area = sorted(theme_counts, key=lambda item: item[1], reverse=True)[0]
            candidate_focus_area = f"{top_theme}"
            if focus_area:
                candidate_focus_area += f" in {focus_area}"

        return {
            "patterns": patterns,
            "candidate_focus_area": candidate_focus_area,
            "pattern_basis": pattern_basis,
        }

    def _build_fallback_summary(self, state: AuditPatternState) -> str:
        primary_rows = state.get("primary_rows", [])
        patterns = state.get("patterns", [])
        pattern_basis = state.get("pattern_basis", "supporting rows")
        if not patterns:
            return (
                f"The question-specific SQL returned {len(primary_rows)} rows. "
                f"The {pattern_basis} did not show a clear repeated narrative pattern in the current scope."
            )

        top_lines = " ".join(pattern["detail"] for pattern in patterns[:3])
        return (
            f"The question-specific SQL returned {len(primary_rows)} rows. "
            f"Using the {pattern_basis}, the strongest repeated themes are: {top_lines} "
            f"Candidate focus area: {state['candidate_focus_area']}."
        )

    def _write_summary(self, state: AuditPatternState) -> AuditPatternState:
        patterns = state.get("patterns", [])
        if not patterns:
            return {"summary": self._build_fallback_summary(state)}

        evidence_lines: list[str] = []
        for table_name, rows in state.get("evidence_tables", {}).items():
            if not rows:
                continue
            evidence_lines.append(f"{table_name}: {rows[:2]}")

        pattern_lines = "\n".join(f"- {pattern['pattern']}: {pattern['detail']}" for pattern in patterns)
        prompt = (
            f"Question: {state['question']}\n\n"
            f"Question-specific SQL returned {len(state.get('primary_rows', []))} rows.\n\n"
            f"Pattern basis: {state.get('pattern_basis', 'supporting evidence')}\n\n"
            f"Detected themes:\n{pattern_lines}\n\n"
            f"Supporting evidence snapshots:\n" + "\n".join(evidence_lines) + "\n\n"
            f"Candidate focus area: {state['candidate_focus_area']}\n\n"
            "Write a short plain-English summary in 3 to 4 sentences. "
            "Group similar issues together, use cautious language, and do not invent facts. "
            "End with a final sentence that starts with 'Candidate focus area:'."
        )

        try:
            response = self.summary_llm.invoke(
                [
                    SystemMessage(
                        content=(
                            "You summarize recurring issue patterns from structured tables. "
                            "Your job is to explain the repeated themes clearly, not to recommend actions."
                        )
                    ),
                    HumanMessage(content=prompt),
                ]
            )
            summary = response.content.strip() if isinstance(response.content, str) else self._build_fallback_summary(state)
        except Exception:
            summary = self._build_fallback_summary(state)

        return {"summary": summary}

    def _build_graph(self):
        builder = StateGraph(AuditPatternState)
        builder.add_node("retrieve_primary_rows", self._retrieve_primary_rows)
        builder.add_node("retrieve_supporting_evidence", self._retrieve_supporting_evidence)
        builder.add_node("detect_patterns", self._detect_patterns)
        builder.add_node("write_summary", self._write_summary)
        builder.add_edge(START, "retrieve_primary_rows")
        builder.add_edge("retrieve_primary_rows", "retrieve_supporting_evidence")
        builder.add_edge("retrieve_supporting_evidence", "detect_patterns")
        builder.add_edge("detect_patterns", "write_summary")
        builder.add_edge("write_summary", END)
        return builder.compile()

    def ask(self, question: str, filters: dict[str, list[str]] | None = None) -> AuditPatternResult:
        result = self.graph.invoke({"question": question, "filters": filters or {}})
        return AuditPatternResult(
            primary_sql=result["primary_sql"],
            primary_rows=result["primary_rows"],
            evidence_tables=result["evidence_tables"],
            patterns=result["patterns"],
            summary=result["summary"],
            candidate_focus_area=result["candidate_focus_area"],
        )

    def get_mermaid(self) -> str:
        return self.graph.get_graph().draw_mermaid()
