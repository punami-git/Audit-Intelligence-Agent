# Audit Pattern Detection Assistant

This project is a Streamlit app for semantic issue-pattern analysis over mock audit-style tables. It uses:
- a LangGraph SQL agent to retrieve relevant rows from SQLite
- a second LangGraph pattern workflow to detect recurring themes across narrative issue text
- Mistral to generate SQL and write the final plain-English summary

## Tech Stack
- Streamlit
- SQLite
- LangGraph
- LangChain tools
- Mistral (`MISTRAL_API_KEY`)

## Local Run

```bash
cd "/Users/punamichowdary/Documents/New project/audit_pattern_detection_assistant"
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export MISTRAL_API_KEY="your_mistral_key"
python scripts/seed_audit_pattern_db.py
streamlit run app.py
```

## App Features
- browse the mock `control_failures`, `audit_findings`, and `risky_transactions` tables
- filter the data by team, system, severity, status, and region
- ask semantic questions about recurring issue themes, messy reviewer notes, and broader process weaknesses
- inspect the generated SQL, result tables, supporting evidence tables, detected patterns, and summary text

## Notebook
The notebooks live at:

- `/Users/punamichowdary/Documents/New project/audit_pattern_detection_assistant/notebooks/audit_pattern_detection_assistant.ipynb`
- `/Users/punamichowdary/Documents/New project/audit_pattern_detection_assistant/notebooks/audit_pattern_detection_assistant_all_in_one.ipynb`

## Streamlit Cloud
If you deploy this app on Streamlit Community Cloud, add this secret:

```toml
MISTRAL_API_KEY="your_mistral_key"
```
