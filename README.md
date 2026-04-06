# Audit Pattern Detection Assistant

This project is a Streamlit app for pattern detection over mock audit style tables. It uses:
- a LangGraph SQL agent to retrieve relevant rows from SQLite
- a second LangGraph pattern workflow to detect recurring themes across narrative issue text
- Mistral to generate SQL and write the final plain English summary

# Streamlit App
https://audit-intelligence-agent.streamlit.app/

## Tech Stack
- Streamlit
- SQLite
- LangGraph
- LangChain tools
- Mistral 



## App Features
- browse the mock `control_failures`, `audit_findings`, and `risky_transactions` tables
- ask semantic questions about recurring issue themes, messy reviewer notes, and broader process weaknesses
- inspect the generated SQL, result tables, supporting evidence tables, detected patterns, and summary text

```
