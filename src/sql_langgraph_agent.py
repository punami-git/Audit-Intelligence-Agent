import json
import re
import sqlite3
from dataclasses import dataclass
from typing import Any

from langchain_core.messages import AIMessage, SystemMessage
from langchain_core.tools import tool
from langchain_mistralai import ChatMistralAI
from langgraph.graph import END, MessagesState, StateGraph
from langgraph.prebuilt import ToolNode

DISALLOWED_SQL_PATTERNS = [
    r"\\bINSERT\\b",
    r"\\bUPDATE\\b",
    r"\\bDELETE\\b",
    r"\\bDROP\\b",
    r"\\bALTER\\b",
    r"\\bTRUNCATE\\b",
    r"\\bCREATE\\b",
    r"\\bATTACH\\b",
    r"\\bDETACH\\b",
    r"\\bPRAGMA\\b",
]


@dataclass
class AgentResult:
    final_answer: str
    sql: str
    rows: list[dict[str, Any]]


class SQLTools:
    def __init__(self, db_path: str, row_limit: int = 200) -> None:
        self.db_path = db_path
        self.row_limit = row_limit

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _validate_read_only(self, sql: str) -> None:
        normalized = sql.strip()
        if not normalized:
            raise ValueError("SQL query is empty.")

        if normalized.count(";") > 1 or (";" in normalized and not normalized.endswith(";")):
            raise ValueError("Only one SQL statement is allowed.")

        head = normalized.lstrip("(").upper()
        if not (head.startswith("SELECT") or head.startswith("WITH")):
            raise ValueError("Only SELECT/WITH statements are allowed.")

        for pattern in DISALLOWED_SQL_PATTERNS:
            if re.search(pattern, normalized, flags=re.IGNORECASE):
                raise ValueError("Unsafe SQL detected. Read-only queries only.")

    def get_schema(self) -> str:
        """Return all table names with columns and types from the SQLite database."""
        with self._connect() as conn:
            tables = conn.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type='table'
                  AND name NOT LIKE 'sqlite_%'
                ORDER BY name
                """
            ).fetchall()

            lines: list[str] = []
            for table in tables:
                name = table["name"]
                columns = conn.execute(f"PRAGMA table_info({name})").fetchall()
                cols = ", ".join(f"{c['name']} {c['type'] or 'TEXT'}" for c in columns)
                lines.append(f"- {name}({cols})")

        return "\\n".join(lines) if lines else "No tables found."

    def run_sql(self, sql: str) -> str:
        """Execute a read-only SQL query against the SQLite database and return JSON rows."""
        self._validate_read_only(sql)
        cleaned = sql.rstrip(";")

        # Keep result size bounded for UI responsiveness.
        limited_sql = f"SELECT * FROM ({cleaned}) LIMIT {self.row_limit}"

        with self._connect() as conn:
            rows = conn.execute(limited_sql).fetchall()
            data = [dict(row) for row in rows]

        return json.dumps(
            {
                "sql": cleaned,
                "row_count": len(data),
                "rows": data,
                "limited_to": self.row_limit,
            },
            default=str,
        )


def _should_continue(state: MessagesState) -> str:
    last = state["messages"][-1]
    if isinstance(last, AIMessage) and last.tool_calls:
        return "tools"
    return END


class LangGraphSQLAgent:
    def __init__(
        self,
        db_path: str,
        model: str = "mistral-large-latest",
        system_prompt: str | None = None,
    ) -> None:
        self.tools = SQLTools(db_path=db_path)
        self.system_prompt = system_prompt or (
            "You are an analytics assistant for structured data. "
            "Always call get_schema first if schema is not already known in the conversation. "
            "Then call run_sql with a valid SQLite SELECT/WITH query that answers the user question. "
            "After tool results are available, return a concise answer that includes business interpretation. "
            "Never fabricate data; only use tool outputs."
        )
        get_schema_tool = tool("get_schema", description="Return all table names with columns and types from the SQLite database.")(self.tools.get_schema)
        run_sql_tool = tool("run_sql", description="Execute a read-only SQL query against the SQLite database and return JSON rows.")(self.tools.run_sql)
        self.tool_list = [get_schema_tool, run_sql_tool]
        self.tool_node = ToolNode(self.tool_list)
        self.llm = ChatMistralAI(model=model, temperature=0, max_retries=2).bind_tools(self.tool_list)
        self.graph = self._build_graph()

    def _assistant_node(self, state: MessagesState) -> MessagesState:
        system_message = SystemMessage(
            content=self.system_prompt
        )
        response = self.llm.invoke([system_message] + state["messages"])
        return {"messages": [response]}

    def _build_graph(self):
        graph_builder = StateGraph(MessagesState)
        graph_builder.add_node("assistant", self._assistant_node)
        graph_builder.add_node("tools", self.tool_node)
        graph_builder.set_entry_point("assistant")
        graph_builder.add_conditional_edges("assistant", _should_continue, {"tools": "tools", END: END})
        graph_builder.add_edge("tools", "assistant")
        return graph_builder.compile()

    def ask(self, question: str) -> AgentResult:
        result = self.graph.invoke({"messages": [("user", question)]})
        messages = result["messages"]

        last_sql = ""
        rows: list[dict[str, Any]] = []
        for msg in messages:
            if getattr(msg, "name", "") == "run_sql":
                payload = json.loads(msg.content)
                last_sql = payload.get("sql", "")
                rows = payload.get("rows", [])

        final_answer = messages[-1].content if messages else "No response generated."
        return AgentResult(final_answer=final_answer, sql=last_sql, rows=rows)

    def get_mermaid(self) -> str:
        return self.graph.get_graph().draw_mermaid()
