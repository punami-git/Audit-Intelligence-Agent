import sqlite3
from pathlib import Path


DDL = """
DROP TABLE IF EXISTS risky_transactions;
DROP TABLE IF EXISTS audit_findings;
DROP TABLE IF EXISTS control_failures;

CREATE TABLE control_failures (
    failure_id TEXT PRIMARY KEY,
    team_name TEXT NOT NULL,
    system_name TEXT NOT NULL,
    severity TEXT NOT NULL,
    status TEXT NOT NULL,
    issue_note TEXT NOT NULL,
    reviewer_comment TEXT NOT NULL
);

CREATE TABLE audit_findings (
    finding_id TEXT PRIMARY KEY,
    team_name TEXT NOT NULL,
    system_name TEXT NOT NULL,
    severity TEXT NOT NULL,
    finding_summary TEXT NOT NULL,
    remediation_note TEXT NOT NULL,
    status TEXT NOT NULL
);

CREATE TABLE risky_transactions (
    transaction_id INTEGER PRIMARY KEY,
    amount REAL NOT NULL,
    region TEXT NOT NULL,
    flag_reason TEXT NOT NULL,
    analyst_note TEXT NOT NULL,
    resolution_status TEXT NOT NULL
);
"""


def seed_audit_pattern_db(db_path: str = "audit_patterns_mock.db") -> Path:
    path = Path(db_path)
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    cur.executescript(DDL)

    control_failures = [
        ("F001", "Payments", "PayFlow", "High", "Open", "Approval was missing for release", "Final sign-off not attached"),
        ("F002", "Payments", "PayFlow", "High", "Open", "Manager review not completed before release", "Looks like review step was skipped"),
        ("F003", "Ops", "VendorHub", "Medium", "Open", "Team relied on email confirmation", "Process handled outside system"),
        ("F004", "Ops", "VendorHub", "Medium", "In Progress", "Manual spreadsheet tracking was used to confirm review", "No automated reminder existed"),
        ("F005", "Ops", "AccessPro", "High", "Open", "User retained elevated access after transfer", "Role update was delayed"),
        ("F006", "Ops", "AccessPro", "Medium", "Open", "Evidence of periodic access review incomplete", "Review documentation missing"),
    ]

    audit_findings = [
        ("A001", "Payments", "PayFlow", "High", "Several payments were released without final approval evidence", "Enforce final approval before release", "Open"),
        ("A002", "Payments", "PayFlow", "High", "Review stage appears to have been skipped in multiple cases", "Add workflow block for unsigned releases", "Open"),
        ("A003", "Ops", "VendorHub", "Medium", "Team relied on offline communication to complete checks", "Move review steps into tracked system workflow", "Open"),
        ("A004", "Ops", "AccessPro", "High", "Users kept elevated access after role changes", "Automate access removal trigger", "Open"),
        ("A005", "Ops", "AccessPro", "Medium", "Periodic access review was not consistently documented", "Add quarterly review certification step", "Open"),
        ("A006", "Payments", "PayFlow", "High", "The approval trail was fragmented and final sign-off was missing", "Add a release gate that checks for final approval", "Open"),
    ]

    risky_transactions = [
        (1001, 22000.0, "North", "Missing approval", "Sign-off appears absent from workflow", "Open"),
        (1002, 18000.0, "North", "Review incomplete", "Manager review not recorded", "Open"),
        (1003, 9500.0, "West", "Manual override", "Team confirmed outside the platform", "Open"),
        (1004, 15000.0, "East", "Access issue", "Privileged access stayed active after transfer", "Open"),
        (1005, 12000.0, "East", "Review evidence missing", "Quarterly review file was not attached", "Open"),
        (1006, 26000.0, "North", "No documented approval", "Final sign-off could not be located in the workflow", "Open"),
        (1007, 12500.0, "West", "Manual follow-up", "Checklist completion was tracked in a spreadsheet", "Open"),
    ]

    cur.executemany("INSERT INTO control_failures VALUES (?, ?, ?, ?, ?, ?, ?)", control_failures)
    cur.executemany("INSERT INTO audit_findings VALUES (?, ?, ?, ?, ?, ?, ?)", audit_findings)
    cur.executemany("INSERT INTO risky_transactions VALUES (?, ?, ?, ?, ?, ?)", risky_transactions)

    conn.commit()
    conn.close()
    return path


if __name__ == "__main__":
    db = seed_audit_pattern_db()
    print(f"Created {db} with mock audit pattern data.")
