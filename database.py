from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "irpf.db"


SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS team_members (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    role TEXT,
    active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS clients (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    normalized_name TEXT NOT NULL UNIQUE,
    full_name TEXT NOT NULL,
    group_name TEXT,
    meeting_status TEXT,
    complexity_level TEXT,
    tax_status TEXT,
    assigned_preparer TEXT,
    post_filing_status TEXT,
    documentation_status TEXT,
    active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS client_private (
    client_id INTEGER PRIMARY KEY,
    cpf TEXT,
    phone TEXT,
    gov_password TEXT,
    has_digital_certificate INTEGER NOT NULL DEFAULT 0,
    power_of_attorney TEXT,
    notes TEXT,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (client_id) REFERENCES clients(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS documents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id INTEGER NOT NULL,
    document_type TEXT,
    institution TEXT,
    status TEXT,
    last_update TEXT,
    control_key TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (client_id) REFERENCES clients(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS contact_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id INTEGER NOT NULL,
    contact_date TEXT NOT NULL,
    channel TEXT,
    subject TEXT,
    notes TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (client_id) REFERENCES clients(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS declaration_checkpoints (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id INTEGER NOT NULL,
    step_key TEXT NOT NULL,
    step_label TEXT NOT NULL,
    completed INTEGER NOT NULL DEFAULT 0,
    note TEXT,
    updated_by TEXT,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (client_id) REFERENCES clients(id) ON DELETE CASCADE,
    UNIQUE (client_id, step_key)
);
"""


def now_iso() -> str:
    return datetime.now().replace(microsecond=0).isoformat(sep=" ")


def get_connection(db_path: Path | str = DB_PATH) -> sqlite3.Connection:
    DATA_DIR.mkdir(exist_ok=True)
    connection = sqlite3.connect(str(db_path))
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def initialize_database(db_path: Path | str = DB_PATH) -> None:
    with get_connection(db_path) as connection:
        connection.executescript(SCHEMA)
        columns = {
            row["name"] for row in connection.execute("PRAGMA table_info(client_private)").fetchall()
        }
        if "has_digital_certificate" not in columns:
            connection.execute(
                "ALTER TABLE client_private ADD COLUMN has_digital_certificate INTEGER NOT NULL DEFAULT 0"
            )


def seed_team_members(connection: sqlite3.Connection, members: list[tuple[str, str]]) -> None:
    timestamp = now_iso()
    for name, role in members:
        connection.execute(
            """
            INSERT INTO team_members (name, role, created_at, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
                role = excluded.role,
                updated_at = excluded.updated_at
            """,
            (name, role, timestamp, timestamp),
        )


def upsert_client(
    connection: sqlite3.Connection,
    *,
    normalized_name: str,
    full_name: str,
    group_name: str = "",
    meeting_status: str = "",
    complexity_level: str = "",
    tax_status: str = "",
    assigned_preparer: str = "",
    post_filing_status: str = "",
    documentation_status: str = "",
    cpf: str = "",
    phone: str = "",
    gov_password: str = "",
    has_digital_certificate: bool = False,
    power_of_attorney: str = "",
    notes: str = "",
) -> int:
    timestamp = now_iso()
    connection.execute(
        """
        INSERT INTO clients (
            normalized_name,
            full_name,
            group_name,
            meeting_status,
            complexity_level,
            tax_status,
            assigned_preparer,
            post_filing_status,
            documentation_status,
            created_at,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(normalized_name) DO UPDATE SET
            full_name = excluded.full_name,
            group_name = excluded.group_name,
            meeting_status = excluded.meeting_status,
            complexity_level = excluded.complexity_level,
            tax_status = excluded.tax_status,
            assigned_preparer = excluded.assigned_preparer,
            post_filing_status = excluded.post_filing_status,
            documentation_status = excluded.documentation_status,
            active = 1,
            updated_at = excluded.updated_at
        """,
        (
            normalized_name,
            full_name,
            group_name,
            meeting_status,
            complexity_level,
            tax_status,
            assigned_preparer,
            post_filing_status,
            documentation_status,
            timestamp,
            timestamp,
        ),
    )
    client_id = connection.execute(
        "SELECT id FROM clients WHERE normalized_name = ?",
        (normalized_name,),
    ).fetchone()["id"]
    connection.execute(
        """
        INSERT INTO client_private (
            client_id,
            cpf,
            phone,
            gov_password,
            has_digital_certificate,
            power_of_attorney,
            notes,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(client_id) DO UPDATE SET
            cpf = excluded.cpf,
            phone = excluded.phone,
            gov_password = excluded.gov_password,
            has_digital_certificate = excluded.has_digital_certificate,
            power_of_attorney = excluded.power_of_attorney,
            notes = excluded.notes,
            updated_at = excluded.updated_at
        """,
        (
            client_id,
            cpf,
            phone,
            gov_password,
            int(has_digital_certificate),
            power_of_attorney,
            notes,
            timestamp,
        ),
    )
    return client_id


def replace_client_documents(
    connection: sqlite3.Connection,
    client_id: int,
    documents: list[dict[str, Any]],
) -> None:
    timestamp = now_iso()
    connection.execute("DELETE FROM documents WHERE client_id = ?", (client_id,))
    connection.executemany(
        """
        INSERT INTO documents (
            client_id,
            document_type,
            institution,
            status,
            last_update,
            control_key,
            created_at,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                client_id,
                document.get("document_type", ""),
                document.get("institution", ""),
                document.get("status", ""),
                document.get("last_update", ""),
                document.get("control_key", ""),
                timestamp,
                timestamp,
            )
            for document in documents
        ],
    )


def list_clients(
    connection: sqlite3.Connection,
    *,
    search: str = "",
    tax_status: str = "",
    assigned_preparer: str = "",
    documentation_status: str = "",
) -> list[sqlite3.Row]:
    query = """
    SELECT
        clients.id,
        clients.full_name,
        clients.group_name,
        clients.meeting_status,
        clients.complexity_level,
        clients.tax_status,
        clients.assigned_preparer,
        clients.post_filing_status,
        clients.documentation_status,
        COUNT(documents.id) AS total_documents,
        SUM(CASE WHEN documents.status = 'RECEBIDO' THEN 1 ELSE 0 END) AS received_documents
    FROM clients
    LEFT JOIN documents ON documents.client_id = clients.id
    WHERE clients.active = 1
      AND (? = '' OR clients.full_name LIKE ?)
      AND (? = '' OR clients.tax_status = ?)
      AND (? = '' OR clients.assigned_preparer = ?)
      AND (? = '' OR clients.documentation_status = ?)
    GROUP BY clients.id
    ORDER BY clients.full_name
    """
    search_term = f"%{search}%"
    return connection.execute(
        query,
        (
            search,
            search_term,
            tax_status,
            tax_status,
            assigned_preparer,
            assigned_preparer,
            documentation_status,
            documentation_status,
        ),
    ).fetchall()


def get_client(connection: sqlite3.Connection, client_id: int) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT
            clients.*,
            client_private.cpf,
            client_private.phone,
            client_private.gov_password,
            client_private.has_digital_certificate,
            client_private.power_of_attorney,
            client_private.notes
        FROM clients
        LEFT JOIN client_private ON client_private.client_id = clients.id
        WHERE clients.id = ?
        """,
        (client_id,),
    ).fetchone()


def get_client_documents(connection: sqlite3.Connection, client_id: int) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT
            id,
            document_type,
            institution,
            status,
            last_update,
            control_key
        FROM documents
        WHERE client_id = ?
        ORDER BY document_type, institution
        """,
        (client_id,),
    ).fetchall()


def delete_client(connection: sqlite3.Connection, client_id: int) -> None:
    connection.execute("UPDATE clients SET active = 0, updated_at = ? WHERE id = ?", (now_iso(), client_id))
