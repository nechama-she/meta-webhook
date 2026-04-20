"""PostgreSQL client – look up leads and save followups."""

import json
import os

import boto3

_secret_arn = os.environ.get("RDS_SECRET_ARN", "")
_db_host = os.environ.get("RDS_HOST", "")
_db_name = os.environ.get("RDS_DB_NAME", "moving_crm")

_conn = None


def _get_credentials() -> dict:
    """Fetch DB credentials from Secrets Manager."""
    client = boto3.client("secretsmanager")
    resp = client.get_secret_value(SecretId=_secret_arn)
    return json.loads(resp["SecretString"])


def _get_connection():
    """Return a reusable psycopg2 connection (created on first call)."""
    import psycopg2

    global _conn
    if _conn is None or _conn.closed:
        creds = _get_credentials()
        _conn = psycopg2.connect(
            host=_db_host,
            port=creds.get("port", 5432),
            dbname=_db_name,
            user=creds["username"],
            password=creds["password"],
            sslmode="require",
            connect_timeout=10,
        )
        _conn.autocommit = True
    return _conn


def get_smartmoving_id(facebook_user_id: str) -> str | None:
    """Look up the smartmoving_id for a given facebook_user_id.

    Returns the smartmoving_id string or None if not found.
    """
    try:
        conn = _get_connection()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT smartmoving_id FROM leads WHERE facebook_user_id = %s LIMIT 1",
                (facebook_user_id,),
            )
            row = cur.fetchone()
            return row[0] if row else None
    except Exception as exc:
        print(f"RDS lookup error: {repr(exc)}")
        # Reset connection on error so next call retries
        global _conn
        _conn = None
        return None


def get_smartmoving_id_by_phone(phone: str) -> str | None:
    """Look up the smartmoving_id for a given phone number.

    Returns the smartmoving_id string or None if not found.
    """
    try:
        conn = _get_connection()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT smartmoving_id FROM leads WHERE phone = %s LIMIT 1",
                (phone,),
            )
            row = cur.fetchone()
            return row[0] if row else None
    except Exception as exc:
        print(f"RDS phone lookup error: {repr(exc)}")
        global _conn
        _conn = None
        return None


_followups_table_created = False


def _ensure_followups_table():
    """Create the followups table if it doesn't exist."""
    global _followups_table_created
    if _followups_table_created:
        return
    try:
        conn = _get_connection()
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS followups (
                    note_id UUID PRIMARY KEY,
                    smartmoving_id UUID NOT NULL,
                    type INTEGER,
                    title TEXT,
                    assigned_to_id UUID,
                    due_date_time TIMESTAMPTZ,
                    completed_at_utc TIMESTAMPTZ,
                    notes TEXT,
                    completed BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_followups_smartmoving_id
                ON followups (smartmoving_id)
            """)
            cur.execute("""
                ALTER TABLE followups ADD COLUMN IF NOT EXISTS assigned_to_id UUID
            """)
        _followups_table_created = True
        print("followups table ensured")
    except Exception as exc:
        print(f"RDS create followups table error: {repr(exc)}")
        global _conn
        _conn = None


def save_followup(followup: dict) -> bool:
    """Upsert a followup record into the followups table.

    Returns True on success, False on error.
    """
    _ensure_followups_table()
    try:
        conn = _get_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO followups (
                    note_id, smartmoving_id, type, title, assigned_to_id,
                    due_date_time, completed_at_utc, notes, completed
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (note_id) DO UPDATE SET
                    type = EXCLUDED.type,
                    title = EXCLUDED.title,
                    assigned_to_id = EXCLUDED.assigned_to_id,
                    due_date_time = EXCLUDED.due_date_time,
                    completed_at_utc = EXCLUDED.completed_at_utc,
                    notes = EXCLUDED.notes,
                    completed = EXCLUDED.completed
                """,
                (
                    followup["id"],
                    followup["opportunityId"],
                    followup.get("type"),
                    followup.get("title"),
                    followup.get("assignedToId"),
                    followup.get("dueDateTime"),
                    followup.get("completedAtUtc"),
                    followup.get("notes"),
                    followup.get("completed", False),
                ),
            )
            print(f"Followup saved: {followup['id']}")
            return True
    except Exception as exc:
        print(f"RDS save followup error: {repr(exc)}")
        global _conn
        _conn = None
        return False


def delete_followup(note_id: str) -> bool:
    """Delete a followup by note_id.

    Returns True on success, False on error.
    """
    _ensure_followups_table()
    try:
        conn = _get_connection()
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM followups WHERE note_id = %s",
                (note_id,),
            )
            print(f"Followup deleted: {note_id}")
            return True
    except Exception as exc:
        print(f"RDS delete followup error: {repr(exc)}")
        global _conn
        _conn = None
        return False


def get_sales_rep(name: str) -> str | None:
    """Look up aircall_number_id for a sales rep by name.

    Returns aircall_number_id string or None if not found.
    """
    try:
        conn = _get_connection()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT aircall_number_id FROM sales_reps WHERE name = %s LIMIT 1",
                (name,),
            )
            row = cur.fetchone()
            return row[0] if row else None
    except Exception as exc:
        print(f"RDS sales_rep lookup error: {repr(exc)}")
        global _conn
        _conn = None
        return None


def get_lead_by_smartmoving_id(smartmoving_id: str) -> dict | None:
    """Look up lead info by smartmoving_id, joining companies for company name.

    Returns dict with full_name, phone, company_name or None if not found.
    """
    try:
        conn = _get_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT l.full_name, l.phone, c.name
                FROM leads l
                LEFT JOIN companies c ON l.company_id = c.id
                WHERE l.smartmoving_id = %s
                LIMIT 1
                """,
                (smartmoving_id,),
            )
            row = cur.fetchone()
            if not row:
                return None
            return {
                "full_name": row[0],
                "phone": row[1],
                "company_name": row[2],
            }
    except Exception as exc:
        print(f"RDS lead by smartmoving_id error: {repr(exc)}")
        global _conn
        _conn = None
        return None
