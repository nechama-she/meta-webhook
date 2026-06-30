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


def lead_exists_by_leadgen_id(leadgen_id: str) -> bool:
    """Return True if a lead with this leadgen_id already exists in RDS."""
    try:
        conn = _get_connection()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM leads WHERE leadgen_id = %s LIMIT 1",
                (leadgen_id,),
            )
            return cur.fetchone() is not None
    except Exception as exc:
        print(f"RDS leadgen_id check error: {repr(exc)}")
        global _conn
        _conn = None
        return False


def get_lead_id_by_facebook_user_id(facebook_user_id: str) -> str | None:
    """Look up the lead PK (id) for a given facebook_user_id."""
    try:
        conn = _get_connection()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM leads WHERE facebook_user_id = %s LIMIT 1",
                (facebook_user_id,),
            )
            row = cur.fetchone()
            return str(row[0]) if row else None
    except Exception as exc:
        print(f"RDS lead_id lookup error: {repr(exc)}")
        global _conn
        _conn = None
        return None


def get_lead_id_by_phone(phone: str) -> str | None:
    """Look up the lead PK (id) for a given phone number (digits, no country code)."""
    try:
        conn = _get_connection()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM leads WHERE phone = %s LIMIT 1",
                (phone,),
            )
            row = cur.fetchone()
            return str(row[0]) if row else None
    except Exception as exc:
        print(f"RDS lead_id by phone lookup error: {repr(exc)}")
        global _conn
        _conn = None
        return None


def is_company_number(aircall_number_id: int) -> bool:
    """Return True if the given Aircall number_id belongs to a company line (not a rep)."""
    try:
        conn = _get_connection()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM companies WHERE aircall_number_id = %s LIMIT 1",
                (str(aircall_number_id),),
            )
            return cur.fetchone() is not None
    except Exception as exc:
        print(f"RDS is_company_number error: {repr(exc)}")
        global _conn
        _conn = None
        return False


def get_company_by_aircall_number_id(aircall_number_id: int | str) -> dict | None:
    """Return company details for an Aircall number ID.

    Uses companies.aircall_number_id as the source of truth so webhook
    processing can map Aircall events to the canonical company name.
    """
    try:
        conn = _get_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, name, aircall_name, aircall_number_id, phone
                FROM companies
                WHERE aircall_number_id = %s
                LIMIT 1
                """,
                (str(aircall_number_id),),
            )
            row = cur.fetchone()
            if not row:
                return None
            return {
                "id": row[0],
                "name": row[1],
                "aircall_name": row[2],
                "aircall_number_id": row[3],
                "phone": row[4],
            }
    except Exception as exc:
        print(f"RDS company by aircall_number_id error: {repr(exc)}")
        global _conn
        _conn = None
        return None


def get_smartmoving_id_by_phone(phone: str, company_id: str | None = None) -> str | None:
    """Look up the smartmoving_id for a given phone number.

    If company_id is provided, filters by company id to avoid
    returning the wrong lead when the same phone exists across companies.
    Falls back to phone-only lookup if no company match is found.
    Returns the smartmoving_id string or None if not found.
    """
    try:
        conn = _get_connection()
        with conn.cursor() as cur:
            if company_id:
                sql = cur.mogrify(
                    "SELECT smartmoving_id "
                    "FROM leads "
                    "WHERE phone = %s "
                    "AND company_id = %s "
                    "AND smartmoving_id IS NOT NULL "
                    "LIMIT 1",
                    (phone, company_id),
                )
                print(
                    sql
                )
                cur.execute(
                   sql
                )
                row = cur.fetchone()
                print(f"RDS phone lookup response: {row!r}")
                if row:
                    return row[0]
                print(f"RDS phone lookup: no match for phone={phone} company_id={company_id!r}, falling back to phone-only")
                cur.execute(
                    "SELECT smartmoving_id FROM leads WHERE phone = %s AND smartmoving_id IS NOT NULL LIMIT 1",
                    (phone,),
                )
                row = cur.fetchone()
                print(f"RDS phone lookup fallback response: {row!r}")
                return row[0] if row else None
            else:
                cur.execute(
                    "SELECT smartmoving_id FROM leads WHERE phone = %s AND smartmoving_id IS NOT NULL LIMIT 1",
                    (phone,),
                )
                row = cur.fetchone()
                print(f"RDS phone lookup response: {row!r}")
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
                "SELECT aircall_number_id FROM users WHERE name = %s LIMIT 1",
                (name,),
            )
            row = cur.fetchone()
            return row[0] if row else None
    except Exception as exc:
        print(f"RDS sales_rep lookup error: {repr(exc)}")
        global _conn
        _conn = None
        return None


def get_user_id_by_name(name: str) -> str | None:
    """Look up user id from users table by name."""
    try:
        conn = _get_connection()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM users WHERE name = %s LIMIT 1",
                (name,),
            )
            row = cur.fetchone()
            print(f"RDS user_id lookup response for {name!r}: {row!r}")
            return str(row[0]) if row else None
    except Exception as exc:
        print(f"RDS user_id lookup error: {repr(exc)}")
        global _conn
        _conn = None
        return None


def _exec_fetchone(query: str, params: tuple, label: str):
    """Mogrify, print, execute and fetchone. Returns row or None."""
    try:
        conn = _get_connection()
        with conn.cursor() as cur:
            sql = cur.mogrify(query, params)
            print(sql)
            cur.execute(sql)
            row = cur.fetchone()
            print(f"RDS {label} response: {row!r}")
            return row
    except Exception as exc:
        print(f"RDS {label} error: {repr(exc)}")
        global _conn
        _conn = None
        return None


def get_user_id_by_aircall_number_id(aircall_number_id: int | str) -> str | None:
    """Look up user id (rep_id) from users table by aircall_number_id."""
    row = _exec_fetchone(
        "SELECT id FROM users WHERE aircall_number_id = %s LIMIT 1",
        (str(aircall_number_id),),
        "rep_id lookup",
    )
    return str(row[0]) if row else None


def get_smartmoving_id_by_assign_to(phone: str, rep_id: str) -> str | None:
    """Look up smartmoving_id by phone and assigned_to (rep_id)."""
    row = _exec_fetchone(
        "SELECT smartmoving_id "
        "FROM leads "
        "WHERE phone = %s "
        "AND assigned_to = %s "
        "AND smartmoving_id IS NOT NULL "
        "LIMIT 1",
        (phone, rep_id),
        "phone+assigned_to lookup",
    )
    return row[0] if row else None


def set_lead_assigned_to(smartmoving_id: str, user_id: str) -> bool:
    """Update leads.assigned_to for the given smartmoving_id."""
    try:
        conn = _get_connection()
        with conn.cursor() as cur:
            sql = cur.mogrify(
                "UPDATE leads SET assigned_to = %s WHERE smartmoving_id = %s",
                (user_id, smartmoving_id),
            )
            print(sql)
            cur.execute(sql)
            print(f"RDS set assigned_to rowcount: {cur.rowcount}")
            return cur.rowcount > 0
    except Exception as exc:
        print(f"RDS set assigned_to error: {repr(exc)}")
        global _conn
        _conn = None
        return False


def get_company_id_by_name(name: str) -> str | None:
    """Look up company id by name."""
    row = _exec_fetchone(
        "SELECT id FROM companies WHERE name = %s LIMIT 1",
        (name,),
        "company_id by name lookup",
    )
    return str(row[0]) if row else None


def set_lead_company_id(smartmoving_id: str, company_id: str) -> bool:
    """Update leads.company_id for the given smartmoving_id."""
    row = _exec_fetchone(
        "UPDATE leads SET company_id = %s WHERE smartmoving_id = %s RETURNING smartmoving_id",
        (company_id, smartmoving_id),
        "set company_id",
    )
    return row is not None


def set_lead_status(smartmoving_id: str, status: str) -> bool:
    """Update leads.status for the given smartmoving_id."""
    row = _exec_fetchone(
        "UPDATE leads SET status = %s WHERE smartmoving_id = %s RETURNING smartmoving_id",
        (status, smartmoving_id),
        "set status",
    )
    return row is not None


def get_lead_by_smartmoving_id(smartmoving_id: str) -> dict | None:
    """Look up lead info by smartmoving_id, joining companies for company name.

    Returns dict with id, full_name, phone, company_name, company_id, company_phone or None if not found.
    """
    try:
        conn = _get_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT l.id, l.full_name, l.phone, c.name, c.id, c.phone
                FROM leads l
                LEFT JOIN companies c ON l.company_id = c.id
                WHERE l.smartmoving_id = %s
                LIMIT 2
                """,
                (smartmoving_id,),
            )
            rows = cur.fetchall()
            if not rows:
                return None
            if len(rows) > 1:
                print(
                    "RDS lead by smartmoving_id error: "
                    f"ambiguous match for smartmoving_id={smartmoving_id!r} (count>1)"
                )
                return None
            row = rows[0]
            return {
                "id": str(row[0]),
                "full_name": row[1],
                "phone": row[2],
                "company_name": row[3],
                "company_id": row[4],
                "company_phone": row[5],
            }
    except Exception as exc:
        print(f"RDS lead by smartmoving_id error: {repr(exc)}")
        global _conn
        _conn = None
        return None


def get_company_template(company_id: str, column: str) -> str | None:
    """Look up a message template column for a company from company_message_templates."""
    row = _exec_fetchone(
        f"SELECT {column} FROM company_message_templates WHERE company_id = %s LIMIT 1",
        (str(company_id),),
        f"{column} lookup",
    )
    return row[0] if row else None
