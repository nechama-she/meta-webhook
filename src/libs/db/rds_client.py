"""PostgreSQL client – look up leads by facebook_user_id."""

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
