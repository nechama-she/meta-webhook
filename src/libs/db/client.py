"""DynamoDB persistence layer."""

import uuid

import boto3
from boto3.dynamodb.conditions import Key

from db.config import EVENTS_TABLE, CONVERSATIONS_TABLE, LEADS_TABLE, CACHE_TABLE

_dynamo = boto3.resource("dynamodb")
_client = boto3.client("dynamodb")


def _ensure_table(name: str, key_schema: list, attr_defs: list) -> None:
    """Create a DynamoDB table if it doesn't already exist."""
    existing = _client.list_tables()["TableNames"]
    if name in existing:
        return
    _client.create_table(
        TableName=name,
        KeySchema=key_schema,
        AttributeDefinitions=attr_defs,
        BillingMode="PAY_PER_REQUEST",
    )
    _client.get_waiter("table_exists").wait(TableName=name)
    print(f"Created DynamoDB table '{name}'")


_ensure_table(
    CACHE_TABLE,
    [{"AttributeName": "cache_key", "KeyType": "HASH"}],
    [{"AttributeName": "cache_key", "AttributeType": "S"}],
)

_events_table = _dynamo.Table(EVENTS_TABLE)
_conversations_table = _dynamo.Table(CONVERSATIONS_TABLE)
_leads_table = _dynamo.Table(LEADS_TABLE)
_cache_table = _dynamo.Table(CACHE_TABLE)


# ── Cache ─────────────────────────────────────────────────────────────

def cache_get(key: str) -> str | None:
    """Return the cached value for *key*, or None if not found."""
    try:
        resp = _cache_table.get_item(Key={"cache_key": key})
        item = resp.get("Item")
        return item["value"] if item else None
    except Exception as exc:
        print(f"Cache get error for '{key}': {repr(exc)}")
        return None


def cache_set(key: str, value: str) -> None:
    """Store a value in the cache."""
    try:
        _cache_table.put_item(Item={"cache_key": key, "value": value})
    except Exception as exc:
        print(f"Cache set error for '{key}': {repr(exc)}")


# ── Generic event persistence ────────────────────────────────────────

def save_event(event_body: dict, *, table_name: str = EVENTS_TABLE, primary_key: str = "event_id") -> None:
    """Save an arbitrary event dict to *table_name*."""
    db_table = _dynamo.Table(table_name)
    if primary_key not in event_body:
        event_body[primary_key] = str(uuid.uuid4())
    try:
        db_table.put_item(Item=event_body)
        print(f"Event saved to {table_name}")
    except Exception as exc:
        print(f"DynamoDB error saving to {table_name}: {repr(exc)}")


def save_lead_if_new(item: dict) -> bool:
    """Save a lead only if its ``leadgen_id`` does not already exist.

    Returns ``True`` if the lead was saved, ``False`` if it was a duplicate.
    """
    try:
        _leads_table.put_item(
            Item=item,
            ConditionExpression="attribute_not_exists(leadgen_id)",
        )
        print(f"Lead {item['leadgen_id']} saved (new)")
        return True
    except _dynamo.meta.client.exceptions.ConditionalCheckFailedException:
        print(f"Lead {item['leadgen_id']} already exists, skipping")
        return False
    except Exception as exc:
        print(f"DynamoDB error saving lead: {repr(exc)}")
        return False


# ── Conversations ─────────────────────────────────────────────────────

def get_conversation(user_id: str) -> list[dict]:
    """Return all conversation messages for *user_id*, oldest first."""
    try:
        response = _conversations_table.query(
            KeyConditionExpression=Key("user_id").eq(user_id),
            ScanIndexForward=True,
        )
        messages = response.get("Items", [])
        print(f"Retrieved {len(messages)} messages for user {user_id}")
        return messages
    except Exception as exc:
        print("Conversation retrieval error:", repr(exc))
        return []


def save_conversation_message(
    *,
    user_id: str,
    message_id: str,
    text: str,
    platform: str,
    page_id: str,
    timestamp: int,
    role: str,
    sales_name: str | None = None,
) -> None:
    """Persist a single conversation message."""
    item: dict = {
        "user_id": user_id,
        "timestamp": timestamp,
        "message_id": message_id,
        "text": text,
        "platform": platform,
        "page_id": page_id,
        "role": role,
    }
    if role == "sales" and sales_name:
        item["sales_name"] = sales_name
    try:
        _conversations_table.put_item(
            Item=item,
            ConditionExpression="attribute_not_exists(message_id)",
        )
        print(f"Conversation saved for user {user_id} as {role}")
    except Exception as exc:
        print("Conversation save error:", repr(exc))


def replace_summary(
    user_id: str,
    new_summary_item: dict,
    old_summary_item: dict | None = None,
) -> None:
    """Atomically replace (or insert) a conversation summary."""
    transact_items: list[dict] = []
    if old_summary_item:
        transact_items.append(
            {
                "Delete": {
                    "TableName": _conversations_table.name,
                    "Key": {
                        "user_id": old_summary_item["user_id"],
                        "timestamp": old_summary_item["timestamp"],
                    },
                }
            }
        )
    transact_items.append(
        {"Put": {"TableName": _conversations_table.name, "Item": new_summary_item}}
    )
    try:
        _conversations_table.meta.client.transact_write_items(TransactItems=transact_items)
        print(f"Summary updated for user {user_id}")
    except Exception as exc:
        print("DynamoDB transaction error:", repr(exc))
