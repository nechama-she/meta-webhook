"""Provision message-table indexes and streams, including the calls table."""

import time

import boto3


MESSAGE_ID_INDEX = "message_id_index"
TIMESTAMP_INDEX = "record-type-timestamp-index"
TABLES = ("sms_messages", "conversations", "calls")

dynamodb = boto3.client("dynamodb")


def ensure_calls_table() -> None:
    try:
        dynamodb.describe_table(TableName="calls")
        return
    except dynamodb.exceptions.ResourceNotFoundException:
        pass
    print("Creating calls table")
    dynamodb.create_table(
        TableName="calls",
        AttributeDefinitions=[
            {"AttributeName": "phone_number", "AttributeType": "S"},
            {"AttributeName": "timestamp", "AttributeType": "N"},
            {"AttributeName": "message_id", "AttributeType": "S"},
            {"AttributeName": "record_type", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "phone_number", "KeyType": "HASH"},
            {"AttributeName": "timestamp", "KeyType": "RANGE"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": MESSAGE_ID_INDEX,
                "KeySchema": [{"AttributeName": "message_id", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            },
            {
                "IndexName": TIMESTAMP_INDEX,
                "KeySchema": [
                    {"AttributeName": "record_type", "KeyType": "HASH"},
                    {"AttributeName": "timestamp", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
        ],
        StreamSpecification={"StreamEnabled": True, "StreamViewType": "NEW_IMAGE"},
        BillingMode="PAY_PER_REQUEST",
    )
    dynamodb.get_waiter("table_exists").wait(TableName="calls")


def ensure_index(
    table_name: str,
    index_name: str,
    key_schema: list[dict],
    attribute_definitions: list[dict],
) -> None:
    table = dynamodb.describe_table(TableName=table_name)["Table"]
    indexes = table.get("GlobalSecondaryIndexes", [])
    existing = next((item for item in indexes if item["IndexName"] == index_name), None)
    if not existing:
        print(f"Creating {index_name} on {table_name}")
        dynamodb.update_table(
            TableName=table_name,
            AttributeDefinitions=attribute_definitions,
            GlobalSecondaryIndexUpdates=[
                {
                    "Create": {
                        "IndexName": index_name,
                        "KeySchema": key_schema,
                        "Projection": {"ProjectionType": "ALL"},
                    }
                }
            ],
        )

    while True:
        table = dynamodb.describe_table(TableName=table_name)["Table"]
        index = next(
            (item for item in table.get("GlobalSecondaryIndexes", []) if item["IndexName"] == index_name),
            None,
        )
        if index and index["IndexStatus"] == "ACTIVE":
            print(f"{index_name} is active on {table_name}")
            return
        print(f"Waiting for {index_name} on {table_name}")
        time.sleep(10)


def ensure_stream(table_name: str) -> None:
    table = dynamodb.describe_table(TableName=table_name)["Table"]
    stream = table.get("StreamSpecification") or {}
    if stream.get("StreamEnabled") and stream.get("StreamViewType") == "NEW_IMAGE":
        print(f"NEW_IMAGE stream is already enabled on {table_name}")
        return

    print(f"Enabling NEW_IMAGE stream on {table_name}")
    dynamodb.update_table(
        TableName=table_name,
        StreamSpecification={
            "StreamEnabled": True,
            "StreamViewType": "NEW_IMAGE",
        },
    )
    while True:
        table = dynamodb.describe_table(TableName=table_name)["Table"]
        stream = table.get("StreamSpecification") or {}
        if (
            table.get("TableStatus") == "ACTIVE"
            and stream.get("StreamEnabled")
            and stream.get("StreamViewType") == "NEW_IMAGE"
        ):
            print(f"NEW_IMAGE stream is enabled on {table_name}")
            return
        time.sleep(5)


def main() -> None:
    ensure_calls_table()
    for table_name in TABLES:
        ensure_index(
            table_name,
            MESSAGE_ID_INDEX,
            [{"AttributeName": "message_id", "KeyType": "HASH"}],
            [{"AttributeName": "message_id", "AttributeType": "S"}],
        )
        ensure_index(
            table_name,
            TIMESTAMP_INDEX,
            [
                {"AttributeName": "record_type", "KeyType": "HASH"},
                {"AttributeName": "timestamp", "KeyType": "RANGE"},
            ],
            [
                {"AttributeName": "record_type", "AttributeType": "S"},
                {"AttributeName": "timestamp", "AttributeType": "N"},
            ],
        )
        ensure_stream(table_name)


if __name__ == "__main__":
    main()
