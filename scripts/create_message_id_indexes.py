"""Create message_id_index on the SMS and conversation tables."""

import time

import boto3


INDEX_NAME = "message_id_index"
TABLES = ("sms_messages", "conversations")

dynamodb = boto3.client("dynamodb")


def ensure_index(table_name: str) -> None:
    table = dynamodb.describe_table(TableName=table_name)["Table"]
    indexes = table.get("GlobalSecondaryIndexes", [])
    existing = next((item for item in indexes if item["IndexName"] == INDEX_NAME), None)
    if not existing:
        print(f"Creating {INDEX_NAME} on {table_name}")
        dynamodb.update_table(
            TableName=table_name,
            AttributeDefinitions=[
                {"AttributeName": "message_id", "AttributeType": "S"},
            ],
            GlobalSecondaryIndexUpdates=[
                {
                    "Create": {
                        "IndexName": INDEX_NAME,
                        "KeySchema": [
                            {"AttributeName": "message_id", "KeyType": "HASH"},
                        ],
                        "Projection": {"ProjectionType": "ALL"},
                    }
                }
            ],
        )

    while True:
        table = dynamodb.describe_table(TableName=table_name)["Table"]
        index = next(
            (item for item in table.get("GlobalSecondaryIndexes", []) if item["IndexName"] == INDEX_NAME),
            None,
        )
        if index and index["IndexStatus"] == "ACTIVE":
            print(f"{INDEX_NAME} is active on {table_name}")
            return
        print(f"Waiting for {INDEX_NAME} on {table_name}")
        time.sleep(10)


def main() -> None:
    for table_name in TABLES:
        ensure_index(table_name)


if __name__ == "__main__":
    main()
