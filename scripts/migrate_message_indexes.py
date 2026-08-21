"""Create and backfill the global chronological message indexes."""

import time

import boto3


INDEX_NAME = "record-type-timestamp-index"
TABLES = {
    "conversations": "user_id",
    "sms_messages": "phone_number",
}

dynamodb = boto3.client("dynamodb")


def ensure_index(table_name: str) -> None:
    table = dynamodb.describe_table(TableName=table_name)["Table"]
    indexes = table.get("GlobalSecondaryIndexes", [])
    existing = next((index for index in indexes if index["IndexName"] == INDEX_NAME), None)
    if not existing:
        print(f"Creating {INDEX_NAME} on {table_name}")
        dynamodb.update_table(
            TableName=table_name,
            AttributeDefinitions=[
                {"AttributeName": "record_type", "AttributeType": "S"},
                {"AttributeName": "timestamp", "AttributeType": "N"},
            ],
            GlobalSecondaryIndexUpdates=[
                {
                    "Create": {
                        "IndexName": INDEX_NAME,
                        "KeySchema": [
                            {"AttributeName": "record_type", "KeyType": "HASH"},
                            {"AttributeName": "timestamp", "KeyType": "RANGE"},
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
            return
        print(f"Waiting for {INDEX_NAME} on {table_name}")
        time.sleep(10)


def backfill(table_name: str, partition_key: str) -> None:
    start_key = None
    updated = 0
    while True:
        params = {
            "TableName": table_name,
            "ProjectionExpression": "#pk, #ts, record_type",
            "ExpressionAttributeNames": {"#pk": partition_key, "#ts": "timestamp"},
        }
        if start_key:
            params["ExclusiveStartKey"] = start_key
        page = dynamodb.scan(**params)
        for item in page.get("Items", []):
            if "record_type" in item:
                continue
            dynamodb.update_item(
                TableName=table_name,
                Key={partition_key: item[partition_key], "timestamp": item["timestamp"]},
                UpdateExpression="SET record_type = if_not_exists(record_type, :message)",
                ExpressionAttributeValues={":message": {"S": "message"}},
            )
            updated += 1
        start_key = page.get("LastEvaluatedKey")
        if not start_key:
            break
    print(f"Backfilled {updated} records in {table_name}")


def main() -> None:
    for table_name, partition_key in TABLES.items():
        ensure_index(table_name)
        backfill(table_name, partition_key)


if __name__ == "__main__":
    main()
