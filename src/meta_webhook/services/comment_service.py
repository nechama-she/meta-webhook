"""Comment moderation - classify, delete bad comments, block abusive users."""

from meta_webhook.clients.openai_client import classify_sentiment
from meta_webhook.clients.facebook_client import delete_comment, block_user
from meta_webhook.clients.dynamodb_client import save_event


def process_comment(entry: dict, change_value: dict) -> None:
    """Classify a feed comment and take moderation action if needed."""
    comment_id = change_value.get("comment_id")
    comment_text = (change_value.get("message") or "").strip()
    user_id = change_value.get("from", {}).get("id")
    page_id = entry.get("id")

    if not comment_text or not comment_id:
        return

    result = classify_sentiment(comment_text)
    print(f"Comment: {comment_text}")
    print(f"Classifier: {result}")

    if result == "Bad":
        delete_comment(comment_id, page_id)
        if user_id:
            block_user(user_id, page_id)
        save_event(
            {
                "entry_id": page_id,
                "comment_id": comment_id,
                "user_id": user_id,
                "message": comment_text,
                "classifier": result,
                "raw_value": change_value,
            }
        )
