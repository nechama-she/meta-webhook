"""Comment moderation - classify, delete bad comments, block abusive users."""

from ai import classify_sentiment
from meta_api import delete_comment, block_user
from db import save_event


def process_comment(entry: dict, change_value: dict) -> None:
    """Classify a feed comment and take moderation action if needed."""
    comment_id = change_value.get("comment_id")
    comment_text = (change_value.get("message") or "").strip()
    user_id = change_value.get("from", {}).get("id")
    page_id = entry.get("id")

    print(f"\n══ Comment: comment_id={comment_id}, user={user_id}, page={page_id} ══")

    if not comment_text or not comment_id:
        print("Comment skipped: empty text or missing comment_id")
        return

    result = classify_sentiment(comment_text)
    print(f"Comment text: {comment_text!r}")
    print(f"Sentiment: {result}")

    if result == "Bad":
        print(f"Bad comment → deleting, blocking user={user_id}")
        delete_comment(comment_id, page_id)
        if user_id:
            block_user(user_id, page_id)
        else:
            print("No user_id, skipping block")
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
    else:
        print("Good comment — no action taken")
