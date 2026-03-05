import os
import json
import urllib.request
import urllib.error
import hmac
import hashlib
import boto3
import uuid
import time
import re

VERIFY_TOKEN       = os.environ["VERIFY_TOKEN"]
OPENAI_API_KEY     = os.environ["OPENAI_API_KEY"]
COMMENTS_DETECTION_USER_TOKEN = os.environ["COMMENTS_DETECTION_USER_TOKEN"]
APP_SECRET         = os.environ["APP_SECRET"]



OPENAI_URL   = "https://api.openai.com/v1/chat/completions"
GRAPH_API_URL = "https://graph.facebook.com/v21.0"

def call_openai(text: str, timeout: int = 10) -> str:
    payload = {
        "model": "gpt-4o-mini",
        "messages": [
            {"role": "system", "content": """Classify the following text as 'Bad' if it expresses any negative sentiment, complaint, or dissatisfaction. Classify as 'Good' if it is positive or neutral.
Reply with only one word: Good or Bad."""},
            {"role": "user", "content": text}
        ],
        "max_tokens": 10
    }
    req = urllib.request.Request(
        OPENAI_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers=HEADERS,
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            output = (data["choices"][0]["message"]["content"] or "").strip()
            return "Bad" if output.lower().startswith("bad") else "Good"
    except urllib.error.HTTPError as e:
        print("HTTPError:", e.code, e.read().decode("utf-8", "ignore"))
        return "Good"
    except Exception as e:
        print("Error:", repr(e))
        return "Good"

def send_messenger_message(recipient_id, message_text, page_access_token):
    """Send a text message to a Messenger user via Facebook Graph API."""
    url = f"https://graph.facebook.com/v18.0/me/messages?access_token={page_access_token}"
    payload = {
        "recipient": {"id": recipient_id},
        "message": {"text": message_text}
    }
    headers = {"Content-Type": "application/json"}
    try:
        req = urllib.request.Request(url, data=json.dumps(payload).encode("utf-8"), headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = resp.read().decode("utf-8")
            print(f"Sent message to {recipient_id}: {data}")
    except Exception as e:
        print(f"Error sending message to {recipient_id}: {repr(e)}")

# Control OpenAI answering with env var
ENABLE_OPENAI_ANSWER = os.environ.get("ENABLE_OPENAI_ANSWER", "true").lower() == "true"

HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {OPENAI_API_KEY}",
}

dynamo = boto3.resource("dynamodb")
table = dynamo.Table("fb_events")
conversations_table = dynamo.Table("conversations")


def get_conversation(user_id: str):
    """Retrieve full conversation by user_id."""
    try:
        from boto3.dynamodb.conditions import Key
        response = conversations_table.query(
            KeyConditionExpression=Key("user_id").eq(user_id),
            ScanIndexForward=True
        )
        messages = response.get("Items", [])
        print(f"Retrieved {len(messages)} messages for user {user_id}")
        return messages
    except Exception as e:
        print("Conversation retrieval error:", repr(e))

    req = urllib.request.Request(
        OPENAI_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers=HEADERS,
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            output = (data["choices"][0]["message"]["content"] or "").strip()
            # Normalize: ensure only "Good" or "Bad"
            return "Bad" if output.lower().startswith("bad") else "Good"
    except urllib.error.HTTPError as e:
        print("HTTPError:", e.code, e.read().decode("utf-8", "ignore"))
        return "Good"
    except Exception as e:
        print("Error:", repr(e))
        return "Good"


def fb_api(path: str, method="DELETE", data=None, page_token=None):
    url = f"{GRAPH_API_URL}/{path}?access_token={page_token}"
    if data:
        req = urllib.request.Request(
            url,
            data=json.dumps(data).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method=method,
        )
    else:
        req = urllib.request.Request(url, method=method)
    try:
        with urllib.request.urlopen(req) as resp:
            print("FB API response:", resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        print("FB API error:", e.code, e.read().decode("utf-8", "ignore"))
    except Exception as e:
        print("FB API exception:", repr(e))


def delete_comment(comment_id: str, page_token: str):
    print(f"Deleting comment {comment_id}")
    fb_api(f"{comment_id}", "DELETE", page_token=page_token)


def block_user(user_id: str, page_token: str):
    print(f"Blocking user {user_id}")
    fb_api("me/blocked", "POST", {"uid": user_id} , page_token=page_token)

def get_page_token(page_id):
    url = f"https://graph.facebook.com/v24.0/me/accounts?access_token={COMMENTS_DETECTION_USER_TOKEN}"
    with urllib.request.urlopen(url) as resp:
        data = json.loads(resp.read().decode("utf-8"))

    for page in data.get("data", []):
        if page.get("id") == page_id:
            return page.get("access_token")

    raise Exception("Page token not found")

def summarize_and_save(conversation, user_id, page_id):
    """Summarize the conversation and keep only one summary per conversation (update or insert), but only if 10 new messages have arrived since the last summary."""
    # Find the latest summary (if any)
    summary_idx = None
    for idx, m in enumerate(conversation):
        if m.get('role') == 'summary':
            summary_idx = idx
            break
    if summary_idx is not None:
        summary_item = conversation[summary_idx]
        non_summary_msgs = [m for m in conversation[summary_idx+1:] if m.get('role') != 'summary']
    else:
        summary_item = None
        non_summary_msgs = [m for m in conversation if m.get('role') != 'summary']

    # Only summarize if there are 10 new messages since the last summary
    if len(non_summary_msgs) % 10 != 0:
        return  # Do nothing unless exactly 10 new messages

    # Build the text to summarize: previous summary (if any) + new messages
    text_to_summarize = ""
    if summary_item:
        text_to_summarize += f"Previous summary:\n{summary_item['text']}\n\n"
    text_to_summarize += "\n".join([
        f"{m.get('role', 'user')}: {m['text']}" for m in non_summary_msgs
    ])
    payload = {
        "model": "gpt-4o-mini",
        "messages": [
            {"role": "system", "content": "Summarize the following conversation between a user, assistant, and sales in a few sentences, preserving important context and questions."},
            {"role": "user", "content": text_to_summarize}
        ],
        "max_tokens": 200
    }
    req = urllib.request.Request(
        OPENAI_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers=HEADERS,
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            summary = (data["choices"][0]["message"]["content"] or "").strip()
            # Prepare new summary item
            new_summary_item = {
                "user_id": user_id,
                "timestamp": int(time.time()),
                "message_id": str(uuid.uuid4()),
                "text": summary,
                "platform": "system",
                "page_id": page_id,
                "role": "summary"
            }
            # Build DynamoDB transaction: delete old summary (if exists), put new summary
            transact_items = []
            if summary_item:
                transact_items.append({
                    "Delete": {
                        "TableName": conversations_table.name,
                        "Key": {
                            "user_id": summary_item["user_id"],
                            "timestamp": summary_item["timestamp"]
                        }
                    }
                })
            transact_items.append({
                "Put": {
                    "TableName": conversations_table.name,
                    "Item": new_summary_item
                }
            })
            # Execute transaction
            try:
                conversations_table.meta.client.transact_write_items(TransactItems=transact_items)
                print(f"Summary updated for user {user_id}")
            except Exception as e:
                print("DynamoDB transaction error:", repr(e))
    except Exception as e:
        print("Summarization error:", repr(e))


def save_event_to_db(event_body: dict, table_name: str = "fb_events"):
    """Save event to specified DynamoDB table."""
    db_table = dynamo.Table(table_name)
    try:
        db_table.put_item(
            Item={
                "event_id": str(uuid.uuid4()),
                "body": json.dumps(event_body),
            }
        )
        print(f"Event saved to DynamoDB table {table_name}")
    except Exception as e:
        print(f"DynamoDB error saving to {table_name}:", repr(e))


def save_conversation(user_id: str, message_id: str, text: str, platform: str, page_id: str, timestamp: int, role: str, sales_name: str = None):
    """Save a message to the conversation table, with role (user/assistant/sales) and optional salesperson info."""
    # Store platform as a separate attribute (not in text)
    item = {
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
        conversations_table.put_item(
            Item=item,
            ConditionExpression="attribute_not_exists(message_id)"
        )
        print(f"Conversation saved for user {user_id} as {role}")
    except Exception as e:
        print("Conversation save error:", repr(e))


def lambda_handler(event, context):
    method = (event.get("requestContext", {}).get("http", {}) or {}).get("method", "GET")

    # 1) Webhook verification
    if method == "GET":
        q = event.get("queryStringParameters") or {}
        if q.get("hub.verify_token") == VERIFY_TOKEN:
            return {"statusCode": 200, "body": q.get("hub.challenge", "")}
        return {"statusCode": 403, "body": "Forbidden"}

    # 2) POST from Meta (Facebook comments, feed events, and Messenger messages)
    if method == "POST":
        try:
            body = json.loads(event.get("body") or "{}")
            print("Event: ", body)

            for entry in body.get("entry", []):
                # Handle feed comments
                for change in entry.get("changes", []):
                    if change.get("field") != "feed":
                        # Not a feed change
                        continue

                    v = change.get("value") or {}
                    if v.get("item") != "comment":
                        # Skip reactions and other items
                        continue

                    comment_id = v.get("comment_id")
                    comment_text = (v.get("message") or "").strip()
                    user_id = v.get("from", {}).get("id")

                    if not comment_text or not comment_id:
                        continue

                    result = call_openai(comment_text)
                    print("Comment:", comment_text)
                    print("Classifier:", result)

                    if result == "Bad":
                        page_id = entry.get("id")
                        page_token = get_page_token(page_id)
                        delete_comment(comment_id, page_token)
                        if user_id:
                            block_user(user_id, page_token)
                    
                    # Save event to DynamoDB
                    save_event_to_db(
                        {
                            "entry_id": entry.get("id"),
                            "comment_id": comment_id,
                            "user_id": user_id,
                            "message": comment_text,
                            "classifier": result,
                            "raw_value": v,
                        }
                    )

                # Handle Leadgen events
                for change in entry.get("changes", []):
                    if change.get("field") == "leadgen":
                        lead_value = change.get("value", {})
                        leadgen_id = lead_value.get("leadgen_id")
                        page_id = lead_value.get("page_id")
                        print(f"Leadgen event detected: leadgen_id={leadgen_id}, page_id={page_id}")
                        if leadgen_id and page_id:
                            try:
                                page_token = get_page_token(page_id)
                                lead_url = f"https://graph.facebook.com/v18.0/{leadgen_id}?access_token={page_token}"
                                with urllib.request.urlopen(lead_url) as resp:
                                    lead_data = json.loads(resp.read().decode("utf-8"))
                                    print(f"Lead details: {json.dumps(lead_data)}")
                                    # You can add custom processing here, e.g., save to DB, notify, etc.
                                    # Save lead details to 'leads' table
                                    save_event_to_db({
                                        "entry_id": entry.get("id"),
                                        "leadgen_id": leadgen_id,
                                        "page_id": page_id,
                                        "lead_data": lead_data,
                                        "raw_value": lead_value
                                    }, table_name="leads")
                                    print("Lead saved to leads table")
                            except Exception as e:
                                print(f"Error fetching lead details: {repr(e)}")

                # Handle Messenger messages
                for messaging in entry.get("messaging", []):
                    sender_id = messaging.get("sender", {}).get("id")
                    recipient_id = messaging.get("recipient", {}).get("id")
                    message_data = messaging.get("message") or {}
                    message_text = (message_data.get("text") or "").strip()
                    message_id = message_data.get("mid")
                    is_echo = message_data.get("is_echo", False)

                    if not message_text or not message_id:
                        continue

                    if is_echo:
                        # This is a page/admin (human or bot) message, save as 'sales' reply
                        save_conversation(
                            user_id=recipient_id,  # recipient is the user
                            message_id=message_id,
                            text=message_text,
                            platform="messenger",
                            page_id=entry.get("id"),
                            timestamp=messaging.get("timestamp", 0),
                            role="sales"
                        )
                    else:
                        # Save all user messages to conversation
                        save_conversation(
                            user_id=sender_id,
                            message_id=message_id,
                            text=message_text,
                            platform="messenger",
                            page_id=entry.get("id"),
                            timestamp=messaging.get("timestamp", 0),
                            role="user"
                        )

                        # Pull all previous messages for this user
                        conversation = get_conversation(sender_id)
                        for msg in conversation:
                            msg_platform = msg.get('platform', 'unknown')
                            msg_time = msg.get('timestamp', 0)
                            try:
                                msg_time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(msg_time))
                            except Exception:
                                msg_time_str = str(msg_time)
                            print(f"[{msg_time_str}] [{msg_platform}] {msg.get('role', 'unknown')}: {msg.get('text', '')}")

                        # Always call summarize_and_save; it will decide if summarization is needed
                        summarize_and_save(conversation, sender_id, entry.get("id"))

                        # Build messages for API: summary (if exists) + all messages after summary
                        messages_for_api = []
                        summary_idx = None
                        for idx, m in enumerate(conversation):
                            if m.get("role") == "summary":
                                summary_idx = idx
                                break
                        if summary_idx is not None:
                            # Add summary
                            summary_msg = conversation[summary_idx]
                            messages_for_api.append({"role": summary_msg.get("role"), "content": summary_msg["text"]})
                            # Add all messages after summary
                            for m in conversation[summary_idx+1:]:
                                messages_for_api.append({"role": m.get("role"), "content": m["text"]})
                        else:
                            # No summary, send all messages
                            for m in conversation:
                                messages_for_api.append({"role": m.get("role"), "content": m["text"]})


                        reply_text = None
                        # 1. If OpenAI is enabled, get answer
                        if ENABLE_OPENAI_ANSWER:
                            answer_payload = {
                                "model": "gpt-4o-mini",
                                "messages": messages_for_api,
                                "max_tokens": 200
                            }
                            req = urllib.request.Request(
                                OPENAI_URL,
                                data=json.dumps(answer_payload).encode("utf-8"),
                                headers=HEADERS,
                                method="POST",
                            )
                            try:
                                with urllib.request.urlopen(req, timeout=15) as resp:
                                    data = json.loads(resp.read().decode("utf-8"))
                                    answer = (data["choices"][0]["message"]["content"] or "").strip()
                                    # Save answer to conversation
                                    save_conversation(
                                        user_id=sender_id,
                                        message_id=str(uuid.uuid4()),
                                        text=answer,
                                        platform="openai",
                                        page_id=entry.get("id"),
                                        timestamp=int(messaging.get("timestamp", 0)) + 1,
                                        role="assistant" 
                                    )
                                    print(f"OpenAI answer for user {sender_id}: {answer}")
                                    reply_text = answer
                            except Exception as e:
                                print("OpenAI answer error:", repr(e))


                        if "move size: storage" in message_text.lower():
                            reply_text = "What size is the storage unit, and approximately what percentage of it is full?"
                            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] [messenger] Reply set for storage size question.")
                        else:
                            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] [messenger] Checking for specific message patterns in: {message_text}")
                            match = re.search(r"are you moving within the state or out of state\?[:\s]*([a-zA-Z ]+)", message_text, re.IGNORECASE)
                            if match:
                                answer = match.group(1).strip().lower()
                                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] [messenger] Pattern matched: {answer}")
                                if answer == "out of state":
                                    reply_text = "Thank you for reaching out to Gorilla Haulers.\nFor out-of-state moves, pricing is based on the total size of your shipment. To give you an accurate quote, we need a list of items that will not go into boxes, such as furniture or appliances, and about how many boxes you expect. You can list the items here in the chat, send pictures, or we can schedule a call to create the inventory together. You can also call us anytime at Gorilla Haulers for a quick estimate at (202) 937-2625."
                                    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] [messenger] Reply set for out of state move.")
                                elif answer == "within the state":
                                    reply_text = "Thank you for reaching out to Gorilla Haulers.\nFor local moves, pricing is based on the number of hours the move takes. To give you an accurate estimate, we need a list of items that will not go into boxes, such as furniture or appliances, and about how many boxes you expect. You can list the items here in the chat, send pictures, or we can schedule a call to create the inventory together. You can also call us anytime at Gorilla Haulers for a quick estimate at (202) 937-2625."
                                    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] [messenger] Reply set for within the state move.")
                        


                        # 3. Only send if reply_text is set
                        if reply_text:
                            print(f"Sending reply to {sender_id}: {reply_text}")
                            page_token = get_page_token(entry.get("id"))
                            send_messenger_message(sender_id, reply_text, page_token)
                            print("Reply sent.")

        except Exception as e:
            print("Handler error:", repr(e))

        return {"statusCode": 200, "body": "OK"}

    return {"statusCode": 405, "body": "Method Not Allowed"}
