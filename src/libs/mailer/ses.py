"""Email client using AWS SES."""

import boto3


def send_email(
    from_addr: str,
    to: list[str],
    subject: str,
    body_html: str,
    body_text: str | None = None,
) -> bool:
    """Send an email via SES. Returns True on success."""
    if not from_addr:
        print("Mailer: no from_addr provided, skipping")
        return False
    if not to:
        print("Mailer: no recipients, skipping")
        return False

    try:
        ses = boto3.client("ses")
        ses.send_email(
            Source=from_addr,
            Destination={"ToAddresses": to},
            Message={
                "Subject": {"Data": subject, "Charset": "UTF-8"},
                "Body": {
                    "Html": {"Data": body_html, "Charset": "UTF-8"},
                    "Text": {"Data": body_text or body_html, "Charset": "UTF-8"},
                },
            },
        )
        print(f"Email sent to {to} (subject: {subject!r})")
        return True
    except Exception as exc:
        print(f"Mailer error: {repr(exc)}")
        return False
