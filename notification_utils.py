import logging
import os
import smtplib
from email.message import EmailMessage
from typing import Optional

DEFAULT_RECIPIENTS = [
    "andreizdrali@gmail.com",
    "alexandru.grecu27@gmail.com",
]

logger = logging.getLogger("Notifications")


def get_notification_recipients() -> list[str]:
    raw = os.getenv("EMAIL_NOTIFY_RECIPIENTS", "")
    if not raw.strip():
        return list(DEFAULT_RECIPIENTS)
    recipients = [item.strip() for item in raw.split(",") if item.strip()]
    return recipients or list(DEFAULT_RECIPIENTS)


def send_gmail_notification(subject: str, body: str, recipients: Optional[list[str]] = None) -> bool:
    sender = os.getenv("GMAIL_SMTP_USER", "").strip()
    app_password = os.getenv("GMAIL_SMTP_APP_PASSWORD", "").strip()

    if not sender or not app_password:
        logger.warning(
            "Email alert skipped: set GMAIL_SMTP_USER and GMAIL_SMTP_APP_PASSWORD")
        return False

    target_recipients = recipients or get_notification_recipients()
    if not target_recipients:
        logger.warning("Email alert skipped: no recipients configured")
        return False

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = sender
    message["To"] = ", ".join(target_recipients)
    message.set_content(body)

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=30) as server:
            server.login(sender, app_password)
            server.send_message(message)
        return True
    except Exception as exc:
        logger.warning("Email alert failed: %s", exc)
        return False
