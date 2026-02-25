import argparse
import datetime as dt
import socket
import sys

from dotenv import load_dotenv

from logger_utils import configure_logging
from notification_utils import get_notification_recipients, send_gmail_notification


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Send a dummy Gmail notification using project notifier settings"
    )
    parser.add_argument(
        "--subject",
        default="[RedditStoriesGen] Test email",
        help="Email subject",
    )
    parser.add_argument(
        "--message",
        default="This is a dummy test email from RedditStoriesGen.",
        help="Email body",
    )
    parser.add_argument(
        "--to",
        nargs="*",
        default=None,
        help="Optional recipient override (space-separated)",
    )
    return parser


def main() -> int:
    load_dotenv()
    logger = configure_logging("GmailTest")
    args = build_parser().parse_args()

    recipients = args.to if args.to else get_notification_recipients()
    timestamp = dt.datetime.now(dt.timezone.utc).isoformat()
    hostname = socket.gethostname()

    body = (
        f"{args.message}\n\n"
        f"Time (UTC): {timestamp}\n"
        f"Host: {hostname}\n"
        f"Recipients: {', '.join(recipients)}"
    )

    sent = send_gmail_notification(
        subject=args.subject,
        body=body,
        recipients=recipients,
    )

    if sent:
        logger.info("Dummy email sent successfully to %s",
                    ", ".join(recipients))
        return 0

    logger.error(
        "Dummy email failed. Check GMAIL_SMTP_USER/GMAIL_SMTP_APP_PASSWORD and logs."
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
