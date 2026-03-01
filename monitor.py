import psutil
import time
import smtplib
from email.message import EmailMessage
import datetime
import os
from dotenv import load_dotenv

load_dotenv()

# --- Configuration ---
# Update this to match the exact name of your main script file
TARGET_SCRIPT_NAME = "channel_scheduler.py"
CHECK_INTERVAL_SECONDS = 10 * 60  # 10 minutes

EMAIL_SENDER = os.environ.get("GMAIL_SMTP_USER")
# If using Gmail, you will need to generate an "App Password" in your Google Account security settings
EMAIL_PASSWORD = os.environ.get("GMAIL_SMTP_APP_PASSWORD")
EMAIL_RECEIVERS = ["alexandru.grecu27@gmail.com", "andreizdrali@gmail.com"]
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 465  # For SSL
# ---------------------


def send_alert_email():
    """Sends an email notification that the script was killed."""
    msg = EmailMessage()

    # Since it's killed by OOM, it disappears without a python stack trace.
    # We send a generic OOM/Killed error message.
    msg.set_content(
        f"Alert! The script '{TARGET_SCRIPT_NAME}' is no longer running.\n"
        f"It likely ran out of memory (OOM) and was killed by the OS.\n\n"
        f"Time detected: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )

    msg["Subject"] = f"ALERT: Script {TARGET_SCRIPT_NAME} Stopped/Killed"
    msg["From"] = EMAIL_SENDER
    msg["To"] = ", ".join(EMAIL_RECEIVERS)

    try:
        print(f"Sending alert email to {', '.join(EMAIL_RECEIVERS)}...")
        with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as server:
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.send_message(msg)
        print("Email sent successfully.")
    except Exception as e:
        print(f"Failed to send email: {e}")


def is_script_running(script_name):
    """Checks if a python process with the given script name is currently running."""
    for proc in psutil.process_iter(["pid", "name", "cmdline"]):
        try:
            # Check if it's a Python process
            name = proc.info.get("name", "").lower()
            if "python" in name or "py" in name:
                cmdline = proc.info.get("cmdline")
                if cmdline:
                    # Check if the script name is present in the command line arguments
                    for arg in cmdline:
                        if script_name in arg:
                            return True
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    return False


def main():
    print(f"Starting monitor for '{TARGET_SCRIPT_NAME}'...")
    print(f"Checking every {CHECK_INTERVAL_SECONDS / 60} minutes.")

    # Initial check
    was_running_last_check = is_script_running(TARGET_SCRIPT_NAME)

    if not was_running_last_check:
        print(
            f"Warning: '{TARGET_SCRIPT_NAME}' does not appear to be running right now."
        )
        print("Monitoring will arm once it detects the script has started.")

    while True:
        time.sleep(CHECK_INTERVAL_SECONDS)

        is_running_now = is_script_running(TARGET_SCRIPT_NAME)

        # If it was running last time we checked, but isn't running now, it must have been killed.
        if was_running_last_check and not is_running_now:
            print(
                f"[{datetime.datetime.now()}] Script was running, but is now dead! Sending alert..."
            )
            send_alert_email()

            # Mark it as dead so we don't spam emails every 10 mins
            was_running_last_check = False

        # If it wasn't running but is now (restarted), re-arm the monitor hook
        elif not was_running_last_check and is_running_now:
            print(
                f"[{datetime.datetime.now()}] Detected that the script is running. Monitoring armed."
            )
            was_running_last_check = True

        else:
            status = "RUNNING" if is_running_now else "WAITING/DEAD"
            print(f"[{datetime.datetime.now()}] Check complete. Status: {status}")


if __name__ == "__main__":
    main()
