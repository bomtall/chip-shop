import os
import smtplib
from email.message import EmailMessage

from dotenv import load_dotenv

import fryer.datetime

load_dotenv()


def send_email(
    subject: str, content: str, to_emails: list[str], attachment: str | None = None
) -> None:
    """Send an email with the given subject and content to the given email addresses."""
    email_account = os.environ.get("EMAIL_ACCOUNT")
    email_app_password = os.environ.get("EMAIL_APP_PASSWORD")

    # Create message.
    msg = EmailMessage()
    msg["To"] = "; ".join(to_emails)
    msg["From"] = email_account
    msg["Subject"] = subject
    msg.set_content(content)

    if attachment:
        # figure out how to attach different filetypes
        msg.add_attachment()

    # Send email.
    with smtplib.SMTP_SSL(host="smtp.gmail.com", port=465) as smtp:
        smtp.login(user=str(email_account), password=str(email_app_password))
        smtp.send_message(msg)


def send_a_test_email(to_emails: list[str]) -> None:
    """Test the send_email function."""
    msg = f"This is a test email sent at {fryer.datetime.now()}."
    send_email("Chip-Shop Test Email", msg, to_emails)
