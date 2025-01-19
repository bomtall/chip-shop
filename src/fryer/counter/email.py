import os
import smtplib
from email.message import EmailMessage

from dotenv import load_dotenv

_ = load_dotenv()

# Read email address and app password from environment variables.


def send_email(
    subject: str, content: str, to_emails: list[str], attachment: str | None = None
) -> None:
    """Send an email with the given subject and content to the given email addresses."""
    gmail_account = os.environ.get("GMAIL_ACCOUNT")
    gmail_password = os.environ.get("GMAIL_PASSWORD")

    # Create message.
    message = EmailMessage()
    message["To"] = "; ".join(to_emails)
    message["From"] = "chipshop.automation@gmail.com"
    message["Subject"] = subject
    message.set_content(content)

    if attachment:
        # figure out how to attach different filetypes
        message.add_attachment()

    # Send email.
    with smtplib.SMTP_SSL(host="smtp.gmail.com", port=465) as smtp:
        smtp.login(user=str(gmail_account), password=str(gmail_password))
        smtp.send_message(message)


def test_send_email(to_emails: list[str]) -> None:
    """Test the send_email function."""
    send_email("Test", "This is a test email", to_emails)
