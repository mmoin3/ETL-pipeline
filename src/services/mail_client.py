"""
Simple email sender using SMTP with Outlook.
Uses App Password for 2FA accounts.
"""
import os, smtplib, logging, mimetypes
from email.message import EmailMessage
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

def send_email(subject: str,
               body: str,
               to_email: list[str],
               cc_emails: list[str] = None,
               bcc_emails: list[str] = None,
               attachments: list[str] = None,
               smtp_server: str = "smtp.office365.com",
               smtp_port: int = 587):
    """
    Send an email via Outlook SMTP.
    
    For 2FA accounts, use an App Password (not your regular password).
    Generate at: https://security.microsoft.com -> App passwords
    
    Args:
        subject: Email subject
        body: Email body text
        to_email: Recipient email address
        cc_emails: Optional list of CC recipient addresses
        bcc_emails: Optional list of BCC recipient addresses
        attachments: Optional list of file paths to attach
    """
    # Get settings from .env
    from_email = os.getenv("EMAIL_ADDRESS")
    password = os.getenv("EMAIL_APP_PASSWORD")  # This is the App Password
    
    if not all([from_email, password]):
        logger.error("Missing Outlook credentials in .env")
        return False

    def normalize_recipients(value) -> list[str]:
        if not value:
            return []
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        return [str(item).strip() for item in value if str(item).strip()]

    to_recipients = normalize_recipients(to_email)
    cc_recipients = normalize_recipients(cc_emails)
    bcc_recipients = normalize_recipients(bcc_emails)
    all_recipients = to_recipients + cc_recipients + bcc_recipients

    if not all_recipients:
        logger.error("No recipients provided (to/cc/bcc are all empty)")
        return False
    
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_email
    msg["To"] = ", ".join(to_recipients)
    if cc_recipients:
        msg["Cc"] = ", ".join(cc_recipients)
    msg.set_content(body)

    for file_path in attachments or []:
        if not os.path.isfile(file_path):
            logger.error(f"Attachment not found: {file_path}")
            return False

        mime_type, _ = mimetypes.guess_type(file_path)
        if mime_type is None:
            maintype, subtype = "application", "octet-stream"
        else:
            maintype, subtype = mime_type.split("/", 1)

        with open(file_path, "rb") as handle:
            msg.add_attachment(
                handle.read(),
                maintype=maintype,
                subtype=subtype,
                filename=os.path.basename(file_path),
            )

    # Send
    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(from_email, password)  # App password works here
            server.send_message(msg, to_addrs=all_recipients)
        
        logger.info(
            f"Email sent to {len(to_recipients)} to, {len(cc_recipients)} cc, {len(bcc_recipients)} bcc recipients"
        )
        return None
        
    except Exception as e:
        logger.error(f"Failed to send email: {e}")
        return None