"""
Simple email sender using SMTP with Outlook.
Uses App Password for 2FA accounts.
"""
import os
import logging
import smtplib
from email.mime.text import MIMEText
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


def send_email(subject: str, body: str, to_email: str):
    """
    Send an email via Outlook SMTP.
    
    For 2FA accounts, use an App Password (not your regular password).
    Generate at: https://security.microsoft.com -> App passwords
    
    Args:
        subject: Email subject
        body: Email body text
        to_email: Recipient email address
    """
    # Get settings from .env
    from_email = os.getenv("OUTLOOK_EMAIL")
    password = os.getenv("OUTLOOK_APP_PASSWORD")  # This is the App Password
    
    if not all([from_email, password]):
        logger.error("Missing Outlook credentials in .env")
        return False
    
    # Create message
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = from_email
    msg['To'] = to_email
    
    # Outlook SMTP settings
    smtp_server = "smtp.office365.com"
    smtp_port = 587
    
    # Send
    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(from_email, password)  # App password works here
            server.send_message(msg)
        
        logger.info(f"Email sent to {to_email}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to send email: {e}")
        return False