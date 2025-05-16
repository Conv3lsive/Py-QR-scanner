import smtplib
from email.message import EmailMessage
import os
import logging
from sendconfig import SMTP_EMAIL, SMTP_PASSWORD, SMTP_HOST, SMTP_PORT

def send_email_smtp(recipient_email, subject, body, attachment_path):
    msg = EmailMessage()
    msg['From'] = SMTP_EMAIL
    msg['To'] = recipient_email
    msg['Subject'] = subject
    msg.set_content(body)

    with open(attachment_path, 'rb') as f:
        file_data = f.read()
        file_name = os.path.basename(attachment_path)
        msg.add_attachment(file_data, maintype='application', subtype='zip', filename=file_name)

    try:
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT) as smtp:
            smtp.login(SMTP_EMAIL, SMTP_PASSWORD)
            smtp.send_message(msg)
            logging.info(f"Отправлено {recipient_email} — {file_name}")
    except Exception as e:
        logging.error(f"Ошибка отправки {recipient_email}: {e}")