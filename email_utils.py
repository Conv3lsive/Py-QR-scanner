import logging
import os
import re
import smtplib
from email.message import EmailMessage

from app_config import get_smtp_config


SMTP_CFG = get_smtp_config()
SMTP_EMAIL = SMTP_CFG['SMTP_EMAIL']
SMTP_PASSWORD = SMTP_CFG['SMTP_PASSWORD']
SMTP_HOST = SMTP_CFG['SMTP_HOST']
SMTP_PORT = SMTP_CFG['SMTP_PORT']
EMAIL_REGEX = re.compile(
    r"(?i)^(?=.{1,254}$)(?=.{1,64}@)[a-z0-9.!#$%&'*+/=?^_`{|}~-]+@(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}$"
)


def is_valid_email(email):
    normalized = (email or '').strip()
    if not normalized or '..' in normalized:
        return False
    return EMAIL_REGEX.fullmatch(normalized) is not None

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
            return True
    except Exception as e:
        logging.error(f"Ошибка отправки {recipient_email}: {e}")
        return False


def validate_emails(emails_dict, max_workers=6, progress_callback=None):
    _ = max_workers

    valid_emails = {}
    invalid_emails = []

    total = len(emails_dict)
    if total == 0 and progress_callback:
        progress_callback(0, 0, 'email', 'Нет email для проверки')

    completed = 0
    for name, email in emails_dict.items():
        normalized_email = (email or '').strip()
        if is_valid_email(normalized_email):
            logging.info(f"Email валиден: {name} — {normalized_email}")
            valid_emails[name] = normalized_email
        else:
            logging.warning(f"Email невалиден: {name} — {normalized_email}. Причина: Неверный формат")
            invalid_emails.append((name, normalized_email))

        completed += 1
        if progress_callback:
            progress_callback(completed, total, 'email', 'Проверка email')

    logging.info(f"Проверка завершена. Валидных email: {len(valid_emails)}, неверных: {len(invalid_emails)}")
    return valid_emails, invalid_emails