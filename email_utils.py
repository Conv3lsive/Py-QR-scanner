import smtplib
from email.message import EmailMessage
import os
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from app_config import get_smtp_config


SMTP_CFG = get_smtp_config()
SMTP_EMAIL = SMTP_CFG['SMTP_EMAIL']
SMTP_PASSWORD = SMTP_CFG['SMTP_PASSWORD']
SMTP_HOST = SMTP_CFG['SMTP_HOST']
SMTP_PORT = SMTP_CFG['SMTP_PORT']

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


def validate_emails(emails_dict, max_workers=6):
    import re
    import smtplib
    import socket
    email_regex = re.compile(r"[^@]+@[^@]+\.[^@]+")

    valid_emails = {}
    invalid_emails = []

    def validate(name, email):
        if not email_regex.match(email):
            return name, email, False, 'Неверный формат'
        try:
            with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=10) as smtp:
                smtp.login(SMTP_EMAIL, SMTP_PASSWORD)
                code, message = smtp.noop()
                if code == 250:
                    return name, email, True, 'OK'
                else:
                    return name, email, False, f'SMTP NOOP {code} {message}'
        except (smtplib.SMTPException, socket.timeout) as e:
            return name, email, False, str(e)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(validate, name, email) for name, email in emails_dict.items()]
        for future in as_completed(futures):
            name, email, is_valid, reason = future.result()
            if is_valid:
                logging.info(f"Email валиден: {name} — {email}")
                valid_emails[name] = email
            else:
                logging.warning(f"Email невалиден: {name} — {email}. Причина: {reason}")
                invalid_emails.append((name, email))

    logging.info(f"Проверка завершена. Валидных email: {len(valid_emails)}, неверных: {len(invalid_emails)}")
    return valid_emails, invalid_emails