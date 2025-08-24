import smtplib
from email.message import EmailMessage
import os
import logging
from sendconfig import SMTP_EMAIL, SMTP_PASSWORD, SMTP_HOST, SMTP_PORT
from concurrent.futures import ThreadPoolExecutor, as_completed

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


def validate_emails(emails_dict):
    import re
    import smtplib
    import socket
    email_regex = re.compile(r"[^@]+@[^@]+\.[^@]+")

    valid_emails = {}
    invalid_emails = []

    def validate(name, email):
        if not email_regex.match(email):
            logging.warning(f"Неверный формат email для {name}: {email}")
            invalid_emails.append((name, email))
            return
        try:
            with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=10) as smtp:
                smtp.login(SMTP_EMAIL, SMTP_PASSWORD)
                code, message = smtp.noop()
                if code == 250:
                    logging.info(f"Email валиден: {name} — {email}")
                    valid_emails[name] = email
                else:
                    logging.warning(f"SMTP NOOP не прошёл для {name}: {email} — {code} {message}")
                    invalid_emails.append((name, email))
        except (smtplib.SMTPException, socket.timeout) as e:
            logging.error(f"Ошибка проверки email {name}: {email} — {e}")
            invalid_emails.append((name, email))

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(validate, name, email) for name, email in emails_dict.items()]
        for future in as_completed(futures):
            future.result()

    logging.info(f"Проверка завершена. Валидных email: {len(valid_emails)}, неверных: {len(invalid_emails)}")