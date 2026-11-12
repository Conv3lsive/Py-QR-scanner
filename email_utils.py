import logging
import os
import re
import smtplib
from concurrent.futures import ThreadPoolExecutor, as_completed
from email.message import EmailMessage

from app_config import get_smtp_config
from worker_pool import auto_workers


EMAIL_REGEX = re.compile(
    r"(?i)^(?=.{1,254}$)(?=.{1,64}@)[a-z0-9.!#$%&'*+/=?^_`{|}~-]+@(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}$"
)

logger = logging.getLogger(__name__)


def is_valid_email(email):
    normalized = (email or '').strip()
    if not normalized or '..' in normalized:
        return False
    return EMAIL_REGEX.fullmatch(normalized) is not None


def _get_smtp_config():
    cfg = get_smtp_config()
    missing = [key for key in ('SMTP_EMAIL', 'SMTP_PASSWORD', 'SMTP_HOST') if not (cfg.get(key) or '').strip()]
    if missing:
        raise RuntimeError(
            f"SMTP не настроен: отсутствуют переменные {', '.join(missing)}. "
            "Заполните .env или sendconfig.py перед отправкой."
        )
    return cfg


def send_email_smtp(recipient_email, subject, body, attachment_path):
    if not os.path.isfile(attachment_path):
        logger.error("Файл вложения не найден: %s", attachment_path)
        return False

    cfg = _get_smtp_config()
    smtp_email = cfg['SMTP_EMAIL']
    smtp_password = cfg['SMTP_PASSWORD']
    smtp_host = cfg['SMTP_HOST']
    smtp_port = int(cfg['SMTP_PORT'])

    msg = EmailMessage()
    msg['From'] = smtp_email
    msg['To'] = recipient_email
    msg['Subject'] = subject
    msg.set_content(body)

    with open(attachment_path, 'rb') as f:
        file_data = f.read()
        file_name = os.path.basename(attachment_path)
        msg.add_attachment(file_data, maintype='application', subtype='zip', filename=file_name)

    try:
        with smtplib.SMTP_SSL(smtp_host, smtp_port) as smtp:
            smtp.login(smtp_email, smtp_password)
            smtp.send_message(msg)
            logger.info("Отправлено %s — %s", recipient_email, file_name)
            return True
    except Exception as e:
        logger.error("Ошибка отправки %s: %s", recipient_email, e)
        return False


def _validate_one(name_email_pair):
    name, email = name_email_pair
    normalized = (email or '').strip()
    valid = is_valid_email(normalized)
    return name, normalized, valid


def validate_emails(emails_dict, max_workers=None, progress_callback=None):
    valid_emails = {}
    invalid_emails = []

    total = len(emails_dict)
    if total == 0:
        if progress_callback:
            progress_callback(0, 0, 'email', 'Нет email для проверки')
        return valid_emails, invalid_emails

    items = list(emails_dict.items())
    worker_count = auto_workers(total) if max_workers is None else max(1, min(max_workers, total))
    completed = 0

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_to_idx = {executor.submit(_validate_one, item): item for item in items}
        for future in as_completed(future_to_idx):
            try:
                name, normalized, valid = future.result()
                if valid:
                    logger.info("Email валиден: %s — %s", name, normalized)
                    valid_emails[name] = normalized
                else:
                    logger.warning("Email невалиден: %s — %s. Причина: Неверный формат", name, normalized)
                    invalid_emails.append((name, normalized))
            except Exception as exc:
                logger.error("Ошибка валидации email: %s", exc)
            finally:
                completed += 1
                if progress_callback:
                    progress_callback(completed, total, 'email', 'Проверка email')

    logger.info(
        "Проверка завершена. Валидных email: %d, неверных: %d",
        len(valid_emails),
        len(invalid_emails),
    )
    return valid_emails, invalid_emails
