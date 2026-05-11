import os
from pathlib import Path

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None


ROOT_DIR = Path(__file__).resolve().parent
ENV_PATH = ROOT_DIR / '.env'


def load_environment():
    if load_dotenv is not None:
        load_dotenv(ENV_PATH)


def _fallback_sendconfig(name: str, default: str = ''):
    try:
        import sendconfig
        return getattr(sendconfig, name, default)
    except Exception:
        return default


def get_smtp_config():
    load_environment()
    smtp_email = os.getenv('SMTP_EMAIL') or _fallback_sendconfig('SMTP_EMAIL', '')
    smtp_password = os.getenv('SMTP_PASSWORD') or _fallback_sendconfig('SMTP_PASSWORD', '')
    smtp_host = os.getenv('SMTP_HOST') or _fallback_sendconfig('SMTP_HOST', 'smtp.example.com')
    smtp_port = int(os.getenv('SMTP_PORT') or _fallback_sendconfig('SMTP_PORT', 465))

    return {
        'SMTP_EMAIL': smtp_email,
        'SMTP_PASSWORD': smtp_password,
        'SMTP_HOST': smtp_host,
        'SMTP_PORT': smtp_port,
    }


def get_fallback_smtp_config():
    """Return fallback SMTP credentials from env, or None if not configured."""
    load_environment()
    fallback_email = os.getenv('FALLBACK_SMTP_EMAIL') or _fallback_sendconfig('FALLBACK_SMTP_EMAIL', '')
    fallback_password = os.getenv('FALLBACK_SMTP_PASSWORD') or _fallback_sendconfig('FALLBACK_SMTP_PASSWORD', '')
    fallback_host = os.getenv('FALLBACK_SMTP_HOST') or _fallback_sendconfig('FALLBACK_SMTP_HOST', '')
    fallback_port = int(os.getenv('FALLBACK_SMTP_PORT') or _fallback_sendconfig('FALLBACK_SMTP_PORT', 465))

    if not fallback_email or not fallback_host:
        return None

    return {
        'FALLBACK_SMTP_EMAIL': fallback_email,
        'FALLBACK_SMTP_PASSWORD': fallback_password,
        'FALLBACK_SMTP_HOST': fallback_host,
        'FALLBACK_SMTP_PORT': fallback_port,
    }


def get_email_config():
    load_environment()
    email_subject = os.getenv('EMAIL_SUBJECT') or _fallback_sendconfig('EMAIL_SUBJECT', 'Ваша работа')
    email_body = os.getenv('EMAIL_BODY') or _fallback_sendconfig('EMAIL_BODY', 'Пожалуйста, проверьте архив')

    return {
        'EMAIL_SUBJECT': email_subject,
        'EMAIL_BODY': email_body,
    }
