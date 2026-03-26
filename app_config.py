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
