"""Тесты модуля app_config.py: приоритет env > sendconfig, fallback SMTP, .env, ошибки порта."""
import sys
from types import SimpleNamespace

import pytest

import app_config


_SMTP_KEYS = [
    'SMTP_EMAIL', 'SMTP_PASSWORD', 'SMTP_HOST', 'SMTP_PORT',
    'FALLBACK_SMTP_EMAIL', 'FALLBACK_SMTP_PASSWORD', 'FALLBACK_SMTP_HOST', 'FALLBACK_SMTP_PORT',
    'EMAIL_SUBJECT', 'EMAIL_BODY',
]


def install_sendconfig(monkeypatch, **attrs):
    """Подменяем модуль sendconfig на заглушку (реальный — плейсхолдеры)."""
    monkeypatch.setitem(sys.modules, 'sendconfig', SimpleNamespace(**attrs))


@pytest.fixture(autouse=True)
def _clean_env_and_sendconfig(monkeypatch):
    for key in _SMTP_KEYS:
        monkeypatch.delenv(key, raising=False)
    install_sendconfig(monkeypatch)  # пустой модуль -> _fallback_sendconfig отдаёт default


# --------------------------------------------------------------------------- #
# get_smtp_config
# --------------------------------------------------------------------------- #

def test_smtp_env_overrides_sendconfig(monkeypatch):
    install_sendconfig(monkeypatch, SMTP_EMAIL='sc@x.com', SMTP_HOST='sc.host', SMTP_PORT=25)
    monkeypatch.setenv('SMTP_EMAIL', 'env@x.com')
    cfg = app_config.get_smtp_config()
    assert cfg['SMTP_EMAIL'] == 'env@x.com'
    assert cfg['SMTP_HOST'] == 'sc.host'  # не задано в env -> из sendconfig
    assert cfg['SMTP_PORT'] == 25


def test_smtp_defaults_when_unconfigured():
    cfg = app_config.get_smtp_config()
    assert cfg['SMTP_EMAIL'] == ''
    assert cfg['SMTP_PASSWORD'] == ''
    assert cfg['SMTP_HOST'] == 'smtp.example.com'
    assert cfg['SMTP_PORT'] == 465


def test_smtp_port_from_env_as_int(monkeypatch):
    monkeypatch.setenv('SMTP_PORT', '2525')
    cfg = app_config.get_smtp_config()
    assert cfg['SMTP_PORT'] == 2525


def test_smtp_port_from_sendconfig(monkeypatch):
    install_sendconfig(monkeypatch, SMTP_PORT=587)
    cfg = app_config.get_smtp_config()
    assert cfg['SMTP_PORT'] == 587


def test_smtp_host_from_sendconfig(monkeypatch):
    install_sendconfig(monkeypatch, SMTP_HOST='mail.corp.ru')
    cfg = app_config.get_smtp_config()
    assert cfg['SMTP_HOST'] == 'mail.corp.ru'


def test_smtp_port_non_numeric_falls_back(monkeypatch):
    monkeypatch.setenv('SMTP_PORT', 'abc')
    cfg = app_config.get_smtp_config()
    assert cfg['SMTP_PORT'] == 465


# --------------------------------------------------------------------------- #
# get_fallback_smtp_config
# --------------------------------------------------------------------------- #

def test_fallback_none_when_unconfigured():
    assert app_config.get_fallback_smtp_config() is None


def test_fallback_none_when_only_email_set(monkeypatch):
    monkeypatch.setenv('FALLBACK_SMTP_EMAIL', 'fb@x.com')
    assert app_config.get_fallback_smtp_config() is None


def test_fallback_uses_default_port(monkeypatch):
    monkeypatch.setenv('FALLBACK_SMTP_EMAIL', 'fb@x.com')
    monkeypatch.setenv('FALLBACK_SMTP_HOST', 'fallback.host')
    cfg = app_config.get_fallback_smtp_config()
    assert cfg['FALLBACK_SMTP_EMAIL'] == 'fb@x.com'
    assert cfg['FALLBACK_SMTP_HOST'] == 'fallback.host'
    assert cfg['FALLBACK_SMTP_PORT'] == 465
    assert cfg.get('FALLBACK_SMTP_PASSWORD') is None or cfg.get('FALLBACK_SMTP_PASSWORD') == ''


def test_fallback_from_sendconfig(monkeypatch):
    install_sendconfig(
        monkeypatch,
        FALLBACK_SMTP_EMAIL='fb@x.com',
        FALLBACK_SMTP_HOST='fb.host',
        FALLBACK_SMTP_PORT=2525,
        FALLBACK_SMTP_PASSWORD='secret',
    )
    cfg = app_config.get_fallback_smtp_config()
    assert cfg['FALLBACK_SMTP_PORT'] == 2525
    assert cfg['FALLBACK_SMTP_PASSWORD'] == 'secret'


def test_fallback_env_overrides_sendconfig(monkeypatch):
    install_sendconfig(monkeypatch, FALLBACK_SMTP_EMAIL='sc@x.com', FALLBACK_SMTP_HOST='sc.host')
    monkeypatch.setenv('FALLBACK_SMTP_EMAIL', 'env@x.com')
    cfg = app_config.get_fallback_smtp_config()
    assert cfg['FALLBACK_SMTP_EMAIL'] == 'env@x.com'
    assert cfg['FALLBACK_SMTP_HOST'] == 'sc.host'


def test_fallback_port_non_numeric(monkeypatch):
    monkeypatch.setenv('FALLBACK_SMTP_EMAIL', 'fb@x.com')
    monkeypatch.setenv('FALLBACK_SMTP_HOST', 'fallback.host')
    monkeypatch.setenv('FALLBACK_SMTP_PORT', 'abc')
    cfg = app_config.get_fallback_smtp_config()
    assert cfg['FALLBACK_SMTP_PORT'] == 465


# --------------------------------------------------------------------------- #
# get_email_config
# --------------------------------------------------------------------------- #

def test_email_config_defaults():
    cfg = app_config.get_email_config()
    assert cfg['EMAIL_SUBJECT'] == 'Ваша работа'
    assert cfg['EMAIL_BODY'] == 'Пожалуйста, проверьте архив'


def test_email_config_env_override(monkeypatch):
    monkeypatch.setenv('EMAIL_SUBJECT', 'Моя тема')
    monkeypatch.setenv('EMAIL_BODY', 'Моё тело')
    cfg = app_config.get_email_config()
    assert cfg['EMAIL_SUBJECT'] == 'Моя тема'
    assert cfg['EMAIL_BODY'] == 'Моё тело'


def test_email_config_from_sendconfig(monkeypatch):
    install_sendconfig(monkeypatch, EMAIL_SUBJECT='Тема из sendconfig')
    cfg = app_config.get_email_config()
    assert cfg['EMAIL_SUBJECT'] == 'Тема из sendconfig'


# --------------------------------------------------------------------------- #
# .env через ENV_PATH
# --------------------------------------------------------------------------- #

def test_env_file_loaded(monkeypatch, tmp_path):
    env_file = tmp_path / 'test.env'
    env_file.write_text(
        'SMTP_EMAIL=dot@x.com\nSMTP_HOST=dot.host\nSMTP_PORT=2525\nEMAIL_SUBJECT=Тема из .env\n',
        encoding='utf-8',
    )
    monkeypatch.setattr(app_config, 'ENV_PATH', str(env_file))

    smtp = app_config.get_smtp_config()
    assert smtp['SMTP_EMAIL'] == 'dot@x.com'
    assert smtp['SMTP_HOST'] == 'dot.host'
    assert smtp['SMTP_PORT'] == 2525

    email_cfg = app_config.get_email_config()
    assert email_cfg['EMAIL_SUBJECT'] == 'Тема из .env'


def test_existing_env_wins_over_dotenv(monkeypatch, tmp_path):
    # load_dotenv не перезаписывает уже заданные env-переменные
    env_file = tmp_path / 'test.env'
    env_file.write_text('SMTP_EMAIL=dot@x.com\n', encoding='utf-8')
    monkeypatch.setattr(app_config, 'ENV_PATH', str(env_file))
    monkeypatch.setenv('SMTP_EMAIL', 'process@x.com')

    assert app_config.get_smtp_config()['SMTP_EMAIL'] == 'process@x.com'


def test_load_environment_noop_without_dotenv(monkeypatch):
    monkeypatch.setattr(app_config, 'load_dotenv', None)
    monkeypatch.setenv('SMTP_EMAIL', 'keep@x.com')
    app_config.load_environment()  # не должно падать
    assert app_config.get_smtp_config()['SMTP_EMAIL'] == 'keep@x.com'