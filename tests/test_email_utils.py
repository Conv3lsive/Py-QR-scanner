import smtplib

import pytest

import email_utils


def _full_config():
    return {
        'SMTP_EMAIL': 'sender@example.com',
        'SMTP_PASSWORD': 'secret',
        'SMTP_HOST': 'smtp.example.com',
        'SMTP_PORT': 465,
    }


# --- is_valid_email ---


@pytest.mark.parametrize('email', [
    'simple@example.com',
    'user+tag@example.com',
    'user.name@example.co.uk',
    'UPPER@EXAMPLE.COM',
    'a@b.co',
])
def test_is_valid_email_valid(email):
    assert email_utils.is_valid_email(email) is True


@pytest.mark.parametrize('email', [
    '',
    '   ',
    '..',
    'no-at-sign',
    '@example.com',
    'user@',
    'user@.com',
    'user@example',
    'user@example..com',
    'a..b@example.com',
    'тест@почта.рф',
    'user@пример.рф',
    'user@example.c',  # TLD из одной буквы
])
def test_is_valid_email_invalid(email):
    assert email_utils.is_valid_email(email) is False


def test_is_valid_email_length_boundary():
    local = 'a' * 64
    domain_254 = 'b' * 63 + '.' + 'c' * 63 + '.' + 'd' * 58 + '.' + 'co'
    email_254 = local + '@' + domain_254
    assert len(email_254) == 254
    assert email_utils.is_valid_email(email_254) is True

    domain_255 = 'b' * 63 + '.' + 'c' * 63 + '.' + 'd' * 59 + '.' + 'co'
    email_255 = local + '@' + domain_255
    assert len(email_255) == 255
    assert email_utils.is_valid_email(email_255) is False


# --- _get_smtp_config ---


def test_get_smtp_config_raises_on_empty(monkeypatch):
    monkeypatch.setattr(email_utils, 'get_smtp_config', lambda: {})
    with pytest.raises(RuntimeError):
        email_utils._get_smtp_config()


def test_get_smtp_config_raises_on_partial(monkeypatch):
    monkeypatch.setattr(email_utils, 'get_smtp_config', lambda: {'SMTP_EMAIL': 'x@y.com'})
    with pytest.raises(RuntimeError) as exc_info:
        email_utils._get_smtp_config()
    assert 'SMTP_PASSWORD' in str(exc_info.value)


def test_get_smtp_config_ok(monkeypatch):
    monkeypatch.setattr(email_utils, 'get_smtp_config', _full_config)
    cfg = email_utils._get_smtp_config()
    assert cfg['SMTP_EMAIL'] == 'sender@example.com'


# --- send_email_smtp ---


class _FakeSMTP:
    last_instance = None

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.login_args = None
        self.sent = None
        _FakeSMTP.last_instance = self

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def login(self, user, password):
        self.login_args = (user, password)

    def send_message(self, msg):
        self.sent = msg


class _RaisingSMTP:
    def __init__(self, host, port):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def login(self, *args):
        raise smtplib.SMTPException('connection refused')

    def send_message(self, msg):
        pass


def test_send_email_smtp_success(monkeypatch, tmp_path):
    monkeypatch.setattr(email_utils, 'get_smtp_config', _full_config)
    monkeypatch.setattr(email_utils.smtplib, 'SMTP_SSL', _FakeSMTP)

    attachment = tmp_path / 'sub' / 'archive.zip'
    attachment.parent.mkdir()
    attachment.write_bytes(b'PK\x03\x04fakezip')

    ok = email_utils.send_email_smtp('to@example.com', 'Тема', 'Тело', str(attachment))
    assert ok is True

    smtp = _FakeSMTP.last_instance
    assert smtp is not None
    assert smtp.login_args == ('sender@example.com', 'secret')
    assert smtp.sent is not None
    assert str(smtp.sent['From']) == 'sender@example.com'
    assert str(smtp.sent['To']) == 'to@example.com'
    assert str(smtp.sent['Subject']) == 'Тема'

    attachments = list(smtp.sent.iter_attachments())
    assert len(attachments) == 1
    part = attachments[0]
    assert part.get_filename() == 'archive.zip'  # basename, не полный путь
    assert part.get_content_type() == 'application/zip'


def test_send_email_smtp_missing_attachment_returns_false(monkeypatch, tmp_path):
    monkeypatch.setattr(email_utils, 'get_smtp_config', _full_config)
    _FakeSMTP.last_instance = None
    monkeypatch.setattr(email_utils.smtplib, 'SMTP_SSL', _FakeSMTP)

    ok = email_utils.send_email_smtp('to@example.com', 'S', 'B', str(tmp_path / 'nope.zip'))
    assert ok is False
    assert _FakeSMTP.last_instance is None  # SMTP вообще не трогали


def test_send_email_smtp_smtp_exception_returns_false(monkeypatch, tmp_path):
    monkeypatch.setattr(email_utils, 'get_smtp_config', _full_config)
    monkeypatch.setattr(email_utils.smtplib, 'SMTP_SSL', _RaisingSMTP)

    attachment = tmp_path / 'archive.zip'
    attachment.write_bytes(b'data')

    ok = email_utils.send_email_smtp('to@example.com', 'S', 'B', str(attachment))
    assert ok is False  # исключение не пробрасывается


# --- validate_emails ---


def test_validate_emails_split(monkeypatch):
    emails = {'a': 'a@x.com', 'b': 'bad', 'c': 'c@x.com', 'd': ''}
    valid, invalid = email_utils.validate_emails(emails, max_workers=2)
    assert set(valid) == {'a', 'c'}
    assert valid['a'] == 'a@x.com'
    assert sorted(invalid, key=lambda x: x[0]) == [('b', 'bad'), ('d', '')]


def test_validate_emails_empty_dict():
    valid, invalid = email_utils.validate_emails({})
    assert valid == {}
    assert invalid == []


def test_validate_emails_progress_callback(monkeypatch):
    emails = {'a': 'a@x.com', 'b': 'bad', 'c': 'c@x.com', 'd': ''}
    calls = []

    def cb(done, total, unit, msg):
        calls.append((done, total, unit))

    email_utils.validate_emails(emails, max_workers=2, progress_callback=cb)

    assert len(calls) == 4  # ровно total вызовов
    assert [done for done, _, _ in calls] == [1, 2, 3, 4]
    assert all(total == 4 for _, total, _ in calls)
    assert all(unit == 'email' for _, _, unit in calls)


def test_validate_emails_empty_progress_callback():
    calls = []
    email_utils.validate_emails({}, progress_callback=lambda d, t, u, m: calls.append((d, t)))
    assert calls == [(0, 0)]