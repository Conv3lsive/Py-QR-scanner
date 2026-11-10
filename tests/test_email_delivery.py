"""Тесты модуля email_delivery (per-email retry, exponential backoff, fallback SMTP).

Покрытие: SmtpConfig / EmailTask, классификация ошибок, расчёт backoff,
сборка сообщения, низкоуровневая отправка _send_one (моки smtplib),
SQLite-хранилище EmailRetryStore, оркестратор send_batch_with_retry
(с моком времени и _send_one). Реальные письма не отправляются.
"""

import socket
import sqlite3
import smtplib
import threading

import pytest

import email_delivery
from email_delivery import (
    EmailRetryStore,
    EmailTask,
    SendAttemptResult,
    SmtpConfig,
    _build_message,
    _parse_retry_after,
    _parse_smtp_code,
    _send_one,
    calculate_backoff,
    classify_error,
    send_batch_with_retry,
)


# ---------------------------------------------------------------------------
# Фикстуры и утилиты
# ---------------------------------------------------------------------------

class FakeClock:
    """Контролируемое время: time/sleep/monotonic управляются тестом."""

    def __init__(self, start=1000.0):
        self.t = start
        self.sleeps = []

    def time(self):
        return self.t

    def monotonic(self):
        return self.t

    def sleep(self, sec):
        self.sleeps.append(sec)
        self.t += sec


@pytest.fixture
def clock(monkeypatch):
    c = FakeClock()
    monkeypatch.setattr(email_delivery.time, 'time', c.time)
    monkeypatch.setattr(email_delivery.time, 'sleep', c.sleep)
    monkeypatch.setattr(email_delivery.time, 'monotonic', c.monotonic)
    return c


@pytest.fixture
def no_jitter(monkeypatch):
    """random() -> 0.5 ⇒ jitter = 0, backoff равен базовой таблице."""
    monkeypatch.setattr(email_delivery.random, 'random', lambda: 0.5)


@pytest.fixture
def primary_cfg():
    return SmtpConfig(
        email='sender@x.com', password='secret',
        host='smtp.primary.test', port=465, use_ssl=True, name='primary',
    )


@pytest.fixture
def fallback_cfg():
    return SmtpConfig(
        email='sender@y.com', password='secret',
        host='smtp.fallback.test', port=465, use_ssl=True, name='fallback',
    )


def _make_task(task_id='t-1', recipient='to@x.com', **kw):
    t = EmailTask(
        recipient=recipient, subject='sub', body='body', attachment_path='',
        task_id=task_id, **kw,
    )
    return t


def _res(task, cfg, *, success=True, permanent=False, rate_limited=False,
         error_code=None, error_message='', message_id=None,
         retry_after_sec=None):
    """Фабрика SendAttemptResult с attempt_number = task.attempt_count + 1."""
    return SendAttemptResult(
        task_id=task.task_id,
        attempt_number=task.attempt_count + 1,
        success=success,
        permanent=permanent,
        rate_limited=rate_limited,
        error_code=error_code,
        error_message=error_message,
        message_id=message_id or ('ok-msgid' if success else ''),
        retry_after_sec=retry_after_sec,
        duration_ms=1,
        provider_name=cfg.name,
    )


def _read_table(db_path, sql):
    conn = sqlite3.connect(db_path)
    try:
        conn.row_factory = sqlite3.Row
        return [dict(r) for r in conn.execute(sql).fetchall()]
    finally:
        conn.close()


def _install_fake_smtp(monkeypatch, raise_on_connect=None, raise_on_login=None,
                       raise_on_send=None):
    """Подменяет smtplib.SMTP/SMTP_SSL; возвращает наблюдаемый state."""
    state = {'created': [], 'starttls': 0, 'login': [], 'send_message': []}

    def make(tag):
        class FakeSmtp:
            def __init__(self, host, port, timeout=None):
                state['created'].append((tag, host, port, timeout))
                if raise_on_connect is not None:
                    raise raise_on_connect

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def login(self, user, pwd):
                state['login'].append((user, pwd))
                if raise_on_login is not None:
                    raise raise_on_login

            def send_message(self, msg, *a, **kw):
                state['send_message'].append(msg)
                if raise_on_send is not None:
                    raise raise_on_send

            def starttls(self):
                state['starttls'] += 1

        return FakeSmtp

    monkeypatch.setattr(email_delivery.smtplib, 'SMTP_SSL', make('ssl'))
    monkeypatch.setattr(email_delivery.smtplib, 'SMTP', make('plain'))
    return state


# ---------------------------------------------------------------------------
# SmtpConfig / EmailTask
# ---------------------------------------------------------------------------

def test_smtp_config_defaults():
    c = SmtpConfig(email='a@b.c', password='p', host='h')
    assert c.port == 465
    assert c.use_ssl is True
    assert c.name == 'primary'


def test_smtp_config_missing_email_raises():
    with pytest.raises(ValueError):
        SmtpConfig(email='', password='p', host='h')


def test_smtp_config_missing_host_raises():
    with pytest.raises(ValueError):
        SmtpConfig(email='a@b.c', password='p', host='')


def test_email_task_defaults():
    t = EmailTask('to@x.com', 'sub', 'body', '')
    assert t.task_id
    assert t.max_attempts == 4
    assert t.status == 'pending'
    assert t.attempt_count == 0
    assert t.next_retry_at > 0


# ---------------------------------------------------------------------------
# _parse_smtp_code / _parse_retry_after / classify_error
# ---------------------------------------------------------------------------

def test_parse_smtp_code_from_smtp_code_attr():
    assert _parse_smtp_code(smtplib.SMTPResponseException(550, 'no')) == 550


def test_parse_smtp_code_from_smtp_error_attr():
    class FakeExc(Exception):
        smtp_code = None
        smtp_error = '421 try later'

    assert _parse_smtp_code(FakeExc()) == 421


def test_parse_smtp_code_from_message():
    assert _parse_smtp_code(Exception('550 mailbox unavailable')) == 550


def test_parse_smtp_code_none():
    assert _parse_smtp_code(Exception('some generic error')) is None


@pytest.mark.parametrize('text,expected', [
    ('retry-after: 120', 120.0),
    ('Retry-After: 30', 30.0),
    ('please wait 45 s', 45.0),
    ('retry in 9', 9.0),
    ('wait 200 seconds', 200.0),
    ('no digits here', None),
])
def test_parse_retry_after(text, expected):
    assert _parse_retry_after(text) == expected


def test_classify_auth_permanent():
    exc = smtplib.SMTPAuthenticationError(535, b'bad credentials')
    perm, rated, code, _msg, retry = classify_error(exc)
    assert perm is True and rated is False and code == 535 and retry is None


def test_classify_smtp_550_permanent():
    perm, rated, code, _m, retry = classify_error(
        smtplib.SMTPResponseException(550, 'rejected'))
    assert perm is True and rated is False and code == 550 and retry is None


def test_classify_smtp_421_rate_limited_default_wait():
    exc = smtplib.SMTPResponseException(421, '4.7.0 Try again later')
    perm, rated, code, _m, retry = classify_error(exc)
    assert perm is False and rated is True and code == 421 and retry == 60.0


def test_classify_smtp_450_temp_no_wait():
    perm, rated, code, _m, retry = classify_error(
        smtplib.SMTPResponseException(450, 'mailbox busy'))
    assert perm is False and rated is False and code == 450 and retry is None


def test_classify_smtp_unknown_code_temp():
    perm, rated, code, _m, retry = classify_error(
        smtplib.SMTPResponseException(599, 'weird'))
    assert perm is False and rated is False and code == 599 and retry is None


def test_classify_recipients_refused_permanent():
    exc = smtplib.SMTPRecipientsRefused({'a@b.c': (550, b'refused')})
    perm, rated, code, _m, retry = classify_error(exc)
    assert perm is True and rated is False and code == 550 and retry is None


def test_classify_recipients_refused_rate_limited():
    exc = smtplib.SMTPRecipientsRefused({'a@b.c': (421, b'slow down')})
    perm, rated, code, _m, retry = classify_error(exc)
    assert perm is False and rated is True and code == 421 and retry == 60.0


def test_classify_network_timeout_temp():
    perm, rated, code, _m, retry = classify_error(socket.timeout('timed out'))
    assert perm is False and rated is False and code is None and retry is None


def test_classify_oserror_temp():
    perm, rated, code, _m, retry = classify_error(OSError('conn refused'))
    assert perm is False and rated is False and code is None and retry is None


def test_classify_generic_smtp_exception_temp():
    perm, rated, code, _m, retry = classify_error(
        smtplib.SMTPException('some transient'))
    assert perm is False and rated is False and code is None and retry is None


# ---------------------------------------------------------------------------
# calculate_backoff
# ---------------------------------------------------------------------------

def test_backoff_table_values(monkeypatch):
    monkeypatch.setattr(email_delivery.random, 'random', lambda: 0.5)
    assert calculate_backoff(1) == 15.0
    assert calculate_backoff(2) == 60.0
    assert calculate_backoff(3) == 180.0
    assert calculate_backoff(4) == 180.0   # clamp к последней строке
    assert calculate_backoff(99) == 180.0


def test_backoff_override_used(monkeypatch):
    monkeypatch.setattr(email_delivery.random, 'random', lambda: 0.5)
    assert calculate_backoff(1, override_sec=30.0) == 30.0


def test_backoff_clamps_negative_override(monkeypatch):
    monkeypatch.setattr(email_delivery.random, 'random', lambda: 0.5)
    assert calculate_backoff(1, override_sec=-5.0) == 0.0


def test_backoff_jitter_bounds(monkeypatch):
    monkeypatch.setattr(email_delivery.random, 'random', lambda: 0.0)
    low = calculate_backoff(1, jitter_ratio=0.25)
    assert low == 15.0 * 0.75

    monkeypatch.setattr(email_delivery.random, 'random', lambda: 0.999)
    high = calculate_backoff(1, jitter_ratio=0.25)
    assert 15.0 < high <= 15.0 * 1.25


# ---------------------------------------------------------------------------
# _build_message
# ---------------------------------------------------------------------------

def test_build_message_headers_and_body():
    task = EmailTask('to@x.com', 'Тема', 'Тело', '')
    msg, msg_id = _build_message(task, 'from@x.com')
    assert msg['From'] == 'from@x.com'
    assert msg['To'] == 'to@x.com'
    assert msg['Subject'] == 'Тема'
    assert msg_id and msg_id.startswith('<') and '@' in msg_id
    assert 'Тело' in msg.get_content()


def test_build_message_attachment(tmp_path):
    p = tmp_path / 'result.zip'
    p.write_bytes(b'PK\x03\x04fakezip')
    task = EmailTask('to@x.com', 's', 'b', str(p))
    msg, _ = _build_message(task, 'from@x.com')
    parts = list(msg.iter_attachments())
    assert len(parts) == 1
    part = parts[0]
    assert part.get_filename() == 'result.zip'
    assert part.get_content_maintype() == 'application'
    assert part.get_content_subtype() == 'zip'


def test_build_message_missing_attachment_skipped(tmp_path):
    task = EmailTask('to@x.com', 's', 'b', str(tmp_path / 'nope.zip'))
    msg, _ = _build_message(task, 'from@x.com')
    assert list(msg.iter_attachments()) == []


def test_build_message_no_at_sign_domain_localhost():
    task = EmailTask('to@x.com', 's', 'b', '')
    msg, msg_id = _build_message(task, 'noaddress')
    assert msg['From'] == 'noaddress'
    assert 'localhost' in msg_id


# ---------------------------------------------------------------------------
# _send_one (мок smtplib)
# ---------------------------------------------------------------------------
#
# ВАЖНО: _send_one сейчас СЛОМАН и в любом сценарии бросает KeyError('error_code').
# Причина — оба return-оператора конструируют SendAttemptResult как
#   SendAttemptResult(**base, <поля, уже лежащие в base>, ...)
# дублируя имена полей:
#   - успешный путь (строка 344): дублируется message_id
#   - путь ошибки   (строка 359): дублируются error_code/error_message/retry_after_sec
# При успехе TypeError из-за дубля message_id ещё и ловится собственным except,
# поэтому успешная отправка ошибочно классифицируется как temp_fail, и лишь затем
# из функции вылетает KeyError('error_code') из пути ошибки.
# Транспортный слой (SSL/STARTTLS/login/send_message) при этом работает корректно —
# он проверяется ниже отдельно, а сам баг зафиксирован xfail.

def test_send_one_transport_uses_ssl_login_send(monkeypatch):
    state = _install_fake_smtp(monkeypatch)
    cfg = SmtpConfig('sender@x.com', 'secret', 'smtp.x.com', 465, True, 'primary')
    with pytest.raises(KeyError):   # баг: ломается на конструировании результата
        _send_one(EmailTask('to@x.com', 's', 'b', ''), cfg)
    tag, host, port, timeout = state['created'][0]
    assert (tag, host, port, timeout) == ('ssl', 'smtp.x.com', 465, 30)
    assert state['starttls'] == 0
    assert state['login'] == [('sender@x.com', 'secret')]
    assert len(state['send_message']) == 1


def test_send_one_transport_starttls_non_ssl(monkeypatch):
    state = _install_fake_smtp(monkeypatch)
    cfg = SmtpConfig('sender@x.com', 'secret', 'smtp.x.com', 587, False, 'primary')
    with pytest.raises(KeyError):
        _send_one(EmailTask('to@x.com', 's', 'b', ''), cfg)
    assert state['created'][0][0] == 'plain'
    assert state['starttls'] == 1


def test_send_one_transport_connect_error_skips_login_send(monkeypatch):
    state = _install_fake_smtp(
        monkeypatch,
        raise_on_connect=smtplib.SMTPConnectError(421, 'connect fail'))
    cfg = SmtpConfig('sender@x.com', 's', 'smtp.x.com', 465, True, 'primary')
    with pytest.raises(KeyError):   # classify отрабатывает, но return сломан
        _send_one(EmailTask('to@x.com', 's', 'b', ''), cfg)
    assert state['created']       # конструктор вызывался (connect-ошибка внутри)
    assert state['login'] == []   # до login не дошло
    assert state['send_message'] == []


@pytest.mark.xfail(strict=True,
                   reason='баг: SendAttemptResult(**base, message_id=…) дублирует '
                          'kwarg message_id (строка 344); успех вместо результата '
                          'вылетает Exception и классифицируется как temp_fail')
def test_send_one_success_returns_result(monkeypatch):
    _install_fake_smtp(monkeypatch)
    cfg = SmtpConfig('sender@x.com', 'secret', 'smtp.x.com', 465, True, 'primary')
    res = _send_one(EmailTask('to@x.com', 'sub', 'body', ''), cfg)
    assert res.success is True and res.permanent is False and res.rate_limited is False
    assert res.message_id and res.provider_name == 'primary'
    assert res.attempt_number == 1


@pytest.mark.xfail(strict=True,
                   reason='баг: SendAttemptResult(**base, error_code/error_message/'
                          'retry_after_sec=…) дублирует kwargs (строка 359) — путь '
                          'ошибки всегда бросает KeyError("error_code")')
def test_send_one_auth_error_returns_permanent_result(monkeypatch):
    _install_fake_smtp(
        monkeypatch,
        raise_on_login=smtplib.SMTPAuthenticationError(535, b'bad'))
    cfg = SmtpConfig('sender@x.com', 's', 'smtp.x.com', 465, True, 'primary')
    res = _send_one(EmailTask('to@x.com', 's', 'b', ''), cfg)
    assert res.success is False and res.permanent is True and res.error_code == 535


@pytest.mark.xfail(strict=True,
                   reason='баг: путь ошибки _send_one бросает KeyError (строка 359), '
                          'поэтому retry_after_sec из 421-ответа не возвращается')
def test_send_one_rate_limited_returns_retry_after(monkeypatch):
    _install_fake_smtp(
        monkeypatch,
        raise_on_send=smtplib.SMTPResponseException(
            421, 'too many connections, retry after 120 seconds'))
    cfg = SmtpConfig('sender@x.com', 's', 'smtp.x.com', 465, True, 'primary')
    res = _send_one(EmailTask('to@x.com', 's', 'b', ''), cfg)
    assert res.rate_limited is True and res.retry_after_sec == 120.0


# ---------------------------------------------------------------------------
# EmailRetryStore
# ---------------------------------------------------------------------------

def test_store_creates_parent_dir(tmp_path):
    db = tmp_path / 'sub' / 'nested' / 'db.sqlite'
    EmailRetryStore(str(db))
    assert db.exists()


def test_store_upsert_and_is_sent(tmp_path):
    store = EmailRetryStore(str(tmp_path / 'db.sqlite'))
    t = _make_task('id-1', status='pending')
    store.upsert_task(t)
    assert store.is_sent('id-1') is False


def test_store_mark_sent(tmp_path):
    store = EmailRetryStore(str(tmp_path / 'db.sqlite'))
    store.upsert_task(_make_task('id-1'))
    store.mark_sent('id-1', 'msgid-1', 'primary')
    assert store.is_sent('id-1') is True
    rows = _read_table(str(tmp_path / 'db.sqlite'),
                       "SELECT status, message_id, provider_used FROM email_tasks WHERE task_id='id-1'")
    assert rows[0]['status'] == 'sent'
    assert rows[0]['message_id'] == 'msgid-1'
    assert rows[0]['provider_used'] == 'primary'


def test_store_get_pending_ordered_and_excludes_sent(tmp_path):
    store = EmailRetryStore(str(tmp_path / 'db.sqlite'))
    for t in [
        _make_task('a', status='failed', next_retry_at=100.0),
        _make_task('b', status='pending', next_retry_at=10.0),
        _make_task('c', status='sent'),
    ]:
        store.upsert_task(t)
    pending = store.get_pending_tasks()
    assert [t.task_id for t in pending] == ['b', 'a']
    assert pending[0].next_retry_at == 10.0


def test_store_summary(tmp_path):
    store = EmailRetryStore(str(tmp_path / 'db.sqlite'))
    store.upsert_task(_make_task('1', status='sent'))
    store.upsert_task(_make_task('2', status='failed'))
    store.upsert_task(_make_task('3', status='pending'))
    assert store.summary() == {'sent': 1, 'failed': 1, 'pending': 1}


def test_store_record_attempt_truncates_error(tmp_path):
    store = EmailRetryStore(str(tmp_path / 'db.sqlite'))
    res = SendAttemptResult(
        task_id='id-1', attempt_number=1, success=False, permanent=False,
        rate_limited=False, error_code=450, error_message='x' * 1000,
        message_id='', retry_after_sec=None, duration_ms=5, provider_name='primary')
    store.record_attempt(res)
    rows = _read_table(str(tmp_path / 'db.sqlite'),
                       "SELECT error_message FROM send_attempts WHERE task_id='id-1'")
    assert len(rows[0]['error_message']) == 500


def test_store_persists_across_instances(tmp_path):
    path = str(tmp_path / 'db.sqlite')
    s1 = EmailRetryStore(path)
    t = _make_task('persist-1', status='failed', attempt_count=2,
                   last_error='oops', next_retry_at=123.5)
    s1.upsert_task(t)

    s2 = EmailRetryStore(path)   # «рестарт процесса»
    assert s2.is_sent('persist-1') is False
    pending = s2.get_pending_tasks()
    assert len(pending) == 1
    r = pending[0]
    assert (r.task_id, r.attempt_count, r.status, r.last_error, r.next_retry_at) == \
        ('persist-1', 2, 'failed', 'oops', 123.5)
    assert s2.summary() == {'failed': 1}


def test_store_thread_safety(tmp_path):
    store = EmailRetryStore(str(tmp_path / 'db.sqlite'))
    n = 24

    def work(i):
        store.upsert_task(_make_task(f'id-{i}'))
        store.record_attempt(SendAttemptResult(
            task_id=f'id-{i}', attempt_number=1, success=False, permanent=False,
            rate_limited=False, error_code=None, error_message='', message_id='',
            retry_after_sec=None, duration_ms=1, provider_name='primary'))

    threads = [threading.Thread(target=work, args=(i,)) for i in range(n)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    assert store.summary().get('pending') == n
    cnt = _read_table(str(tmp_path / 'db.sqlite'), 'SELECT COUNT(*) AS c FROM send_attempts')[0]['c']
    assert cnt == n


# ---------------------------------------------------------------------------
# send_batch_with_retry (оркестратор)
# ---------------------------------------------------------------------------

def test_empty_batch_zero_summary():
    s = send_batch_with_retry([], primary_cfg=SmtpConfig('a@b.c', 'p', 'h'))
    assert (s.total, s.sent, s.permanent_failed, s.temp_failed,
            s.skipped_duplicate, s.total_attempts, s.retried) == (0, 0, 0, 0, 0, 0, 0)


def test_success_no_retry(clock, no_jitter, primary_cfg, monkeypatch, tmp_path):
    store = EmailRetryStore(str(tmp_path / 'db.sqlite'))
    sends = []
    monkeypatch.setattr(email_delivery, '_send_one',
                        lambda t, c: sends.append(t.task_id) or _res(t, c, success=True))

    s = send_batch_with_retry([_make_task('a')], primary_cfg, store=store, threads=1)

    assert sends == ['a']
    assert (s.sent, s.retried, s.total_attempts) == (1, 0, 1)
    assert clock.sleeps == []
    assert store.is_sent('a') is True


def test_permanent_failure_no_retry(clock, no_jitter, primary_cfg, monkeypatch, tmp_path):
    store = EmailRetryStore(str(tmp_path / 'db.sqlite'))
    monkeypatch.setattr(email_delivery, '_send_one',
                        lambda t, c: _res(t, c, success=False, permanent=True,
                                          error_code=535))
    s = send_batch_with_retry([_make_task('a')], primary_cfg, store=store, threads=1)
    assert (s.permanent_failed, s.retried, s.total_attempts) == (1, 0, 1)
    assert clock.sleeps == []
    row = _read_table(str(tmp_path / 'db.sqlite'),
                      "SELECT status FROM email_tasks WHERE task_id='a'")[0]
    assert row['status'] == 'permanent_fail'


def test_temp_exhausts_max_attempts(clock, no_jitter, primary_cfg, monkeypatch, tmp_path):
    store = EmailRetryStore(str(tmp_path / 'db.sqlite'))
    sends = []
    monkeypatch.setattr(email_delivery, '_send_one',
                        lambda t, c: sends.append(t.task_id) or
                        _res(t, c, success=False, permanent=False, error_code=450))

    s = send_batch_with_retry([_make_task('a')], primary_cfg, store=store, threads=1)

    # Инвариант: не более max_attempts (4) попыток; retry = 4-1 = 3.
    assert sends == ['a', 'a', 'a', 'a']
    assert (s.temp_failed, s.retried, s.total_attempts) == (1, 3, 4)
    # Экспоненциальный backoff: фактические задержки 15/60/180.
    assert clock.sleeps == [15.0, 60.0, 180.0]
    row = _read_table(str(tmp_path / 'db.sqlite'),
                      "SELECT status, attempt_count FROM email_tasks WHERE task_id='a'")[0]
    assert row['status'] == 'failed'
    assert row['attempt_count'] == 4
    attempts = _read_table(str(tmp_path / 'db.sqlite'),
                           "SELECT COUNT(*) AS c FROM send_attempts WHERE task_id='a'")[0]['c']
    assert attempts == 4


def test_retry_then_success_stops(clock, no_jitter, primary_cfg, monkeypatch, tmp_path):
    store = EmailRetryStore(str(tmp_path / 'db.sqlite'))
    calls = {'n': 0}
    sends = []

    def fake(t, c):
        sends.append(t.task_id)
        calls['n'] += 1
        return _res(t, c, success=(calls['n'] >= 2),
                    permanent=False, error_code=None if calls['n'] >= 2 else 450)

    monkeypatch.setattr(email_delivery, '_send_one', fake)
    s = send_batch_with_retry([_make_task('a')], primary_cfg, store=store, threads=1)

    assert sends == ['a', 'a']
    assert (s.sent, s.retried, s.total_attempts) == (1, 1, 2)
    assert clock.sleeps == [15.0]
    assert store.is_sent('a') is True


def test_retry_after_override_used_for_sleep(clock, no_jitter, primary_cfg, monkeypatch):
    calls = {'n': 0}

    def fake(t, c):
        calls['n'] += 1
        if calls['n'] == 1:
            return _res(t, c, success=False, permanent=False, rate_limited=True,
                        error_code=421, retry_after_sec=30.0)
        return _res(t, c, success=True)

    monkeypatch.setattr(email_delivery, '_send_one', fake)
    s = send_batch_with_retry([_make_task('a')], primary_cfg, threads=1)
    assert s.sent == 1
    assert clock.sleeps == [30.0]


def test_fallback_used_after_two_primary_failures(clock, no_jitter, primary_cfg,
                                                  fallback_cfg, monkeypatch, tmp_path):
    store = EmailRetryStore(str(tmp_path / 'db.sqlite'))
    rec = []

    def fake(t, c):
        rec.append((c.name, t.attempt_count))
        if c.name == 'fallback':
            return _res(t, c, success=True)
        return _res(t, c, success=False, permanent=False, error_code=450)

    monkeypatch.setattr(email_delivery, '_send_one', fake)
    s = send_batch_with_retry([_make_task('a')], primary_cfg,
                              fallback_cfg=fallback_cfg, store=store, threads=1)

    assert [name for name, _ in rec] == ['primary', 'primary', 'fallback']
    assert rec[2][1] == 2            # переключение при attempt_count >= 2
    assert (s.sent, s.retried, s.total_attempts) == (1, 2, 3)
    assert clock.sleeps == [15.0, 60.0]
    row = _read_table(str(tmp_path / 'db.sqlite'),
                      "SELECT provider_used FROM email_tasks WHERE task_id='a'")[0]
    assert row['provider_used'] == 'fallback'


def test_duplicate_already_sent_skipped(clock, no_jitter, primary_cfg, monkeypatch, tmp_path):
    store = EmailRetryStore(str(tmp_path / 'db.sqlite'))
    store.upsert_task(_make_task('dup', status='sent', message_id='prev'))
    sends = []
    monkeypatch.setattr(email_delivery, '_send_one',
                        lambda t, c: sends.append(t.task_id) or _res(t, c, success=True))

    s = send_batch_with_retry([_make_task('dup')], primary_cfg, store=store, threads=1)
    assert sends == []
    assert (s.skipped_duplicate, s.sent, s.total_attempts) == (1, 0, 0)


def test_custom_max_attempts(clock, no_jitter, primary_cfg, monkeypatch):
    sends = []
    monkeypatch.setattr(email_delivery, '_send_one',
                        lambda t, c: sends.append(t.task_id) or
                        _res(t, c, success=False, permanent=False, error_code=450))
    s = send_batch_with_retry([_make_task('a', max_attempts=2)], primary_cfg, threads=1)
    assert sends == ['a', 'a']
    assert (s.temp_failed, s.retried, s.total_attempts) == (1, 1, 2)
    assert clock.sleeps == [15.0]


def test_progress_callback_called(clock, no_jitter, primary_cfg, monkeypatch):
    monkeypatch.setattr(email_delivery, '_send_one',
                        lambda t, c: _res(t, c, success=True))
    calls = []
    send_batch_with_retry(
        [_make_task('a'), _make_task('b')], primary_cfg, threads=2,
        progress_callback=lambda done, total, unit, msg: calls.append((done, total, unit, msg)),
    )
    assert len(calls) == 2
    assert calls[-1] == (2, 2, 'email', 'Отправка email')


def test_multiple_tasks_concurrent_success(clock, no_jitter, primary_cfg, monkeypatch, tmp_path):
    store = EmailRetryStore(str(tmp_path / 'db.sqlite'))
    monkeypatch.setattr(email_delivery, '_send_one',
                        lambda t, c: _res(t, c, success=True))
    tasks = [_make_task(f't-{i}') for i in range(5)]
    s = send_batch_with_retry(tasks, primary_cfg, store=store, threads=3)
    assert (s.sent, s.retried, s.total_attempts) == (5, 0, 5)
    assert store.summary().get('sent') == 5


@pytest.mark.xfail(strict=True,
                   reason='баг: _send_one всегда бросает KeyError("error_code") из-за '
                          'дублирования kwargs в SendAttemptResult, поэтому реальный прогон '
                          'не доставляет письма и не делает retry: всё падает в temp_failed '
                          'на первой же попытке')
def test_orchestrator_end_to_end_via_smtp(clock, no_jitter, primary_cfg, monkeypatch, tmp_path):
    """Полный проход: timeout на первой отправке -> retry -> успех на второй."""
    store = EmailRetryStore(str(tmp_path / 'db.sqlite'))
    calls = {'send': 0}

    def make_fake():
        class Fake:
            def __init__(self, host, port, timeout=None):
                self._sent = 0

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def login(self, u, p):
                pass

            def send_message(self, msg):
                self._sent += 1
                calls['send'] += 1
                if self._sent == 1 and calls['send'] == 1:
                    raise socket.timeout('timed out')

            def starttls(self):
                pass

        return Fake

    monkeypatch.setattr(email_delivery.smtplib, 'SMTP_SSL', make_fake())
    monkeypatch.setattr(email_delivery.smtplib, 'SMTP', make_fake())

    s = send_batch_with_retry([_make_task('a')], primary_cfg, store=store, threads=1)

    assert (s.sent, s.retried, s.total_attempts) == (1, 1, 2)
    assert clock.sleeps == [15.0]
    assert store.is_sent('a') is True


# ---------------------------------------------------------------------------
# Найденные баги (xfail)
# ---------------------------------------------------------------------------

@pytest.mark.xfail(strict=True,
                   reason='баг: задача, исчерпавшая max_attempts, отправляется '
                          'повторно при рестарте (лишняя попытка №5)')
def test_resume_exhausted_task_cannot_exceed_max_attempts(
        clock, no_jitter, primary_cfg, monkeypatch, tmp_path):
    store = EmailRetryStore(str(tmp_path / 'db.sqlite'))
    t = _make_task('fixed', max_attempts=4, status='failed',
                   attempt_count=4, last_error='exhausted')
    store.upsert_task(t)

    pending = store.get_pending_tasks()
    assert len(pending) == 1 and pending[0].attempt_count == 4

    sends = []
    monkeypatch.setattr(email_delivery, '_send_one',
                        lambda task, c: sends.append(task.task_id) or
                        _res(task, c, success=False, permanent=False, error_code=450))

    send_batch_with_retry(pending, primary_cfg, store=store, threads=1)

    # Инвариант: «каждая задача ≤ max_attempts». 4 попытки уже были в store,
    # новых отправок быть не должно.
    assert sends == []