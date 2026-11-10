"""Reliable email delivery with per-task tracking, exponential backoff,
rate-limit awareness, duplicate prevention, and optional fallback SMTP.

Public API
----------
SmtpConfig          – SMTP server credentials + flags
EmailTask           – a single email job (recipient, body, attachment, …)
SendAttemptResult   – result of one send attempt
BatchSendSummary    – aggregate result of send_batch_with_retry()
EmailRetryStore     – SQLite-backed persistence for tasks and attempt logs
send_batch_with_retry() – main orchestrator

Error classification
--------------------
- Permanent (5xx, auth errors)   → no retry, mark permanent_fail
- Temporary (4xx, connect errors) → retry with exponential backoff + jitter
- Rate-limited (421 or keyword)   → retry with Retry-After or default wait

Duplicate prevention
--------------------
Each EmailTask has a stable task_id (UUID).  Before sending, the store is
checked; already-sent tasks are skipped without re-sending.
"""

from __future__ import annotations

import contextlib
import email.utils
import logging
import os
import random
import re
import smtplib
import socket
import sqlite3
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.message import EmailMessage
from typing import Callable, Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DEFAULT_MAX_ATTEMPTS = 4

# Seconds to wait before each attempt (index = attempt number, 1-based)
# attempt 1 → immediate (no prior wait)
# attempt 2 → ~15 s
# attempt 3 → ~60 s
# attempt 4 → ~180 s
_BACKOFF_BASE_SECONDS: Tuple[float, ...] = (0.0, 15.0, 60.0, 180.0)

# ±25 % random jitter applied to each backoff interval
_BACKOFF_JITTER_RATIO = 0.25

# Used when a rate-limit is detected but no Retry-After value is available
_DEFAULT_RATE_LIMIT_WAIT_SEC = 60.0

# Single timeout (seconds) for both connection and per-operation socket I/O
_SMTP_TIMEOUT_SEC = 30

# SMTP response codes that indicate a TEMPORARY failure → eligible for retry
_TEMPORARY_SMTP_CODES = frozenset({
    421,  # Service temporarily unavailable / rate limit
    450,  # Mailbox unavailable (temporary)
    451,  # Requested action aborted: local processing error
    452,  # Insufficient system storage
})

# SMTP response codes that indicate a PERMANENT failure → no retry
_PERMANENT_SMTP_CODES = frozenset({
    500, 501, 502, 503, 504,   # Protocol / syntax errors
    510, 511,                   # Bad email address
    535,                        # Authentication failed
    541,                        # No answer from host
    550, 551, 552, 553, 554, 556,  # Mailbox / policy / spam rejection
})

# Phrases that indicate the server is enforcing a rate limit
_RATE_LIMIT_PHRASES = (
    'rate limit', 'too many', 'too fast', 'slow down',
    'exceed', 'throttl', 'greylisted', 'greylist', 'blocked',
)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class SmtpConfig:
    """SMTP connection credentials and transport settings."""
    email: str
    password: str
    host: str
    port: int = 465
    use_ssl: bool = True   # True → SMTP_SSL (port 465); False → STARTTLS (port 587)
    name: str = 'primary'

    def __post_init__(self):
        if not self.email or not self.host:
            raise ValueError(f'SmtpConfig "{self.name}": email and host are required')


@dataclass
class EmailTask:
    """A single email delivery job."""
    recipient: str
    subject: str
    body: str
    attachment_path: str
    task_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    max_attempts: int = _DEFAULT_MAX_ATTEMPTS
    attempt_count: int = 0
    status: str = 'pending'        # pending | sent | failed | permanent_fail
    last_error: str = ''
    message_id: str = ''           # SMTP Message-ID of the successfully sent message
    created_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    next_retry_at: float = field(default_factory=time.time)
    provider_used: str = ''


@dataclass
class SendAttemptResult:
    """Result of a single send attempt for one EmailTask."""
    task_id: str
    attempt_number: int
    success: bool
    permanent: bool
    rate_limited: bool
    error_code: Optional[int]
    error_message: str
    message_id: str
    retry_after_sec: Optional[float]
    duration_ms: int
    provider_name: str


@dataclass
class BatchSendSummary:
    """Aggregate outcome of send_batch_with_retry()."""
    total: int
    sent: int
    permanent_failed: int
    temp_failed: int
    skipped_duplicate: int
    total_attempts: int
    retried: int


# ---------------------------------------------------------------------------
# Error classification helpers
# ---------------------------------------------------------------------------

def _parse_smtp_code(exc: Exception) -> Optional[int]:
    for attr in ('smtp_code', 'smtp_error'):
        value = getattr(exc, attr, None)
        if value is None:
            continue
        text = str(value).strip()
        if text[:3].isdigit():
            return int(text[:3])
    msg = str(exc).strip()
    if msg[:3].isdigit():
        return int(msg[:3])
    return None


def _parse_retry_after(text: str) -> Optional[float]:
    """Extract a wait duration (seconds) from an SMTP error message."""
    for pattern in (
        r'retry[- ]?after[:\s]+(\d+)',
        r'wait\s+(\d+)\s*s',
        r'retry\s+in\s+(\d+)',
        r'(\d+)\s*second',
    ):
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            return float(m.group(1))
    return None


def classify_error(
    exc: Exception,
) -> Tuple[bool, bool, Optional[int], str, Optional[float]]:
    """Classify a send exception.

    Returns
    -------
    (is_permanent, is_rate_limited, smtp_code, message, retry_after_sec)
    """
    message = str(exc).strip()
    retry_after = _parse_retry_after(message)
    msg_lower = message.lower()

    # Authentication is always permanent
    if isinstance(exc, smtplib.SMTPAuthenticationError):
        return True, False, _parse_smtp_code(exc), message, None

    # Per-recipient refusal
    if isinstance(exc, smtplib.SMTPRecipientsRefused):
        codes: set = set()
        for _addr, (code, _resp) in exc.recipients.items():
            codes.add(int(code))
        rate_limited = bool(codes & {421}) or any(p in msg_lower for p in _RATE_LIMIT_PHRASES)
        all_permanent = bool(codes) and codes.issubset(_PERMANENT_SMTP_CODES)
        return all_permanent, rate_limited, (min(codes) if codes else None), message, (
            retry_after or _DEFAULT_RATE_LIMIT_WAIT_SEC if rate_limited else retry_after
        )

    # General SMTP response errors
    if isinstance(exc, smtplib.SMTPResponseException):
        code = getattr(exc, 'smtp_code', None)
        if code is None:
            code = _parse_smtp_code(exc)
        rate_limited = (code == 421) or any(p in msg_lower for p in _RATE_LIMIT_PHRASES)
        if code and code in _PERMANENT_SMTP_CODES:
            return True, False, code, message, None
        if code and code in _TEMPORARY_SMTP_CODES:
            wait = retry_after or (_DEFAULT_RATE_LIMIT_WAIT_SEC if rate_limited else None)
            return False, rate_limited, code, message, wait
        # Unknown SMTP code → treat as temporary
        return False, rate_limited, code, message, retry_after

    # Network / connection errors → always temporary
    if isinstance(exc, (
        smtplib.SMTPConnectError,
        smtplib.SMTPServerDisconnected,
        socket.timeout,
        TimeoutError,
        ConnectionRefusedError,
        ConnectionResetError,
        OSError,
    )):
        return False, False, None, message, retry_after

    # Catch-all for remaining smtplib exceptions → temporary
    if isinstance(exc, smtplib.SMTPException):
        return False, False, _parse_smtp_code(exc), message, retry_after

    return False, False, None, message, retry_after


# ---------------------------------------------------------------------------
# Backoff calculation
# ---------------------------------------------------------------------------

def calculate_backoff(
    attempt_number: int,
    override_sec: Optional[float] = None,
    jitter_ratio: float = _BACKOFF_JITTER_RATIO,
) -> float:
    """Return seconds to wait before the next retry (attempt_number is 1-based).

    If *override_sec* is given (e.g. from a Retry-After header), it is used as
    the base; otherwise the built-in table is consulted.
    Jitter of ±*jitter_ratio* is always applied to avoid thundering-herd.
    """
    if override_sec is not None:
        base = max(0.0, float(override_sec))
    else:
        idx = min(attempt_number, len(_BACKOFF_BASE_SECONDS) - 1)
        base = _BACKOFF_BASE_SECONDS[idx]

    jitter = base * jitter_ratio * (2.0 * random.random() - 1.0)
    return max(0.0, base + jitter)


# ---------------------------------------------------------------------------
# Message building
# ---------------------------------------------------------------------------

def _build_message(task: EmailTask, sender_email: str) -> Tuple[EmailMessage, str]:
    domain = sender_email.split('@')[-1] if '@' in sender_email else 'localhost'
    msg_id = email.utils.make_msgid(domain=domain)

    msg = EmailMessage()
    msg['From'] = sender_email
    msg['To'] = task.recipient
    msg['Subject'] = task.subject
    msg['Message-ID'] = msg_id
    msg['Date'] = email.utils.formatdate(localtime=True)
    msg.set_content(task.body)

    if task.attachment_path and os.path.isfile(task.attachment_path):
        with open(task.attachment_path, 'rb') as fh:
            msg.add_attachment(
                fh.read(),
                maintype='application',
                subtype='zip',
                filename=os.path.basename(task.attachment_path),
            )
    return msg, msg_id


# ---------------------------------------------------------------------------
# Low-level single send
# ---------------------------------------------------------------------------

def _send_one(task: EmailTask, cfg: SmtpConfig) -> SendAttemptResult:
    """Attempt to send *task* via *cfg*; always returns a SendAttemptResult."""
    t_start = time.monotonic()
    attempt_number = task.attempt_count + 1
    base = dict(
        task_id=task.task_id,
        attempt_number=attempt_number,
        provider_name=cfg.name,
    )

    try:
        msg, msg_id = _build_message(task, cfg.email)

        if cfg.use_ssl:
            smtp_cls = smtplib.SMTP_SSL
        else:
            smtp_cls = smtplib.SMTP

        with smtp_cls(cfg.host, cfg.port, timeout=_SMTP_TIMEOUT_SEC) as smtp:
            if not cfg.use_ssl:
                smtp.starttls()
            smtp.login(cfg.email, cfg.password)
            smtp.send_message(msg)

        duration_ms = int((time.monotonic() - t_start) * 1000)
        logger.info(
            '[delivery] attempt=%d task=%s recipient=%s provider=%s '
            'status=sent msg_id=%s duration=%dms',
            attempt_number, task.task_id[:8], task.recipient,
            cfg.name, msg_id, duration_ms,
        )
        return SendAttemptResult(
            **base, success=True, permanent=False,
            rate_limited=False, message_id=msg_id,
            error_code=None, error_message='', retry_after_sec=None,
            duration_ms=duration_ms,
        )

    except Exception as exc:
        duration_ms = int((time.monotonic() - t_start) * 1000)
        is_permanent, is_rate_limited, smtp_code, err_msg, retry_after = classify_error(exc)
        logger.warning(
            '[delivery] attempt=%d task=%s recipient=%s provider=%s '
            'status=%s code=%s rate_limited=%s retry_after=%s err=%.120s duration=%dms',
            attempt_number, task.task_id[:8], task.recipient, cfg.name,
            'permanent_fail' if is_permanent else 'temp_fail',
            smtp_code, is_rate_limited, retry_after, err_msg, duration_ms,
        )
        return SendAttemptResult(
            **base, success=False, permanent=is_permanent,
            rate_limited=is_rate_limited, error_code=smtp_code,
            error_message=err_msg, retry_after_sec=retry_after,
            message_id='', duration_ms=duration_ms,
        )


# ---------------------------------------------------------------------------
# SQLite-backed retry store
# ---------------------------------------------------------------------------

class EmailRetryStore:
    """Persists email tasks and send-attempt logs to an SQLite database.

    Provides duplicate-prevention: once a task is marked 'sent' it is never
    re-sent, even across process restarts.
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        self._lock = threading.Lock()
        self._init_db()

    @contextlib.contextmanager
    def _connect(self):
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.execute('PRAGMA journal_mode=WAL')
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_db(self):
        with self._connect() as conn:
            conn.executescript('''
                CREATE TABLE IF NOT EXISTS email_tasks (
                    task_id         TEXT PRIMARY KEY,
                    recipient       TEXT NOT NULL,
                    subject         TEXT NOT NULL,
                    body            TEXT NOT NULL,
                    attachment_path TEXT NOT NULL,
                    max_attempts    INTEGER NOT NULL DEFAULT 4,
                    attempt_count   INTEGER NOT NULL DEFAULT 0,
                    status          TEXT NOT NULL DEFAULT 'pending',
                    last_error      TEXT,
                    message_id      TEXT,
                    created_at      TEXT NOT NULL,
                    next_retry_at   REAL NOT NULL DEFAULT 0.0,
                    sent_at         TEXT,
                    provider_used   TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_et_status    ON email_tasks(status);
                CREATE INDEX IF NOT EXISTS idx_et_recipient ON email_tasks(recipient);

                CREATE TABLE IF NOT EXISTS send_attempts (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id         TEXT    NOT NULL,
                    attempt_number  INTEGER NOT NULL,
                    attempted_at    TEXT    NOT NULL,
                    success         INTEGER NOT NULL,
                    error_code      INTEGER,
                    error_message   TEXT,
                    message_id      TEXT,
                    duration_ms     INTEGER,
                    provider_name   TEXT,
                    FOREIGN KEY(task_id) REFERENCES email_tasks(task_id)
                );
                CREATE INDEX IF NOT EXISTS idx_sa_task_id ON send_attempts(task_id);
            ''')

    # --- write operations (always guarded by self._lock) ---

    def upsert_task(self, task: EmailTask) -> None:
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    '''
                    INSERT INTO email_tasks
                        (task_id, recipient, subject, body, attachment_path,
                         max_attempts, attempt_count, status, last_error,
                         message_id, created_at, next_retry_at, provider_used)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(task_id) DO UPDATE SET
                        attempt_count = excluded.attempt_count,
                        status        = excluded.status,
                        last_error    = excluded.last_error,
                        message_id    = excluded.message_id,
                        next_retry_at = excluded.next_retry_at,
                        provider_used = excluded.provider_used
                    ''',
                    (
                        task.task_id, task.recipient, task.subject, task.body,
                        task.attachment_path, task.max_attempts, task.attempt_count,
                        task.status, task.last_error or '', task.message_id or '',
                        task.created_at, task.next_retry_at, task.provider_used or '',
                    ),
                )

    def mark_sent(self, task_id: str, message_id: str, provider_name: str) -> None:
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    '''
                    UPDATE email_tasks
                    SET status='sent', message_id=?, sent_at=?, provider_used=?
                    WHERE task_id=?
                    ''',
                    (message_id, datetime.now(timezone.utc).isoformat(), provider_name, task_id),
                )

    def record_attempt(self, result: SendAttemptResult) -> None:
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    '''
                    INSERT INTO send_attempts
                        (task_id, attempt_number, attempted_at, success, error_code,
                         error_message, message_id, duration_ms, provider_name)
                    VALUES (?,?,?,?,?,?,?,?,?)
                    ''',
                    (
                        result.task_id, result.attempt_number,
                        datetime.now(timezone.utc).isoformat(),
                        int(result.success), result.error_code,
                        (result.error_message or '')[:500],
                        result.message_id or '', result.duration_ms,
                        result.provider_name,
                    ),
                )

    # --- read operations ---

    def is_sent(self, task_id: str) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM email_tasks WHERE task_id=? AND status='sent'",
                (task_id,),
            ).fetchone()
        return row is not None

    def get_pending_tasks(self) -> List[EmailTask]:
        """Return all tasks with status 'pending' or 'failed'."""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM email_tasks WHERE status IN ('pending','failed') "
                'ORDER BY next_retry_at ASC'
            ).fetchall()
        return [_row_to_task(r) for r in rows]

    def summary(self) -> Dict[str, int]:
        """Return a count of tasks grouped by status."""
        with self._connect() as conn:
            rows = conn.execute(
                'SELECT status, COUNT(*) FROM email_tasks GROUP BY status'
            ).fetchall()
        return {row[0]: row[1] for row in rows}


def _row_to_task(row: sqlite3.Row) -> EmailTask:
    return EmailTask(
        task_id=row['task_id'],
        recipient=row['recipient'],
        subject=row['subject'],
        body=row['body'],
        attachment_path=row['attachment_path'],
        max_attempts=row['max_attempts'],
        attempt_count=row['attempt_count'],
        status=row['status'],
        last_error=row['last_error'] or '',
        message_id=row['message_id'] or '',
        created_at=row['created_at'],
        next_retry_at=float(row['next_retry_at']),
        provider_used=row['provider_used'] or '',
    )


# ---------------------------------------------------------------------------
# Batch send with retry orchestrator
# ---------------------------------------------------------------------------

def send_batch_with_retry(
    tasks: Sequence[EmailTask],
    primary_cfg: SmtpConfig,
    fallback_cfg: Optional[SmtpConfig] = None,
    store: Optional[EmailRetryStore] = None,
    threads: int = 4,
    progress_callback: Optional[Callable[[int, int, str, str], None]] = None,
) -> BatchSendSummary:
    """Send all *tasks* using *primary_cfg*, retrying temporary failures.

    Algorithm
    ---------
    1. Persist all tasks to *store* (if provided); skip already-sent ones.
    2. Send the initial batch concurrently.
       - Success  → mark sent; never re-sent.
       - Permanent failure (5xx / auth) → mark permanent_fail; not retried.
       - Temporary failure (4xx / timeout / network) → schedule retry with
         exponential backoff (+ optional Retry-After override).
    3. Wait for the earliest retry time, then send only the failed subset.
       Switch to *fallback_cfg* after the second failed attempt if available.
    4. Repeat until the retry queue is empty or all tasks have exhausted
       their *max_attempts* budget.

    Parameters
    ----------
    tasks           : email jobs to send
    primary_cfg     : primary SMTP provider
    fallback_cfg    : optional secondary SMTP provider
    store           : SQLite persistence; pass None to run in-memory only
    threads         : parallel send workers
    progress_callback : (done, total, unit, message) callback
    """
    total = len(tasks)
    if total == 0:
        return BatchSendSummary(0, 0, 0, 0, 0, 0, 0)

    sent = permanent_failed = temp_failed = skipped_duplicate = 0
    total_attempts = retried = 0

    # --- Persist tasks and filter already-sent duplicates ---
    pending: List[EmailTask] = []
    for task in tasks:
        if store and store.is_sent(task.task_id):
            skipped_duplicate += 1
            logger.info(
                '[delivery] task=%s recipient=%s skipped=already_sent',
                task.task_id[:8], task.recipient,
            )
            continue
        if task.attempt_count >= task.max_attempts:
            # Бюджет попыток исчерпан (например, задача пришла из
            # get_pending_tasks после рестарта) — не отправляем повторно.
            task.status = 'failed'
            if not task.last_error:
                task.last_error = 'exhausted max_attempts before batch start'
            if store:
                store.upsert_task(task)
            temp_failed += 1
            logger.warning(
                '[delivery] task=%s recipient=%s exhausted max_attempts=%d '
                'before batch start — skipped',
                task.task_id[:8], task.recipient, task.max_attempts,
            )
            continue
        if store:
            store.upsert_task(task)
        pending.append(task)

    logger.info(
        '[delivery] batch start: total=%d pending=%d skipped_duplicate=%d threads=%d',
        total, len(pending), skipped_duplicate, threads,
    )

    done_count = skipped_duplicate

    def _attempt(task: EmailTask, cfg: SmtpConfig) -> Tuple[EmailTask, SendAttemptResult]:
        result = _send_one(task, cfg)
        if store:
            store.record_attempt(result)
        return task, result

    def _process_result(
        task: EmailTask,
        result: SendAttemptResult,
        retry_list: List[EmailTask],
    ) -> None:
        """Update counters and routing based on a single result."""
        nonlocal sent, permanent_failed, temp_failed
        task.attempt_count += 1
        if result.success:
            task.status = 'sent'
            task.message_id = result.message_id
            task.provider_used = result.provider_name
            if store:
                store.mark_sent(task.task_id, result.message_id, result.provider_name)
            sent += 1
        elif result.permanent:
            task.status = 'permanent_fail'
            task.last_error = result.error_message
            if store:
                store.upsert_task(task)
            permanent_failed += 1
            logger.error(
                '[delivery] task=%s recipient=%s PERMANENT failure: %s',
                task.task_id[:8], task.recipient, result.error_message[:100],
            )
        else:
            # Temporary failure
            backoff = calculate_backoff(task.attempt_count, result.retry_after_sec)
            task.next_retry_at = time.time() + backoff
            task.status = 'failed'
            task.last_error = result.error_message
            if store:
                store.upsert_task(task)
            if task.attempt_count < task.max_attempts:
                retry_list.append(task)
                logger.info(
                    '[delivery] task=%s recipient=%s temp_fail attempt=%d/%d '
                    'retry_in=%.1fs',
                    task.task_id[:8], task.recipient,
                    task.attempt_count, task.max_attempts, backoff,
                )
            else:
                temp_failed += 1
                logger.warning(
                    '[delivery] task=%s recipient=%s exhausted max_attempts=%d '
                    'last_error=%s',
                    task.task_id[:8], task.recipient,
                    task.max_attempts, task.last_error[:80],
                )

    # --- Initial send pass ---
    retry_queue: List[EmailTask] = []
    worker_count = max(1, min(threads, len(pending)))
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = {executor.submit(_attempt, t, primary_cfg): t for t in pending}
        for future in as_completed(futures):
            total_attempts += 1
            try:
                task, result = future.result()
                _process_result(task, result, retry_queue)
            except Exception as exc:
                task = futures[future]
                logger.error('[delivery] unexpected error task=%s: %s', task.task_id[:8], exc)
                task.last_error = str(exc)
                task.attempt_count += 1
                task.status = 'failed'
                if store:
                    store.upsert_task(task)
                temp_failed += 1
            done_count += 1
            if progress_callback:
                progress_callback(done_count, total, 'email', 'Отправка email')

    # --- Retry passes ---
    retry_round = 0
    while retry_queue:
        retry_round += 1
        now = time.time()
        wait_sec = max(0.0, min(t.next_retry_at for t in retry_queue) - now)
        if wait_sec > 0:
            logger.info(
                '[delivery] retry round %d: %d task(s) queued — waiting %.1fs',
                retry_round, len(retry_queue), wait_sec,
            )
            time.sleep(wait_sec)

        ready = [t for t in retry_queue if t.next_retry_at <= time.time()]
        retry_queue = [t for t in retry_queue if t not in ready]

        logger.info(
            '[delivery] retry round %d: sending %d task(s)', retry_round, len(ready),
        )

        next_retry: List[EmailTask] = []
        worker_count_r = max(1, min(threads, len(ready)))
        with ThreadPoolExecutor(max_workers=worker_count_r) as executor:
            futures = {}
            for task in ready:
                # Switch to fallback provider after ≥2 prior failed attempts
                cfg = (
                    fallback_cfg
                    if (fallback_cfg and task.attempt_count >= 2)
                    else primary_cfg
                )
                futures[executor.submit(_attempt, task, cfg)] = task

            for future in as_completed(futures):
                total_attempts += 1
                retried += 1
                try:
                    task, result = future.result()
                    _process_result(task, result, next_retry)
                except Exception as exc:
                    task = futures[future]
                    logger.error(
                        '[delivery] retry unexpected error task=%s: %s',
                        task.task_id[:8], exc,
                    )
                    task.last_error = str(exc)
                    task.attempt_count += 1
                    task.status = 'failed'
                    if store:
                        store.upsert_task(task)
                    temp_failed += 1

        retry_queue = next_retry

    summary = BatchSendSummary(
        total=total,
        sent=sent,
        permanent_failed=permanent_failed,
        temp_failed=temp_failed,
        skipped_duplicate=skipped_duplicate,
        total_attempts=total_attempts,
        retried=retried,
    )
    logger.info(
        '[delivery] batch complete: total=%d sent=%d permanent_failed=%d '
        'temp_failed=%d skipped_duplicate=%d total_attempts=%d retried=%d',
        summary.total, summary.sent, summary.permanent_failed,
        summary.temp_failed, summary.skipped_duplicate,
        summary.total_attempts, summary.retried,
    )
    return summary
