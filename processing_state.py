import os
import sqlite3
import threading
from datetime import datetime


class ProcessingState:
    def __init__(self, db_path):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._lock = threading.Lock()
        self._init_db()

    def _connect(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute('PRAGMA journal_mode=WAL;')
        return conn

    def _init_db(self):
        with self._connect() as conn:
            conn.execute(
                '''
                CREATE TABLE IF NOT EXISTS processed_files (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    file_hash TEXT,
                    qr_value TEXT,
                    file_path TEXT,
                    status TEXT,
                    output_path TEXT,
                    created_at TEXT
                )
                '''
            )
            conn.execute('CREATE INDEX IF NOT EXISTS idx_file_hash ON processed_files(file_hash)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_qr_value ON processed_files(qr_value)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_file_path ON processed_files(file_path)')

    def has_file_hash(self, file_hash):
        with self._connect() as conn:
            row = conn.execute(
                'SELECT 1 FROM processed_files WHERE file_hash = ? LIMIT 1',
                (file_hash,),
            ).fetchone()
            return row is not None

    def has_qr_for_hash(self, qr_value, file_hash):
        with self._connect() as conn:
            row = conn.execute(
                'SELECT 1 FROM processed_files WHERE qr_value = ? AND file_hash = ? LIMIT 1',
                (qr_value, file_hash),
            ).fetchone()
            return row is not None

    def add_record(self, file_hash, qr_value, file_path, status, output_path=''):
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    '''
                    INSERT INTO processed_files(file_hash, qr_value, file_path, status, output_path, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ''',
                    (file_hash, qr_value, file_path, status, output_path, datetime.utcnow().isoformat()),
                )

    def stats(self):
        with self._connect() as conn:
            rows = conn.execute(
                'SELECT status, COUNT(*) FROM processed_files GROUP BY status'
            ).fetchall()
            return {status: count for status, count in rows}
