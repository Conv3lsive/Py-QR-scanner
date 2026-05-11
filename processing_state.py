import os
import sqlite3
import threading
from datetime import datetime


SQLITE_MAX_VARIABLES = 900


def _chunked(items, chunk_size):
    for index in range(0, len(items), chunk_size):
        yield items[index:index + chunk_size]


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
        return file_hash in self.get_existing_file_hashes([file_hash])

    def has_qr_for_hash(self, qr_value, file_hash):
        return (qr_value, file_hash) in self.get_existing_qr_hash_pairs([(qr_value, file_hash)])

    def get_existing_file_hashes(self, file_hashes):
        unique_hashes = [file_hash for file_hash in dict.fromkeys(file_hashes) if file_hash]
        if not unique_hashes:
            return set()

        existing_hashes = set()
        with self._connect() as conn:
            for chunk in _chunked(unique_hashes, SQLITE_MAX_VARIABLES):
                placeholders = ', '.join('?' for _ in chunk)
                rows = conn.execute(
                    f'SELECT DISTINCT file_hash FROM processed_files WHERE file_hash IN ({placeholders})',
                    tuple(chunk),
                ).fetchall()
                existing_hashes.update(row[0] for row in rows if row and row[0])

        return existing_hashes

    def get_existing_qr_hash_pairs(self, qr_hash_pairs):
        normalized_pairs = [(qr_value, file_hash) for qr_value, file_hash in qr_hash_pairs if qr_value and file_hash]
        if not normalized_pairs:
            return set()

        existing_pairs = set()
        max_pairs_per_query = max(1, SQLITE_MAX_VARIABLES // 2)

        with self._connect() as conn:
            for chunk in _chunked(normalized_pairs, max_pairs_per_query):
                conditions = ' OR '.join('(qr_value = ? AND file_hash = ?)' for _ in chunk)
                params = [value for pair in chunk for value in pair]
                rows = conn.execute(
                    f'SELECT DISTINCT qr_value, file_hash FROM processed_files WHERE {conditions}',
                    params,
                ).fetchall()
                existing_pairs.update((qr_value, file_hash) for qr_value, file_hash in rows if qr_value and file_hash)

        return existing_pairs

    def add_record(self, file_hash, qr_value, file_path, status, output_path=''):
        self.add_records([(file_hash, qr_value, file_path, status, output_path)])

    def add_records(self, records):
        prepared_records = [
            (file_hash, qr_value, file_path, status, output_path, datetime.utcnow().isoformat())
            for file_hash, qr_value, file_path, status, output_path in records
        ]
        if not prepared_records:
            return

        with self._lock:
            with self._connect() as conn:
                conn.executemany(
                    '''
                    INSERT INTO processed_files(file_hash, qr_value, file_path, status, output_path, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ''',
                    prepared_records,
                )

    def stats(self):
        with self._connect() as conn:
            rows = conn.execute(
                'SELECT status, COUNT(*) FROM processed_files GROUP BY status'
            ).fetchall()
            return {status: count for status, count in rows}
