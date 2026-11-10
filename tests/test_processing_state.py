import os
import sqlite3
import threading

import pytest

from processing_state import ProcessingState


@pytest.fixture
def state(tmp_path):
    return ProcessingState(str(tmp_path / 'state.sqlite'))


def test_add_record_and_has(state):
    state.add_record('h1', 'qr1', '/p/1.jpg', 'processed', '')
    assert state.has_file_hash('h1') is True
    assert state.has_file_hash('nope') is False
    assert state.has_qr_for_hash('qr1', 'h1') is True
    assert state.has_qr_for_hash('qr1', 'other') is False
    assert state.has_qr_for_hash('other', 'h1') is False


def test_add_records_batch(state):
    state.add_records([
        ('h1', 'q1', 'p1', 'processed', ''),
        ('h2', 'q2', 'p2', 'processed', ''),
    ])
    assert state.has_file_hash('h1') and state.has_file_hash('h2')
    assert state.has_qr_for_hash('q1', 'h1')
    assert state.has_qr_for_hash('q2', 'h2')


def test_duplicate_hash_add_keeps_both_rows(state):
    state.add_record('h1', 'q1', 'p1', 'processed', '')
    state.add_record('h1', 'q1', 'p1', 'processed', '')
    # has_* остаются согласованными
    assert state.has_file_hash('h1') is True
    assert state.has_qr_for_hash('q1', 'h1') is True
    assert state.get_existing_file_hashes(['h1']) == {'h1'}
    # но UNIQUE-ограничения нет: дубликат реально вставлен второй строкой
    assert state.stats() == {'processed': 2}


def test_get_existing_file_hashes_large(state):
    hashes = ['h%04d' % i for i in range(2000)]
    state.add_records([(h, 'q', 'p', 'processed', '') for h in hashes])
    existing = state.get_existing_file_hashes(hashes)
    assert existing == set(hashes)
    assert len(existing) == 2000


def test_get_existing_file_hashes_filters_none_and_empty(state):
    state.add_record('h0000', 'q', 'p', 'processed', '')
    assert state.get_existing_file_hashes([None, '', 'h0000']) == {'h0000'}
    assert state.get_existing_file_hashes([None, '']) == set()
    assert state.get_existing_file_hashes([]) == set()
    assert state.has_file_hash('') is False
    assert state.has_file_hash(None) is False


def test_get_existing_qr_hash_pairs_chunked(state):
    pairs = [('q%04d' % i, 'h%04d' % i) for i in range(1000)]
    state.add_records([(fh, qr, 'p', 'processed', '') for qr, fh in pairs])
    existing = state.get_existing_qr_hash_pairs(pairs)
    assert existing == set(pairs)


def test_get_existing_qr_hash_pairs_filters_none_and_empty(state):
    state.add_record('h0000', 'q0000', 'p', 'processed', '')
    found = state.get_existing_qr_hash_pairs([
        (None, 'h0000'), ('q0000', None), ('', ''), ('q0000', 'h0000'),
    ])
    assert found == {('q0000', 'h0000')}
    assert state.get_existing_qr_hash_pairs([]) == set()


def test_stats_by_status(state):
    state.add_records([
        ('h1', 'q1', 'p', 'processed', ''),
        ('h2', '', 'p', 'noqrcode', ''),
        ('h3', '', 'p', 'duplicate_hash', ''),
    ])
    assert state.stats() == {'processed': 1, 'noqrcode': 1, 'duplicate_hash': 1}


def test_stats_empty(tmp_path):
    assert ProcessingState(str(tmp_path / 'empty.sqlite')).stats() == {}


def test_wal_mode(tmp_path):
    db = str(tmp_path / 'w.sqlite')
    state = ProcessingState(db)
    state.add_record('h', 'q', 'p', 'processed', '')
    conn = sqlite3.connect(db)
    try:
        mode = conn.execute('PRAGMA journal_mode').fetchone()[0].lower()
    finally:
        conn.close()
    assert mode == 'wal'


def test_db_path_creates_nested_directory(tmp_path):
    db = str(tmp_path / 'nested' / 'dir' / 'db.sqlite')
    state = ProcessingState(db)
    state.add_record('h', 'q', 'p', 'processed', '')
    assert state.has_file_hash('h')
    assert os.path.exists(db)


def test_concurrent_writes(tmp_path):
    state = ProcessingState(str(tmp_path / 'c.sqlite'))
    errors = []
    n_threads, per_thread = 8, 25

    def worker(idx):
        try:
            for j in range(per_thread):
                h = 'h%d_%d' % (idx, j)
                state.add_record(h, 'q' + h, 'p', 'processed', '')
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    hashes = ['h%d_%d' % (i, j) for i in range(n_threads) for j in range(per_thread)]
    assert len(state.get_existing_file_hashes(hashes)) == n_threads * per_thread


def test_concurrent_reads_and_writes(tmp_path):
    state = ProcessingState(str(tmp_path / 'rw.sqlite'))
    errors = []
    stop = threading.Event()
    hashes = ['r%d' % i for i in range(200)]

    def reader():
        try:
            while not stop.is_set():
                state.get_existing_file_hashes(hashes)
                state.get_existing_qr_hash_pairs(list(zip(hashes, hashes)))
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    def writer(idx):
        try:
            for j in range(20):
                h = 'r%d_%d' % (idx, j)
                state.add_record(h, h, 'p', 'processed', '')
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    reader_t = threading.Thread(target=reader)
    reader_t.start()
    writers = [threading.Thread(target=writer, args=(i,)) for i in range(4)]
    for t in writers:
        t.start()
    for t in writers:
        t.join()
    stop.set()
    reader_t.join()

    assert not errors


@pytest.mark.xfail(
    strict=True,
    reason='баг: processing_state.py:19 os.makedirs("") падает на db_path без каталога '
           '(FileNotFoundError) вместо создания рядом',
)
def test_db_path_without_directory(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    state = ProcessingState('x.sqlite')
    state.add_record('h', 'q', 'p', 'processed', '')
    assert os.path.exists(os.path.join(str(tmp_path), 'x.sqlite'))