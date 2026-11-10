"""Тесты модуля watch_mode.py.

Покрытие: FolderWatcherService (конструктор/валидация пути, start/stop),
ScanEventHandler (фильтрация расширений, debounce), логика стабильности
файла (partial write), requeue_unstable, process_existing_on_start,
контракт on_stats, worker-цикл.

Все интервалы мокаются или реальные ожидания отсутствуют (stable_checks=1,
debounce=0). Никаких реальных sleep. Фоновые потоки всегда гасятся
stop()+join и проверяются is_alive() в конце.
"""

import os
import threading
import time
from collections import defaultdict

import pytest

import pipeline
import watch_mode
from watch_mode import FolderWatcherService, ScanEventHandler
from watchdog.events import (
    DirCreatedEvent,
    FileCreatedEvent,
    FileModifiedEvent,
    FileMovedEvent,
    FileSystemEvent,
)

REQUIRED_STATS_KEYS = {
    'incoming', 'processed', 'duplicates', 'unrecognized',
    'ready', 'stable', 'unstable', 'requeued', 'missing', 'queue_size',
    'sample_ready', 'sample_stable', 'sample_unstable', 'batch_skipped',
}


class _Clock:
    def __init__(self, start=1000.0):
        self.now = start

    def time(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def make_service(tmp_path, **overrides):
    csv_path = tmp_path / 'data.csv'
    csv_path.write_text('код,ФИО\n', encoding='utf-8')
    out = tmp_path / 'out'
    out.mkdir()
    params = dict(
        input_folder=str(tmp_path / 'input'),
        csv_path=str(csv_path),
        name_fields=['ФИО'],
        output_folder=str(out),
        code_field='код',
        move_mode='copy',
        threads=2,
        state=None,
        debounce_sec=0.0,
        stable_checks=1,
        stable_interval=1.0,
        poll_interval=0.05,
        process_existing_on_start=False,
        requeue_unstable=True,
        detailed_stats=False,
        sample_limit=5,
        on_stats=None,
    )
    params.update(overrides)
    return FolderWatcherService(**params)


def _run_worker(svc, on_stats, timeout=10.0):
    """Гоняет worker-цикл в потоке, останавливая его после первого on_stats."""
    done = threading.Event()

    def wrapped(stats):
        on_stats(stats)
        svc.stop_event.set()
        svc.queue_event.set()
        done.set()

    svc.on_stats = wrapped
    t = threading.Thread(target=svc._worker_loop, daemon=True)
    t.start()
    try:
        assert done.wait(timeout=timeout)
    finally:
        svc.stop_event.set()
        svc.queue_event.set()
        t.join(timeout=10.0)
    assert not t.is_alive()
    return svc


def _noop_process(stats=None, result=None):
    def fake(*args, **kwargs):
        if stats is not None:
            stats['called'] = True
            stats['args'] = args
            stats['kwargs'] = kwargs
        return result if result is not None else \
            {'incoming': 0, 'processed': 0, 'duplicates': 0, 'unrecognized': 0}
    return fake


# ---------------------------------------------------------------------------
# ScanEventHandler
# ---------------------------------------------------------------------------

def test_handler_filters_non_image_extensions():
    qd = defaultdict(float)
    lock = threading.Lock()
    wake = threading.Event()
    h = ScanEventHandler(qd, lock, 2.0, wake)

    h.on_created(FileCreatedEvent('/tmp/x.txt'))
    h.on_modified(FileModifiedEvent('/tmp/y.csv'))
    h.on_moved(FileMovedEvent('/tmp/z.gif', '/tmp/z2.gif'))
    assert len(qd) == 0


def test_handler_tracks_image_and_wakes():
    qd = defaultdict(float)
    lock = threading.Lock()
    wake = threading.Event()
    h = ScanEventHandler(qd, lock, 2.0, wake)

    before = time.time()
    h.on_created(FileCreatedEvent('/tmp/x.jpg'))
    assert '/tmp/x.jpg' in qd
    assert qd['/tmp/x.jpg'] >= before + 2.0 - 0.5  # debounce
    assert wake.is_set()


def test_handler_ignores_directories():
    qd = defaultdict(float)
    lock = threading.Lock()
    wake = threading.Event()
    h = ScanEventHandler(qd, lock, 2.0, wake)

    h.on_created(DirCreatedEvent('/tmp/dir'))
    assert len(qd) == 0


def test_handler_on_moved_uses_dest_path():
    qd = defaultdict(float)
    lock = threading.Lock()
    wake = threading.Event()
    h = ScanEventHandler(qd, lock, 2.0, wake)

    h.on_moved(FileMovedEvent('/tmp/a.jpg', '/tmp/b.jpg'))
    assert '/tmp/b.jpg' in qd
    assert '/tmp/a.jpg' not in qd


def test_handler_on_modified_tracks_image():
    qd = defaultdict(float)
    lock = threading.Lock()
    wake = threading.Event()
    h = ScanEventHandler(qd, lock, 2.0, wake)

    h.on_modified(FileModifiedEvent('/tmp/c.png'))
    assert '/tmp/c.png' in qd


# ---------------------------------------------------------------------------
# FolderWatcherService: нормализация пути, sample, prime
# ---------------------------------------------------------------------------

def test_normalize_watch_path(tmp_path):
    # существующий файл -> родительская папка
    f = tmp_path / 'f.jpg'
    f.write_bytes(b'x')
    assert FolderWatcherService._normalize_watch_path(str(f)) == str(tmp_path)

    # существующая папка -> как есть
    d = tmp_path / 'd'
    d.mkdir()
    assert FolderWatcherService._normalize_watch_path(str(d)) == str(d)

    # несуществующий путь с расширением -> родительская папка
    assert FolderWatcherService._normalize_watch_path(str(tmp_path / 'nope.jpg')) == str(tmp_path)

    # несуществующий путь без расширения -> как есть
    assert FolderWatcherService._normalize_watch_path(str(tmp_path / 'nope')) == str(tmp_path / 'nope')


def test_sample_paths(tmp_path):
    svc = make_service(tmp_path, sample_limit=2)
    assert svc._sample_paths(['/x/a.jpg', '/x/b.png', '/x/c.jpg']) == ['a.jpg', 'b.png']
    assert svc._sample_paths([]) == []
    svc.sample_limit = 0
    assert svc._sample_paths(['/x/a.jpg']) == []


def test_prime_existing_files(tmp_path):
    inp = tmp_path / 'input'
    inp.mkdir()
    (inp / 'a.jpg').write_bytes(b'1')
    (inp / 'b.png').write_bytes(b'2')
    (inp / 'c.txt').write_bytes(b'3')
    sub = inp / 'sub'
    sub.mkdir()
    (sub / 'd.jpg').write_bytes(b'4')

    svc = make_service(tmp_path)
    seeded = svc._prime_existing_files()
    assert seeded == 2
    assert set(svc.queue_dict) == {str(inp / 'a.jpg'), str(inp / 'b.png')}
    assert svc.queue_event.is_set()


# ---------------------------------------------------------------------------
# Логика стабильности (_is_stable, partial write)
# ---------------------------------------------------------------------------

def test_is_stable_single_check(tmp_path):
    p = tmp_path / 'a.jpg'
    p.write_bytes(b'X')
    svc = make_service(tmp_path, stable_checks=1)
    assert svc._is_stable(str(p)) is True
    assert svc._is_stable(str(tmp_path / 'gone.jpg')) is False


def test_is_stable_requires_consecutive_unchanged_checks(tmp_path, monkeypatch):
    p = tmp_path / 'a.jpg'
    p.write_bytes(b'AAAA')
    clock = _Clock()
    monkeypatch.setattr(watch_mode.time, 'time', clock.time)

    svc = make_service(tmp_path, stable_checks=2, stable_interval=1.0)

    # первый взгляд: только 1 hit -> ещё не стабилен
    assert svc._is_stable(str(p)) is False
    # файл меняется между проверками -> сигнатура сбрасывается
    p.write_bytes(b'AAAA+more')
    clock.advance(1.0)
    assert svc._is_stable(str(p)) is False
    # без изменений следующие проверки -> стабилен
    clock.advance(1.0)
    assert svc._is_stable(str(p)) is True


# ---------------------------------------------------------------------------
# drain / timeout
# ---------------------------------------------------------------------------

def test_drain_and_next_timeout(tmp_path, monkeypatch):
    clock = _Clock(50.0)
    monkeypatch.setattr(watch_mode.time, 'time', clock.time)

    svc = make_service(tmp_path, poll_interval=0.5)
    svc.queue_dict['/future'] = 100.0
    svc.queue_dict['/ready'] = 25.0

    assert svc._drain_ready_files() == ['/ready']
    assert '/ready' not in svc.queue_dict
    # осталась /future (100 > 50)
    assert svc.queue_dict == {'/future': 100.0}
    # next_ready_at - now = 50 -> ограничено poll_interval 0.5
    assert svc._next_wait_timeout() == 0.5


# ---------------------------------------------------------------------------
# Worker-цикл: обработка, on_stats-контракт, requeue, расширения
# ---------------------------------------------------------------------------

def test_worker_processes_stable_file_and_reports_stats(tmp_path, monkeypatch):
    img = tmp_path / 'input' / 'a.jpg'
    img.parent.mkdir()
    img.write_bytes(b'x' * 10)

    proc = {}
    monkeypatch.setattr(
        watch_mode, 'process_watch_batch',
        _noop_process(proc, result={'incoming': 1, 'processed': 1,
                                    'duplicates': 0, 'unrecognized': 0}),
    )

    svc = make_service(tmp_path)
    svc.queue_dict[str(img)] = 0.0
    svc.queue_event.set()

    calls = []
    _run_worker(svc, calls.append)

    assert len(calls) == 1
    stats = calls[0]
    assert set(stats.keys()) == REQUIRED_STATS_KEYS
    assert stats['batch_skipped'] is False
    assert stats['stable'] == 1
    assert stats['ready'] == 1
    assert stats['incoming'] == 1
    # process_watch_batch вызван со стабильными файлами и параметрами
    assert proc['called'] is True
    assert proc['args'][0] == [str(img)]
    assert proc['kwargs']['csv_path'] == svc.csv_path
    assert proc['kwargs']['move_mode'] == 'copy'
    assert proc['kwargs']['state'] is None


def test_worker_missing_file_reports_skipped(tmp_path, monkeypatch):
    spy = {}
    monkeypatch.setattr(watch_mode, 'process_watch_batch', _noop_process(spy))

    svc = make_service(tmp_path)
    svc.queue_dict[str(tmp_path / 'missing.jpg')] = 0.0
    svc.queue_event.set()

    calls = []
    _run_worker(svc, calls.append)

    assert spy.get('called') is not True  # process_watch_batch не вызывался
    assert len(calls) == 1
    stats = calls[0]
    assert set(stats.keys()) == REQUIRED_STATS_KEYS
    assert stats['batch_skipped'] is True
    assert stats['missing'] == 1
    assert stats['ready'] == 1
    assert stats['stable'] == 0
    assert stats['sample_unstable'] == []


def test_worker_requeue_unstable_false_drops(tmp_path, monkeypatch):
    img = tmp_path / 'input' / 'a.jpg'
    img.parent.mkdir()
    img.write_bytes(b'x')

    spy = {}
    monkeypatch.setattr(watch_mode, 'process_watch_batch', _noop_process(spy))

    svc = make_service(tmp_path, stable_checks=2, requeue_unstable=False)
    svc.queue_dict[str(img)] = 0.0
    svc.queue_event.set()

    calls = []
    _run_worker(svc, calls.append)

    assert spy.get('called') is not True
    stats = calls[0]
    assert stats['batch_skipped'] is True
    assert stats['unstable'] == 1
    assert stats['requeued'] == 0
    assert stats['queue_size'] == 0


def test_worker_requeue_unstable_true_keeps(tmp_path, monkeypatch):
    img = tmp_path / 'input' / 'a.jpg'
    img.parent.mkdir()
    img.write_bytes(b'x')

    spy = {}
    monkeypatch.setattr(watch_mode, 'process_watch_batch', _noop_process(spy))

    svc = make_service(tmp_path, stable_checks=2, requeue_unstable=True)
    svc.queue_dict[str(img)] = 0.0
    svc.queue_event.set()

    calls = []
    _run_worker(svc, calls.append)

    assert spy.get('called') is not True
    stats = calls[0]
    assert stats['batch_skipped'] is True
    assert stats['unstable'] == 1
    assert stats['requeued'] == 1
    # файл остался в очереди для повторной проверки
    assert svc._queue_size() == 1
    assert stats['sample_unstable'] == ['a.jpg']


# ---------------------------------------------------------------------------
# start/stop жизненный цикл (реальный Observer, но очень быстро)
# ---------------------------------------------------------------------------

def test_start_stop_lifecycle(tmp_path, monkeypatch):
    monkeypatch.setattr(
        watch_mode, 'process_watch_batch',
        lambda **k: {'incoming': 0, 'processed': 0, 'duplicates': 0, 'unrecognized': 0},
    )
    svc = make_service(tmp_path)
    svc.start()
    try:
        assert svc.observer is not None
        assert svc.worker_thread is not None
        assert svc.worker_thread.is_alive()
    finally:
        svc.stop()
    assert svc.observer is None
    assert not svc.worker_thread.is_alive()


def test_start_is_idempotent(tmp_path, monkeypatch):
    monkeypatch.setattr(
        watch_mode, 'process_watch_batch',
        lambda **k: {'incoming': 0, 'processed': 0, 'duplicates': 0, 'unrecognized': 0},
    )
    svc = make_service(tmp_path)
    first_observer = None
    try:
        svc.start()
        first_observer = svc.observer
        svc.start()  # повторный вызов не должен пересоздавать
        assert svc.observer is first_observer
    finally:
        svc.stop()
    assert svc.observer is None


# ---------------------------------------------------------------------------
# Найденный баг: исключение on_stats в ветке batch_skipped роняет worker-поток
# ---------------------------------------------------------------------------

@pytest.mark.xfail(
    strict=True,
    reason='баг: on_stats вызывается вне try в ветке batch_skipped '
           '(watch_mode.py:245-261); исключение каллбека роняет worker-поток, '
           'тогда как в ветке stable (строка 309) оно перехватывается',
)
def test_worker_on_stats_exception_is_contained(tmp_path):
    svc = make_service(tmp_path)
    svc.queue_dict[str(tmp_path / 'missing.jpg')] = 0.0
    svc.queue_event.set()

    calls = []

    def on_stats(stats):
        calls.append(stats)
        svc.stop_event.set()
        svc.queue_event.set()
        raise RuntimeError('каллбек статистики упал')

    svc.on_stats = on_stats
    # корректное поведение: исключение глотается, цикл завершается по stop_event
    svc._worker_loop()
    assert calls is not None