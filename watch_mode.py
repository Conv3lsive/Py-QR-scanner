import logging
import os
import threading
import time
from collections import defaultdict

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from pipeline import WATCH_EXTENSIONS, process_watch_batch


logger = logging.getLogger(__name__)


class ScanEventHandler(FileSystemEventHandler):
    def __init__(self, queue_dict, lock, debounce_sec, wake_event):
        super().__init__()
        self.queue_dict = queue_dict
        self.lock = lock
        self.debounce_sec = debounce_sec
        self.wake_event = wake_event

    def _track(self, file_path):
        ext = os.path.splitext(file_path)[1].lower()
        if ext not in WATCH_EXTENSIONS:
            return
        with self.lock:
            self.queue_dict[file_path] = time.time() + self.debounce_sec
        self.wake_event.set()

    def on_created(self, event):
        if not event.is_directory:
            self._track(event.src_path)

    def on_modified(self, event):
        if not event.is_directory:
            self._track(event.src_path)

    def on_moved(self, event):
        if not event.is_directory:
            self._track(event.dest_path)


class FolderWatcherService:
    def __init__(self, input_folder, csv_path, name_fields, output_folder, code_field='код',
                 move_mode='copy', threads=4, state=None, debounce_sec=2.0,
                 csv_delimiter='auto',
                 stable_checks=2, stable_interval=1.0, poll_interval=0.5,
                 process_existing_on_start=False, requeue_unstable=True,
                 detailed_stats=False, sample_limit=5, on_stats=None):
        self.input_folder = self._normalize_watch_path(input_folder)
        self.csv_path = csv_path
        self.name_fields = name_fields
        self.output_folder = output_folder
        self.code_field = code_field
        self.csv_delimiter = csv_delimiter
        self.move_mode = move_mode
        self.threads = threads
        self.state = state
        self.debounce_sec = debounce_sec
        self.stable_checks = stable_checks
        self.stable_interval = stable_interval
        self.poll_interval = poll_interval
        self.process_existing_on_start = process_existing_on_start
        self.requeue_unstable = requeue_unstable
        self.detailed_stats = detailed_stats
        self.sample_limit = sample_limit
        self.on_stats = on_stats

        self.queue_dict = defaultdict(float)
        self.queue_lock = threading.Lock()
        self.queue_event = threading.Event()
        self.stop_event = threading.Event()
        self.worker_thread = None
        self.observer = None
        self._stability_state = {}

    def _queue_size(self):
        with self.queue_lock:
            return len(self.queue_dict)

    def _requeue_files(self, file_paths, delay_sec=None):
        ready_at = time.time() + max(0.0, self.stable_interval if delay_sec is None else delay_sec)
        with self.queue_lock:
            for path in file_paths:
                self.queue_dict[path] = ready_at
        self.queue_event.set()

    def _prime_existing_files(self):
        seeded = 0
        ready_at = time.time()
        with self.queue_lock:
            for name in os.listdir(self.input_folder):
                path = os.path.join(self.input_folder, name)
                if not os.path.isfile(path):
                    continue
                ext = os.path.splitext(path)[1].lower()
                if ext not in WATCH_EXTENSIONS:
                    continue
                self.queue_dict[path] = ready_at
                seeded += 1
        if seeded:
            self.queue_event.set()
        return seeded

    def _sample_paths(self, file_paths):
        if not file_paths:
            return []
        if self.sample_limit <= 0:
            return []
        return [os.path.basename(path) for path in file_paths[:self.sample_limit]]

    @staticmethod
    def _normalize_watch_path(input_path):
        normalized = os.path.abspath(os.path.expanduser(input_path))

        if os.path.isfile(normalized):
            parent = os.path.dirname(normalized)
            logger.warning(
                'Watcher получил путь к файлу (%s). Будет отслеживаться родительская папка: %s',
                normalized,
                parent,
            )
            return parent

        if not os.path.exists(normalized) and os.path.splitext(os.path.basename(normalized))[1]:
            parent = os.path.dirname(normalized)
            logger.warning(
                'Watcher получил путь, похожий на файл (%s). Будет отслеживаться родительская папка: %s',
                normalized,
                parent,
            )
            return parent

        return normalized

    def _is_stable(self, file_path):
        if not os.path.exists(file_path):
            self._stability_state.pop(file_path, None)
            return False

        try:
            stat = os.stat(file_path)
            current = (
                stat.st_size,
                getattr(stat, 'st_mtime_ns', int(stat.st_mtime * 1_000_000_000)),
            )
        except FileNotFoundError:
            self._stability_state.pop(file_path, None)
            return False

        required_hits = max(1, self.stable_checks)
        now = time.time()
        state = self._stability_state.get(file_path)

        if state is None:
            self._stability_state[file_path] = {
                'signature': current,
                'hits': 1,
                'last_checked': now,
            }
            return required_hits <= 1

        if now - state['last_checked'] < self.stable_interval:
            return False

        if current == state['signature']:
            state['hits'] += 1
        else:
            state['signature'] = current
            state['hits'] = 1
        state['last_checked'] = now

        if state['hits'] >= required_hits:
            self._stability_state.pop(file_path, None)
            return True

        return False

    def _drain_ready_files(self):
        ready = []
        now = time.time()
        with self.queue_lock:
            for file_path, ts in list(self.queue_dict.items()):
                if ts <= now:
                    ready.append(file_path)
                    self.queue_dict.pop(file_path, None)
        return ready

    def _next_wait_timeout(self):
        with self.queue_lock:
            if not self.queue_dict:
                return self.poll_interval
            next_ready_at = min(self.queue_dict.values())

        delay = max(0.0, next_ready_at - time.time())
        if self.poll_interval <= 0:
            return delay
        return min(self.poll_interval, delay) if delay > 0 else 0.0

    def _worker_loop(self):
        logger.info('Watcher worker started')
        while not self.stop_event.is_set():
            ready_files = self._drain_ready_files()
            if not ready_files:
                self.queue_event.clear()
                ready_files = self._drain_ready_files()
                if ready_files:
                    continue
                self.queue_event.wait(timeout=self._next_wait_timeout())
                continue

            stable_files = []
            unstable_files = []
            missing_files = []

            for path in ready_files:
                if not os.path.exists(path):
                    missing_files.append(path)
                    self._stability_state.pop(path, None)
                    continue
                if self._is_stable(path):
                    stable_files.append(path)
                else:
                    unstable_files.append(path)

            requeued = 0
            if unstable_files and self.requeue_unstable:
                self._requeue_files(unstable_files, delay_sec=self.stable_interval)
                requeued = len(unstable_files)

            if not stable_files:
                if self.detailed_stats:
                    logger.info(
                        'Watcher batch skipped: ready=%d stable=0 unstable=%d requeued=%d missing=%d queue=%d sample_ready=%s',
                        len(ready_files),
                        len(unstable_files),
                        requeued,
                        len(missing_files),
                        self._queue_size(),
                        self._sample_paths(ready_files),
                    )

                if self.on_stats:
                    self.on_stats({
                        'incoming': 0,
                        'processed': 0,
                        'duplicates': 0,
                        'unrecognized': 0,
                        'ready': len(ready_files),
                        'stable': 0,
                        'unstable': len(unstable_files),
                        'requeued': requeued,
                        'missing': len(missing_files),
                        'queue_size': self._queue_size(),
                        'sample_ready': self._sample_paths(ready_files),
                        'sample_stable': [],
                        'sample_unstable': self._sample_paths(unstable_files),
                        'batch_skipped': True,
                    })
                continue

            try:
                processing_stats = process_watch_batch(
                    stable_files,
                    csv_path=self.csv_path,
                    name_fields=self.name_fields,
                    output_folder=self.output_folder,
                    code_field=self.code_field,
                    csv_delimiter=self.csv_delimiter,
                    move_mode=self.move_mode,
                    threads=self.threads,
                    state=self.state,
                )

                stats = {
                    **processing_stats,
                    'ready': len(ready_files),
                    'stable': len(stable_files),
                    'unstable': len(unstable_files),
                    'requeued': requeued,
                    'missing': len(missing_files),
                    'queue_size': self._queue_size(),
                    'sample_ready': self._sample_paths(ready_files),
                    'sample_stable': self._sample_paths(stable_files),
                    'sample_unstable': self._sample_paths(unstable_files),
                    'batch_skipped': False,
                }

                if self.detailed_stats:
                    logger.info(
                        'Watcher batch: ready=%d stable=%d unstable=%d requeued=%d missing=%d | incoming=%d processed=%d duplicates=%d unrecognized=%d queue=%d sample_stable=%s',
                        stats['ready'],
                        stats['stable'],
                        stats['unstable'],
                        stats['requeued'],
                        stats['missing'],
                        stats['incoming'],
                        stats['processed'],
                        stats['duplicates'],
                        stats['unrecognized'],
                        stats['queue_size'],
                        stats['sample_stable'],
                    )
                else:
                    logger.info('Batch processed: %s', processing_stats)

                if self.on_stats:
                    self.on_stats(stats)
            except Exception as exc:
                logger.error('Ошибка watcher batch: %s', exc)

    def start(self):
        if self.observer is not None:
            return

        os.makedirs(self.input_folder, exist_ok=True)
        self.stop_event.clear()
        self.observer = Observer()
        handler = ScanEventHandler(self.queue_dict, self.queue_lock, self.debounce_sec, self.queue_event)
        self.observer.schedule(handler, self.input_folder, recursive=False)
        self.observer.start()

        if self.process_existing_on_start:
            seeded = self._prime_existing_files()
            logger.info('Watcher initial scan queued %d existing files', seeded)

        self.worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
        self.worker_thread.start()
        logger.info(
            'Watcher started for %s (debounce=%ss stable_checks=%s stable_interval=%ss poll=%ss requeue_unstable=%s detailed_stats=%s)',
            self.input_folder,
            self.debounce_sec,
            self.stable_checks,
            self.stable_interval,
            self.poll_interval,
            self.requeue_unstable,
            self.detailed_stats,
        )

    def stop(self):
        self.stop_event.set()
        self.queue_event.set()
        if self.observer:
            self.observer.stop()
            self.observer.join(timeout=5)
            self.observer = None
        if self.worker_thread and self.worker_thread.is_alive():
            self.worker_thread.join(timeout=5)
        self._stability_state.clear()
        logger.info('Watcher stopped')
