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
    def __init__(self, queue_dict, lock):
        super().__init__()
        self.queue_dict = queue_dict
        self.lock = lock

    def _track(self, file_path):
        ext = os.path.splitext(file_path)[1].lower()
        if ext not in WATCH_EXTENSIONS:
            return
        with self.lock:
            self.queue_dict[file_path] = time.time()

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
                 stable_checks=2, stable_interval=1.0, on_stats=None):
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
        self.on_stats = on_stats

        self.queue_dict = defaultdict(float)
        self.queue_lock = threading.Lock()
        self.stop_event = threading.Event()
        self.worker_thread = None
        self.observer = None

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
            return False

        previous = None
        stable_hits = 0
        for _ in range(max(1, self.stable_checks)):
            try:
                stat = os.stat(file_path)
                current = (stat.st_size, stat.st_mtime)
            except FileNotFoundError:
                return False

            if current == previous:
                stable_hits += 1
            else:
                stable_hits = 0
            previous = current
            time.sleep(self.stable_interval)

        return stable_hits >= 1

    def _drain_ready_files(self):
        ready = []
        now = time.time()
        with self.queue_lock:
            for file_path, ts in list(self.queue_dict.items()):
                if now - ts >= self.debounce_sec:
                    ready.append(file_path)
                    self.queue_dict.pop(file_path, None)
        return ready

    def _worker_loop(self):
        logger.info('Watcher worker started')
        while not self.stop_event.is_set():
            ready_files = self._drain_ready_files()
            if not ready_files:
                time.sleep(0.5)
                continue

            stable_files = [path for path in ready_files if self._is_stable(path)]
            if not stable_files:
                continue

            try:
                stats = process_watch_batch(
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
                logger.info('Batch processed: %s', stats)
                if self.on_stats:
                    self.on_stats(stats)
            except Exception as exc:
                logger.error('Ошибка watcher batch: %s', exc)

    def start(self):
        if self.observer is not None:
            return

        os.makedirs(self.input_folder, exist_ok=True)
        self.observer = Observer()
        handler = ScanEventHandler(self.queue_dict, self.queue_lock)
        self.observer.schedule(handler, self.input_folder, recursive=False)
        self.observer.start()

        self.worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
        self.worker_thread.start()
        logger.info('Watcher started for %s', self.input_folder)

    def stop(self):
        self.stop_event.set()
        if self.observer:
            self.observer.stop()
            self.observer.join(timeout=5)
            self.observer = None
        if self.worker_thread and self.worker_thread.is_alive():
            self.worker_thread.join(timeout=5)
        logger.info('Watcher stopped')
