import logging
import os
import sys
import time

from PySide6.QtCore import QObject, QThread, Signal, Qt
from PySide6.QtWidgets import (
    QApplication,
    QCheckBox,
    QComboBox,
    QFileDialog,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QProgressBar,
    QTabWidget,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from pipeline import run_action
from processing_state import ProcessingState
from watch_mode import FolderWatcherService


logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)


class Worker(QObject):
    finished = Signal(dict)
    failed = Signal(str)
    progress = Signal(dict)

    def __init__(self, kwargs):
        super().__init__()
        self.kwargs = kwargs

    def _emit_progress(self, payload):
        self.progress.emit(payload)

    def run(self):
        try:
            result = run_action(progress_callback=self._emit_progress, **self.kwargs)
            self.finished.emit(result)
        except Exception as exc:
            self.failed.emit(str(exc))


class MainWindow(QMainWindow):
    watch_stats_received = Signal(dict)

    def __init__(self):
        super().__init__()
        self.setWindowTitle('QR-сканер — сортировка файлов')
        self.resize(980, 700)

        self.watcher = None
        self.worker_thread = None
        self.worker = None
        self.progress_bars = {}
        self.progress_labels = {}
        self.progress_state = {}
        self.watch_started_at = None
        self.watch_totals = {'incoming': 0, 'processed': 0, 'duplicates': 0, 'unrecognized': 0}
        self.active_action_button = None
        self.active_progress_key = None

        self.watch_debounce_edit = QLineEdit('2.0')
        self.watch_stable_checks_edit = QLineEdit('2')
        self.watch_stable_interval_edit = QLineEdit('1.0')
        self.watch_poll_interval_edit = QLineEdit('0.5')
        self.watch_sample_limit_edit = QLineEdit('5')
        self.watch_process_existing_check = QCheckBox('Обработать существующие файлы при старте')
        self.watch_requeue_unstable_check = QCheckBox('Повторно ставить нестабильные файлы в очередь')
        self.watch_requeue_unstable_check.setChecked(True)
        self.watch_detailed_stats_check = QCheckBox('Подробные batch-логи watcher')
        self.watch_detailed_stats_check.setChecked(True)

        self.watch_stats_received.connect(self._on_watch_stats, Qt.QueuedConnection)

        root = QWidget(self)
        self.setCentralWidget(root)

        layout = QVBoxLayout(root)
        grid = QGridLayout()

        self.input_edit = QLineEdit()
        self.csv_edit = QLineEdit()
        self.output_edit = QLineEdit()
        self.name_fields_edit = QLineEdit('Фамилия Имя')
        self.code_field_edit = QLineEdit('код')
        self.email_field_edit = QLineEdit('email')
        self.csv_delimiter_combo = QComboBox()
        self.csv_delimiter_combo.addItems(['auto', ';', ',', '\\t'])
        self.threads_edit = QLineEdit('4')

        self._add_path_row(grid, 0, 'Папка с исходными файлами', self.input_edit, self.pick_folder)
        self._add_path_row(grid, 1, 'CSV-таблица', self.csv_edit, self.pick_csv)
        self._add_path_row(grid, 2, 'Папка результатов', self.output_edit, self.pick_folder)

        grid.addWidget(QLabel('Разделитель CSV'), 3, 0)
        grid.addWidget(self.csv_delimiter_combo, 3, 1, 1, 2)

        grid.addWidget(QLabel('Название столбцов Фамилия,Имя (через пробел)'), 4, 0)
        grid.addWidget(self.name_fields_edit, 4, 1, 1, 2)

        grid.addWidget(QLabel('Название столбцов с кодом'), 5, 0)
        grid.addWidget(self.code_field_edit, 5, 1, 1, 2)

        grid.addWidget(QLabel('Название столбца с email'), 6, 0)
        grid.addWidget(self.email_field_edit, 6, 1, 1, 2)

        grid.addWidget(QLabel('Потоки'), 7, 0)
        grid.addWidget(self.threads_edit, 7, 1, 1, 2)

        layout.addLayout(grid)

        self.tabs = QTabWidget()
        layout.addWidget(self.tabs)

        self._init_action_tabs()

        self.log_view = QTextEdit()
        self.log_view.setReadOnly(True)
        layout.addWidget(self.log_view)

        self.stats_label = QLabel('Статистика: ожидание...')
        layout.addWidget(self.stats_label)

    def _add_path_row(self, grid, row, label, line_edit, picker):
        grid.addWidget(QLabel(label), row, 0)
        grid.addWidget(line_edit, row, 1)
        btn = QPushButton('...')
        btn.clicked.connect(lambda: picker(line_edit))
        grid.addWidget(btn, row, 2)

    def pick_folder(self, line_edit):
        folder = QFileDialog.getExistingDirectory(self, 'Выберите папку')
        if folder:
            line_edit.setText(folder)

    def pick_csv(self, line_edit):
        path, _ = QFileDialog.getOpenFileName(self, 'Выберите CSV', filter='CSV Files (*.csv)')
        if path:
            line_edit.setText(path)

    def _init_action_tabs(self):
        self.tab_action0_btn = QPushButton('Запустить переименование внутри папки')   
        self.tab_action0_btn.clicked.connect(lambda: self.run_simple_action(0, self.tab_action0_btn))
        self.tabs.addTab(self._wrap_tab('Требуется: папка с исходными файлами', self.tab_action0_btn, 'action0'), 'Переименование внутри папки')

        self.tab_action1_move_mode = QComboBox()
        self.tab_action1_move_mode.addItems(['copy', 'move'])
        self.tab_action1_btn = QPushButton('Запустить перенос по CSV')
        self.tab_action1_btn.clicked.connect(self.run_action1)
        tab1 = QWidget()
        tab1_layout = QVBoxLayout(tab1)
        row1 = QHBoxLayout()
        row1.addWidget(QLabel('Режим переноса:'))
        row1.addWidget(self.tab_action1_move_mode)
        row1.addStretch(1)
        tab1_layout.addLayout(row1)
        tab1_layout.addWidget(self.tab_action1_btn)
        self._create_progress_widgets(tab1_layout, 'action1')
        tab1_layout.addStretch(1)
        self.tabs.addTab(tab1, 'Перенос по CSV')

        self.tab_action2_btn = QPushButton('Запустить архивацию')
        self.tab_action2_btn.clicked.connect(lambda: self.run_simple_action(2, self.tab_action2_btn))
        self.tabs.addTab(self._wrap_tab('Требуется: папка результатов', self.tab_action2_btn, 'action2'), 'Архивация')

        self.tab_action3_btn = QPushButton('Запустить email-рассылку')
        self.tab_action3_btn.clicked.connect(lambda: self.run_simple_action(3, self.tab_action3_btn))
        self.tabs.addTab(self._wrap_tab('Требуется: CSV-таблица, столбцы с именем, папка результатов', self.tab_action3_btn, 'action3'), 'Email-рассылка')

        self.tab_action4_btn = QPushButton('Запустить валидацию email')
        self.tab_action4_btn.clicked.connect(lambda: self.run_simple_action(4, self.tab_action4_btn))
        self.tabs.addTab(self._wrap_tab('Требуется: CSV-таблица, столбцы с именем', self.tab_action4_btn, 'action4'), 'Валидация email')

        self.tab_watch_move_mode = QComboBox()
        self.tab_watch_move_mode.addItems(['copy', 'move'])
        self.watch_start_btn = QPushButton('Запустить фоновую сортировку по csv')
        self.watch_stop_btn = QPushButton('Остановить')
        self.watch_stop_btn.setEnabled(False)
        self.watch_start_btn.clicked.connect(self.start_watcher)
        self.watch_stop_btn.clicked.connect(self.stop_watcher)

        tab_watch = QWidget()
        tab_watch_layout = QVBoxLayout(tab_watch)
        row_watch = QHBoxLayout()
        row_watch.addWidget(QLabel('Режим переноса:'))
        row_watch.addWidget(self.tab_watch_move_mode)
        row_watch.addStretch(1)
        tab_watch_layout.addLayout(row_watch)
        controls = QHBoxLayout()
        controls.addWidget(self.watch_start_btn)
        controls.addWidget(self.watch_stop_btn)
        controls.addStretch(1)

        watch_settings_grid = QGridLayout()
        watch_settings_grid.addWidget(QLabel('Debounce (сек)'), 0, 0)
        watch_settings_grid.addWidget(self.watch_debounce_edit, 0, 1)
        watch_settings_grid.addWidget(QLabel('Проверок стабильности'), 0, 2)
        watch_settings_grid.addWidget(self.watch_stable_checks_edit, 0, 3)

        watch_settings_grid.addWidget(QLabel('Интервал стабильности (сек)'), 1, 0)
        watch_settings_grid.addWidget(self.watch_stable_interval_edit, 1, 1)
        watch_settings_grid.addWidget(QLabel('Интервал опроса (сек)'), 1, 2)
        watch_settings_grid.addWidget(self.watch_poll_interval_edit, 1, 3)

        watch_settings_grid.addWidget(QLabel('Лимит образцов'), 2, 0)
        watch_settings_grid.addWidget(self.watch_sample_limit_edit, 2, 1)
        watch_settings_grid.addWidget(self.watch_process_existing_check, 2, 2, 1, 2)

        watch_settings_grid.addWidget(self.watch_requeue_unstable_check, 3, 0, 1, 2)
        watch_settings_grid.addWidget(self.watch_detailed_stats_check, 3, 2, 1, 2)

        tab_watch_layout.addLayout(watch_settings_grid)
        tab_watch_layout.addLayout(controls)
        self._create_progress_widgets(tab_watch_layout, 'watch')
        tab_watch_layout.addStretch(1)
        self.tabs.addTab(tab_watch, 'Фоновая сортировка по CSV')

    def _wrap_tab(self, hint, button, progress_key):
        tab = QWidget()
        tab_layout = QVBoxLayout(tab)
        tab_layout.addWidget(QLabel(hint))
        tab_layout.addWidget(button)
        self._create_progress_widgets(tab_layout, progress_key)
        tab_layout.addStretch(1)
        return tab

    def _create_progress_widgets(self, tab_layout, key):
        bar = QProgressBar()
        bar.setRange(0, 100)
        bar.setValue(0)
        label = QLabel('Прогресс: ожидание')
        tab_layout.addWidget(bar)
        tab_layout.addWidget(label)
        self.progress_bars[key] = bar
        self.progress_labels[key] = label
        self.progress_state[key] = None

    @staticmethod
    def _format_seconds(seconds):
        seconds = max(0, int(seconds))
        h, rem = divmod(seconds, 3600)
        m, s = divmod(rem, 60)
        if h > 0:
            return f'{h:02d}:{m:02d}:{s:02d}'
        return f'{m:02d}:{s:02d}'

    def _reset_progress_ui(self, key, title='Ожидание'):
        bar = self.progress_bars.get(key)
        label = self.progress_labels.get(key)
        if not bar or not label:
            return

        bar.setRange(0, 100)
        bar.setValue(0)
        self.progress_state[key] = {
            'started_at': time.monotonic(),
            'last_done': 0,
            'last_total': None,
        }
        label.setText(f'{title} | Прогресс: 0 | скорость: 0.00 ед/с | прошло: 00:00 | ETA: —')

    def _update_progress_ui(self, key, done, total=None, unit='ед', message='Выполняется'):
        bar = self.progress_bars.get(key)
        label = self.progress_labels.get(key)
        if not bar or not label:
            return

        state = self.progress_state.get(key)
        if not state:
            self._reset_progress_ui(key, message)
            state = self.progress_state.get(key)

        done = int(done or 0)
        normalized_total = int(total) if total not in (None, 0) else None

        if (normalized_total is not None and state.get('last_total') not in (None, normalized_total) and done < state.get('last_done', 0)) or done < state.get('last_done', 0):
            state['started_at'] = time.monotonic()

        state['last_done'] = done
        state['last_total'] = normalized_total

        elapsed = max(0.001, time.monotonic() - state['started_at'])
        speed = done / elapsed if done > 0 else 0.0

        if normalized_total is not None:
            bar.setRange(0, normalized_total)
            bar.setValue(min(done, normalized_total))
            remaining = max(normalized_total - done, 0)
            eta = (remaining / speed) if speed > 0 else None
            progress_text = f'{done}/{normalized_total} {unit}'
        else:
            bar.setRange(0, 0)
            eta = None
            progress_text = f'{done} {unit}'

        eta_text = self._format_seconds(eta) if eta is not None else '—'
        label.setText(
            f'{message} | {progress_text} | скорость: {speed:.2f} {unit}/с | '
            f'прошло: {self._format_seconds(elapsed)} | ETA: {eta_text}'
        )

    def _complete_progress_ui(self, key, is_success=True):
        bar = self.progress_bars.get(key)
        label = self.progress_labels.get(key)
        state = self.progress_state.get(key)
        if not bar or not label or not state:
            return

        if bar.maximum() == 0:
            bar.setRange(0, 100)
            bar.setValue(100 if is_success else 0)
        else:
            bar.setValue(bar.maximum() if is_success else bar.value())

        elapsed = max(0, time.monotonic() - state['started_at'])
        suffix = 'завершено' if is_success else 'завершено с ошибкой'
        label.setText(f'{suffix} | прошло: {self._format_seconds(elapsed)}')
        self.progress_state[key] = None

    def _run_with_button(self, kwargs, button, start_message, progress_key):
        if self.worker_thread and self.worker_thread.isRunning():
            QMessageBox.warning(self, 'Занято', 'Дождитесь завершения текущей операции')
            return

        self.active_action_button = button
        self.active_progress_key = progress_key
        button.setEnabled(False)
        self._append_log(start_message)
        self._reset_progress_ui(progress_key, start_message)

        self.worker_thread = QThread(self)
        self.worker = Worker(kwargs)
        self.worker.moveToThread(self.worker_thread)
        self.worker_thread.started.connect(self.worker.run)

        self.worker.finished.connect(self._on_worker_finished, Qt.QueuedConnection)
        self.worker.failed.connect(self._on_worker_failed, Qt.QueuedConnection)
        self.worker.progress.connect(self._on_worker_progress, Qt.QueuedConnection)
        self.worker.finished.connect(self.worker_thread.quit)
        self.worker.failed.connect(self.worker_thread.quit)
        self.worker_thread.finished.connect(self._cleanup_worker_thread, Qt.QueuedConnection)
        self.worker_thread.start()

    def _cleanup_worker_thread(self):
        self.worker = None
        self.worker_thread = None

    def _on_worker_finished(self, result):
        button = self.active_action_button
        progress_key = self.active_progress_key

        self._append_log(f'Операция завершена: {result}')
        if button:
            button.setEnabled(True)
        if progress_key:
            self._complete_progress_ui(progress_key, is_success=True)

        self.active_action_button = None
        self.active_progress_key = None

    def _on_worker_failed(self, error):
        button = self.active_action_button
        progress_key = self.active_progress_key

        self._append_log(f'Ошибка: {error}')
        if button:
            button.setEnabled(True)
        if progress_key:
            self._complete_progress_ui(progress_key, is_success=False)

        self.active_action_button = None
        self.active_progress_key = None

    def _on_worker_progress(self, payload):
        action = payload.get('action')
        key = f'action{action}' if action is not None else self.active_progress_key
        if not key:
            return

        self._update_progress_ui(
            key,
            done=payload.get('done', 0),
            total=payload.get('total'),
            unit=payload.get('unit', 'ед'),
            message=payload.get('message', 'Выполняется'),
        )

    def _collect_shared_args(self):
        name_fields = [f for f in self.name_fields_edit.text().split() if f]
        threads = int(self.threads_edit.text().strip() or '4')
        return {
            'image_folder': self.input_edit.text().strip(),
            'csv_path': self.csv_edit.text().strip(),
            'output_folder': self.output_edit.text().strip(),
            'name_fields': name_fields,
            'code_field': self.code_field_edit.text().strip() or 'код',
            'email_field': self.email_field_edit.text().strip() or 'email',
            'csv_delimiter': self.csv_delimiter_combo.currentText(),
            'threads': threads,
        }

    def _validate_for_action(self, args, action):
        if action == 0 and not args['image_folder']:
            raise ValueError('Для action 0 укажите Input folder')
        if action == 1 and not all([args['image_folder'], args['csv_path'], args['name_fields'], args['output_folder']]):
            raise ValueError('Для action 1 укажите Input/CSV/Output и Name fields')
        if action == 2 and not args['output_folder']:
            raise ValueError('Для action 2 укажите Output folder')
        if action == 3 and not all([args['csv_path'], args['name_fields'], args['output_folder']]):
            raise ValueError('Для action 3 укажите CSV/Output и Name fields')
        if action == 4 and not all([args['csv_path'], args['name_fields']]):
            raise ValueError('Для action 4 укажите CSV и Name fields')

    def run_simple_action(self, action, button):
        try:
            kwargs = self._collect_shared_args()
            self._validate_for_action(kwargs, action)
            kwargs['action'] = action
        except Exception as exc:
            QMessageBox.warning(self, 'Ошибка', str(exc))
            return

        _action_names = {
            0: 'Переименование',
            1: 'Перенос по CSV',
            2: 'Архивация',
            3: 'Email-рассылка',
            4: 'Валидация email',
        }
        self._run_with_button(kwargs, button, f'Запуск: {_action_names.get(action, str(action))}...', progress_key=f'action{action}')

    def _append_log(self, message):
        self.log_view.append(message)

    def run_action1(self):
        try:
            kwargs = self._collect_shared_args()
            self._validate_for_action(kwargs, 1)
            kwargs['action'] = 1
            kwargs['move_mode'] = self.tab_action1_move_mode.currentText()
        except Exception as exc:
            QMessageBox.warning(self, 'Ошибка', str(exc))
            return

        self._run_with_button(kwargs, self.tab_action1_btn, 'Запуск: Перенос по CSV...', progress_key='action1')

    def _on_watch_stats(self, stats):
        if QThread.currentThread() != self.thread():
            self.watch_stats_received.emit(stats)
            return

        for key in self.watch_totals:
            self.watch_totals[key] += int(stats.get(key, 0) or 0)

        self.stats_label.setText(
            f"Статистика: получено={stats.get('incoming', 0)} "
            f"обработано={stats.get('processed', 0)} "
            f"дублей={stats.get('duplicates', 0)} "
            f"нераспознано={stats.get('unrecognized', 0)} "
            f"| готово={stats.get('ready', 0)} стабильных={stats.get('stable', 0)} "
            f"повторно={stats.get('requeued', 0)} очередь={stats.get('queue_size', 0)}"
        )

        processed_total = self.watch_totals['processed']
        incoming_total = self.watch_totals['incoming']
        duplicates_total = self.watch_totals['duplicates']
        unrecognized_total = self.watch_totals['unrecognized']
        message = (
            f"Наблюдение: получено={incoming_total} дублей={duplicates_total} "
            f"нераспознано={unrecognized_total}"
        )
        self._update_progress_ui('watch', done=processed_total, total=None, unit='файлов', message=message)

        batch_line = (
            f"Наблюдение (пакет): готово={stats.get('ready', 0)} стабильных={stats.get('stable', 0)} "
            f"нестабильных={stats.get('unstable', 0)} повторно={stats.get('requeued', 0)} "
            f"отсутствует={stats.get('missing', 0)} очередь={stats.get('queue_size', 0)} "
            f"получено={stats.get('incoming', 0)} обработано={stats.get('processed', 0)} "
            f"дублей={stats.get('duplicates', 0)} нераспознано={stats.get('unrecognized', 0)}"
        )
        self._append_log(batch_line)

        sample_stable = stats.get('sample_stable') or []
        sample_unstable = stats.get('sample_unstable') or []
        if sample_stable:
            self._append_log(f"  стабильные файлы: {', '.join(sample_stable)}")
        if sample_unstable:
            self._append_log(f"  нестабильные файлы: {', '.join(sample_unstable)}")

    @staticmethod
    def _parse_positive_float(raw_value, label):
        value = float(raw_value)
        if value <= 0:
            raise ValueError(f'«{label}» должно быть больше 0')
        return value

    @staticmethod
    def _parse_positive_int(raw_value, label):
        value = int(raw_value)
        if value <= 0:
            raise ValueError(f'«{label}» должно быть целым числом больше 0')
        return value

    def start_watcher(self):
        try:
            args = self._collect_shared_args()
            self._validate_for_action(args, 1)

            debounce_sec = self._parse_positive_float(self.watch_debounce_edit.text().strip() or '2.0', 'Задержка (дебаунс)')
            stable_checks = self._parse_positive_int(self.watch_stable_checks_edit.text().strip() or '2', 'Проверок стабильности')
            stable_interval = self._parse_positive_float(self.watch_stable_interval_edit.text().strip() or '1.0', 'Интервал стабильности')
            poll_interval = self._parse_positive_float(self.watch_poll_interval_edit.text().strip() or '0.5', 'Интервал опроса')
            sample_limit = self._parse_positive_int(self.watch_sample_limit_edit.text().strip() or '5', 'Лимит образцов')

            self.watch_started_at = time.monotonic()
            self.watch_totals = {'incoming': 0, 'processed': 0, 'duplicates': 0, 'unrecognized': 0}
            self._reset_progress_ui('watch', 'Наблюдение запущено')
            state_db = os.path.join(args['output_folder'], 'state', 'processed.sqlite')
            state = ProcessingState(state_db)
            self.watcher = FolderWatcherService(
                input_folder=args['image_folder'],
                csv_path=args['csv_path'],
                name_fields=args['name_fields'],
                output_folder=args['output_folder'],
                code_field=args['code_field'],
                csv_delimiter=args['csv_delimiter'],
                move_mode=self.tab_watch_move_mode.currentText(),
                threads=args['threads'],
                state=state,
                debounce_sec=debounce_sec,
                stable_checks=stable_checks,
                stable_interval=stable_interval,
                poll_interval=poll_interval,
                process_existing_on_start=self.watch_process_existing_check.isChecked(),
                requeue_unstable=self.watch_requeue_unstable_check.isChecked(),
                detailed_stats=self.watch_detailed_stats_check.isChecked(),
                sample_limit=sample_limit,
                on_stats=self._on_watch_stats,
            )
            self.watcher.start()
            self._append_log('Наблюдение запущено')
            self.watch_start_btn.setEnabled(False)
            self.watch_stop_btn.setEnabled(True)
        except Exception as exc:
            QMessageBox.warning(self, 'Ошибка', str(exc))

    def stop_watcher(self):
        if self.watcher:
            self.watcher.stop()
            self.watcher = None
            self._append_log('Наблюдение остановлено')
            self._complete_progress_ui('watch', is_success=True)
        self.watch_start_btn.setEnabled(True)
        self.watch_stop_btn.setEnabled(False)

    def closeEvent(self, event):
        try:
            self.stop_watcher()
        finally:
            event.accept()


def run_gui():
    app = QApplication(sys.argv)
    w = MainWindow()
    w.show()
    return app.exec()


if __name__ == '__main__':
    sys.exit(run_gui())
