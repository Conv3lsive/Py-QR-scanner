import logging
import os
import sys

from PySide6.QtCore import QObject, QThread, Signal
from PySide6.QtWidgets import (
    QApplication,
    QComboBox,
    QFileDialog,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPushButton,
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

    def __init__(self, kwargs):
        super().__init__()
        self.kwargs = kwargs

    def run(self):
        try:
            result = run_action(**self.kwargs)
            self.finished.emit(result)
        except Exception as exc:
            self.failed.emit(str(exc))


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle('Py QR Scanner GUI')
        self.resize(980, 700)

        self.watcher = None
        self.worker_thread = None
        self.worker = None

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
        self.threads_edit = QLineEdit('4')

        self._add_path_row(grid, 0, 'Input folder', self.input_edit, self.pick_folder)
        self._add_path_row(grid, 1, 'CSV file', self.csv_edit, self.pick_csv)
        self._add_path_row(grid, 2, 'Output folder', self.output_edit, self.pick_folder)

        grid.addWidget(QLabel('Название столбцов Фамилия,Имя (через пробел)'), 3, 0)
        grid.addWidget(self.name_fields_edit, 3, 1, 1, 2)

        grid.addWidget(QLabel('Название столбцов с кодом'), 4, 0)
        grid.addWidget(self.code_field_edit, 4, 1, 1, 2)

        grid.addWidget(QLabel('Название столбца с email'), 5, 0)
        grid.addWidget(self.email_field_edit, 5, 1, 1, 2)

        grid.addWidget(QLabel('Threads'), 6, 0)
        grid.addWidget(self.threads_edit, 6, 1, 1, 2)

        layout.addLayout(grid)

        self.tabs = QTabWidget()
        layout.addWidget(self.tabs)

        self._init_action_tabs()

        self.log_view = QTextEdit()
        self.log_view.setReadOnly(True)
        layout.addWidget(self.log_view)

        self.stats_label = QLabel('Stats: waiting...')
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
        self.tabs.addTab(self._wrap_tab('Требуется: Input folder', self.tab_action0_btn), 'Переименование внутри папки')

        self.tab_action1_move_mode = QComboBox()
        self.tab_action1_move_mode.addItems(['copy', 'move'])
        self.tab_action1_btn = QPushButton('Запустить перенос по CSV')
        self.tab_action1_btn.clicked.connect(self.run_action1)
        tab1 = QWidget()
        tab1_layout = QVBoxLayout(tab1)
        row1 = QHBoxLayout()
        row1.addWidget(QLabel('Move mode:'))
        row1.addWidget(self.tab_action1_move_mode)
        row1.addStretch(1)
        tab1_layout.addLayout(row1)
        tab1_layout.addWidget(self.tab_action1_btn)
        tab1_layout.addStretch(1)
        self.tabs.addTab(tab1, 'Перенос по CSV')

        self.tab_action2_btn = QPushButton('Запустить архивацию')
        self.tab_action2_btn.clicked.connect(lambda: self.run_simple_action(2, self.tab_action2_btn))
        self.tabs.addTab(self._wrap_tab('Требуется: Output folder', self.tab_action2_btn), 'Архивация')

        self.tab_action3_btn = QPushButton('Запустить email-рассылку')
        self.tab_action3_btn.clicked.connect(lambda: self.run_simple_action(3, self.tab_action3_btn))
        self.tabs.addTab(self._wrap_tab('Требуется: CSV, Name fields, Output folder', self.tab_action3_btn), 'Email-рассылка')

        self.tab_action4_btn = QPushButton('Запустить валидацию email')
        self.tab_action4_btn.clicked.connect(lambda: self.run_simple_action(4, self.tab_action4_btn))
        self.tabs.addTab(self._wrap_tab('Требуется: CSV, Name fields', self.tab_action4_btn), 'Валидация email')

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
        row_watch.addWidget(QLabel('Move mode:'))
        row_watch.addWidget(self.tab_watch_move_mode)
        row_watch.addStretch(1)
        tab_watch_layout.addLayout(row_watch)
        controls = QHBoxLayout()
        controls.addWidget(self.watch_start_btn)
        controls.addWidget(self.watch_stop_btn)
        controls.addStretch(1)
        tab_watch_layout.addLayout(controls)
        tab_watch_layout.addStretch(1)
        self.tabs.addTab(tab_watch, 'Фоновая сортировка по CSV')

    def _wrap_tab(self, hint, button):
        tab = QWidget()
        tab_layout = QVBoxLayout(tab)
        tab_layout.addWidget(QLabel(hint))
        tab_layout.addWidget(button)
        tab_layout.addStretch(1)
        return tab

    def _run_with_button(self, kwargs, button, start_message):
        if self.worker_thread and self.worker_thread.isRunning():
            QMessageBox.warning(self, 'Занято', 'Дождитесь завершения текущей операции')
            return

        button.setEnabled(False)
        self._append_log(start_message)

        self.worker_thread = QThread(self)
        self.worker = Worker(kwargs)
        self.worker.moveToThread(self.worker_thread)
        self.worker_thread.started.connect(self.worker.run)

        def on_finished(result):
            self._append_log(f'Операция завершена: {result}')
            button.setEnabled(True)

        def on_failed(error):
            self._append_log(f'Ошибка: {error}')
            button.setEnabled(True)

        self.worker.finished.connect(on_finished)
        self.worker.failed.connect(on_failed)
        self.worker.finished.connect(self.worker_thread.quit)
        self.worker.failed.connect(self.worker_thread.quit)
        self.worker_thread.start()

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

        self._run_with_button(kwargs, button, f'Запуск action {action}...')

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

        self._run_with_button(kwargs, self.tab_action1_btn, 'Запуск action 1...')

    def _on_watch_stats(self, stats):
        self.stats_label.setText(
            f"Stats: incoming={stats.get('incoming', 0)} "
            f"processed={stats.get('processed', 0)} "
            f"duplicates={stats.get('duplicates', 0)} "
            f"unrecognized={stats.get('unrecognized', 0)}"
        )
        self._append_log(f'Watcher batch: {stats}')

    def start_watcher(self):
        try:
            args = self._collect_shared_args()
            self._validate_for_action(args, 1)
            state_db = os.path.join(args['output_folder'], 'state', 'processed.sqlite')
            state = ProcessingState(state_db)
            self.watcher = FolderWatcherService(
                input_folder=args['image_folder'],
                csv_path=args['csv_path'],
                name_fields=args['name_fields'],
                output_folder=args['output_folder'],
                code_field=args['code_field'],
                move_mode=self.tab_watch_move_mode.currentText(),
                threads=args['threads'],
                state=state,
                on_stats=self._on_watch_stats,
            )
            self.watcher.start()
            self._append_log('Watcher запущен')
            self.watch_start_btn.setEnabled(False)
            self.watch_stop_btn.setEnabled(True)
        except Exception as exc:
            QMessageBox.warning(self, 'Ошибка', str(exc))

    def stop_watcher(self):
        if self.watcher:
            self.watcher.stop()
            self.watcher = None
            self._append_log('Watcher остановлен')
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
