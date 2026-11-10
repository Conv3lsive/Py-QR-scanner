# -*- coding: utf-8 -*-
"""Тесты UI (gui_app.py). QApplication — offscreen, из conftest (qapp)."""
import time

import pytest

import gui_app
from gui_app import MainWindow, Worker


# ---------------------------------------------------------------- helpers

class _FakeMessageBox:
    """Подмена QMessageBox: PySide6-классам нельзя подменять атрибуты,
    поэтому подменяем ссылку на класс в модуле gui_app."""

    def __init__(self):
        self.warnings = []

    def warning(self, parent, title, text):
        self.warnings.append((title, text))
        return None


class _FakeWatcherService:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.started = False
        self.stopped = False

    def start(self):
        self.started = True

    def stop(self):
        self.stopped = True


class _FakeProcessingState:
    def __init__(self, db_path):
        self.db_path = db_path


class _FakeRunningThread:
    """Имитация QThread, который ещё работает."""

    def __init__(self):
        self.quit_calls = 0
        self.wait_calls = []

    def isRunning(self):
        return True

    def quit(self):
        self.quit_calls += 1

    def wait(self, timeout=0):
        self.wait_calls.append(timeout)
        return True


@pytest.fixture
def window(qapp):
    w = MainWindow()
    yield w
    if getattr(w, 'watcher', None) is not None:
        w.stop_watcher()
    w.close()


@pytest.fixture
def msgbox(monkeypatch):
    fake = _FakeMessageBox()
    monkeypatch.setattr(gui_app, 'QMessageBox', fake)
    return fake


@pytest.fixture
def fake_watcher_cls(monkeypatch):
    instances = []

    class _Cls(_FakeWatcherService):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            instances.append(self)

    monkeypatch.setattr(gui_app, 'FolderWatcherService', _Cls)
    return instances


@pytest.fixture
def fake_state_cls(monkeypatch):
    instances = []

    class _Cls(_FakeProcessingState):
        def __init__(self, db_path):
            super().__init__(db_path)
            instances.append(self)

    monkeypatch.setattr(gui_app, 'ProcessingState', _Cls)
    return instances


def _fill_action1_fields(window, tmp_path):
    window.input_edit.setText(str(tmp_path / 'input'))
    window.csv_edit.setText(str(tmp_path / 'students.csv'))
    window.output_edit.setText(str(tmp_path / 'output'))
    window.name_fields_edit.setText('Фамилия Имя')


def _process_events_until(qapp, condition, timeout=5.0):
    deadline = time.time() + timeout
    while not condition() and time.time() < deadline:
        qapp.processEvents()
        time.sleep(0.01)
    qapp.processEvents()


# ------------------------------------------------------- чистая логика

@pytest.mark.parametrize('seconds, expected', [
    (0, '00:00'),
    (3.9, '00:03'),
    (59, '00:59'),
    (60, '01:00'),
    (3599, '59:59'),
    (3600, '01:00:00'),
    (3661, '01:01:01'),
    (-5, '00:00'),
])
def test_format_seconds(window, seconds, expected):
    assert MainWindow._format_seconds(seconds) == expected


@pytest.mark.parametrize('raw, value', [('2.0', 2.0), ('0.5', 0.5), ('10', 10.0)])
def test_parse_positive_float_ok(window, raw, value):
    assert MainWindow._parse_positive_float(raw, 'x') == value


@pytest.mark.parametrize('raw', ['0', '-1', 'abc', ''])
def test_parse_positive_float_rejects(window, raw):
    with pytest.raises(ValueError):
        MainWindow._parse_positive_float(raw, 'x')


@pytest.mark.parametrize('raw', ['nan', 'inf'])
def test_parse_positive_float_rejects_nan_inf(window, raw):
    with pytest.raises(ValueError):
        MainWindow._parse_positive_float(raw, 'x')


@pytest.mark.parametrize('raw, value', [('2', 2), ('1', 1), ('10', 10)])
def test_parse_positive_int_ok(window, raw, value):
    assert MainWindow._parse_positive_int(raw, 'x') == value


@pytest.mark.parametrize('raw', ['0', '-1', '2.5', 'abc', ''])
def test_parse_positive_int_rejects(window, raw):
    with pytest.raises(ValueError):
        MainWindow._parse_positive_int(raw, 'x')


def test_collect_shared_args_defaults_and_parsing(window):
    window.input_edit.setText('  /tmp/in  ')
    window.csv_edit.setText('/tmp/a.csv')
    window.output_edit.setText('/tmp/out')
    window.name_fields_edit.setText('  Фамилия    Имя  ')
    window.code_field_edit.setText('')
    window.email_field_edit.setText('')
    window.threads_edit.setText('  8 ')

    args = window._collect_shared_args()

    assert args['image_folder'] == '/tmp/in'
    assert args['name_fields'] == ['Фамилия', 'Имя']
    assert args['code_field'] == 'код'
    assert args['email_field'] == 'email'
    assert args['threads'] == 8
    assert args['csv_delimiter'] == 'auto'


def test_collect_shared_args_threads_empty_defaults_to_4(window):
    window.threads_edit.setText('')
    assert window._collect_shared_args()['threads'] == 4


@pytest.mark.parametrize('raw', ['abc', '0', '-3', '1.5'])
def test_collect_shared_args_threads_invalid(window, raw):
    window.threads_edit.setText(raw)
    with pytest.raises(ValueError) as exc:
        window._collect_shared_args()
    assert 'Потоки' in str(exc.value)


# --------------------------------------------------- структура интерфейса

def test_window_structure(window):
    assert window.windowTitle().startswith('QR-сканер')
    assert window.tabs.count() == 6
    expected_keys = {'action0', 'action1', 'action2', 'action3', 'action4', 'watch'}
    assert set(window.progress_bars) == expected_keys
    assert set(window.progress_labels) == expected_keys
    assert set(window.progress_state) == expected_keys


def test_watch_controls_defaults(window):
    assert window.watch_debounce_edit.text() == '2.0'
    assert window.watch_stable_checks_edit.text() == '2'
    assert window.watch_stable_interval_edit.text() == '1.0'
    assert window.watch_poll_interval_edit.text() == '0.5'
    assert window.watch_sample_limit_edit.text() == '5'
    assert window.watch_requeue_unstable_check.isChecked()
    assert window.watch_detailed_stats_check.isChecked()
    assert not window.watch_process_existing_check.isChecked()
    assert window.watch_start_btn.isEnabled()
    assert not window.watch_stop_btn.isEnabled()


# -------------------------------------------------------- прогресс UI

def test_update_progress_ui_with_total(window):
    window._reset_progress_ui('action1', 'Тест')
    window._update_progress_ui('action1', done=2, total=10, unit='этап', message='Работа')

    bar = window.progress_bars['action1']
    label = window.progress_labels['action1']
    assert (bar.minimum(), bar.maximum(), bar.value()) == (0, 10, 2)
    assert '2/10' in label.text()
    assert 'этап' in label.text()
    assert 'Работа' in label.text()
    assert window.progress_state['action1']['last_done'] == 2


def test_update_progress_ui_without_total_busy_bar(window):
    window._reset_progress_ui('watch', 'Тест')
    window._update_progress_ui('watch', done=5, unit='файлов', message='Наблюдение')

    bar = window.progress_bars['watch']
    assert (bar.minimum(), bar.maximum()) == (0, 0)  # busy-режим
    assert '5 файлов' in window.progress_labels['watch'].text()


def test_update_progress_ui_resets_timer_on_done_decrease(window, monkeypatch):
    ticks = iter(range(100))
    monkeypatch.setattr(gui_app.time, 'monotonic', lambda: next(ticks))
    window._reset_progress_ui('action1', 'Тест')
    state = window.progress_state['action1']
    first_start = state['started_at']

    window._update_progress_ui('action1', done=5, total=10)
    window._update_progress_ui('action1', done=3, total=10)

    assert state['started_at'] > first_start
    assert state['last_done'] == 3


def test_update_progress_ui_keeps_timer_when_total_changes_only(window, monkeypatch):
    # Слабое место (документируем текущее поведение): при смене total без
    # уменьшения done окно скорости/ETA не сбрасывается — оценка ETA искажается.
    ticks = iter(range(100))
    monkeypatch.setattr(gui_app.time, 'monotonic', lambda: next(ticks))
    window._reset_progress_ui('action1', 'Тест')
    state = window.progress_state['action1']
    first_start = state['started_at']

    window._update_progress_ui('action1', done=5, total=10)
    window._update_progress_ui('action1', done=6, total=999)

    assert state['started_at'] == first_start


def test_update_progress_ui_autocreates_state(window):
    window.progress_state['action2'] = None
    window._update_progress_ui('action2', done=1, total=4)
    assert window.progress_state['action2']['last_done'] == 1


def test_update_progress_ui_unknown_key_noop(window):
    window._update_progress_ui('no-such-key', done=1, total=2)  # не должно падать


def test_complete_progress_ui_success_and_failure(window):
    window._reset_progress_ui('action1', 'Тест')
    window._update_progress_ui('action1', done=2, total=10)

    window._complete_progress_ui('action1', is_success=True)
    bar = window.progress_bars['action1']
    assert bar.value() == bar.maximum() == 10
    assert 'завершено' in window.progress_labels['action1'].text()
    assert window.progress_state['action1'] is None

    window._reset_progress_ui('action1', 'Тест')
    window._update_progress_ui('action1', done=2, total=10)
    window._complete_progress_ui('action1', is_success=False)
    assert window.progress_bars['action1'].value() == 2  # остался на месте
    assert 'с ошибкой' in window.progress_labels['action1'].text()


def test_complete_progress_ui_from_busy_bar(window):
    window._reset_progress_ui('watch', 'Тест')
    window._update_progress_ui('watch', done=7)
    window._complete_progress_ui('watch', is_success=True)
    bar = window.progress_bars['watch']
    assert (bar.minimum(), bar.maximum(), bar.value()) == (0, 100, 100)


def test_reset_progress_ui_label_format(window):
    window._reset_progress_ui('action3', 'Старт')
    label = window.progress_labels['action3'].text()
    assert label.startswith('Старт | Прогресс: 0 | скорость: 0.00')
    assert window.progress_bars['action3'].value() == 0


# -------------------------------------------------- маршрутизация прогресса

def test_on_worker_progress_routes_by_action(window):
    window._reset_progress_ui('action1', 'Т')
    window._reset_progress_ui('action2', 'Т')
    window._on_worker_progress({'action': 1, 'done': 3, 'total': 7, 'message': 'м1'})
    assert window.progress_bars['action1'].value() == 3
    assert window.progress_bars['action2'].value() == 0


def test_on_worker_progress_fallback_to_active_key(window):
    window.active_progress_key = 'action2'
    window._reset_progress_ui('action2', 'Т')
    window._on_worker_progress({'done': 4, 'total': 9})
    assert window.progress_bars['action2'].value() == 4


def test_on_worker_progress_defaults(window):
    window._reset_progress_ui('action1', 'Т')
    window._on_worker_progress({'action': 1})
    assert window.progress_state['action1']['last_done'] == 0


def test_on_worker_progress_no_key_ignored(window):
    window.active_progress_key = None
    window._on_worker_progress({'done': 1})  # не должно падать


# ------------------------------------------------------------- лог

def test_append_log_escapes_html(window):
    window._append_log('<b>жирный</b> & "кавычки"')
    html = window.log_view.toHtml()
    text = window.log_view.toPlainText()
    assert '&lt;b&gt;' in html          # тег экранирован, не исполняется
    assert '<b>жирный</b>' in text      # виден как текст
    assert 'кавычки' in text


def test_append_log_multiline(window):
    window._append_log('строка1\nстрока2')
    text = window.log_view.toPlainText()
    assert 'строка1' in text and 'строка2' in text


# --------------------------------------------------------- статистика watcher

def test_on_watch_stats_accumulates_and_labels(window):
    stats = {
        'incoming': 2, 'processed': 1, 'duplicates': 1, 'unrecognized': 0,
        'ready': 2, 'stable': 2, 'unstable': 0, 'requeued': 0,
        'queue_size': 0,
        'sample_stable': ['a.jpg'], 'sample_unstable': ['b.jpg'],
    }
    window._on_watch_stats(stats)
    window._on_watch_stats(stats)

    assert window.watch_totals == {'incoming': 4, 'processed': 2, 'duplicates': 2, 'unrecognized': 0}
    assert 'получено=2' in window.stats_label.text()
    log_text = window.log_view.toPlainText()
    assert log_text.count('Наблюдение (пакет)') == 2
    assert 'стабильные файлы: a.jpg' in log_text
    assert 'нестабильные файлы: b.jpg' in log_text


def test_on_watch_stats_partial_dict_no_crash(window):
    window._on_watch_stats({'incoming': 1})
    assert window.watch_totals['incoming'] == 1
    assert 'получено=1' in window.stats_label.text()


# ------------------------------------------------------ валидация перед запуском

@pytest.mark.parametrize('action', [0, 1, 2, 3, 4])
def test_run_simple_action_validation_blocks_and_warns(window, msgbox, action, tmp_path):
    # Все поля пустые — валидация должна отклонить любой action
    window.worker_thread = None
    window._append_log = lambda *a, **k: None
    try:
        window.run_simple_action(action, window.tab_action0_btn)
    except Exception:
        pytest.fail('run_simple_action не должен бросать исключение')

    assert len(msgbox.warnings) == 1
    assert msgbox.warnings[0][0] == 'Ошибка'
    assert window.worker_thread is None  # поток не создан


def test_run_action1_passes_move_mode(window, monkeypatch, tmp_path):
    captured = {}

    def fake_run(kwargs, button, start_message, progress_key):
        captured.update(kwargs=kwargs)

    monkeypatch.setattr(window, '_run_with_button', fake_run)
    _fill_action1_fields(window, tmp_path)
    window.tab_action1_move_mode.setCurrentText('move')

    window.run_action1()

    assert captured['kwargs']['action'] == 1
    assert captured['kwargs']['move_mode'] == 'move'
    assert captured['kwargs']['csv_path'] == str(tmp_path / 'students.csv')


def test_run_action1_validation_warns(window, msgbox, monkeypatch, tmp_path):
    called = []
    monkeypatch.setattr(window, '_run_with_button',
                        lambda *a, **k: called.append(a))
    window.csv_edit.setText('')  # не хватает CSV

    window.run_action1()

    assert not called
    assert len(msgbox.warnings) == 1


# ------------------------------------------------------------- watcher

def test_start_watcher_happy_path(window, msgbox, fake_watcher_cls, fake_state_cls, tmp_path):
    _fill_action1_fields(window, tmp_path)
    window.watch_debounce_edit.setText('3.5')
    window.watch_stable_checks_edit.setText('4')
    window.tab_watch_move_mode.setCurrentText('move')

    window.start_watcher()

    assert not msgbox.warnings
    assert len(fake_watcher_cls) == 1
    watcher = fake_watcher_cls[0]
    assert watcher.started
    kwargs = watcher.kwargs
    assert kwargs['input_folder'] == str(tmp_path / 'input')
    assert kwargs['csv_path'] == str(tmp_path / 'students.csv')
    assert kwargs['name_fields'] == ['Фамилия', 'Имя']
    assert kwargs['move_mode'] == 'move'
    assert kwargs['debounce_sec'] == 3.5
    assert kwargs['stable_checks'] == 4
    assert kwargs['requeue_unstable'] is True
    assert kwargs['detailed_stats'] is True
    assert kwargs['process_existing_on_start'] is False
    assert kwargs['on_stats'] == window._on_watch_stats
    assert kwargs['state'] is fake_state_cls[0]
    assert fake_state_cls[0].db_path == str(tmp_path / 'output' / 'state' / 'processed.sqlite')
    assert not window.watch_start_btn.isEnabled()
    assert window.watch_stop_btn.isEnabled()
    assert window.watch_started_at is not None

    window.stop_watcher()

    assert watcher.stopped
    assert window.watcher is None
    assert window.watch_start_btn.isEnabled()
    assert not window.watch_stop_btn.isEnabled()
    assert window.progress_state['watch'] is None


def test_start_watcher_resets_totals(window, fake_watcher_cls, fake_state_cls, tmp_path):
    _fill_action1_fields(window, tmp_path)
    window._on_watch_stats({'incoming': 5, 'processed': 5, 'duplicates': 5, 'unrecognized': 5})

    window.start_watcher()
    window.stop_watcher()

    assert window.watch_totals == {'incoming': 0, 'processed': 0, 'duplicates': 0, 'unrecognized': 0}


@pytest.mark.parametrize('field, value', [
    ('watch_debounce_edit', 'abc'),
    ('watch_stable_checks_edit', '0'),
    ('watch_stable_interval_edit', '-1'),
    ('watch_sample_limit_edit', '1.5'),
])
def test_start_watcher_rejects_bad_params(window, msgbox, fake_watcher_cls,
                                          fake_state_cls, tmp_path, field, value):
    _fill_action1_fields(window, tmp_path)
    getattr(window, field).setText(value)

    window.start_watcher()

    assert len(msgbox.warnings) == 1
    assert not fake_watcher_cls
    assert window.watch_start_btn.isEnabled()
    assert window.watcher is None


def test_start_watcher_forbids_zero_sample_limit(window, msgbox, fake_watcher_cls,
                                                 fake_state_cls, tmp_path):
    # watch_mode._sample_paths поддерживает sample_limit<=0 («без образцов»),
    # но GUI это запрещает — несоответствие, документируем текущее поведение.
    _fill_action1_fields(window, tmp_path)
    window.watch_sample_limit_edit.setText('0')

    window.start_watcher()

    assert len(msgbox.warnings) == 1


def test_stop_watcher_without_watcher_safe(window):
    window.stop_watcher()
    assert window.watch_start_btn.isEnabled()
    assert not window.watch_stop_btn.isEnabled()


def test_close_event_stops_watcher(window, fake_watcher_cls, fake_state_cls, tmp_path):
    _fill_action1_fields(window, tmp_path)
    window.start_watcher()
    watcher = fake_watcher_cls[0]

    window.close()

    assert watcher.stopped


# ------------------------------------------------------------- Worker

def test_worker_emits_finished_with_result(monkeypatch):
    def fake_run_action(progress_callback=None, **kwargs):
        progress_callback({'action': 0, 'done': 1, 'total': 2})
        return {'status': 'ok'}

    monkeypatch.setattr(gui_app, 'run_action', fake_run_action)
    worker = Worker({'action': 0})
    results, progress, failures = [], [], []
    worker.finished.connect(results.append)
    worker.progress.connect(progress.append)
    worker.failed.connect(failures.append)

    worker.run()

    assert results == [{'status': 'ok'}]
    assert progress == [{'action': 0, 'done': 1, 'total': 2}]
    assert not failures


def test_worker_emits_failed_on_exception(monkeypatch):
    def fake_run_action(**kwargs):
        raise RuntimeError('бум')

    monkeypatch.setattr(gui_app, 'run_action', fake_run_action)
    worker = Worker({'action': 0})
    results, failures = [], []
    worker.finished.connect(results.append)
    worker.failed.connect(failures.append)

    worker.run()

    assert not results
    assert failures == ['бум']


# ----------------------------------------------- полный цикл операции (QThread)

def test_full_operation_lifecycle(window, qapp, monkeypatch, tmp_path):
    calls = []

    def fake_run_action(progress_callback=None, **kwargs):
        calls.append(kwargs)
        progress_callback({'action': 0, 'done': 1, 'total': 2, 'unit': 'этап', 'message': 'Шаг'})
        return {'status': 'ok', 'message': 'Файлы переименованы'}

    monkeypatch.setattr(gui_app, 'run_action', fake_run_action)
    window.input_edit.setText(str(tmp_path))
    btn = window.tab_action0_btn

    window.run_simple_action(0, btn)
    assert not btn.isEnabled()

    _process_events_until(qapp, lambda: window.worker_thread is None)

    assert window.worker_thread is None and window.worker is None
    assert btn.isEnabled()
    assert window.active_action_button is None
    assert calls and calls[0]['action'] == 0
    log_text = window.log_view.toPlainText()
    assert 'Операция завершена' in log_text
    assert 'завершено' in window.progress_labels['action0'].text()
    assert window.progress_bars['action0'].value() == window.progress_bars['action0'].maximum()


def test_full_operation_failure_path(window, qapp, monkeypatch, tmp_path):
    def fake_run_action(**kwargs):
        raise RuntimeError('всё сломалось')

    monkeypatch.setattr(gui_app, 'run_action', fake_run_action)
    window.input_edit.setText(str(tmp_path))
    btn = window.tab_action0_btn

    window.run_simple_action(0, btn)
    _process_events_until(qapp, lambda: window.worker_thread is None)

    assert btn.isEnabled()
    assert 'Ошибка: всё сломалось' in window.log_view.toPlainText()
    assert 'с ошибкой' in window.progress_labels['action0'].text()


def test_run_with_button_busy_guard(window, msgbox):
    window.worker_thread = _FakeRunningThread()
    window._run_with_button({'action': 0}, window.tab_action0_btn, 'msg', 'action0')

    assert len(msgbox.warnings) == 1
    assert msgbox.warnings[0][0] == 'Занято'
    # Новый поток не создан: ссылка не перезаписана
    assert isinstance(window.worker_thread, _FakeRunningThread)
    window.worker_thread = None


# ------------------------------- исправленные баги UI (были xfail)

def test_button_stays_disabled_until_thread_stops(window):
    # _on_worker_finished включает кнопку, хотя worker_thread ещё жив:
    # _cleanup_worker_thread сработает только по thread.finished.
    fake_thread = _FakeRunningThread()
    window.worker_thread = fake_thread
    btn = window.tab_action0_btn
    btn.setEnabled(False)
    window.active_action_button = btn
    window.active_progress_key = 'action0'

    window._on_worker_finished({'status': 'ok'})

    assert not btn.isEnabled()
    window.worker_thread = None


def test_close_event_stops_worker_thread(window):
    fake_thread = _FakeRunningThread()
    window.worker_thread = fake_thread

    window.close()

    assert fake_thread.quit_calls > 0
    window.worker_thread = None
