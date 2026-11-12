"""Тесты main.py: setup_logging и парсинг аргументов / диспетчеризация в run_action."""
import logging
import logging.handlers
import sys

import pytest

import main
import pipeline
from log_formatting import ColoredLevelFormatter


@pytest.fixture
def captured_basic_config(monkeypatch):
    """Перехватываем logging.basicConfig, чтобы pytest не влиял на root-логгер."""
    captured = {}

    def fake_basic_config(**kwargs):
        captured['kwargs'] = kwargs

    monkeypatch.setattr(logging, 'basicConfig', fake_basic_config)
    return captured


def _stream_handler(handlers):
    for h in handlers:
        if isinstance(h, logging.StreamHandler):
            return h
    return None


# --------------------------------------------------------------------------- #
# setup_logging
# --------------------------------------------------------------------------- #

def test_setup_logging_stream_handler_colored_formatter(monkeypatch, captured_basic_config):
    monkeypatch.setenv('TERM', 'dumb')
    main.setup_logging()
    handlers = captured_basic_config['kwargs']['handlers']
    fmt = _stream_handler(handlers).formatter
    assert isinstance(fmt, ColoredLevelFormatter)
    assert fmt.use_color is False  # TERM=dumb
    rec = logging.LogRecord('x', logging.INFO, __file__, 1, 'msg', None, None)
    assert '\033' not in fmt.format(rec)


def test_setup_logging_force_color(monkeypatch, captured_basic_config):
    monkeypatch.setenv('FORCE_COLOR', '1')
    main.setup_logging()
    handlers = captured_basic_config['kwargs']['handlers']
    assert _stream_handler(handlers).formatter.use_color is True


def test_setup_logging_sets_level(captured_basic_config):
    main.setup_logging(level=logging.DEBUG)
    assert captured_basic_config['kwargs']['level'] == logging.DEBUG


def test_setup_logging_creates_and_writes_file(captured_basic_config, tmp_path):
    log_file = tmp_path / 'logs' / 'app.log'
    main.setup_logging(logging.INFO, log_file=str(log_file))
    assert log_file.exists()  # RotatingFileHandler открывает файл сразу

    handlers = captured_basic_config['kwargs']['handlers']
    file_handlers = [
        h for h in handlers if isinstance(h, logging.handlers.RotatingFileHandler)
    ]
    assert len(file_handlers) == 1
    # в файл пишется обычным форматтером, без ANSI-цветов
    assert not isinstance(file_handlers[0].formatter, ColoredLevelFormatter)

    file_handlers[0].emit(
        logging.LogRecord('x', logging.ERROR, __file__, 1, 'файловая запись', None, None)
    )
    file_handlers[0].flush()
    assert 'файловая запись' in log_file.read_text(encoding='utf-8')


def test_setup_logging_no_file_handler_without_log_file(captured_basic_config):
    main.setup_logging()
    handlers = captured_basic_config['kwargs']['handlers']
    assert not any(isinstance(h, logging.handlers.RotatingFileHandler) for h in handlers)


# --------------------------------------------------------------------------- #
# main(): диспетчеризация и аргументы
# --------------------------------------------------------------------------- #

def _patch_run_action(monkeypatch):
    calls = {}

    def fake_run_action(**kwargs):
        calls.update(kwargs)
        return 'ok'

    monkeypatch.setattr(pipeline, 'run_action', fake_run_action)
    return calls


def _patch_setup_logging(monkeypatch):
    info = {}

    def fake_setup_logging(level=logging.INFO, log_file=None):
        info['level'] = level
        info['log_file'] = log_file

    monkeypatch.setattr(main, 'setup_logging', fake_setup_logging)
    return info


def test_main_version_exits_zero(monkeypatch, capsys):
    monkeypatch.setattr(sys, 'argv', ['main.py', '--version'])
    with pytest.raises(SystemExit) as exc:
        main.main()
    assert exc.value.code == 0
    assert main.__version__ in capsys.readouterr().out


def test_main_no_action_errors(monkeypatch):
    _patch_setup_logging(monkeypatch)
    monkeypatch.setattr(sys, 'argv', ['main.py'])
    with pytest.raises(SystemExit) as exc:
        main.main()
    assert exc.value.code == 2


def test_main_action_zero_kwargs(monkeypatch):
    calls = _patch_run_action(monkeypatch)
    _patch_setup_logging(monkeypatch)
    monkeypatch.setattr(sys, 'argv', ['main.py', '--action', '0', '--image-folder', '/tmp/img'])
    main.main()

    assert calls['action'] == 0
    assert calls['image_folder'] == '/tmp/img'
    assert 'threads' not in calls
    assert calls['code_field'] == 'код'
    assert calls['email_field'] == 'email'
    assert calls['csv_delimiter'] == 'auto'
    assert calls['move_mode'] == 'copy'
    assert calls['csv_path'] is None
    assert calls['name_fields'] is None
    assert calls['output_folder'] is None


def test_main_action_one_kwargs(monkeypatch):
    calls = _patch_run_action(monkeypatch)
    _patch_setup_logging(monkeypatch)
    monkeypatch.setattr(sys, 'argv', [
        'main.py', '--action', '1', '--csv-path', '/tmp/a.csv',
        '--name-fields', 'Имя', 'Фамилия', '--output-folder', '/tmp/out',
        '--move-mode', 'move', '--code-field', 'code',
    ])
    main.main()

    assert calls['action'] == 1
    assert calls['csv_path'] == '/tmp/a.csv'
    assert calls['name_fields'] == ['Имя', 'Фамилия']
    assert calls['output_folder'] == '/tmp/out'
    assert calls['move_mode'] == 'move'
    assert calls['code_field'] == 'code'


def test_main_action_five_uses_watch_mode(monkeypatch):
    run_calls = []
    monkeypatch.setattr(pipeline, 'run_action', lambda **kw: run_calls.append(kw))
    watch_calls = []
    monkeypatch.setattr(main, '_run_watch_mode', lambda args: watch_calls.append(args))
    _patch_setup_logging(monkeypatch)
    monkeypatch.setattr(sys, 'argv', [
        'main.py', '--action', '5', '--image-folder', '/tmp/i',
        '--csv-path', '/tmp/c', '--name-fields', 'a', '--output-folder', '/tmp/o',
    ])
    main.main()

    assert run_calls == []
    assert len(watch_calls) == 1
    assert watch_calls[0].action == 5


def test_main_default_log_level(monkeypatch):
    _patch_run_action(monkeypatch)
    sl = _patch_setup_logging(monkeypatch)
    monkeypatch.setattr(sys, 'argv', ['main.py', '--action', '0'])
    main.main()
    assert sl['level'] == logging.INFO
    assert sl['log_file'] is None


def test_main_log_level_debug(monkeypatch):
    _patch_run_action(monkeypatch)
    sl = _patch_setup_logging(monkeypatch)
    monkeypatch.setattr(sys, 'argv', ['main.py', '--action', '0', '--log-level', 'DEBUG'])
    main.main()
    assert sl['level'] == logging.DEBUG


def test_main_log_level_invalid_falls_back_to_info(monkeypatch):
    # неверный --log-level молча падает на INFO (getattr с дефолтом)
    _patch_run_action(monkeypatch)
    sl = _patch_setup_logging(monkeypatch)
    monkeypatch.setattr(sys, 'argv', ['main.py', '--action', '0', '--log-level', 'BOGUS'])
    main.main()
    assert sl['level'] == logging.INFO


def test_main_passes_log_file(monkeypatch):
    _patch_run_action(monkeypatch)
    sl = _patch_setup_logging(monkeypatch)
    monkeypatch.setattr(sys, 'argv', ['main.py', '--action', '0', '--log-file', '/tmp/x.log'])
    main.main()
    assert sl['log_file'] == '/tmp/x.log'