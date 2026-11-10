"""Тесты модуля log_formatting.py: форматтер, цвет, GUI-классификация и HTML-экранирование."""
import logging

import pytest

import log_formatting
from log_formatting import (
    ColoredLevelFormatter,
    LOG_FORMAT,
    _classify_gui_log,
    _GUI_COLORS,
    gui_log_html,
    stream_supports_color,
)


class _FakeStream:
    def __init__(self, isatty):
        self._isatty = isatty

    def isatty(self):
        return self._isatty


@pytest.fixture(autouse=True)
def _clean_color_env(monkeypatch):
    for name in ('FORCE_COLOR', 'NO_COLOR', 'TERM'):
        monkeypatch.delenv(name, raising=False)


# --------------------------------------------------------------------------- #
# stream_supports_color
# --------------------------------------------------------------------------- #

def test_stream_force_color_wins_over_dumb(monkeypatch):
    monkeypatch.setenv('FORCE_COLOR', '1')
    monkeypatch.setenv('TERM', 'dumb')
    assert stream_supports_color(_FakeStream(False)) is True


def test_stream_no_color_wins_over_tty(monkeypatch):
    monkeypatch.setenv('NO_COLOR', '1')
    assert stream_supports_color(_FakeStream(True)) is False


def test_stream_term_dumb_disables(monkeypatch):
    monkeypatch.setenv('TERM', 'dumb')
    assert stream_supports_color(_FakeStream(True)) is False


def test_stream_isatty_true(monkeypatch):
    monkeypatch.setenv('TERM', 'xterm-256color')
    assert stream_supports_color(_FakeStream(True)) is True


def test_stream_isatty_false(monkeypatch):
    monkeypatch.setenv('TERM', 'xterm-256color')
    assert stream_supports_color(_FakeStream(False)) is False


def test_stream_no_isatty_returns_false():
    class NoIsatty:
        pass

    assert stream_supports_color(NoIsatty()) is False


def test_stream_force_color_empty_string_forces_true(monkeypatch):
    # Документируем фактическое поведение: пустое значение FORCE_COLOR
    # (т.е. FORCE_COLOR= без значения) тоже включает цвет — сомнительно.
    monkeypatch.setenv('FORCE_COLOR', '')
    assert stream_supports_color(_FakeStream(False)) is True


# --------------------------------------------------------------------------- #
# ColoredLevelFormatter
# --------------------------------------------------------------------------- #

def _record(levelno, msg='msg'):
    rec = logging.LogRecord('x', levelno, __file__, 1, msg, None, None)
    return rec


def test_formatter_use_color_false_no_ansi():
    fmt = ColoredLevelFormatter(LOG_FORMAT, use_color=False)
    rec = _record(logging.INFO)
    out = fmt.format(rec)
    assert '\033' not in out
    assert 'INFO' in out


def test_formatter_use_color_true_adds_ansi():
    fmt = ColoredLevelFormatter(LOG_FORMAT, use_color=True)
    out = fmt.format(_record(logging.INFO))
    assert '\033[32m' in out  # зелёный для INFO
    assert '\033[0m' in out


@pytest.mark.parametrize('levelno,color', [
    (logging.DEBUG, '\033[2m'),
    (logging.INFO, '\033[32m'),
    (logging.WARNING, '\033[33m'),
    (logging.ERROR, '\033[31m'),
    (logging.CRITICAL, '\033[1;37;41m'),
])
def test_formatter_color_per_level(levelno, color):
    fmt = ColoredLevelFormatter(LOG_FORMAT, use_color=True)
    out = fmt.format(_record(levelno))
    assert color in out


def test_formatter_unknown_level_no_color():
    fmt = ColoredLevelFormatter(LOG_FORMAT, use_color=True)
    rec = _record(15)  # произвольный уровень вне словаря цветов
    rec.levelname = 'Level 15'
    out = fmt.format(rec)
    assert '\033' not in out
    assert rec.levelname == 'Level 15'


def test_formatter_restores_levelname_after_color():
    fmt = ColoredLevelFormatter(LOG_FORMAT, use_color=True)
    rec = _record(logging.ERROR)
    assert rec.levelname == 'ERROR'
    fmt.format(rec)
    # record не должен «портиться» после format
    assert rec.levelname == 'ERROR'


def test_formatter_restores_levelname_use_color_false():
    fmt = ColoredLevelFormatter(LOG_FORMAT, use_color=False)
    rec = _record(logging.WARNING)
    fmt.format(rec)
    assert rec.levelname == 'WARNING'


# --------------------------------------------------------------------------- #
# gui_log_html
# --------------------------------------------------------------------------- #

def test_gui_html_escapes_script_tag():
    out = gui_log_html('<script>alert(1)</script>')
    assert '<script>' not in out
    assert '&lt;script&gt;' in out


def test_gui_html_escapes_ampersand():
    out = gui_log_html('a & b')
    assert '&amp;' in out
    assert ' & ' not in out


def test_gui_html_escapes_quotes():
    assert '&quot;' in gui_log_html('say "hi"')
    assert '&#x27;' in gui_log_html("it's")


def test_gui_html_newline_to_br():
    out = gui_log_html('line1\nline2')
    assert '<br>' in out
    assert '\n' not in out


def test_gui_html_double_space_to_nbsp():
    out = gui_log_html('a  b')
    assert '&nbsp;&nbsp;' in out


def test_gui_html_wraps_in_span_and_sets_color():
    out = gui_log_html('операция завершена')
    assert out.startswith('<span style="color: ')
    assert out.endswith('</span>')
    assert _GUI_COLORS['success'] in out


def test_gui_html_default_color_for_neutral_text():
    assert _GUI_COLORS['default'] in gui_log_html('просто текст')


# --------------------------------------------------------------------------- #
# _classify_gui_log — все ветки и пограничные случаи
# --------------------------------------------------------------------------- #

def test_classify_default_empty_string():
    assert _classify_gui_log('') == 'default'


def test_classify_default_neutral():
    assert _classify_gui_log('какой-то текст') == 'default'


def test_classify_default_number_input():
    # gui_log_html приводит к str, но сам классификатор работает со строками
    assert _classify_gui_log('42') == 'default'


def test_classify_info_starts_with_zapusk():
    assert _classify_gui_log('запуск процесса') == 'info'


def test_classify_info_zapushcheno():
    assert _classify_gui_log('процесс запущено') == 'info'


def test_classify_success_starts_with_stable():
    assert _classify_gui_log('стабильные файлы: 5') == 'success'


def test_classify_success_operaciya_zavershena():
    assert _classify_gui_log('операция завершена') == 'success'


def test_classify_success_zaversheno_middle():
    assert _classify_gui_log('файл завершено успешно') == 'success'


def test_classify_success_status_ok():
    assert _classify_gui_log("'status': 'ok'") == 'success'


def test_classify_warning_starts_with_unstable():
    assert _classify_gui_log('нестабильные файлы: 2') == 'warning'


def test_classify_warning_nevalid():
    assert _classify_gui_log('обнаружен невалид') == 'warning'


def test_classify_warning_keyword():
    assert _classify_gui_log('warning: низкое качество') == 'warning'


def test_classify_error_oshibka():
    assert _classify_gui_log('ошибка в файле') == 'error'


def test_classify_error_starts_with_error():
    assert _classify_gui_log('Error: сбой') == 'error'


def test_classify_error_exception():
    assert _classify_gui_log('exception raised') == 'error'


def test_classify_error_failed():
    assert _classify_gui_log('failed to read') == 'error'


def test_classify_error_substring_error():
    # ' error' внутри текста
    assert _classify_gui_log('файл error при чтении') == 'error'


def test_classify_muted_ostanovleno():
    assert _classify_gui_log('служба остановлено') == 'muted'


def test_classify_error_wins_over_success():
    assert _classify_gui_log('операция завершена с ошибкой') == 'error'


def test_classify_error_wins_over_warning():
    assert _classify_gui_log('ошибка: невалидные данные') == 'error'


# --------------------------------------------------------------------------- #
# Документирование слабых мест (фактическое поведение) — НЕ баги
# --------------------------------------------------------------------------- #

def test_classify_zapushchen_not_matched():
    # «запущен» (распространённая форма) не ловится: проверяется только «запущено»
    assert _classify_gui_log('процесс запущен') == 'default'


def test_classify_zavershena_not_matched():
    # «завершена» не ловится: проверяется только форма «завершено»
    assert _classify_gui_log('обработка завершена') == 'default'