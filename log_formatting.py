import html
import logging
import os


LOG_FORMAT = '%(asctime)s [%(levelname)s] %(message)s'

_RESET = '\033[0m'
_LEVEL_COLORS = {
    logging.DEBUG: '\033[2m',
    logging.INFO: '\033[32m',
    logging.WARNING: '\033[33m',
    logging.ERROR: '\033[31m',
    logging.CRITICAL: '\033[1;37;41m',
}

_GUI_COLORS = {
    'default': '#344054',
    'info': '#175cd3',
    'success': '#067647',
    'warning': '#b54708',
    'error': '#b42318',
    'muted': '#667085',
}


def stream_supports_color(stream):
    if os.environ.get('FORCE_COLOR') is not None:
        return True
    if os.environ.get('NO_COLOR') is not None:
        return False
    if os.environ.get('TERM') == 'dumb':
        return False
    return hasattr(stream, 'isatty') and stream.isatty()


class ColoredLevelFormatter(logging.Formatter):
    def __init__(self, fmt=None, datefmt=None, style='%', use_color=True):
        super().__init__(fmt=fmt, datefmt=datefmt, style=style)
        self.use_color = use_color

    def format(self, record):
        if not self.use_color:
            return super().format(record)

        color = _LEVEL_COLORS.get(record.levelno)
        if not color:
            return super().format(record)

        original_levelname = record.levelname
        record.levelname = f'{color}{original_levelname}{_RESET}'
        try:
            return super().format(record)
        finally:
            record.levelname = original_levelname


def gui_log_html(message):
    text = str(message)
    escaped = html.escape(text).replace('  ', '&nbsp;&nbsp;').replace('\n', '<br>')
    color = _GUI_COLORS[_classify_gui_log(text)]
    return f'<span style="color: {color};">{escaped}</span>'


def _classify_gui_log(message):
    lower = message.lower()
    stripped = lower.strip()

    if 'ошиб' in lower or stripped.startswith(('error', 'exception', 'failed')) or ' error' in lower:
        return 'error'
    if stripped.startswith('нестабильные файлы') or 'невалид' in lower or 'warning' in lower:
        return 'warning'
    if (
        stripped.startswith('стабильные файлы')
        or 'операция завершена' in lower
        or 'завершено' in lower
        or "'status': 'ok'" in lower
    ):
        return 'success'
    if stripped.startswith('запуск') or 'запущено' in lower:
        return 'info'
    if 'остановлено' in lower:
        return 'muted'
    return 'default'
