# -*- coding: utf-8 -*-
"""Точка входа замороженного (PyInstaller) билда Py-QR-scanner.

Подкладывает бандленый libzbar в ZBAR_LIB_PATH ДО импорта модулей проекта:
Rust-загрузчик pyqr_core и Python-fallback читают этот env при первом
обращении. Затем делегирует обычному CLI (main.main), включая --gui.
"""
import os
import sys

# Имя zbar-библиотеки в бандле (packaging/lib) по платформам.
_ZBAR_NAMES = (
    'libzbar.dylib',     # macOS
    'libzbar.so.0',      # Linux
    'libzbar-64.dll',    # Windows (из wheel pyzbar)
    'libzbar.dll',
    'libzbar.so',
)


def _bundle_dir():
    if hasattr(sys, '_MEIPASS'):  # onefile: каталог распаковки
        return sys._MEIPASS
    return os.path.dirname(os.path.abspath(sys.executable))


def _prime_zbar():
    base = _bundle_dir()
    for name in _ZBAR_NAMES:
        candidate = os.path.join(base, 'lib', name)
        if os.path.isfile(candidate):
            os.environ.setdefault('ZBAR_LIB_PATH', candidate)
            return


def main():
    _prime_zbar()

    # Двойной клик по приложению = запуск без аргументов — открываем GUI
    # сразу (иначе windowed-билд молча упадёт на parser.error «укажите
    # --action»).
    if len(sys.argv) == 1:
        sys.argv.append('--gui')

    import main as cli
    cli.main()


if __name__ == '__main__':
    main()
