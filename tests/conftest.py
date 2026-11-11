import os

import pytest

# GUI-тесты должны работать без дисплея.
os.environ.setdefault('QT_QPA_PLATFORM', 'offscreen')

# По умолчанию сьют гоняет чистый Python-бэкенд; Rust-бэкенд включается
# явно (PYQR_BACKEND=rust) в тестах эквивалентности и бенчмарках.
os.environ.setdefault('PYQR_BACKEND', 'python')


@pytest.fixture(scope='session')
def qapp():
    """Единственный QApplication на всю сессию (offscreen)."""
    from PySide6.QtWidgets import QApplication

    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    yield app
