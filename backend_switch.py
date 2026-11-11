# -*- coding: utf-8 -*-
"""Выбор вычислительного бэкенда: чистый Python или Rust-модуль pyqr_core.

Переменная окружения PYQR_BACKEND:
  python — принудительно чистый Python;
  rust   — принудительно Rust (ошибка, если pyqr_core не собран);
  auto / не задан — Rust, если модуль собран, иначе прозрачный фолбэк на Python.

Сборка Rust: cargo build --release в rust/pyqr-core, затем скопировать
target/release/libpyqr_core.dylib в корень проекта под именем pyqr_core.so.
"""
import os

try:
    import pyqr_core
except ImportError:  # .so не собран — деградируем на чистый Python
    pyqr_core = None

RUST_AVAILABLE = pyqr_core is not None


def use_rust():
    backend = (os.environ.get('PYQR_BACKEND') or 'auto').strip().lower()
    if backend == 'python':
        return False
    if backend == 'rust':
        if not RUST_AVAILABLE:
            raise RuntimeError(
                'PYQR_BACKEND=rust, но pyqr_core не собран: '
                'cargo build --release (в rust/pyqr-core) и скопировать '
                'target/release/libpyqr_core.dylib -> pyqr_core.so в корне'
            )
        return True
    return RUST_AVAILABLE
