# -*- coding: utf-8 -*-
"""Автоматический подбор числа параллельных воркеров по железу."""
import os


def available_parallelism():
    """Число логических ядер CPU (не меньше 1)."""
    try:
        return max(1, os.cpu_count() or 1)
    except Exception:
        return 1


def auto_workers(total=None):
    """Оптимальное число воркеров: все логические ядра, но не больше объёма
    работы (total). При total<=0 возвращает число ядер."""
    cores = available_parallelism()
    if total is not None and total > 0:
        return max(1, min(cores, total))
    return cores