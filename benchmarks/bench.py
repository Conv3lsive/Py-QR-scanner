# -*- coding: utf-8 -*-
"""Бенчмарк-харнесс: одинаковые нагрузки для Python- и Rust-бэкендов.

Запуск:
  ./bin/python benchmarks/bench.py --out benchmarks/results/python_baseline.json
  PYQR_BACKEND=rust ./bin/python benchmarks/bench.py --out benchmarks/results/rust.json

Бэкенд выбирается переменной окружения PYQR_BACKEND=python|rust|auto.
Результаты — JSON с median_ms и всеми прогонами.
"""
import argparse
import json
import os
import platform
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
import uuid

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

DATA = os.path.join(ROOT, 'benchmarks', 'data')

# Импортируем модули проекта (бэкенд выбирается внутри них по PYQR_BACKEND)
import app_config  # noqa: F401
import csv_utils
import barcode_utils
import file_utils
import zip_utils
import processing_state


def cpu_brand():
    try:
        out = subprocess.run(['sysctl', '-n', 'machdep.cpu.brand_string'],
                             capture_output=True, text=True, timeout=5)
        return out.stdout.strip()
    except Exception:
        return platform.processor() or 'unknown'


def git_commit():
    try:
        out = subprocess.run(['git', 'rev-parse', '--short', 'HEAD'],
                             capture_output=True, text=True, timeout=5, cwd=ROOT)
        return out.stdout.strip()
    except Exception:
        return 'unknown'


def backend_name():
    return os.environ.get('PYQR_BACKEND', 'auto')


def timed(fn, reps):
    runs = []
    for _ in range(reps):
        t0 = time.perf_counter()
        fn()
        dt = (time.perf_counter() - t0) * 1000.0
        runs.append(round(dt, 3))
    return {'median_ms': round(statistics.median(runs), 3), 'runs_ms': runs}


def fresh_dir(parent, name):
    path = os.path.join(parent, name)
    if os.path.isdir(path):
        shutil.rmtree(path)
    os.makedirs(path)
    return path


# ---------- подготовка нагрузок ----------

def load_qr_paths():
    qr_dir = os.path.join(DATA, 'qr')
    with open(os.path.join(qr_dir, 'codes.json'), encoding='utf-8') as fh:
        codes = json.load(fh)
    paths = [os.path.join(qr_dir, f) for f in sorted(codes)
             if f != 'codes.json']
    return paths, codes


def load_split_fixture(tmp):
    """student_data + barcodes для split_by_student_folders (50 студентов × 6 файлов)."""
    student_data = csv_utils.read_csv(
        os.path.join(DATA, 'csv', 'split_students.csv'), 'код',
        ('Фамилия', 'Имя', 'Класс'))
    qr_dir = os.path.join(DATA, 'qr')
    with open(os.path.join(qr_dir, 'codes.json'), encoding='utf-8') as fh:
        codes = json.load(fh)
    barcodes = {}
    for filename in sorted(codes):
        barcodes.setdefault(codes[filename], []).append(
            os.path.join(qr_dir, filename))
    return student_data, barcodes


def prepare_zip_fixture(parent):
    """30 папок студентов × 10 файлов; возвращает путь для zip_student_folders."""
    src = os.path.join(DATA, 'junk')
    files = sorted(os.listdir(src))
    out = fresh_dir(parent, 'zip_src')
    for s in range(30):
        student_dir = os.path.join(out, 'student_{:02d}'.format(s))
        os.makedirs(student_dir)
        for f_idx in range(10):
            shutil.copy2(os.path.join(src, files[f_idx]), student_dir)
    return out


# ---------- метрики ----------

def bench_csv(results, reps_light, reps_heavy):
    for label, fname, with_email in (
            ('csv_read_10k', 'big_10k.csv', False),
            ('csv_read_50k', 'big_50k.csv', False),
            ('csv_email_50k', 'big_50k.csv', True)):
        path = os.path.join(DATA, 'csv', fname)

        def run():
            csv_utils._CSV_CACHE.clear()
            if with_email:
                csv_utils.read_csv_with_email(path, 'код', ('Фамилия', 'Имя', 'Класс'),
                                              email_field='email')
            else:
                csv_utils.read_csv(path, 'код', ('Фамилия', 'Имя', 'Класс'))

        results[label] = timed(run, reps_light if '10k' in label else reps_heavy)


def bench_qr(results, tmp, reps_heavy):
    paths, _ = load_qr_paths()
    rotated = [p for i, p in enumerate(paths) if i % 3 == 0][:100]

    def run_all():
        barcode_utils.find_barcodes_in_files(paths, max_workers=6)

    def run_rotated():
        barcode_utils.find_barcodes_in_files(rotated, max_workers=6)

    results['qr_decode_300'] = timed(run_all, reps_heavy)
    results['qr_decode_rotated_100'] = timed(run_rotated, reps_heavy)


def bench_renamer(results, tmp, reps_heavy):
    qr_dir = os.path.join(DATA, 'qr')
    with open(os.path.join(qr_dir, 'codes.json'), encoding='utf-8') as fh:
        codes = json.load(fh)
    src_pool = [os.path.join(qr_dir, f) for f in sorted(codes) if f != 'codes.json']

    def run():
        folder = fresh_dir(tmp, 'renamer_src')
        for src in src_pool:
            shutil.copy2(src, folder)
        barcodes = {}
        for idx, src in enumerate(src_pool):
            code = codes[os.path.basename(src)]
            barcodes.setdefault(code, []).append(os.path.join(
                folder, os.path.basename(src)))
        barcode_utils.file_renamer(folder, barcodes)

    results['file_renamer_300'] = timed(run, reps_heavy)


def bench_split(results, tmp, reps_heavy):
    student_data, barcodes = load_split_fixture(tmp)

    def run():
        out = fresh_dir(tmp, 'split_out')
        barcode_utils.split_by_student_folders(barcodes, student_data, out,
                                               max_workers=6)

    results['split_files_300'] = timed(run, reps_heavy)


def bench_move_unfound(results, tmp, reps_light):
    src = os.path.join(DATA, 'junk')
    files = [os.path.join(src, f) for f in sorted(os.listdir(src))][:300]
    barcodes = {}
    for i, p in enumerate(files):
        barcodes.setdefault('UNKNOWN-{:03d}'.format(i), []).append(p)
    data = {'Студент': {'код': ['нет-такого-кода']}}

    def run():
        out = fresh_dir(tmp, 'unfound_out')
        file_utils.move_unfound(barcodes, data, out, move_mode='copy')

    results['move_unfound_300'] = timed(run, reps_light)


def bench_zip(results, tmp, reps_heavy):
    def run():
        out = fresh_dir(tmp, 'zip_out')
        src = prepare_zip_fixture(tmp)
        zip_utils.zip_student_folders(src, max_workers=6)
        shutil.rmtree(src)

    results['zip_30x10'] = timed(run, reps_heavy)


def bench_state(results, tmp, reps_light):
    records = [
        (uuid.uuid4().hex, 'code-{:06d}'.format(i), 'file_{:06d}.png'.format(i),
         'processed', '')
        for i in range(5000)
    ]

    def run():
        db = os.path.join(fresh_dir(tmp, 'state_db'), 'state.sqlite')
        state = processing_state.ProcessingState(db)
        state.add_records(records)
        hashes = [r[0] for r in records[:1000]]
        state.get_existing_file_hashes(hashes)

    results['state_add_5000'] = timed(run, reps_light)


def bench_pairing(results, reps_light):
    folder = os.path.join(DATA, 'pairing')

    def run():
        file_utils.check_pairing(folder)

    results['check_pairing_600'] = timed(run, reps_light)


# ---------- запуск ----------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--out', required=True)
    args = parser.parse_args()

    reps_light, reps_heavy = 5, 3
    results = {}
    tmp = tempfile.mkdtemp(prefix='pyqr_bench_')
    try:
        bench_csv(results, reps_light, reps_heavy)
        bench_qr(results, tmp, reps_heavy)
        bench_renamer(results, tmp, reps_heavy)
        bench_split(results, tmp, reps_heavy)
        bench_move_unfound(results, tmp, reps_light)
        bench_zip(results, tmp, reps_heavy)
        bench_state(results, tmp, reps_light)
        bench_pairing(results, reps_light)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    payload = {
        'meta': {
            'backend': backend_name(),
            'python': sys.version.split()[0],
            'cpu': cpu_brand(),
            'commit': git_commit(),
        },
        'results': results,
    }
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, 'w', encoding='utf-8') as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=1)
    for key in sorted(results):
        print('{:24s} {:>12.2f} ms'.format(key, results[key]['median_ms']))
    print('backend={}'.format(backend_name()), '->', args.out)


if __name__ == '__main__':
    main()
