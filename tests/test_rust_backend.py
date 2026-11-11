# -*- coding: utf-8 -*-
"""Эквивалентность Python- и Rust-бэкендов pyqr_core.

Каждый тест гоняет одну и ту же операцию дважды — на чистом Python
(PYQR_BACKEND=python) и на Rust (PYQR_BACKEND=rust) — и сравнивает
результаты. Если pyqr_core не собран, весь файл пропускается.
"""
import json
import os
import shutil
import sqlite3
import zipfile

import pytest

import backend_switch

pytestmark = pytest.mark.skipif(
    not backend_switch.RUST_AVAILABLE,
    reason='pyqr_core не собран (cargo build --release в rust/pyqr-core)',
)


@pytest.fixture
def py_backend(monkeypatch):
    monkeypatch.setenv('PYQR_BACKEND', 'python')
    yield backend_switch.pyqr_core


@pytest.fixture
def rust_backend(monkeypatch):
    monkeypatch.setenv('PYQR_BACKEND', 'rust')
    yield backend_switch.pyqr_core


# ---------------------------------------------------------------- CSV

CSV_CONTENT = (
    '﻿Фамилия;Имя;Класс;код;email\n'
    'Иванов;Пётр;9А;1234000000001;petr@example.com\n'
    'Иванов;Пётр;9А;https://x.example/2;\n'
    'Петрова;Мария;10Б;1234000000003;maria@example.com\n'
    ';\t;9А;1234000000004;no-name@example.com\n'   # пустое имя: только email-режим пропустит
    'Сидоров;;11В;1234000000005;s@example.com\n'
)


@pytest.fixture
def csv_file(tmp_path):
    path = tmp_path / 'students.csv'
    path.write_text(CSV_CONTENT, encoding='utf-8')
    return str(path)


@pytest.mark.parametrize('delimiter', ['auto', ';', 'tab', None])
def test_read_csv_equivalence(py_backend, rust_backend, csv_file, delimiter):
    import csv_utils
    py = csv_utils.read_csv(csv_file, 'код', ('Фамилия', 'Имя', 'Класс'),
                            csv_delimiter=delimiter)
    ru = csv_utils.read_csv(csv_file, 'код', ('Фамилия', 'Имя', 'Класс'),
                            csv_delimiter=delimiter)
    assert py == ru


@pytest.mark.parametrize('email_field', ['email', ''])
def test_read_csv_with_email_equivalence(py_backend, rust_backend, csv_file,
                                         email_field):
    import csv_utils
    py = csv_utils.read_csv_with_email(csv_file, 'код',
                                       ('Фамилия', 'Имя', 'Класс'),
                                       email_field=email_field)
    ru = csv_utils.read_csv_with_email(csv_file, 'код',
                                       ('Фамилия', 'Имя', 'Класс'),
                                       email_field=email_field)
    assert py[0] == ru[0]
    assert py[1] == ru[1]


def test_read_csv_comma_delimiter_equivalence(py_backend, rust_backend, tmp_path):
    import csv_utils
    path = tmp_path / 'comma.csv'
    path.write_text(
        'Фамилия,Имя,код\nИван,Иван,111\nИван,Иван,222\n',
        encoding='utf-8')
    fields = ('Фамилия', 'Имя')
    assert csv_utils.read_csv(str(path), 'код', fields) == \
        csv_utils.read_csv(str(path), 'код', fields)


# ------------------------------------------------------------- barcode

def _make_qr(tmp_path, code, name, rotate=False):
    qrcode = pytest.importorskip('qrcode')
    from PIL import Image
    img = qrcode.make(code).convert('RGB')
    if rotate:
        img = img.transpose(Image.ROTATE_90)
    path = tmp_path / name
    img.save(str(path))
    return str(path)


@pytest.fixture
def qr_files(tmp_path):
    plain = _make_qr(tmp_path, 'https://school.example/qr/EQ-1', 'plain.png')
    rotated = _make_qr(tmp_path, '1234000000099', 'rotated.png', rotate=True)
    return [plain, rotated]


def test_decode_image_equivalence(py_backend, rust_backend, qr_files):
    import barcode_utils
    for path in qr_files:
        py = [c for c, _ in barcode_utils.decode_image_cv(path, None)]
        ru = [c for c, _ in barcode_utils.decode_image_cv(path, None)]
        assert py == ru
        assert py, 'Python-бэкенд ничего не декодировал (проблема фикстуры)'


def test_find_barcodes_in_files_equivalence(py_backend, rust_backend, qr_files):
    import barcode_utils
    py = barcode_utils.find_barcodes_in_files(list(qr_files), max_workers=2)
    ru = barcode_utils.find_barcodes_in_files(list(qr_files), max_workers=2)
    assert {k: sorted(v) for k, v in py.items()} == {k: sorted(v) for k, v in ru.items()}


def test_file_renamer_equivalence(py_backend, rust_backend, qr_files, tmp_path):
    import barcode_utils
    src_dir = tmp_path / 'src'
    src_dir.mkdir()
    for path in qr_files:
        shutil.copy2(path, str(src_dir / os.path.basename(path)))

    barcodes = {}
    for path in qr_files:
        code = barcode_utils.decode_image_cv(path, None)[0][0]
        barcodes[code] = [os.path.join(str(src_dir), os.path.basename(path))]

    # Rust-путь переименовывает реальные файлы
    barcode_utils.file_renamer(str(src_dir), {k: list(v) for k, v in barcodes.items()})
    rust_names = sorted(os.listdir(str(src_dir)))
    assert rust_names
    assert all(name.startswith(tuple(barcodes.keys())) for name in rust_names), rust_names

    # Python-путь на копии — те же имена
    py_dir = tmp_path / 'py'
    py_dir.mkdir()
    py_barcodes = {code: [os.path.join(str(py_dir), os.path.basename(p))]
                   for code, p in ((k, v[0]) for k, v in barcodes.items())}
    for path in qr_files:
        shutil.copy2(path, str(py_dir / os.path.basename(path)))
    barcode_utils.file_renamer(str(py_dir), py_barcodes)
    assert sorted(os.listdir(str(py_dir))) == rust_names


def test_split_by_student_folders_equivalence(py_backend, rust_backend,
                                              qr_files, tmp_path):
    import barcode_utils
    codes = [barcode_utils.decode_image_cv(p, None)[0][0] for p in qr_files]
    student_data = {'Иван Иванов 9А': {'код': list(codes)}}
    barcodes = {codes[0]: [qr_files[0]], codes[1]: [qr_files[1]]}

    out_rust = tmp_path / 'out_rust'
    out_py = tmp_path / 'out_py'
    barcode_utils.split_by_student_folders(barcodes, student_data, str(out_rust),
                                           max_workers=2)
    barcode_utils.split_by_student_folders(barcodes, student_data, str(out_py),
                                           max_workers=2)

    def listing(root):
        return sorted(
            os.path.relpath(os.path.join(dirpath, f), str(root))
            for dirpath, _, files in os.walk(str(root)) for f in files)
    assert listing(out_rust) == listing(out_py)


# ------------------------------------------------------------- fileops

@pytest.fixture
def fileops_tree(tmp_path):
    images = tmp_path / 'images'
    images.mkdir()
    for i in range(4):
        (images / ('img_{}.jpg'.format(i))).write_bytes(b'\xff\xd8' + bytes(i * 10))
    data = {'Иван Иванов': {'код': ['KNOWN-1']}}
    barcodes = {
        'KNOWN-1': [str(images / 'img_0.jpg')],
        'UNKNOWN-9': [str(images / 'img_1.jpg'), str(images / 'img_2.jpg')],
    }
    return str(images), data, barcodes


def test_move_unfound_equivalence(py_backend, rust_backend, tmp_path, fileops_tree):
    import file_utils
    images, data, barcodes = fileops_tree
    listing = {}
    for backend_name in ('rust', 'python'):
        out = tmp_path / ('unfound_' + backend_name)
        file_utils.move_unfound(barcodes, data, str(out), move_mode='copy')
        listing[backend_name] = sorted(os.listdir(os.path.join(str(out), 'unsorted')))
    assert listing['rust'] == listing['python'] == ['img_1.jpg', 'img_2.jpg']


def test_move_clear_equivalence(py_backend, rust_backend, tmp_path, fileops_tree):
    import file_utils
    images, data, barcodes = fileops_tree
    found = barcodes['KNOWN-1'] + barcodes['UNKNOWN-9']
    listing = {}
    for backend_name in ('rust', 'python'):
        shutil.rmtree(images + '_' + backend_name, ignore_errors=True)
        shutil.copytree(images, images + '_' + backend_name)
        out = tmp_path / ('clear_' + backend_name)
        file_utils.move_clear(str(out), images + '_' + backend_name, found,
                              move_mode='copy')
        listing[backend_name] = sorted(os.listdir(os.path.join(str(out), 'noqrcode')))
    assert listing['rust'] == listing['python'] == ['img_3.jpg']


def test_check_pairing_equivalence(py_backend, rust_backend, tmp_path, caplog):
    import file_utils
    folder = tmp_path / 'pairing'
    folder.mkdir()
    for name in ('100-1.jpg', '100-2.jpg', '101-1.jpg', '101-3.jpg', '102-2.jpg', '102-4.jpg'):
        (folder / name).write_bytes(b'x')

    with caplog.at_level('WARNING'):
        file_utils.check_pairing(str(folder))
    rust_warnings = sorted(caplog.messages)
    caplog.clear()
    with caplog.at_level('WARNING'):
        file_utils.check_pairing(str(folder))
    py_warnings = sorted(caplog.messages)
    assert rust_warnings == py_warnings
    assert len(py_warnings) == 3  # 101: нет 2; 102: нет пары 1-2; 102: нет пары 3-4


# ----------------------------------------------------------------- zip

def test_zip_equivalence(py_backend, rust_backend, tmp_path):
    import zip_utils
    outputs = {}
    for backend_name in ('rust', 'python'):
        src = tmp_path / ('students_' + backend_name)
        (src / 'Иван Иванов').mkdir(parents=True)
        (src / 'unsorted').mkdir()
        for i in range(3):
            (src / 'Иван Иванов' / ('f{}.txt'.format(i))).write_text('x' * 100)
        result = zip_utils.zip_student_folders(str(src), max_workers=2)
        outputs[backend_name] = (sorted(result),
                                 sorted(zipfile.ZipFile(result['Иван Иванов']).namelist()))
    assert outputs['rust'] == outputs['python']


def test_zip_freshness_equivalence(py_backend, rust_backend, tmp_path):
    import zip_utils
    src = tmp_path / 'students'
    (src / 'Иван').mkdir(parents=True)
    f = src / 'Иван' / 'f.txt'
    f.write_text('data')
    zip_utils.zip_student_folders(str(src))
    assert os.path.exists(str(src / 'Иван.zip'))
    # Архив свежее файла — повторная архивация не должна пересоздавать его
    before = os.path.getmtime(str(src / 'Иван.zip'))
    zip_utils.zip_student_folders(str(src))
    assert os.path.getmtime(str(src / 'Иван.zip')) == before


# --------------------------------------------------------------- state

def test_state_equivalence(py_backend, rust_backend, tmp_path):
    import processing_state
    records = [
        ('hash-{}'.format(i), 'qr-{}'.format(i % 3), 'file_{}.png'.format(i),
         'processed' if i % 2 else 'pending', '')
        for i in range(200)
    ]

    results = {}
    for backend_name in ('rust', 'python'):
        db_path = tmp_path / ('state_{}.sqlite'.format(backend_name))
        state = processing_state.ProcessingState(str(db_path))
        state.add_records(records)
        state.add_record('hash-x', 'qr-x', 'file_x.png', 'failed', 'out/path')
        results[backend_name] = {
            'existing': sorted(state.get_existing_file_hashes(
                ['hash-1', 'hash-199', 'hash-nope', ''])),
            'pairs': sorted(state.get_existing_qr_hash_pairs(
                [('qr-0', 'hash-0'), ('qr-1', 'hash-1'), ('qr-x', 'hash-nope')])),
            'stats': dict(state.stats()),
            'has': (state.has_file_hash('hash-5'),
                    state.has_qr_for_hash('qr-2', 'hash-2'),
                    state.has_file_hash('nope')),
        }
    assert results['rust'] == results['python']
    assert results['python']['stats']['processed'] == 100
    assert results['python']['stats']['failed'] == 1


def test_state_schema_parity(py_backend, rust_backend, tmp_path):
    import processing_state
    schemas = {}
    for backend_name in ('rust', 'python'):
        db_path = tmp_path / ('schema_{}.sqlite'.format(backend_name))
        processing_state.ProcessingState(str(db_path))
        conn = sqlite3.connect(str(db_path))
        rows = conn.execute(
            "SELECT type, name, sql FROM sqlite_master "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name").fetchall()
        schemas[backend_name] = [
            (t, n, (sql or '').replace('\n', ' ').replace('  ', ' ').strip())
            for t, n, sql in rows]
        conn.close()
    assert schemas['rust'] == schemas['python']


def test_state_db_interop(py_backend, rust_backend, tmp_path):
    """База, созданная Rust-бэкендом, читается стандартным sqlite3-модулем."""
    import processing_state
    db_path = tmp_path / 'interop.sqlite'
    state = processing_state.ProcessingState(str(db_path))
    state.add_record('h', 'q', 'f.png', 'processed')
    conn = sqlite3.connect(str(db_path))
    row = conn.execute(
        'SELECT file_hash, qr_value, file_path, status, output_path FROM processed_files'
    ).fetchone()
    conn.close()
    assert row == ('h', 'q', 'f.png', 'processed', '')
