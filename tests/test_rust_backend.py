# -*- coding: utf-8 -*-
"""Эквивалентность Python- и Rust-бэкендов pyqr_core.

Каждый тест гоняет одну и ту же операцию дважды — на чистом Python
(PYQR_BACKEND=python) и на Rust (PYQR_BACKEND=rust) — и сравнивает
результаты. Переключение бэкенда — monkeypatch.setenv внутри итерации
(фикстуры-двойники здесь не годятся: обе поставили бы env на весь тест).

Если pyqr_core не собран, весь файл пропускается.
"""
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


def _run_both(monkeypatch, fn):
    """fn(backend_name) -> результат; возвращает {'python':…, 'rust':…}."""
    out = {}
    for backend in ('python', 'rust'):
        monkeypatch.setenv('PYQR_BACKEND', backend)
        out[backend] = fn(backend)
    return out


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
def test_read_csv_equivalence(monkeypatch, csv_file, delimiter):
    import csv_utils

    def run(_backend):
        return csv_utils.read_csv(csv_file, 'код', ('Фамилия', 'Имя', 'Класс'),
                                  csv_delimiter=delimiter)

    results = _run_both(monkeypatch, run)
    assert results['python'] == results['rust']
    if delimiter in ('auto', ';'):
        assert results['python']['Иванов Пётр 9А']['код'] == [
            '1234000000001', 'https://x.example/2']


def test_read_csv_tab_content_equivalence(monkeypatch, tmp_path):
    import csv_utils
    path = tmp_path / 'tab.csv'
    path.write_text('Фамилия\tИмя\tкод\nИван\tИван\t111\n', encoding='utf-8')

    def run(_backend):
        return csv_utils.read_csv(str(path), 'код', ('Фамилия', 'Имя'),
                                  csv_delimiter='\t')

    results = _run_both(monkeypatch, run)
    assert results['python'] == results['rust']
    assert results['python']['Иван Иван']['код'] == ['111']


@pytest.mark.parametrize('email_field', ['email', ''])
def test_read_csv_with_email_equivalence(monkeypatch, csv_file, email_field):
    import csv_utils

    def run(_backend):
        return csv_utils.read_csv_with_email(csv_file, 'код',
                                             ('Фамилия', 'Имя', 'Класс'),
                                             email_field=email_field)

    results = _run_both(monkeypatch, run)
    assert results['python'][0] == results['rust'][0]
    assert results['python'][1] == results['rust'][1]


def test_read_csv_comma_delimiter_equivalence(monkeypatch, tmp_path):
    import csv_utils
    path = tmp_path / 'comma.csv'
    path.write_text('Фамилия,Имя,код\nИван,Иван,111\nИван,Иван,222\n',
                    encoding='utf-8')

    def run(_backend):
        return csv_utils.read_csv(str(path), 'код', ('Фамилия', 'Имя'))

    results = _run_both(monkeypatch, run)
    assert results['python'] == results['rust']
    assert results['python']['Иван Иван']['код'] == ['111', '222']


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


def test_decode_image_equivalence(monkeypatch, qr_files):
    import barcode_utils

    def run(_backend):
        return [[c for c, _ in barcode_utils.decode_image_cv(p, None)]
                for p in qr_files]

    results = _run_both(monkeypatch, run)
    assert results['python'] == results['rust']
    assert all(results['python']), 'фикстура ничего не декодировала'


def test_find_barcodes_in_files_equivalence(monkeypatch, qr_files):
    import barcode_utils

    def run(_backend):
        barcodes = barcode_utils.find_barcodes_in_files(list(qr_files),
                                                        max_workers=2)
        return {k: sorted(v) for k, v in barcodes.items()}

    results = _run_both(monkeypatch, run)
    assert results['python'] == results['rust']


def test_file_renamer_equivalence(monkeypatch, qr_files, tmp_path):
    import barcode_utils
    codes = [barcode_utils.decode_image_cv(p, None)[0][0] for p in qr_files]

    def run(backend):
        folder = tmp_path / ('renamer_' + backend)
        folder.mkdir()
        for path in qr_files:
            shutil.copy2(path, str(folder / os.path.basename(path)))
        barcodes = {}
        for code, path in zip(codes, qr_files):
            barcodes.setdefault(code, []).append(
                os.path.join(str(folder), os.path.basename(path)))
        barcode_utils.file_renamer(str(folder), barcodes)
        return sorted(os.listdir(str(folder)))

    results = _run_both(monkeypatch, run)
    assert results['python'] == results['rust']
    # '/'+ '\\' + '\x00' в коде санитизируются под имя файла
    assert all('/' not in name and '\\' not in name for name in results['rust'])
    assert len(results['rust']) == 2


def test_split_by_student_folders_equivalence(monkeypatch, qr_files, tmp_path):
    import barcode_utils
    codes = [barcode_utils.decode_image_cv(p, None)[0][0] for p in qr_files]
    student_data = {'Иван Иванов 9А': {'код': list(codes)}}
    barcodes = {codes[0]: [qr_files[0]], codes[1]: [qr_files[1]]}

    def run(backend):
        out = tmp_path / ('out_' + backend)
        barcode_utils.split_by_student_folders(barcodes, student_data,
                                               str(out), max_workers=2)
        return sorted(
            os.path.relpath(os.path.join(dirpath, f), str(out))
            for dirpath, _, files in os.walk(str(out)) for f in files)

    results = _run_both(monkeypatch, run)
    assert results['python'] == results['rust']
    assert len(results['rust']) == 2


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


def test_move_unfound_equivalence(monkeypatch, tmp_path, fileops_tree):
    import file_utils
    images, data, barcodes = fileops_tree

    def run(backend):
        src = tmp_path / ('imgs_' + backend)
        shutil.copytree(images, str(src))
        bc = {code: [str(src / os.path.basename(p)) for p in paths]
              for code, paths in barcodes.items()}
        out = tmp_path / ('unfound_' + backend)
        file_utils.move_unfound(bc, data, str(out), move_mode='copy')
        return sorted(os.listdir(os.path.join(str(out), 'unsorted')))

    results = _run_both(monkeypatch, run)
    assert results['python'] == results['rust'] == ['img_1.jpg', 'img_2.jpg']


def test_move_clear_equivalence(monkeypatch, tmp_path, fileops_tree):
    import file_utils
    images, data, barcodes = fileops_tree

    def run(backend):
        src = tmp_path / ('imgs_' + backend)
        shutil.copytree(images, str(src))
        found = [str(src / os.path.basename(p))
                 for paths in barcodes.values() for p in paths]
        out = tmp_path / ('clear_' + backend)
        file_utils.move_clear(str(out), str(src), found, move_mode='copy')
        return sorted(os.listdir(os.path.join(str(out), 'noqrcode')))

    results = _run_both(monkeypatch, run)
    assert results['python'] == results['rust'] == ['img_3.jpg']


def test_check_pairing_equivalence(monkeypatch, tmp_path, caplog):
    import file_utils
    folder = tmp_path / 'pairing'
    folder.mkdir()
    for name in ('100-1.jpg', '100-2.jpg', '101-1.jpg', '101-3.jpg',
                 '102-2.jpg', '102-4.jpg'):
        (folder / name).write_bytes(b'x')

    def run(_backend):
        caplog.clear()
        with caplog.at_level('WARNING'):
            file_utils.check_pairing(str(folder))
        return sorted(caplog.messages)

    results = _run_both(monkeypatch, run)
    assert results['python'] == results['rust']
    # 101: нет 2 и нет 4; 102: нет 1 и нет 3 → 4 предупреждения
    assert len(results['rust']) == 4


# ----------------------------------------------------------------- zip

def test_zip_equivalence(monkeypatch, tmp_path):
    import zip_utils

    def run(backend):
        src = tmp_path / ('students_' + backend)
        (src / 'Иван Иванов').mkdir(parents=True)
        (src / 'unsorted').mkdir()
        for i in range(3):
            (src / 'Иван Иванов' / ('f{}.txt'.format(i))).write_text('x' * 100)
        result = zip_utils.zip_student_folders(str(src), max_workers=2)
        return (sorted(result),
                sorted(zipfile.ZipFile(result['Иван Иванов']).namelist()))

    results = _run_both(monkeypatch, run)
    assert results['python'] == results['rust']


def test_zip_freshness_equivalence(monkeypatch, tmp_path):
    import zip_utils

    def run(_backend):
        src = tmp_path / 'students'
        if src.exists():
            shutil.rmtree(str(src))
        (src / 'Иван').mkdir(parents=True)
        (src / 'Иван' / 'f.txt').write_text('data')
        zip_utils.zip_student_folders(str(src))
        assert os.path.exists(str(src / 'Иван.zip'))
        before = os.path.getmtime(str(src / 'Иван.zip'))
        zip_utils.zip_student_folders(str(src))
        return os.path.getmtime(str(src / 'Иван.zip')) == before

    results = _run_both(monkeypatch, run)
    assert results['python'] == results['rust'] is True


# --------------------------------------------------------------- state

def test_state_equivalence(monkeypatch, tmp_path):
    import processing_state
    records = [
        ('hash-{}'.format(i), 'qr-{}'.format(i % 3), 'file_{}.png'.format(i),
         'processed' if i % 2 else 'pending', '')
        for i in range(200)
    ]

    def run(backend):
        db_path = tmp_path / ('state_{}.sqlite'.format(backend))
        state = processing_state.ProcessingState(str(db_path))
        state.add_records(records)
        state.add_record('hash-x', 'qr-x', 'file_x.png', 'failed', 'out/path')
        return {
            'existing': sorted(state.get_existing_file_hashes(
                ['hash-1', 'hash-199', 'hash-nope', ''])),
            'pairs': sorted(state.get_existing_qr_hash_pairs(
                [('qr-0', 'hash-0'), ('qr-1', 'hash-1'), ('qr-x', 'hash-nope')])),
            'stats': dict(state.stats()),
            'has': (state.has_file_hash('hash-5'),
                    state.has_qr_for_hash('qr-2', 'hash-2'),
                    state.has_file_hash('nope')),
        }

    results = _run_both(monkeypatch, run)
    assert results['python'] == results['rust']
    assert results['python']['stats']['processed'] == 100
    assert results['python']['stats']['failed'] == 1


def test_state_schema_parity(monkeypatch, tmp_path):
    import processing_state

    def run(backend):
        db_path = tmp_path / ('schema_{}.sqlite'.format(backend))
        processing_state.ProcessingState(str(db_path))
        conn = sqlite3.connect(str(db_path))
        rows = conn.execute(
            "SELECT type, name, sql FROM sqlite_master "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name").fetchall()
        conn.close()
        return [(t, n, ' '.join((sql or '').split()))
                for t, n, sql in rows]

    results = _run_both(monkeypatch, run)
    assert results['python'] == results['rust']


def test_state_db_interop(monkeypatch, tmp_path):
    """База, созданная Rust-бэкендом, читается стандартным sqlite3-модулем."""
    import processing_state
    monkeypatch.setenv('PYQR_BACKEND', 'rust')
    db_path = tmp_path / 'interop.sqlite'
    state = processing_state.ProcessingState(str(db_path))
    state.add_record('h', 'q', 'f.png', 'processed')
    conn = sqlite3.connect(str(db_path))
    row = conn.execute(
        'SELECT file_hash, qr_value, file_path, status, output_path FROM processed_files'
    ).fetchone()
    conn.close()
    assert row == ('h', 'q', 'f.png', 'processed', '')
