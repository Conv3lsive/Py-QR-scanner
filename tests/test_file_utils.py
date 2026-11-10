import logging
import os

import pytest

import file_utils


def _write(path, content=b'img'):
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)
    with open(path, 'wb') as f:
        f.write(content)
    return path


# ---------------------------------------------------------------- _safe_destination

def test_safe_destination_free_name(tmp_path):
    dst = file_utils._safe_destination(str(tmp_path), 'a.jpg')
    assert dst == os.path.join(str(tmp_path), 'a.jpg')
    assert not os.path.exists(dst)


def test_safe_destination_collisions(tmp_path):
    d = str(tmp_path)
    _write(os.path.join(d, 'a.jpg'))
    _write(os.path.join(d, 'a_1.jpg'))
    _write(os.path.join(d, 'a_2.jpg'))
    assert file_utils._safe_destination(d, 'a.jpg') == os.path.join(d, 'a_3.jpg')


def test_safe_destination_name_already_has_suffix(tmp_path):
    d = str(tmp_path)
    # имя уже с '_1' и оно свободно — возвращается как есть
    assert file_utils._safe_destination(d, 'photo_1.jpg') == os.path.join(d, 'photo_1.jpg')
    # но при занятом 'photo_1.jpg' -> 'photo_1_1.jpg'
    _write(os.path.join(d, 'photo_1.jpg'))
    assert file_utils._safe_destination(d, 'photo_1.jpg') == os.path.join(d, 'photo_1_1.jpg')


def test_safe_destination_no_extension(tmp_path):
    d = str(tmp_path)
    assert file_utils._safe_destination(d, 'README') == os.path.join(d, 'README')
    _write(os.path.join(d, 'README'))
    assert file_utils._safe_destination(d, 'README') == os.path.join(d, 'README_1')


# ---------------------------------------------------------------- get_all_files

def test_get_all_files_case_insensitive_and_filter(tmp_path):
    d = str(tmp_path)
    _write(os.path.join(d, 'a.jpg'))
    _write(os.path.join(d, 'b.JPG'))
    _write(os.path.join(d, 'c.PNG'))
    _write(os.path.join(d, 'd.jpeg'))
    _write(os.path.join(d, 'note.txt'))
    _write(os.path.join(d, 'noext'))
    # подкаталог (обычный) не должен попасть
    os.makedirs(os.path.join(d, 'subdir'))
    result = file_utils.get_all_files(d)
    assert result == {
        os.path.join(d, 'a.jpg'),
        os.path.join(d, 'b.JPG'),
        os.path.join(d, 'c.PNG'),
        os.path.join(d, 'd.jpeg'),
    }


def test_get_all_files_nonexistent_folder(tmp_path):
    with pytest.raises(FileNotFoundError):
        file_utils.get_all_files(os.path.join(str(tmp_path), 'missing'))


# ---------------------------------------------------------------- move_unfound

def test_move_unfound_copy(tmp_path):
    d = str(tmp_path)
    p_known = _write(os.path.join(d, 'known.jpg'))
    p_unknown = _write(os.path.join(d, 'unknown.jpg'))
    barcodes = {'CODE1': [p_known], 'CODE2': [p_unknown]}
    data = {'Студент': {'код': ['CODE1']}}
    out = os.path.join(d, 'out')
    file_utils.move_unfound(barcodes, data, out, move_mode='copy')
    unsorted = os.path.join(out, 'unsorted')
    assert os.path.isdir(unsorted)
    assert os.path.exists(os.path.join(unsorted, 'unknown.jpg'))
    # известный код не переносится
    assert not os.path.exists(os.path.join(unsorted, 'known.jpg'))
    # copy: источники на месте
    assert os.path.exists(p_known)
    assert os.path.exists(p_unknown)


def test_move_unfound_move_source_disappears(tmp_path):
    d = str(tmp_path)
    p_unknown = _write(os.path.join(d, 'unknown.jpg'))
    barcodes = {'CODE2': [p_unknown]}
    data = {}
    out = os.path.join(d, 'out')
    file_utils.move_unfound(barcodes, data, out, move_mode='move')
    assert not os.path.exists(p_unknown)
    assert os.path.exists(os.path.join(out, 'unsorted', 'unknown.jpg'))


def test_move_unfound_name_collision(tmp_path):
    d = str(tmp_path)
    _write(os.path.join(d, 'dir1', 'x.jpg'))
    _write(os.path.join(d, 'dir2', 'x.jpg'))
    p1 = os.path.join(d, 'dir1', 'x.jpg')
    p2 = os.path.join(d, 'dir2', 'x.jpg')
    barcodes = {'CODE2': [p1, p2]}
    out = os.path.join(d, 'out')
    file_utils.move_unfound(barcodes, {}, out, move_mode='copy')
    unsorted = os.path.join(out, 'unsorted')
    assert os.path.exists(os.path.join(unsorted, 'x.jpg'))
    assert os.path.exists(os.path.join(unsorted, 'x_1.jpg'))


# ---------------------------------------------------------------- move_clear

def test_move_clear_subtracts_found_files(tmp_path):
    d = str(tmp_path)
    found = _write(os.path.join(d, 'found.jpg'))
    clear = _write(os.path.join(d, 'clear.jpg'))
    _write(os.path.join(d, 'note.txt'))
    out = os.path.join(d, 'out')
    file_utils.move_clear(out, d, [found], move_mode='copy')
    noqrcode = os.path.join(out, 'noqrcode')
    assert os.path.exists(os.path.join(noqrcode, 'clear.jpg'))
    assert os.path.exists(os.path.join(noqrcode, 'found.jpg')) is False
    assert os.path.exists(os.path.join(noqrcode, 'note.txt')) is False
    # copy: источник на месте
    assert os.path.exists(clear)


def test_move_clear_move_mode(tmp_path):
    d = str(tmp_path)
    clear = _write(os.path.join(d, 'clear.jpg'))
    out = os.path.join(d, 'out')
    file_utils.move_clear(out, d, [], move_mode='move')
    assert not os.path.exists(clear)
    assert os.path.exists(os.path.join(out, 'noqrcode', 'clear.jpg'))


# ---------------------------------------------------------------- check_pairing

def _check(tmp_path, names):
    for n in names:
        _write(os.path.join(str(tmp_path), n))
    return tmp_path


def test_check_pairing_full_pairs_no_warning(caplog, tmp_path):
    _check(tmp_path, ['10-1.jpg', '10-2.jpg', '10-3.jpg', '10-4.jpg',
                      '11-1.jpg', '11-2.jpg'])
    with caplog.at_level(logging.WARNING):
        file_utils.check_pairing(str(tmp_path))
    assert caplog.text == ''


def test_check_pairing_incomplete_pairs(caplog, tmp_path):
    # 20: есть 1, нет 2; 21: есть 3, нет 4; 22: 1,2,3 нет 4
    _check(tmp_path, ['20-1.jpg', '21-3.jpg', '22-1.jpg', '22-2.jpg', '22-3.jpg'])
    with caplog.at_level(logging.WARNING):
        file_utils.check_pairing(str(tmp_path))
    text = caplog.text
    assert 'Неполная пара 1-2 для: 20' in text
    assert 'Неполная пара 2-3 для: 20' not in text
    assert 'Неполная пара 3-4 для: 21' in text
    assert 'Неполная пара 3-4 для: 22' in text
    assert 'Неполная пара 1-2 для: 22' not in text


def test_check_pairing_copy_suffix_in_regex(caplog, tmp_path):
    # 'N(1)' — копия. Две копии одной стороны (1(1), 1(2)) не образуют пару 1-2.
    _check(tmp_path, ['30-1(1).jpg', '30-2(1).jpg',  # полная пара с копиями
                      '31-1(1).jpg', '31-1(2).jpg'])  # только сторона 1, копии
    with caplog.at_level(logging.WARNING):
        file_utils.check_pairing(str(tmp_path))
    text = caplog.text
    assert '30' not in text
    assert 'Неполная пара 1-2 для: 31' in text


def test_check_pairing_ignores_non_jpg(caplog, tmp_path):
    _check(tmp_path, ['bar-1.png', 'foo.png', 'hello.txt', '100-1.jpeg', '100-2.jpeg'])
    with caplog.at_level(logging.WARNING):
        file_utils.check_pairing(str(tmp_path))
    assert caplog.text == ''