"""Тесты модуля barcode_utils.

Зона ответственности: чистая логика, парсинг, хелперы, нормализация,
диспетчеризация и агрегация результатов. Нативное декодирование (pyzbar/zbar)
недоступно в venv, поэтому деградация при импорте и OpenCV-fallback проверяются
через моки, без реального распознавания штрихкодов.
"""

import logging
from pathlib import Path

import numpy as np
import pytest
from PIL import Image

import barcode_utils as bu


# --- Вспомогательные классы -------------------------------------------------


class FakeDecoded:
    """Объект с полем .data (байты), имитирует pyzbar.Decoded / DecodedValue."""

    def __init__(self, data):
        self.data = data


class _FakeFuture:
    def __init__(self, result):
        self._result = result

    def result(self):
        return self._result


def _make_fake_executor_class():
    """Фабрика подставного исполнителя (для замены Thread/ProcessPoolExecutor)."""

    class _FakeExecutor:
        instances = []

        def __init__(self, max_workers=0):
            self.max_workers = max_workers
            self.submitted = []
            self.__class__.instances.append(self)

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def submit(self, fn, *args, **kwargs):
            fut = _FakeFuture(fn(*args, **kwargs))
            self.submitted.append((fn, args, kwargs))
            return fut

    return _FakeExecutor


def _make_rgb_image(h=100, w=60):
    """Nеквадратное RGB-изображение с красным маркером 20x20 в левом верхнем углу."""
    arr = np.zeros((h, w, 3), dtype=np.uint8)
    arr[0:20, 0:20] = [255, 0, 0]
    return Image.fromarray(arr)


def _red_mask(arr):
    return (arr[:, :, 0] > 200) & (arr[:, :, 1] < 50) & (arr[:, :, 2] < 50)


# --- Константы и состояние импорта ------------------------------------------


def test_supported_image_extensions_are_lowercase():
    assert bu.SUPPORTED_IMAGE_EXTENSIONS == {'.jpg', '.jpeg', '.png'}


def test_rotation_search_stages_structure():
    assert isinstance(bu.ROTATION_SEARCH_STAGES, tuple)
    for stage in bu.ROTATION_SEARCH_STAGES:
        assert isinstance(stage, tuple)
        assert all(isinstance(a, (int, float)) for a in stage)
        assert 0 not in stage  # нулевой угол пробуется отдельно в decode_image_cv


def test_module_degrades_gracefully_without_pyzbar():
    # pyzbar в этом venv не установлен: импорт не должен падать.
    assert bu._decode is None
    assert isinstance(bu._decode_error, Exception)


def test_decoded_value_is_frozen_byte_holder():
    d = bu.DecodedValue(data=b'abc')
    assert d.data == b'abc'
    assert bu.DecodedValue(b'abc') == d
    with pytest.raises(Exception):
        d.data = b'z'  # frozen dataclass


# --- _build_code_to_students -------------------------------------------------


def test_build_code_to_students_basic():
    data = {'Иванов': {'код': ['A1', 'A2']}, 'Петров': {'код': ['A2', 'B3']}}
    result = bu._build_code_to_students(data)
    assert result == {'A1': {'Иванов'}, 'A2': {'Иванов', 'Петров'}, 'B3': {'Петров'}}


def test_build_code_to_students_skips_empty_and_multi_field():
    data = {
        'Иванов': {'код': ['A1', '', None, 'A1']},
        'Петров': {'код': ['A1'], 'доп': ['C9']},
    }
    result = bu._build_code_to_students(data)
    assert result == {'A1': {'Иванов', 'Петров'}, 'C9': {'Петров'}}


def test_build_code_to_students_empty_input():
    assert bu._build_code_to_students({}) == {}


# --- _emit_progress ----------------------------------------------------------


def test_emit_progress_calls_callback_with_args():
    calls = []
    bu._emit_progress(lambda *a: calls.append(a), 2, 5, 'файлов', 'msg')
    assert calls == [(2, 5, 'файлов', 'msg')]


def test_emit_progress_none_callback_is_noop():
    bu._emit_progress(None, 1, 1, 'файлов', 'msg')  # не должно падать


# --- _iter_zbar_library_candidates --------------------------------------------


def test_iter_candidates_env_order_and_dedup(tmp_path, monkeypatch):
    p1 = tmp_path / 'lib_a.so'
    p2 = tmp_path / 'lib_b.so'
    monkeypatch.setenv('ZBAR_LIB_PATH', str(p1))
    monkeypatch.setenv('LIBZBAR_PATH', str(p1))  # дубликат p1
    monkeypatch.setattr(bu.platform, 'system', lambda: 'Linux')

    cands = list(bu._iter_zbar_library_candidates())

    assert Path(p1) in cands
    assert cands.count(Path(p1)) == 1  # дедупликация
    assert cands.index(Path(p1)) == 0  # env-кандидаты идут первыми
    # после env-кандидатов идут системные пути Linux
    assert cands[1:] == [Path(p) for p in bu.SYSTEM_ZBAR_LIBRARY_PATHS['Linux']]


def test_iter_candidates_homebrew_prefix_only_on_darwin(tmp_path, monkeypatch):
    prefix = tmp_path / 'brew'
    monkeypatch.setenv('HOMEBREW_PREFIX', str(prefix))
    monkeypatch.delenv('ZBAR_LIB_PATH', raising=False)
    monkeypatch.delenv('LIBZBAR_PATH', raising=False)

    monkeypatch.setattr(bu.platform, 'system', lambda: 'Darwin')
    darwin_cands = list(bu._iter_zbar_library_candidates())
    assert Path(prefix, 'lib', 'libzbar.dylib') in darwin_cands

    monkeypatch.setattr(bu.platform, 'system', lambda: 'Linux')
    linux_cands = list(bu._iter_zbar_library_candidates())
    assert all('brew' not in str(c) for c in linux_cands)


def test_iter_candidates_ignores_blank_env(tmp_path, monkeypatch):
    monkeypatch.setenv('ZBAR_LIB_PATH', '   ')
    monkeypatch.setenv('LIBZBAR_PATH', '')
    monkeypatch.setattr(bu.platform, 'system', lambda: 'Linux')
    cands = list(bu._iter_zbar_library_candidates())
    assert len(cands) == len(bu.SYSTEM_ZBAR_LIBRARY_PATHS['Linux'])


# --- _load_system_zbar_library -----------------------------------------------


def test_load_system_zbar_no_candidates_raises_filenotfound(monkeypatch):
    monkeypatch.setattr(bu, '_iter_zbar_library_candidates', lambda: iter(()))
    with pytest.raises(FileNotFoundError):
        bu._load_system_zbar_library()


def test_load_system_zbar_cdll_failure_raises_runtimeerror(tmp_path, monkeypatch):
    fake_lib = tmp_path / 'libzbar.so'
    fake_lib.write_bytes(b'not a real library')
    monkeypatch.setattr(
        bu, '_iter_zbar_library_candidates', lambda: iter((fake_lib,))
    )

    def boom(path):
        raise OSError('cannot load')

    monkeypatch.setattr(bu.ctypes, 'CDLL', boom)
    with pytest.raises(RuntimeError) as excinfo:
        bu._load_system_zbar_library()
    assert 'cannot load' in str(excinfo.value)


# --- _build_decoder_error_message --------------------------------------------


def test_decoder_error_message_darwin_hint(monkeypatch):
    monkeypatch.setattr(bu.platform, 'system', lambda: 'Darwin')
    msg = bu._build_decoder_error_message(RuntimeError('boom'))
    assert 'Не удалось инициализировать pyzbar/zbar.' in msg
    assert 'boom' in msg
    assert 'brew install zbar' in msg
    assert 'резервное распознавание через OpenCV' in msg


def test_decoder_error_message_linux_hint(monkeypatch):
    monkeypatch.setattr(bu.platform, 'system', lambda: 'Linux')
    msg = bu._build_decoder_error_message(RuntimeError('x'))
    assert 'apt install libzbar0' in msg


def test_decoder_error_message_unknown_platform_no_hint(monkeypatch):
    monkeypatch.setattr(bu.platform, 'system', lambda: 'Haiku')
    msg = bu._build_decoder_error_message(RuntimeError('x'))
    assert 'brew install' not in msg
    assert 'apt install' not in msg


# --- decode ------------------------------------------------------------------


def test_decode_uses_pyzbar_result_when_available(monkeypatch):
    monkeypatch.setattr(bu, '_decode', lambda img: [FakeDecoded(b'HELLO')])
    monkeypatch.setattr(bu, '_decode_error', None)
    monkeypatch.setattr(bu, '_decoder_warning_emitted', False)

    result = bu.decode(object())

    assert [r.data for r in result] == [b'HELLO']
    assert bu._decoder_warning_emitted is False  # предупреждение не требуется


def test_decode_falls_back_to_opencv_when_pyzbar_raises(monkeypatch):
    def broken(img):
        raise RuntimeError('zbar crash')

    monkeypatch.setattr(bu, '_decode', broken)
    monkeypatch.setattr(bu, '_decode_error', None)
    monkeypatch.setattr(bu, '_decode_with_opencv', lambda img: ['FALLBACK'])

    assert bu.decode(object()) == ['FALLBACK']


def test_decode_falls_back_to_opencv_when_pyzbar_empty(monkeypatch):
    monkeypatch.setattr(bu, '_decode', lambda img: [])
    monkeypatch.setattr(bu, '_decode_error', None)
    monkeypatch.setattr(bu, '_decode_with_opencv', lambda img: ['FALLBACK'])

    assert bu.decode(object()) == ['FALLBACK']


def test_decode_warning_emitted_once(caplog, monkeypatch):
    monkeypatch.setattr(bu, '_decode', None)
    monkeypatch.setattr(bu, '_decode_error', RuntimeError('no zbar'))
    monkeypatch.setattr(bu, '_decoder_warning_emitted', False)
    monkeypatch.setattr(bu, '_decode_with_opencv', lambda img: [])

    with caplog.at_level(logging.WARNING, logger='barcode_utils'):
        bu.decode(object())
        bu.decode(object())

    warnings = [r for r in caplog.records if 'не удалось инициализировать' in r.message.lower()]
    assert len(warnings) == 1
    assert bu._decoder_warning_emitted is True


def test_decode_with_opencv_returns_empty_on_blank_image():
    # Реальный cv2-fallback, детерминированно: на пустом изображении нет кодов.
    img = Image.new('RGB', (60, 60), (255, 255, 255))
    assert bu._decode_with_opencv(img) == []


# --- rotate_image ------------------------------------------------------------


def test_rotate_image_180_moves_marker_to_opposite_corner():
    arr = np.zeros((100, 100, 3), dtype=np.uint8)
    arr[0:20, 0:20] = [255, 0, 0]
    img = Image.fromarray(arr)
    rot = bu.rotate_image(img, 180)
    assert rot.size == img.size
    mask = _red_mask(np.array(rot))
    ys, xs = np.nonzero(mask)
    # маркер из левого верхнего угла должен оказаться в правом нижнем
    assert xs.min() >= 80 and ys.min() >= 80


def test_rotate_image_90_places_marker_bottom_left_for_square():
    arr = np.zeros((100, 100, 3), dtype=np.uint8)
    arr[0:20, 0:20] = [255, 0, 0]
    img = Image.fromarray(arr)
    rot = bu.rotate_image(img, 90)
    mask = _red_mask(np.array(rot))
    ys, xs = np.nonzero(mask)
    assert xs.max() < 30 and ys.min() >= 80  # нижний левый угол


def test_rotate_image_90_swaps_dimensions_and_preserves_content():
    img = _make_rgb_image(100, 60)  # w=60, h=100
    rot = bu.rotate_image(img, 90)
    assert rot.size == (100, 60)  # ожидается смена размеров
    assert _red_mask(np.array(rot)).any()  # контент должен сохраниться


# --- find_best_rotation ------------------------------------------------------


def test_find_best_rotation_returns_first_productive_stage(monkeypatch):
    calls = []
    monkeypatch.setattr(bu, 'rotate_image', lambda img, angle: angle)

    def fake_decode(x):
        calls.append(x)
        return [FakeDecoded(b'X')] if x == 90 else []

    monkeypatch.setattr(bu, 'decode', fake_decode)

    best_image, best_data = bu.find_best_rotation(Image.new('RGB', (10, 10)))

    assert best_image == 90
    assert [d.data for d in best_data] == [b'X']
    # стадия (90, -90, 180) проходится целиком, пока не отработают все углы
    assert calls == [90, -90, 180]


def test_find_best_rotation_no_results_returns_original(monkeypatch):
    monkeypatch.setattr(bu, 'rotate_image', lambda img, angle: img)
    monkeypatch.setattr(bu, 'decode', lambda img: [])
    img = Image.new('RGB', (10, 10))

    best_image, best_data = bu.find_best_rotation(img)

    assert best_image is img
    assert best_data == []


def test_find_best_rotation_single_angle_crash_is_ignored(monkeypatch):
    # падение на одном угле не должно прерывать поиск
    def fake_rotate(img, angle):
        if angle == 180:
            raise RuntimeError('bad angle')
        return angle

    monkeypatch.setattr(bu, 'rotate_image', fake_rotate)
    monkeypatch.setattr(bu, 'decode', lambda x: [FakeDecoded(b'Y')] if x == -90 else [])

    best_image, best_data = bu.find_best_rotation(Image.new('RGB', (10, 10)))

    assert best_image == -90
    assert [d.data for d in best_data] == [b'Y']


# --- decode_image_cv ---------------------------------------------------------


def _write_png(tmp_path, name='img.png'):
    p = tmp_path / name
    Image.new('RGB', (20, 20), (0, 0, 0)).save(p)
    return str(p)


def test_decode_image_cv_returns_code_path_pairs(tmp_path, monkeypatch):
    path = _write_png(tmp_path)
    monkeypatch.setattr(bu, 'decode', lambda img: [FakeDecoded(b'HELLO')])

    assert bu.decode_image_cv(path, None) == [('HELLO', path)]


def test_decode_image_cv_dedups_codes_and_strips(tmp_path, monkeypatch):
    path = _write_png(tmp_path)
    monkeypatch.setattr(
        bu, 'decode', lambda img: [FakeDecoded(b'HELLO'), FakeDecoded(b'  HELLO ')]
    )
    assert bu.decode_image_cv(path, None) == [('HELLO', path)]


def test_decode_image_cv_skips_non_utf8_and_blank(tmp_path, monkeypatch):
    path = _write_png(tmp_path)
    monkeypatch.setattr(
        bu,
        'decode',
        lambda img: [FakeDecoded(b'\xff\xfe'), FakeDecoded(b'    '), FakeDecoded(b'OK')],
    )
    assert bu.decode_image_cv(path, None) == [('OK', path)]


def test_decode_image_cv_tries_rotation_when_direct_decode_empty(tmp_path, monkeypatch):
    path = _write_png(tmp_path)
    monkeypatch.setattr(bu, 'decode', lambda img: [])  # прямого нет
    monkeypatch.setattr(
        bu, 'find_best_rotation', lambda img: (img, [FakeDecoded(b'ROTATED')])
    )
    assert bu.decode_image_cv(path, None) == [('ROTATED', path)]


def test_decode_image_cv_missing_file_returns_empty(tmp_path):
    assert bu.decode_image_cv(str(tmp_path / 'nope.png'), None) == []


# --- find_barcodes_in_files --------------------------------------------------


def test_find_barcodes_in_files_empty_input_emits_progress(tmp_path):
    calls = []
    result = bu.find_barcodes_in_files(
        [], progress_callback=lambda *a: calls.append(a)
    )
    assert result == {}
    assert calls == [(0, 0, 'файлов', 'Нет изображений для распознавания')]


def test_find_barcodes_in_files_filters_non_images(tmp_path):
    calls = []
    txt = tmp_path / 'note.txt'
    txt.write_text('x')
    missing = tmp_path / 'ghost.jpg'
    # только валидные изображения с нужным расширением
    result = bu.find_barcodes_in_files(
        [str(txt), str(missing)], progress_callback=lambda *a: calls.append(a)
    )
    assert result == {}
    assert calls == [(0, 0, 'файлов', 'Нет изображений для распознавания')]


def test_find_barcodes_in_files_sequential_single_file(tmp_path, monkeypatch):
    path = _write_png(tmp_path)
    monkeypatch.setattr(bu, 'decode_image_cv', lambda p, folder: [(f'code-{p}', p)])

    progress = []
    result = bu.find_barcodes_in_files(
        [path], max_workers=6, progress_callback=lambda *a: progress.append(a)
    )

    assert result == {f'code-{path}': [path]}
    assert progress[-1] == (1, 1, 'файлов', 'Распознавание QR/штрихкодов')


def test_find_barcodes_in_files_uses_threadpool_for_small_batch(tmp_path, monkeypatch):
    paths = [_write_png(tmp_path, f'{i}.png') for i in range(2)]
    ThreadFake = _make_fake_executor_class()
    ProcessFake = _make_fake_executor_class()
    monkeypatch.setattr(bu, 'ThreadPoolExecutor', ThreadFake)
    monkeypatch.setattr(bu, 'ProcessPoolExecutor', ProcessFake)
    monkeypatch.setattr(bu, 'as_completed', lambda futures: iter(futures))
    monkeypatch.setattr(bu, 'decode_image_cv', lambda p, folder: [(f'c-{p}', p)])

    result = bu.find_barcodes_in_files(paths, max_workers=6)

    assert len(result) == 2
    assert ThreadFake.instances and not ProcessFake.instances


def test_find_barcodes_in_files_uses_processpool_for_large_batch(tmp_path, monkeypatch):
    paths = [_write_png(tmp_path, f'{i}.png') for i in range(5)]
    ThreadFake = _make_fake_executor_class()
    ProcessFake = _make_fake_executor_class()
    monkeypatch.setattr(bu, 'ThreadPoolExecutor', ThreadFake)
    monkeypatch.setattr(bu, 'ProcessPoolExecutor', ProcessFake)
    monkeypatch.setattr(bu, 'as_completed', lambda futures: iter(futures))
    monkeypatch.setattr(bu, 'decode_image_cv', lambda p, folder: [(f'c-{p}', p)])

    result = bu.find_barcodes_in_files(paths, max_workers=6)

    assert len(result) == 5
    assert ProcessFake.instances and not ThreadFake.instances


def test_find_barcodes_in_files_aggregates_same_code(tmp_path, monkeypatch):
    # один и тот же код на нескольких файлах должен собраться в общий список
    paths = [_write_png(tmp_path, f'{i}.png') for i in range(3)]
    ThreadFake = _make_fake_executor_class()
    monkeypatch.setattr(bu, 'ThreadPoolExecutor', ThreadFake)
    monkeypatch.setattr(bu, 'ProcessPoolExecutor', _make_fake_executor_class())
    monkeypatch.setattr(bu, 'as_completed', lambda futures: iter(futures))
    monkeypatch.setattr(bu, 'decode_image_cv', lambda p, folder: [('SAME', p)])

    result = bu.find_barcodes_in_files(paths, max_workers=6)

    assert set(result['SAME']) == set(paths)


def test_find_barcodes_in_files_dedups_input_paths(tmp_path, monkeypatch):
    path = _write_png(tmp_path)
    seen = []
    monkeypatch.setattr(bu, 'decode_image_cv', lambda p, folder: seen.append(p) or [])
    result = bu.find_barcodes_in_files([path, path, path], max_workers=6)
    assert result == {}
    assert len(seen) == 1  # дублирующиеся пути обрабатываются один раз


# --- find_barcodes (обёртка над списком файлов папки) ------------------------


def test_find_barcodes_lists_folder_and_delegates(tmp_path, monkeypatch):
    _write_png(tmp_path, 'a.png')
    _write_png(tmp_path, 'b.png')
    delegated = {}

    def fake_find_barcodes_in_files(file_paths, max_workers=6, progress_callback=None):
        delegated['paths'] = sorted(file_paths)
        delegated['max_workers'] = max_workers
        return {'CODE': sorted(file_paths)}

    monkeypatch.setattr(bu, 'find_barcodes_in_files', fake_find_barcodes_in_files)
    result = bu.find_barcodes(str(tmp_path), max_workers=3)

    assert result == {'CODE': sorted([
        str(tmp_path / 'a.png'), str(tmp_path / 'b.png'),
    ])}
    assert delegated['max_workers'] == 3
    assert len(delegated['paths']) == 2


# --- file_renamer ------------------------------------------------------------


def test_file_renamer_renames_with_numbering_and_extensions(tmp_path):
    a = tmp_path / 'a.jpg'
    b = tmp_path / 'b.png'
    c = tmp_path / 'noext'
    a.write_bytes(b'x')
    b.write_bytes(b'y')
    c.write_bytes(b'z')

    barcodes = {'PHOTO': [str(a), str(b)], 'X': [str(c)]}
    bu.file_renamer(str(tmp_path), barcodes)

    assert (tmp_path / 'PHOTO_1.jpg').exists()
    assert (tmp_path / 'PHOTO_2.png').exists()
    assert (tmp_path / 'X_1.jpg').exists()  # нет расширения -> .jpg по умолчанию
    assert not a.exists() and not b.exists() and not c.exists()


def test_file_renamer_empty_emits_progress(tmp_path):
    calls = []
    bu.file_renamer(str(tmp_path), {}, progress_callback=lambda *a: calls.append(a))
    assert calls == [(0, 0, 'файлов', 'Нет файлов для переименования')]


def test_file_renamer_missing_source_does_not_crash(tmp_path):
    # отсутствующий исходник логируется, но не роняет остальные переименования
    p = tmp_path / 'ok.jpg'
    p.write_bytes(b'x')
    bu.file_renamer(
        str(tmp_path),
        {'C': [str(tmp_path / 'missing.jpg'), str(p)]},
    )
    assert (tmp_path / 'C_2.jpg').exists()


def test_file_renamer_handles_code_with_slash(tmp_path):
    p = tmp_path / 'photo.jpg'
    p.write_bytes(b'x')
    bu.file_renamer(str(tmp_path), {'https://example.com/abc': [str(p)]})
    assert not p.exists()  # файл должен быть переименован


# --- split_by_student_folders ------------------------------------------------


def test_split_by_student_folders_copies_to_students(tmp_path):
    src = tmp_path / 'work.jpg'
    src.write_bytes(b'img')
    out = tmp_path / 'out'

    student_data = {
        'Иванов': {'код': ['A1']},
        'Петров': {'код': ['B2']},
    }
    bu.split_by_student_folders({'A1': [str(src)], 'B2': [str(src)]}, student_data, str(out))

    assert (out / 'Иванов' / 'work.jpg').exists()
    assert (out / 'Петров' / 'work.jpg').exists()


def test_split_by_student_folders_shared_code_copies_to_all(tmp_path):
    src = tmp_path / 'w.jpg'
    src.write_bytes(b'x')
    out = tmp_path / 'out'
    # один код принадлежит двум студентам -> копируется обоим
    student_data = {'A': {'код': ['K']}, 'B': {'код': ['K']}}
    bu.split_by_student_folders({'K': [str(src)]}, student_data, str(out))
    assert (out / 'A' / 'w.jpg').exists()
    assert (out / 'B' / 'w.jpg').exists()


def _make_collision_sources(tmp_path):
    dir_a = tmp_path / 'a'
    dir_b = tmp_path / 'b'
    dir_a.mkdir()
    dir_b.mkdir()
    sa = dir_a / 'same.jpg'
    sb = dir_b / 'same.jpg'
    sa.write_bytes(b'1')
    sb.write_bytes(b'2')
    return sa, sb


def test_split_by_student_folders_collision_suffix_sequential(tmp_path):
    # При однопоточном копировании суффикс _1 должен добавиться детерминированно.
    sa, sb = _make_collision_sources(tmp_path)
    out = tmp_path / 'out'
    student_data = {'S': {'код': ['K']}}

    bu.split_by_student_folders(
        {'K': [str(sa), str(sb)]}, student_data, str(out), max_workers=1
    )

    assert (out / 'S' / 'same.jpg').exists()
    assert (out / 'S' / 'same_1.jpg').exists()


def test_split_by_student_folders_collision_no_overwrite_under_concurrency(tmp_path, monkeypatch):
    import threading

    sa, sb = _make_collision_sources(tmp_path)
    out = tmp_path / 'out'
    student_data = {'S': {'код': ['K']}}

    # Заставляем обе задачи одновременно дойти до shutil.copy2 (барьер),
    # чтобы гарантированно воспроизвести гонку exists() -> copy.
    original_copy2 = bu.shutil.copy2
    barrier = threading.Barrier(2)

    def gated_copy2(src, dst, *args, **kwargs):
        barrier.wait(timeout=10)
        return original_copy2(src, dst, *args, **kwargs)

    monkeypatch.setattr(bu.shutil, 'copy2', gated_copy2)

    bu.split_by_student_folders({'K': [str(sa), str(sb)]}, student_data, str(out))

    # Ожидается: оба файла сохранены (same.jpg + same_1.jpg), данных не потеряно.
    assert (out / 'S' / 'same.jpg').exists()
    assert (out / 'S' / 'same_1.jpg').exists()


def test_split_by_student_folders_unknown_code_skipped(tmp_path):
    src = tmp_path / 'w.jpg'
    src.write_bytes(b'x')
    out = tmp_path / 'out'
    bu.split_by_student_folders({'UNKNOWN': [str(src)]}, {'S': {'код': ['K']}}, str(out))
    assert not (out / 'S').exists()


def test_split_by_student_folders_nothing_scheduled_emits_progress(tmp_path):
    calls = []
    out = tmp_path / 'out'
    bu.split_by_student_folders({}, {}, str(out), progress_callback=lambda *a: calls.append(a))
    assert calls == [(0, 0, 'файлов', 'Нет файлов для распределения')]