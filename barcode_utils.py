import ctypes
import importlib
import logging
import os
import platform
import shutil
import sys
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, Optional

import cv2
import numpy as np
from PIL import Image


# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SUPPORTED_IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png'}
ROTATION_SEARCH_STAGES = (
    (90, -90, 180),
    (-30, -20, -10, 10, 20, 30),
    (-25, -15, -5, 5, 15, 25),
)
SYSTEM_ZBAR_LIBRARY_PATHS = {
    'Darwin': (
        '/opt/homebrew/lib/libzbar.dylib',
        '/opt/homebrew/opt/zbar/lib/libzbar.dylib',
        '/usr/local/lib/libzbar.dylib',
        '/usr/local/opt/zbar/lib/libzbar.dylib',
        '/opt/local/lib/libzbar.dylib',
    ),
    'Linux': (
        '/usr/lib/libzbar.so',
        '/usr/lib/libzbar.so.0',
        '/usr/lib64/libzbar.so',
        '/usr/lib64/libzbar.so.0',
        '/usr/local/lib/libzbar.so',
        '/usr/local/lib/libzbar.so.0',
        '/lib/x86_64-linux-gnu/libzbar.so.0',
        '/usr/lib/x86_64-linux-gnu/libzbar.so.0',
        '/lib/aarch64-linux-gnu/libzbar.so.0',
        '/usr/lib/aarch64-linux-gnu/libzbar.so.0',
    ),
}


@dataclass(frozen=True)
class DecodedValue:
    data: bytes


def _iter_zbar_library_candidates():
    seen = set()

    for env_name in ('ZBAR_LIB_PATH', 'LIBZBAR_PATH'):
        raw_path = (os.environ.get(env_name) or '').strip()
        if not raw_path:
            continue

        candidate = Path(raw_path).expanduser()
        candidate_key = str(candidate)
        if candidate_key not in seen:
            seen.add(candidate_key)
            yield candidate

    if platform.system() == 'Darwin':
        brew_prefix = (os.environ.get('HOMEBREW_PREFIX') or '').strip()
        if brew_prefix:
            for raw_path in (
                os.path.join(brew_prefix, 'lib', 'libzbar.dylib'),
                os.path.join(brew_prefix, 'opt', 'zbar', 'lib', 'libzbar.dylib'),
            ):
                candidate = Path(raw_path)
                candidate_key = str(candidate)
                if candidate_key not in seen:
                    seen.add(candidate_key)
                    yield candidate

    for raw_path in SYSTEM_ZBAR_LIBRARY_PATHS.get(platform.system(), ()):
        candidate = Path(raw_path)
        candidate_key = str(candidate)
        if candidate_key not in seen:
            seen.add(candidate_key)
            yield candidate


def _load_system_zbar_library():
    load_errors = []

    for candidate in _iter_zbar_library_candidates():
        if not candidate.exists():
            continue

        try:
            return ctypes.CDLL(str(candidate)), str(candidate)
        except OSError as exc:
            load_errors.append(f'{candidate}: {exc}')

    if load_errors:
        raise RuntimeError('; '.join(load_errors))

    raise FileNotFoundError('Системная библиотека zbar не найдена в стандартных путях')


def _import_pyzbar_decode():
    pyzbar_module = importlib.import_module('pyzbar.pyzbar')
    return pyzbar_module.decode


def _load_pyzbar_decode():
    try:
        return _import_pyzbar_decode(), None
    except Exception as exc:
        initial_error = exc

    try:
        libzbar, library_path = _load_system_zbar_library()
    except Exception:
        return None, initial_error

    try:
        zbar_library = importlib.import_module('pyzbar.zbar_library')
        original_load = zbar_library.load
        zbar_library.load = lambda: (libzbar, [])
        sys.modules.pop('pyzbar.wrapper', None)
        sys.modules.pop('pyzbar.pyzbar', None)
        decode_func = _import_pyzbar_decode()
        logger.info('pyzbar подключен через системную библиотеку zbar: %s', library_path)
        return decode_func, None
    except Exception as exc:
        return None, exc
    finally:
        if 'zbar_library' in locals() and 'original_load' in locals():
            zbar_library.load = original_load


def _decode_with_opencv(image):
    detector = cv2.QRCodeDetector()
    image_bgr = cv2.cvtColor(np.array(image.convert('RGB')), cv2.COLOR_RGB2BGR)
    results = []

    try:
        found, decoded_values, _points, _ = detector.detectAndDecodeMulti(image_bgr)
    except Exception:
        found, decoded_values = False, []

    if found and decoded_values:
        for value in decoded_values:
            normalized = (value or '').strip()
            if normalized:
                results.append(DecodedValue(normalized.encode('utf-8')))

    if results:
        return results

    decoded_value, _points, _ = detector.detectAndDecode(image_bgr)
    normalized = (decoded_value or '').strip()
    if not normalized:
        return []

    return [DecodedValue(normalized.encode('utf-8'))]


def _build_decoder_error_message(error):
    install_hints = {
        'Darwin': 'macOS: установите системную библиотеку zbar через "brew install zbar".',
        'Linux': 'Linux: установите системную библиотеку zbar через пакетный менеджер, например "sudo apt install libzbar0".',
        'Windows': 'Windows: установите ZBar отдельно и добавьте DLL в PATH.',
    }

    base_message = 'Не удалось инициализировать pyzbar/zbar.'
    error_text = str(error).strip()
    if error_text:
        base_message = f'{base_message} {error_text}'

    platform_hint = install_hints.get(platform.system())
    if platform_hint:
        base_message = f'{base_message} {platform_hint}'

    return f'{base_message} Для QR-кодов будет использоваться резервное распознавание через OpenCV.'


_decode, _decode_error = _load_pyzbar_decode()
_decoder_warning_emitted = False


def _build_code_to_students(student_data):
    code_to_students = {}
    for student, codes in student_data.items():
        for code_list in codes.values():
            for code in code_list:
                if not code:
                    continue
                code_to_students.setdefault(code, set()).add(student)
    return code_to_students


def _emit_progress(progress_callback: Optional[Callable], done: int, total: int, unit: str, message: str):
    if progress_callback:
        progress_callback(done, total, unit, message)


def decode(image):
    global _decoder_warning_emitted

    if _decode is not None:
        try:
            decoded_values = _decode(image)
        except Exception as exc:
            logger.debug('pyzbar не смог декодировать изображение, используем OpenCV fallback: %s', exc)
        else:
            if decoded_values:
                return decoded_values

    if _decode_error is not None and not _decoder_warning_emitted:
        logger.warning(_build_decoder_error_message(_decode_error))
        _decoder_warning_emitted = True

    return _decode_with_opencv(image)

def rotate_image(image, angle):
    """Поворот изображения на заданный угол."""
    image_center = tuple(np.array(image.size) / 2)
    rotation_matrix = cv2.getRotationMatrix2D(image_center, angle, 1.0)
    rotated_image = cv2.warpAffine(np.array(image), rotation_matrix, image.size)
    return Image.fromarray(rotated_image)

def find_best_rotation(image):
    try:
        best_image = image
        best_data = []
        max_barcodes = 0

        for stage_angles in ROTATION_SEARCH_STAGES:
            stage_best_image = None
            stage_best_data = []
            stage_best_count = 0

            for angle in stage_angles:
                try:
                    rotated_image = rotate_image(image, angle)
                    barcodes_data = decode(rotated_image)
                    barcode_count = len(barcodes_data)
                    if barcode_count > stage_best_count:
                        stage_best_image = rotated_image
                        stage_best_data = barcodes_data
                        stage_best_count = barcode_count
                except Exception as e:
                    logger.warning(f"Ошибка поворота на {angle}°: {e}")

            if stage_best_count > max_barcodes:
                best_image = stage_best_image or best_image
                best_data = stage_best_data
                max_barcodes = stage_best_count

            if stage_best_count > 0:
                return best_image, best_data

        return best_image, best_data
    except Exception as e:
        logger.error(f"find_best_rotation: критическая ошибка: {e}")
        return image, []

def decode_image_cv(path, image_folder):
    try:
        logger.debug(f"Обрабатывается файл: {path}")
        with Image.open(path) as image:
            # Пробуем сначала распознать QR-код без поворота
            barcodes_data = decode(image)

            if not barcodes_data:
                logger.debug(f"QR код не найден, пробуем поворот изображения: {path}")
                _, barcodes_data = find_best_rotation(image)

        if not barcodes_data:
            logger.debug(f"QR код не найден в {path}")
            return []

        results = []
        seen_codes = set()
        for barcode in barcodes_data:
            try:
                barcode_data = barcode.data.decode('utf-8').strip()
            except Exception as e:
                logger.warning(f"Ошибка декодирования QR: {e}")
                continue

            if not barcode_data or barcode_data in seen_codes:
                continue

            seen_codes.add(barcode_data)
            logger.debug(f"QR код найден в {path}: {barcode_data}")
            results.append((barcode_data, path))
        return results
    except Exception as e:
        logger.error(f"Ошибка при обработке {path}: {e}")
        return []

def find_barcodes_in_files(file_paths: Iterable[str], max_workers=6, progress_callback=None):
    barcodes = {}
    image_files = list(dict.fromkeys(
        path for path in file_paths
        if os.path.isfile(path) and os.path.splitext(path)[1].lower() in SUPPORTED_IMAGE_EXTENSIONS
    ))

    if not image_files:
        _emit_progress(progress_callback, 0, 0, 'файлов', 'Нет изображений для распознавания')
        return barcodes

    total = len(image_files)
    completed = 0
    worker_count = max(1, min(max_workers, total))

    if worker_count == 1:
        for path in image_files:
            try:
                result_list = decode_image_cv(path, None)
                for barcode_data, img_path in result_list:
                    barcodes.setdefault(barcode_data, []).append(img_path)
            except Exception as e:
                logger.error(f"Ошибка при обработке {path}: {e}")

            completed += 1
            _emit_progress(progress_callback, completed, total, 'файлов', 'Распознавание QR/штрихкодов')
        return barcodes

    executor_cls = ThreadPoolExecutor if total <= 3 else ProcessPoolExecutor

    with executor_cls(max_workers=worker_count) as executor:
        future_to_path = {executor.submit(decode_image_cv, path, None): path for path in image_files}

        for future in as_completed(future_to_path):
            path = future_to_path[future]
            try:
                result_list = future.result()
                for result in result_list:
                    if isinstance(result, tuple) and len(result) == 2:
                        barcode_data, img_path = result
                        barcodes.setdefault(barcode_data, []).append(img_path)
                    else:
                        logger.warning(f"Неверный формат результата: {result}")
            except Exception as e:
                logger.error(f"Ошибка при обработке {path}: {e}")

            completed += 1
            _emit_progress(progress_callback, completed, total, 'файлов', 'Распознавание QR/штрихкодов')

    return barcodes

def find_barcodes(image_folder, max_workers=6, progress_callback=None):
    image_files = [os.path.join(image_folder, f) for f in os.listdir(image_folder)]
    return find_barcodes_in_files(image_files, max_workers=max_workers, progress_callback=progress_callback)


def file_renamer(image_folder, barcode, progress_callback=None):
    total = sum(len(paths) for paths in barcode.values())
    processed = 0

    if total == 0:
        _emit_progress(progress_callback, 0, 0, 'файлов', 'Нет файлов для переименования')

    for code, paths in barcode.items():
        for idx, path in enumerate(paths, start=1):
            try:
                new_name = f"{code}_{idx}.jpg"  # Новый формат имени с индексом
                new_path = os.path.join(image_folder, new_name)
                os.rename(path, new_path)
                logger.info(f"Файл {path} переименован в {new_name}")
            except Exception as e:
                logger.error(f"Ошибка переименования {path}: {e}")
            finally:
                processed += 1
                _emit_progress(progress_callback, processed, total, 'файлов', 'Переименование файлов')


# Функция распределения файлов по папкам студентов
def split_by_student_folders(barcodes, student_data, output_folder, max_workers=6, progress_callback=None):
    os.makedirs(output_folder, exist_ok=True)

    code_to_students = _build_code_to_students(student_data)
    student_folders = {}
    copy_jobs = []
    scheduled_jobs = set()

    def copy_to_student_folder(src_path, student_folder):
        try:
            filename = os.path.basename(src_path)
            dst_path = os.path.join(student_folder, filename)
            shutil.copy(src_path, dst_path)
            logger.info(f"Файл {filename} скопирован в {student_folder}")
        except Exception as e:
            logger.error(f"Ошибка копирования файла {src_path}: {e}")

    tasks = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for code, paths in barcodes.items():
            students = code_to_students.get(code)
            if not students:
                continue

            for student in students:
                student_folder = student_folders.get(student)
                if student_folder is None:
                    student_folder = os.path.join(output_folder, student)
                    os.makedirs(student_folder, exist_ok=True)
                    student_folders[student] = student_folder

                for src_path in paths:
                    job_key = (src_path, student_folder)
                    if job_key in scheduled_jobs:
                        continue
                    scheduled_jobs.add(job_key)
                    copy_jobs.append((src_path, student_folder))

        for src_path, student_folder in copy_jobs:
            tasks.append(executor.submit(copy_to_student_folder, src_path, student_folder))

        total = len(tasks)
        if total == 0:
            _emit_progress(progress_callback, 0, 0, 'файлов', 'Нет файлов для распределения')

        completed = 0
        for task in as_completed(tasks):
            task.result()
            completed += 1
            _emit_progress(progress_callback, completed, total, 'файлов', 'Распределение файлов по папкам')