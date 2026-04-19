import logging
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
import os
from PIL import Image
try:
    from pyzbar.pyzbar import decode as _decode
except Exception:
    _decode = None
import cv2
import shutil
import numpy as np
from typing import Iterable, Optional, Callable


# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SUPPORTED_IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png'}
ROTATION_SEARCH_STAGES = (
    (90, -90, 180),
    (-30, -20, -10, 10, 20, 30),
    (-25, -15, -5, 5, 15, 25),
)


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
    if _decode is None:
        raise RuntimeError('Не найдена библиотека zbar. Установите её с помощью "pip install zbar"')
    return _decode(image)

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