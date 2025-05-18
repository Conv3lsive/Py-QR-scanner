import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from PIL import Image
from pyzbar.pyzbar import decode
import cv2
import shutil
import numpy as np


# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def rotate_image(image, angle):
    """Поворот изображения на заданный угол."""
    image_center = tuple(np.array(image.size) / 2)
    rotation_matrix = cv2.getRotationMatrix2D(image_center, angle, 1.0)
    rotated_image = cv2.warpAffine(np.array(image), rotation_matrix, image.size)
    return Image.fromarray(rotated_image)

def find_best_rotation(image):
    try:
        best_image = image
        best_data = None
        max_barcodes = 0

        for angle in [90, -90, 180]:
            try:
                rotated_image = rotate_image(image, angle)
                barcodes_data = decode(rotated_image)
                if len(barcodes_data) > max_barcodes:
                    best_image = rotated_image
                    best_data = barcodes_data
                    max_barcodes = len(barcodes_data)
            except Exception as e:
                logger.warning(f"Ошибка поворота на {angle}°: {e}")

        for angle in range(-30, 31):
            try:
                rotated_image = rotate_image(image, angle)
                barcodes_data = decode(rotated_image)
                if len(barcodes_data) > max_barcodes:
                    best_image = rotated_image
                    best_data = barcodes_data
                    max_barcodes = len(barcodes_data)
            except Exception as e:
                logger.warning(f"Ошибка поворота на {angle}°: {e}")

        return best_image, best_data
    except Exception as e:
        logger.error(f"find_best_rotation: критическая ошибка: {e}")
        return image, []

def decode_image_cv(path, image_folder):
    try:
        logger.info(f"Обрабатывается файл: {path}")
        with Image.open(path) as image:
            # Пробуем сначала распознать QR-код без поворота
            barcodes_data = decode(image)

            if not barcodes_data:
                logger.info(f"QR код не найден, пробуем поворот изображения: {path}")
                best_image, barcodes_data = find_best_rotation(image)
            else:
                best_image = image

            # Удалим дублирующиеся данные, если они есть
            unique_data = set()
            for barcode in barcodes_data:
                try:
                    data = barcode.data.decode('utf-8').strip()
                    if data not in unique_data:
                        unique_data.add(data)
                except Exception as e:
                    logger.warning(f"Ошибка декодирования QR: {e}")

        if not barcodes_data:
            logger.info(f"QR код не найден в {path}")
            return path, None

        results = []
        for barcode in barcodes_data:
            barcode_data = barcode.data.decode('utf-8')
            logger.info(f"QR код найден в {path}: {barcode_data}")
            results.append((barcode_data, path))
        return results
    except Exception as e:
        logger.error(f"Ошибка при обработке {path}: {e}")
        return []

from concurrent.futures import ThreadPoolExecutor, as_completed

def find_barcodes(image_folder):
    barcodes = {}
    image_files = [os.path.join(image_folder, f) for f in os.listdir(image_folder) if f.lower().endswith('.jpg')]

    with ThreadPoolExecutor() as executor:
        future_to_path = {executor.submit(decode_image_cv, path, image_folder): path for path in image_files}

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

    return barcodes


def file_renamer(image_folder, barcode):
    reversed_bc = {path: code for code, paths in barcode.items() for path in paths}
    for code, paths in barcode.items():
        for idx, path in enumerate(paths, start=1):
            try:
                new_name = f"{code}_{idx}.jpg"  # Новый формат имени с индексом
                new_path = os.path.join(image_folder, new_name)
                os.rename(path, new_path)
                logger.info(f"Файл {path} переименован в {new_name}")
            except Exception as e:
                logger.error(f"Ошибка переименования {path}: {e}")


# Функция распределения файлов по папкам студентов
def split_by_student_folders(barcodes, student_data, output_folder):
    os.makedirs(output_folder, exist_ok=True)
    unfound_folder = os.path.join(output_folder, 'unfound')
    os.makedirs(unfound_folder, exist_ok=True)

    def copy_to_student_folder(src_path, student_folder):
        try:
            filename = os.path.basename(src_path)
            dst_path = os.path.join(student_folder, filename)
            shutil.copy(src_path, dst_path)
            logger.info(f"Файл {filename} скопирован в {student_folder}")
        except Exception as e:
            logger.error(f"Ошибка копирования файла {src_path}: {e}")

    tasks = []
    with ThreadPoolExecutor() as executor:
        for student, codes in student_data.items():
            student_folder = os.path.join(output_folder, student)
            os.makedirs(student_folder, exist_ok=True)
            for code_list in codes.values():
                for code in code_list:
                    if code in barcodes:
                        for src_path in barcodes[code]:
                            tasks.append(executor.submit(copy_to_student_folder, src_path, student_folder))
        for task in tasks:
            task.result()