import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from PIL import Image
from pyzbar.pyzbar import decode
import cv2
import shutil


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
    """Пробуем несколько углов для корректировки наклона изображения, если QR-код не найден с первого раза."""
    best_image = image
    best_data = None
    max_barcodes = 0

    # Пробуем повороты на 90 градусов
    for angle in [90, -90, 180]:
        rotated_image = rotate_image(image, angle)
        barcodes_data = decode(rotated_image)

        if len(barcodes_data) > max_barcodes:
            best_image = rotated_image
            best_data = barcodes_data
            max_barcodes = len(barcodes_data)

    for angle in range(-30, 31):
        rotated_image = rotate_image(image, angle)
        barcodes_data = decode(rotated_image)

        if len(barcodes_data) > max_barcodes:
            best_image = rotated_image
            best_data = barcodes_data
            max_barcodes = len(barcodes_data)

    return best_image, best_data

def decode_image_cv(path, image_folder):
    try:
        logger.info(f"Обрабатывается файл: {path}")
        image = Image.open(path)

        # Пробуем сначала распознать QR-код без поворота
        barcodes_data = decode(image)

        if not barcodes_data:
            # Если QR-код не найден, пробуем поворот изображения на 90°
            logger.info(f"QR код не найден, пробуем поворот изображения: {path}")
            best_image, barcodes_data = find_best_rotation(image)

        if len(barcodes_data) > 1:
            double_qr_folder = os.path.join(image_folder, 'double_qr')
            os.makedirs(double_qr_folder, exist_ok=True)
            new_path = os.path.join(double_qr_folder, os.path.basename(path))
            best_image.save(new_path)  # Сохраняем исправленное изображение
            logger.info(f"Изображение с несколькими QR-кодами перемещено в {new_path}")
            return []

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