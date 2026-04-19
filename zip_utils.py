import os
import zipfile
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed


EXCLUDED_ARCHIVE_FOLDERS = {"unsorted", "noqrcode", "unfound", "state"}


def _latest_source_mtime(student_path):
    latest_mtime = os.path.getmtime(student_path)
    for root, _, files in os.walk(student_path):
        for file in files:
            full_path = os.path.join(root, file)
            latest_mtime = max(latest_mtime, os.path.getmtime(full_path))
    return latest_mtime


def _zip_one_student(output_folder, student):
    student_path = os.path.join(output_folder, student)
    zip_path = os.path.join(output_folder, f"{student}.zip")

    try:
        latest_mtime = _latest_source_mtime(student_path)
        if os.path.exists(zip_path) and os.path.getmtime(zip_path) >= latest_mtime:
            logging.info(f"Архив {zip_path} уже актуален")
            return student, zip_path

        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_STORED) as zipf:
            for root, _, files in os.walk(student_path):
                for file in files:
                    full_path = os.path.join(root, file)
                    rel_path = os.path.relpath(full_path, start=student_path)
                    zipf.write(full_path, arcname=rel_path)

        logging.info(f"Папка {student} заархивирована как {zip_path}")
        return student, zip_path
    except Exception as e:
        logging.error(f"Ошибка при архивации папки {student}: {e}")
        return student, None


def zip_student_folders(output_folder, max_workers=6, progress_callback=None):
    if not os.path.isdir(output_folder):
        logging.warning(f"Папка для архивации не найдена: {output_folder}")
        return {}

    students = [
        student
        for student in os.listdir(output_folder)
        if os.path.isdir(os.path.join(output_folder, student)) and student not in EXCLUDED_ARCHIVE_FOLDERS
    ]
    total = len(students)

    if total == 0:
        logging.info("Архивация: нет папок студентов для обработки")
        if progress_callback:
            progress_callback(0, 0, 'папок', 'Нет папок для архивации')
        return {}

    zipped_paths = {}
    completed = 0

    with ThreadPoolExecutor(max_workers=max(1, max_workers)) as executor:
        futures = {
            executor.submit(_zip_one_student, output_folder, student): student
            for student in students
        }

        for future in as_completed(futures):
            student, zip_path = future.result()
            if zip_path:
                zipped_paths[student] = zip_path

            completed += 1
            if progress_callback:
                progress_callback(completed, total, 'папок', 'Архивация папок')

    logging.info("Архивация завершена: %s/%s папок", len(zipped_paths), total)
    return zipped_paths
