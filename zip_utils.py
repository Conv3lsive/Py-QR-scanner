import os
import zipfile
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed

def zip_student_folders(output_folder, max_workers=6):
    def zip_one_student(student):
        student_path = os.path.join(output_folder, student)
        zip_path = os.path.join(output_folder, f"{student}.zip")
        try:
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
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

    zipped_paths = {}
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(zip_one_student, student): student
            for student in os.listdir(output_folder)
            if os.path.isdir(os.path.join(output_folder, student)) and student not in ("unsorted", "noqrcode", "unfound")
        }
        for future in as_completed(futures):
            student, zip_path = future.result()
            if zip_path:
                zipped_paths[student] = zip_path
    return zipped_paths
