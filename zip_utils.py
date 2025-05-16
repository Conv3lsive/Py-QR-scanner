import os
import zipfile
import logging

def zip_student_folders(output_folder):
    zipped_paths = {}
    for student in os.listdir(output_folder):
        student_path = os.path.join(output_folder, student)
        if os.path.isdir(student_path) and student != "unsorted":
            zip_path = os.path.join(output_folder, f"{student}.zip")
            try:
                with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                    for root, _, files in os.walk(student_path):
                        for file in files:
                            full_path = os.path.join(root, file)
                            rel_path = os.path.relpath(full_path, start=student_path)
                            zipf.write(full_path, arcname=rel_path)
                zipped_paths[student] = zip_path
                logging.info(f"Папка {student} заархивирована как {zip_path}")
            except Exception as e:
                logging.error(f"Ошибка при архивации папки {student}: {e}")
    return zipped_paths
