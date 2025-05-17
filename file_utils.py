import os
import shutil
from concurrent.futures import ThreadPoolExecutor


def get_all_files(folder):
    return {os.path.join(folder, f) for f in os.listdir(folder) if f.endswith('.jpg')}


def move_files(data, output_folder, barcodes, move_mode='copy'):
    def move_one(src, dst):
        try:
            (shutil.move if move_mode == 'move' else shutil.copy)(src, dst)
        except Exception as e:
            print(f"Ошибка при копировании {src} -> {dst}: {e}")

    tasks = []

    with ThreadPoolExecutor() as executor:
        for student, codes in data.items():
            student_folder = os.path.join(output_folder, student)
            os.makedirs(student_folder, exist_ok=True)
            for code_list in codes.values():
                for code in code_list:
                    for src in barcodes.get(code, []):
                        dst = os.path.join(student_folder, os.path.basename(src))
                        tasks.append(executor.submit(move_one, src, dst))

        for task in tasks:
            task.result()


def move_unfound(barcodes, data, output_folder, move_mode='copy'):
    barcodes_left = dict(barcodes)
    for codes in data.values():
        for codelist in codes.values():
            for code in codelist:
                barcodes_left.pop(code, None)

    unfound_dir = os.path.join(output_folder, 'unsorted')
    os.makedirs(unfound_dir, exist_ok=True)
    for paths in barcodes_left.values():
        for src in paths:
            dst = os.path.join(unfound_dir, os.path.basename(src))
            (shutil.move if move_mode == 'move' else shutil.copy)(src, dst)


def move_clear(output_folder, image_folder, found_files, move_mode='copy'):
    all_files = get_all_files(image_folder)
    clear_files = all_files - set(found_files)
    clear_dir = os.path.join(output_folder, 'noqrcode')
    os.makedirs(clear_dir, exist_ok=True)
    for src in clear_files:
        dst = os.path.join(clear_dir, os.path.basename(src))
        (shutil.move if move_mode == 'move' else shutil.copy)(src, dst)