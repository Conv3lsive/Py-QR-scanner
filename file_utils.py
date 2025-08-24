import re
import os
import shutil
from concurrent.futures import ProcessPoolExecutor


def get_all_files(folder):
    return {os.path.join(folder, f) for f in os.listdir(folder) if f.endswith('.jpg')}


    def move_one(src, dst):
        try:
            (shutil.move if move_mode == 'move' else shutil.copy)(src, dst)
        except Exception as e:
            print(f"Ошибка при копировании {src} -> {dst}: {e}")

    tasks = []

    with ProcessPoolExecutor() as executor:
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


def check_pairing(image_folder):
    from collections import defaultdict
    files = [f for f in os.listdir(image_folder) if f.endswith('.jpg')]
    pattern = re.compile(r"(\d+)-([1-4])(?:\(\d+\))?\.jpg")

    grouped = defaultdict(set)
    for file in files:
        match = pattern.match(file)
        if match:
            base, suffix = match.groups()
            grouped[base].add(suffix)

    def check_and_print(base, suffixes):
        if ('1' in suffixes) != ('2' in suffixes):
            print(f"Неполная пара 1-2 для: {base}")
        if ('3' in suffixes) != ('4' in suffixes):
            print(f"Неполная пара 3-4 для: {base}")

    with ProcessPoolExecutor() as executor:
        for base, suffixes in grouped.items():
            executor.submit(check_and_print, base, suffixes)