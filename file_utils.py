import logging
import re
import os
import shutil

import backend_switch


SUPPORTED_IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png'}
logger = logging.getLogger(__name__)


def _safe_destination(directory: str, filename: str) -> str:
    """Return a collision-free destination path, appending _N suffix if needed."""
    dst = os.path.join(directory, filename)
    if not os.path.exists(dst):
        return dst
    name, ext = os.path.splitext(filename)
    counter = 1
    while True:
        candidate = os.path.join(directory, f"{name}_{counter}{ext}")
        if not os.path.exists(candidate):
            return candidate
        counter += 1


def _collect_known_codes(data):
    known_codes = set()
    for codes in data.values():
        for code_list in codes.values():
            for code in code_list:
                if code:
                    known_codes.add(code)
    return known_codes


def get_all_files(folder):
    if backend_switch.use_rust():
        return set(backend_switch.pyqr_core.fileops.get_all_files(folder))
    return {
        os.path.join(folder, f)
        for f in os.listdir(folder)
        if os.path.splitext(f)[1].lower() in SUPPORTED_IMAGE_EXTENSIONS
    }


def move_unfound(barcodes, data, output_folder, move_mode='copy'):
    if backend_switch.use_rust():
        backend_switch.pyqr_core.fileops.move_unfound(
            barcodes, data, output_folder, move_mode=move_mode)
        return

    known_codes = _collect_known_codes(data)

    unfound_dir = os.path.join(output_folder, 'unsorted')
    os.makedirs(unfound_dir, exist_ok=True)
    for code, paths in barcodes.items():
        if code in known_codes:
            continue
        for src in paths:
            dst = _safe_destination(unfound_dir, os.path.basename(src))
            (shutil.move if move_mode == 'move' else shutil.copy2)(src, dst)


def move_clear(output_folder, image_folder, found_files, move_mode='copy'):
    if backend_switch.use_rust():
        backend_switch.pyqr_core.fileops.move_clear(
            output_folder, image_folder, list(found_files), move_mode=move_mode)
        return

    all_files = get_all_files(image_folder)
    clear_files = all_files - set(found_files)
    clear_dir = os.path.join(output_folder, 'noqrcode')
    os.makedirs(clear_dir, exist_ok=True)
    for src in clear_files:
        dst = _safe_destination(clear_dir, os.path.basename(src))
        (shutil.move if move_mode == 'move' else shutil.copy2)(src, dst)


def check_pairing(image_folder):
    if backend_switch.use_rust():
        for warning in backend_switch.pyqr_core.fileops.check_pairing(image_folder):
            logger.warning("%s", warning)
        return

    from collections import defaultdict
    files = [
        f for f in os.listdir(image_folder)
        if os.path.splitext(f)[1].lower() in SUPPORTED_IMAGE_EXTENSIONS
    ]
    pattern = re.compile(r"(\d+)-([1-4])(?:\(\d+\))?\.jpg")

    grouped = defaultdict(set)
    for file in files:
        match = pattern.match(file)
        if match:
            base, suffix = match.groups()
            grouped[base].add(suffix)

    for base, suffixes in grouped.items():
        if ('1' in suffixes) != ('2' in suffixes):
            logger.warning("Неполная пара 1-2 для: %s", base)
        if ('3' in suffixes) != ('4' in suffixes):
            logger.warning("Неполная пара 3-4 для: %s", base)