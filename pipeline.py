import hashlib
import logging
import os
import shutil
from typing import Dict, Iterable, List, Optional

from app_config import get_email_config
from csv_utils import read_csv, read_csv_with_email
from email_utils import send_email_smtp, validate_emails
from file_utils import move_clear, move_unfound
from pdf_utils import pdf_to_images
from zip_utils import zip_student_folders


logger = logging.getLogger(__name__)

WATCH_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.pdf'}


def _hash_file(path: str, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with open(path, 'rb') as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _expand_files_with_pdf(file_paths: Iterable[str]) -> List[str]:
    expanded = []
    for path in file_paths:
        ext = os.path.splitext(path)[1].lower()
        if ext == '.pdf':
            try:
                expanded.extend(pdf_to_images(path))
            except Exception as exc:
                logger.error(f'Ошибка конвертации PDF {path}: {exc}')
        else:
            expanded.append(path)
    return expanded


def _move_clear_for_batch(output_folder, considered_files, found_files, move_mode='copy'):
    clear_dir = os.path.join(output_folder, 'noqrcode')
    os.makedirs(clear_dir, exist_ok=True)
    clear_files = set(considered_files) - set(found_files)
    for src in clear_files:
        dst = os.path.join(clear_dir, os.path.basename(src))
        (shutil.move if move_mode == 'move' else shutil.copy)(src, dst)


def run_action(action: int, image_folder: Optional[str] = None, csv_path: Optional[str] = None,
               name_fields: Optional[List[str]] = None, code_field: str = 'код',
               email_field: str = 'email',
               csv_delimiter: str = 'auto',
               output_folder: Optional[str] = None, move_mode: str = 'copy',
               threads: int = 6, state=None):
    if action == 0:
        from barcode_utils import find_barcodes, file_renamer

        if not image_folder:
            raise ValueError('Для action=0 требуется --image-folder')
        barcodes = find_barcodes(image_folder, max_workers=threads)
        file_renamer(image_folder, barcodes)
        return {'status': 'ok', 'message': 'Файлы переименованы'}

    if action == 1:
        from barcode_utils import find_barcodes, split_by_student_folders

        if not all([image_folder, csv_path, name_fields, output_folder]):
            raise ValueError('Для action=1 нужны --image-folder, --csv-path, --name-fields и --output-folder')
        barcodes = find_barcodes(image_folder, max_workers=threads)
        data = read_csv(csv_path, code_field, name_fields, csv_delimiter=csv_delimiter)
        found_files = [p for v in barcodes.values() for p in v]
        move_unfound(barcodes, data, output_folder, move_mode)
        move_clear(output_folder, image_folder, found_files, move_mode)
        split_by_student_folders(barcodes, data, output_folder, max_workers=threads)
        return {
            'status': 'ok',
            'message': 'Файлы распределены',
            'students': len(data),
            'decoded': len(found_files),
        }

    if action == 2:
        if not output_folder:
            raise ValueError('Для action=2 требуется --output-folder')
        archives = zip_student_folders(output_folder, max_workers=threads)
        return {'status': 'ok', 'archives': len(archives)}

    if action == 3:
        if not all([csv_path, output_folder, name_fields]):
            raise ValueError('Для action=3 нужны --csv-path, --output-folder и --name-fields')

        email_cfg = get_email_config()
        email_subject = email_cfg['EMAIL_SUBJECT']
        email_body = email_cfg['EMAIL_BODY']

        archives = zip_student_folders(output_folder, max_workers=threads)
        _, emails = read_csv_with_email(
            csv_path,
            code_field,
            name_fields,
            email_field=email_field,
            csv_delimiter=csv_delimiter,
        )

        sent = 0
        failed = 0
        for student, zip_path in archives.items():
            recipient = emails.get(student)
            if not recipient:
                failed += 1
                continue
            try:
                send_email_smtp(recipient, email_subject, email_body, zip_path)
                sent += 1
            except Exception:
                failed += 1

        return {'status': 'ok', 'sent': sent, 'failed': failed}

    if action == 4:
        if not all([csv_path, name_fields]):
            raise ValueError('Для action=4 нужны --csv-path и --name-fields')
        _, emails = read_csv_with_email(
            csv_path,
            code_field,
            name_fields,
            email_field=email_field,
            csv_delimiter=csv_delimiter,
        )
        validate_emails(emails, max_workers=threads)
        return {'status': 'ok', 'emails': len(emails)}

    raise ValueError(f'Неизвестный action: {action}')


def process_watch_batch(file_paths: Iterable[str], csv_path: str, name_fields: List[str],
                        output_folder: str, code_field: str = 'код', move_mode: str = 'copy',
                        threads: int = 4, state=None, csv_delimiter: str = 'auto') -> Dict[str, int]:
    from barcode_utils import find_barcodes_in_files, split_by_student_folders

    data = read_csv(csv_path, code_field, name_fields, csv_delimiter=csv_delimiter)
    incoming_files = [
        path for path in file_paths
        if os.path.isfile(path) and os.path.splitext(path)[1].lower() in WATCH_EXTENSIONS
    ]

    unique_files = []
    file_hashes = {}
    duplicates = 0

    for path in incoming_files:
        try:
            file_hash = _hash_file(path)
            file_hashes[path] = file_hash
            if state and state.has_file_hash(file_hash):
                duplicates += 1
                state.add_record(file_hash, '', path, 'duplicate_hash')
                continue
            unique_files.append(path)
        except Exception as exc:
            logger.error(f'Ошибка хэширования {path}: {exc}')
            if state:
                state.add_record('', '', path, 'hash_error')

    expanded_files = _expand_files_with_pdf(unique_files)
    barcodes = find_barcodes_in_files(expanded_files, max_workers=threads)

    deduped_barcodes = {}
    for code, paths in barcodes.items():
        for path in paths:
            file_hash = file_hashes.get(path, '')
            if state and file_hash and state.has_qr_for_hash(code, file_hash):
                duplicates += 1
                state.add_record(file_hash, code, path, 'duplicate_qr')
                continue
            deduped_barcodes.setdefault(code, []).append(path)

    found_files = [p for v in deduped_barcodes.values() for p in v]
    move_unfound(deduped_barcodes, data, output_folder, move_mode)
    _move_clear_for_batch(output_folder, expanded_files, found_files, move_mode)
    split_by_student_folders(deduped_barcodes, data, output_folder, max_workers=threads)

    if state:
        for code, paths in deduped_barcodes.items():
            for path in paths:
                state.add_record(file_hashes.get(path, ''), code, path, 'processed')

        not_found = set(expanded_files) - set(found_files)
        for path in not_found:
            state.add_record(file_hashes.get(path, ''), '', path, 'noqrcode')

    return {
        'incoming': len(incoming_files),
        'processed': len(found_files),
        'duplicates': duplicates,
        'unrecognized': len(set(expanded_files) - set(found_files)),
    }
