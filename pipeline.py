import hashlib
import logging
import os
import shutil
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Dict, Iterable, List, Optional

from app_config import get_email_config
from csv_utils import read_csv, read_csv_with_email
from email_utils import is_valid_email, send_email_smtp, validate_emails
from file_utils import move_clear, move_unfound
from zip_utils import zip_student_folders


logger = logging.getLogger(__name__)

WATCH_EXTENSIONS = {'.jpg', '.jpeg', '.png'}
_FILE_HASH_CACHE = {}
_FILE_HASH_CACHE_LOCK = threading.Lock()


def _emit_progress(progress_callback, action, done, total=None, unit='ед.', message=''):
    if not progress_callback:
        return
    payload = {
        'action': action,
        'done': int(done),
        'total': int(total) if total is not None else None,
        'unit': unit,
        'message': message,
    }
    progress_callback(payload)


def _file_signature(path: str):
    stat = os.stat(path)
    return (
        os.path.abspath(path),
        stat.st_size,
        getattr(stat, 'st_mtime_ns', int(stat.st_mtime * 1_000_000_000)),
    )


def _hash_file(path: str, chunk_size: int = 1024 * 1024) -> str:
    signature = _file_signature(path)
    with _FILE_HASH_CACHE_LOCK:
        cached = _FILE_HASH_CACHE.get(signature)
    if cached is not None:
        return cached

    digest = hashlib.sha256()
    with open(path, 'rb') as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    digest_value = digest.hexdigest()

    with _FILE_HASH_CACHE_LOCK:
        if len(_FILE_HASH_CACHE) > 4096:
            _FILE_HASH_CACHE.clear()
        _FILE_HASH_CACHE[signature] = digest_value

    return digest_value


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
               threads: int = 6, state=None, progress_callback: Optional[Callable] = None):
    if action == 0:
        from barcode_utils import find_barcodes, file_renamer

        if not image_folder:
            raise ValueError('Для action=0 требуется --image-folder')

        barcodes = find_barcodes(
            image_folder,
            max_workers=threads,
            progress_callback=lambda done, total, unit, msg: _emit_progress(
                progress_callback, action, done, total, unit, msg
            ),
        )
        file_renamer(
            image_folder,
            barcodes,
            progress_callback=lambda done, total, unit, msg: _emit_progress(
                progress_callback, action, done, total, unit, msg
            ),
        )

        _emit_progress(progress_callback, action, 1, 1, 'этап', 'Переименование завершено')
        return {'status': 'ok', 'message': 'Файлы переименованы'}

    if action == 1:
        from barcode_utils import find_barcodes, split_by_student_folders

        if not all([image_folder, csv_path, name_fields, output_folder]):
            raise ValueError('Для action=1 нужны --image-folder, --csv-path, --name-fields и --output-folder')

        barcodes = find_barcodes(
            image_folder,
            max_workers=threads,
            progress_callback=lambda done, total, unit, msg: _emit_progress(
                progress_callback, action, done, total, unit, msg
            ),
        )
        data = read_csv(csv_path, code_field, name_fields, csv_delimiter=csv_delimiter)
        found_files = [p for v in barcodes.values() for p in v]

        _emit_progress(progress_callback, action, 1, 3, 'этап', 'Перемещение unsorted/noqrcode')
        move_unfound(barcodes, data, output_folder, move_mode)
        move_clear(output_folder, image_folder, found_files, move_mode)

        split_by_student_folders(
            barcodes,
            data,
            output_folder,
            max_workers=threads,
            progress_callback=lambda done, total, unit, msg: _emit_progress(
                progress_callback, action, done, total, unit, msg
            ),
        )
        _emit_progress(progress_callback, action, 3, 3, 'этап', 'Распределение завершено')
        return {
            'status': 'ok',
            'message': 'Файлы распределены',
            'students': len(data),
            'decoded': len(found_files),
        }

    if action == 2:
        if not output_folder:
            raise ValueError('Для action=2 требуется --output-folder')
        archives = zip_student_folders(
            output_folder,
            max_workers=threads,
            progress_callback=lambda done, total, unit, msg: _emit_progress(
                progress_callback, action, done, total, unit, msg
            ),
        )
        _emit_progress(progress_callback, action, 1, 1, 'этап', 'Архивация завершена')
        return {'status': 'ok', 'archives': len(archives)}

    if action == 3:
        if not all([csv_path, output_folder, name_fields]):
            raise ValueError('Для action=3 нужны --csv-path, --output-folder и --name-fields')

        email_cfg = get_email_config()
        email_subject = email_cfg['EMAIL_SUBJECT']
        email_body = email_cfg['EMAIL_BODY']

        archives = zip_student_folders(
            output_folder,
            max_workers=threads,
            progress_callback=lambda done, total, unit, msg: _emit_progress(
                progress_callback, action, done, total, unit, msg
            ),
        )
        _, emails = read_csv_with_email(
            csv_path,
            code_field,
            name_fields,
            email_field=email_field,
            csv_delimiter=csv_delimiter,
        )

        sent = 0
        failed = 0
        total_archives = len(archives)
        done_archives = 0
        max_send_workers = max(1, min(threads, total_archives or 1))
        future_to_recipient = {}

        with ThreadPoolExecutor(max_workers=max_send_workers) as executor:
            for student, zip_path in archives.items():
                recipient = (emails.get(student, '') or '').strip()
                if not recipient or not is_valid_email(recipient):
                    failed += 1
                    done_archives += 1
                    _emit_progress(progress_callback, action, done_archives, total_archives, 'архивов', 'Email-рассылка')
                    continue

                future = executor.submit(send_email_smtp, recipient, email_subject, email_body, zip_path)
                future_to_recipient[future] = recipient

            for future in as_completed(future_to_recipient):
                recipient = future_to_recipient[future]
                try:
                    if future.result():
                        sent += 1
                    else:
                        failed += 1
                        logger.warning('Не удалось отправить письмо на %s', recipient)
                except Exception as exc:
                    failed += 1
                    logger.error('Ошибка параллельной отправки %s: %s', recipient, exc)
                finally:
                    done_archives += 1
                    _emit_progress(progress_callback, action, done_archives, total_archives, 'архивов', 'Email-рассылка')

        _emit_progress(progress_callback, action, 1, 1, 'этап', 'Рассылка завершена')

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
        validate_emails(
            emails,
            max_workers=threads,
            progress_callback=lambda done, total, unit, msg: _emit_progress(
                progress_callback, action, done, total, unit, msg
            ),
        )
        _emit_progress(progress_callback, action, 1, 1, 'этап', 'Валидация завершена')
        return {'status': 'ok', 'emails': len(emails)}

    raise ValueError(f'Неизвестный action: {action}')


def process_watch_batch(file_paths: Iterable[str], csv_path: str, name_fields: List[str],
                        output_folder: str, code_field: str = 'код', move_mode: str = 'copy',
                        threads: int = 4, state=None, csv_delimiter: str = 'auto') -> Dict[str, int]:
    from barcode_utils import find_barcodes_in_files, split_by_student_folders

    data = read_csv(csv_path, code_field, name_fields, csv_delimiter=csv_delimiter)
    incoming_files = list(dict.fromkeys(
        path for path in file_paths
        if os.path.isfile(path) and os.path.splitext(path)[1].lower() in WATCH_EXTENSIONS
    ))

    file_hashes = {}
    duplicates = 0
    hash_error_records = []

    for path in incoming_files:
        try:
            file_hashes[path] = _hash_file(path)
        except Exception as exc:
            logger.error(f'Ошибка хэширования {path}: {exc}')
            if state:
                hash_error_records.append(('', '', path, 'hash_error', ''))

    existing_hashes = state.get_existing_file_hashes(file_hashes.values()) if state else set()
    unique_files = []
    duplicate_hash_records = []

    for path in incoming_files:
        file_hash = file_hashes.get(path)
        if not file_hash:
            continue
        if file_hash in existing_hashes:
            duplicates += 1
            if state:
                duplicate_hash_records.append((file_hash, '', path, 'duplicate_hash', ''))
            continue
        unique_files.append(path)

    barcodes = find_barcodes_in_files(unique_files, max_workers=threads)

    existing_qr_pairs = set()
    if state:
        existing_qr_pairs = state.get_existing_qr_hash_pairs(
            {
                (code, file_hashes.get(path, ''))
                for code, paths in barcodes.items()
                for path in paths
                if file_hashes.get(path)
            }
        )

    deduped_barcodes = {}
    duplicate_qr_records = []
    for code, paths in barcodes.items():
        for path in paths:
            file_hash = file_hashes.get(path, '')
            if state and file_hash and (code, file_hash) in existing_qr_pairs:
                duplicates += 1
                duplicate_qr_records.append((file_hash, code, path, 'duplicate_qr', ''))
                continue
            deduped_barcodes.setdefault(code, []).append(path)

    found_files = sorted({p for v in deduped_barcodes.values() for p in v})
    move_unfound(deduped_barcodes, data, output_folder, move_mode)
    _move_clear_for_batch(output_folder, unique_files, found_files, move_mode)
    split_by_student_folders(deduped_barcodes, data, output_folder, max_workers=threads)

    if state:
        processed_records = []
        for code, paths in deduped_barcodes.items():
            for path in paths:
                processed_records.append((file_hashes.get(path, ''), code, path, 'processed', ''))

        not_found = set(unique_files) - set(found_files)
        no_qr_records = []
        for path in not_found:
            no_qr_records.append((file_hashes.get(path, ''), '', path, 'noqrcode', ''))

        state.add_records(
            hash_error_records
            + duplicate_hash_records
            + duplicate_qr_records
            + processed_records
            + no_qr_records
        )

    not_found = set(unique_files) - set(found_files)

    return {
        'incoming': len(incoming_files),
        'processed': len(found_files),
        'duplicates': duplicates,
        'unrecognized': len(not_found),
    }
