import csv
import os
import threading


_CSV_CACHE = {}
_CSV_CACHE_LOCK = threading.Lock()


def _normalize_delimiter(csv_delimiter):
    if csv_delimiter is None:
        return None

    value = str(csv_delimiter).strip().lower()
    if not value or value == 'auto':
        return None
    if value in {'tab', '\\t'}:
        return '\t'
    if value in {'semicolon', ';'}:
        return ';'
    if value in {'comma', ','}:
        return ','

    return str(csv_delimiter)


def _create_reader(file_obj, csv_delimiter='auto'):
    sample = file_obj.read(4096)
    file_obj.seek(0)

    delimiter = _normalize_delimiter(csv_delimiter)
    if delimiter is None:
        delimiter = ','
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=';,\t')
            delimiter = dialect.delimiter
        except Exception:
            pass

    reader = csv.DictReader(file_obj, delimiter=delimiter)
    if reader.fieldnames:
        reader.fieldnames = [
            (name or '').strip().lstrip('\ufeff') if name is not None else ''
            for name in reader.fieldnames
        ]
    return reader


def _resolve_column_name(headers, target_name):
    if not target_name:
        return None

    normalized_target = target_name.strip().lower()
    if not normalized_target:
        return None

    for header in headers:
        if (header or '').strip().lower() == normalized_target:
            return header

    for header in headers:
        if normalized_target in (header or '').strip().lower():
            return header

    return None


def _find_code_columns(headers, code_name):
    normalized_code_name = (code_name or 'код').strip().lower()
    return [header for header in headers if header and normalized_code_name in header.lower()]


def _build_student_name(row, student_fields):
    parts = []
    for field in student_fields or ():
        value = (row.get(field, '') or '').strip()
        if value:
            parts.append(value)
    return ' '.join(parts).strip()


def _build_cache_key(csv_path, csv_delimiter, code_name, student_fields, email_field=''):
    stat = os.stat(csv_path)
    return (
        os.path.abspath(csv_path),
        stat.st_mtime_ns,
        stat.st_size,
        _normalize_delimiter(csv_delimiter) or 'auto',
        (code_name or 'код').strip().lower(),
        tuple(student_fields or ()),
        (email_field or '').strip().lower(),
    )


def _clone_student_data(data):
    return {
        student: {
            column_name: list(values)
            for column_name, values in codes.items()
        }
        for student, codes in data.items()
    }


def _read_csv_internal(csv_path, code_name, student_fields, email_field='', csv_delimiter='auto'):
    data = {}
    emails = {} if email_field else None

    with open(csv_path, 'r', encoding='utf-8-sig', newline='') as file:
        reader = _create_reader(file, csv_delimiter=csv_delimiter)
        headers = reader.fieldnames or []
        code_columns = _find_code_columns(headers, code_name)
        resolved_email_field = _resolve_column_name(headers, email_field) if email_field else None

        for row in reader:
            student_name = _build_student_name(row, student_fields)

            if emails is not None:
                if not student_name:
                    continue

                if resolved_email_field:
                    email_value = (row.get(resolved_email_field, '') or '').strip()
                    if student_name not in emails or email_value:
                        emails[student_name] = email_value
                else:
                    emails.setdefault(student_name, '')

            if not code_columns:
                continue

            if student_name not in data:
                data[student_name] = {}

            for code_column in code_columns:
                value = (row.get(code_column, '') or '').strip()
                if not value:
                    continue

                data[student_name].setdefault(code_column, []).append(value)

    if emails is None:
        return data
    return data, emails


def read_csv(csv_path, code_name, student_fields, csv_delimiter='auto'):
    if not code_name:
        code_name = 'код'

    cache_key = _build_cache_key(csv_path, csv_delimiter, code_name, student_fields)
    with _CSV_CACHE_LOCK:
        cached = _CSV_CACHE.get(cache_key)
    if cached is not None:
        return _clone_student_data(cached)

    data = _read_csv_internal(csv_path, code_name, student_fields, csv_delimiter=csv_delimiter)
    with _CSV_CACHE_LOCK:
        _CSV_CACHE[cache_key] = _clone_student_data(data)
    return data


# New function: read_csv_with_email
def read_csv_with_email(csv_path, code_name, student_fields, email_field='email', csv_delimiter='auto'):
    if not code_name:
        code_name = 'код'

    cache_key = _build_cache_key(csv_path, csv_delimiter, code_name, student_fields, email_field=email_field)
    with _CSV_CACHE_LOCK:
        cached = _CSV_CACHE.get(cache_key)
    if cached is not None:
        cached_data, cached_emails = cached
        return _clone_student_data(cached_data), dict(cached_emails)

    data, emails = _read_csv_internal(
        csv_path,
        code_name,
        student_fields,
        email_field=email_field,
        csv_delimiter=csv_delimiter,
    )
    with _CSV_CACHE_LOCK:
        _CSV_CACHE[cache_key] = (_clone_student_data(data), dict(emails))
    return data, emails