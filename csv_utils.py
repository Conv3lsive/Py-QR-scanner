import csv


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


def read_csv(csv_path, code_name, student_fields, csv_delimiter='auto'):
    data = {}
    if not code_name:
        code_name = 'код'
    with open(csv_path, 'r', encoding='utf-8-sig', newline='') as file:
        reader = _create_reader(file, csv_delimiter=csv_delimiter)
        for row in reader:
            codes = [col for col in row if col and code_name in col.lower()]
            if codes:
                student_name = ' '.join([row.get(f, '').strip() for f in student_fields])
                if student_name not in data:
                    data[student_name] = {}

                for code in codes:
                    val = row[code].strip()
                    if val:
                        if code not in data[student_name]:
                            data[student_name][code] = []
                        data[student_name][code].append(val)
    return data


# New function: read_csv_with_email
def read_csv_with_email(csv_path, code_name, student_fields, email_field='email', csv_delimiter='auto'):
    data = {}
    emails = {}
    if not code_name:
        code_name = 'код'
    with open(csv_path, 'r', encoding='utf-8-sig', newline='') as file:
        reader = _create_reader(file, csv_delimiter=csv_delimiter)
        headers = reader.fieldnames or []
        resolved_email_field = _resolve_column_name(headers, email_field)

        for row in reader:
            codes = [col for col in row if col and code_name in col.lower()]
            if codes:
                student_name = ' '.join([row.get(f, '').strip() for f in student_fields])
                data[student_name] = {code: [row[code]] for code in codes if row[code]}
                emails[student_name] = row.get(resolved_email_field, '').strip() if resolved_email_field else ''
    return data, emails