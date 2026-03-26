import csv


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


def read_csv(csv_path, code_name, student_fields):
    data = {}
    if not code_name:
        code_name = 'код'
    with open(csv_path, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            codes = [col for col in row if code_name in col.lower()]
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
def read_csv_with_email(csv_path, code_name, student_fields, email_field='email'):
    data = {}
    emails = {}
    if not code_name:
        code_name = 'код'
    with open(csv_path, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        headers = reader.fieldnames or []
        resolved_email_field = _resolve_column_name(headers, email_field)

        for row in reader:
            codes = [col for col in row if code_name in col.lower()]
            if codes:
                student_name = ' '.join([row.get(f, '').strip() for f in student_fields])
                data[student_name] = {code: [row[code]] for code in codes if row[code]}
                emails[student_name] = row.get(resolved_email_field, '').strip() if resolved_email_field else ''
    return data, emails