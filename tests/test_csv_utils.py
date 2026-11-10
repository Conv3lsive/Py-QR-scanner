import os

import pytest

import csv_utils


@pytest.fixture(autouse=True)
def _clear_csv_cache():
    """Изолируем глобальный кэш модуля между тестами."""
    csv_utils._CSV_CACHE.clear()
    yield
    csv_utils._CSV_CACHE.clear()


# --- _normalize_delimiter ---


@pytest.mark.parametrize(
    'value, expected',
    [
        (None, None),
        ('', None),
        ('auto', None),
        ('  AUTO  ', None),
        ('Auto', None),
        ('tab', '\t'),
        ('TAB', '\t'),
        ('\\t', '\t'),
        ('semicolon', ';'),
        (';', ';'),
        ('comma', ','),
        (',', ','),
        ('|', '|'),
    ],
)
def test_normalize_delimiter(value, expected):
    assert csv_utils._normalize_delimiter(value) == expected


def test_normalize_delimiter_raw_tab_char():
    assert csv_utils._normalize_delimiter('\t') == '\t'


# --- снiffer разделителей на реальных файлах ---


@pytest.mark.parametrize('delimiter, line_fmt', [
    (';', 'Код;Имя\n1;Иван\n2;Петр\n'),
    (',', 'Код,Имя\n1,Иван\n2,Петр\n'),
    ('\t', 'Код\tИмя\n1\tИван\n2\tПетр\n'),
])
def test_delimiter_sniff_auto(tmp_path, delimiter, line_fmt):
    p = tmp_path / 's.csv'
    p.write_text(line_fmt, encoding='utf-8')
    data = csv_utils.read_csv(str(p), 'код', ['Имя'])
    assert data == {'Иван': {'Код': ['1']}, 'Петр': {'Код': ['2']}}


# --- BOM и пробелы в заголовках ---


def test_bom_and_header_spaces_via_read_csv(tmp_path):
    p = tmp_path / 'bom.csv'
    # encoding='utf-8-sig' добавляет BOM; пробелы вокруг имён колонок.
    p.write_text('  Код  ;  Имя  \n1;Иван\n', encoding='utf-8-sig')
    data = csv_utils.read_csv(str(p), 'код', ['Имя'])
    assert data == {'Иван': {'Код': ['1']}}


def test_create_reader_strips_bom_from_fieldnames(tmp_path):
    """Прямая проверка lstrip('\\ufeff') в _create_reader (файл без utf-8-sig декодирования)."""
    p = tmp_path / 'raw.csv'
    p.write_bytes('﻿Код;Имя\n1;Иван\n'.encode('utf-8'))
    with open(str(p), 'r', encoding='utf-8', newline='') as f:
        reader = csv_utils._create_reader(f, csv_delimiter=';')
    assert reader.fieldnames == ['Код', 'Имя']


# --- _resolve_column_name ---


def test_resolve_column_name():
    assert csv_utils._resolve_column_name(['Name', 'Email'], 'email') == 'Email'
    assert csv_utils._resolve_column_name(['Name', 'Email'], ' EMAIL ') == 'Email'
    # точное совпадение важнее частичного
    assert csv_utils._resolve_column_name(['Email', 'Email Address'], 'email') == 'Email'
    # частичное совпадение (подстрока)
    assert csv_utils._resolve_column_name(['Email Address'], 'email') == 'Email Address'
    assert csv_utils._resolve_column_name(['Name'], 'email') is None
    assert csv_utils._resolve_column_name([], 'email') is None
    assert csv_utils._resolve_column_name(['Email'], '') is None
    assert csv_utils._resolve_column_name(['Email'], '   ') is None


# --- _find_code_columns ---


def test_find_code_columns_multiple():
    headers = ['Код', 'Код2', 'Штрихкод', 'Имя', 'Email']
    assert csv_utils._find_code_columns(headers, 'код') == ['Код', 'Код2', 'Штрихкод']
    assert csv_utils._find_code_columns(headers, 'КОД') == ['Код', 'Код2', 'Штрихкод']
    assert csv_utils._find_code_columns(headers, None) == ['Код', 'Код2', 'Штрихкод']
    assert csv_utils._find_code_columns(headers, 'штат') == []


# --- _build_student_name ---


def test_build_student_name():
    row = {'Фамилия': 'Иванов', 'Имя': ' Иван '}
    assert csv_utils._build_student_name(row, ['Фамилия', 'Имя']) == 'Иванов Иван'
    assert csv_utils._build_student_name({'Фамилия': '', 'Имя': ' '}, ['Фамилия', 'Имя']) == ''
    assert csv_utils._build_student_name({'Имя': 'Иван'}, None) == ''
    assert csv_utils._build_student_name({}, ['Имя']) == ''
    assert csv_utils._build_student_name({'Отчество': None}, ['Отчество']) == ''


# --- чтение CSV: структура результата и крайние случаи ---


def test_read_csv_multiple_rows_and_code_columns(tmp_path):
    p = tmp_path / 'm.csv'
    p.write_text(
        'Фамилия;Имя;Код;Код2\nИванов;Иван;001;002\nИванов;Иван;003;\n',
        encoding='utf-8',
    )
    data = csv_utils.read_csv(str(p), 'код', ['Фамилия', 'Имя'])
    assert data == {'Иванов Иван': {'Код': ['001', '003'], 'Код2': ['002']}}


def test_missing_file_raises_file_not_found(tmp_path):
    absent = tmp_path / 'nope.csv'
    with pytest.raises(FileNotFoundError):
        csv_utils.read_csv(str(absent), 'код', ['Имя'])


def test_no_code_column_returns_empty_data(tmp_path):
    p = tmp_path / 'nocode.csv'
    p.write_text('Имя;Email\nИван;ivan@x.com\n', encoding='utf-8')
    assert csv_utils.read_csv(str(p), 'код', ['Имя']) == {}


def test_no_code_column_still_collects_emails(tmp_path):
    p = tmp_path / 'nocode2.csv'
    p.write_text('Имя;Email\nИван;ivan@x.com\n', encoding='utf-8')
    data, emails = csv_utils.read_csv_with_email(str(p), 'код', ['Имя'], email_field='email')
    assert data == {}
    assert emails == {'Иван': 'ivan@x.com'}


# --- email_field: регистр, пробелы, пустая строка ---


def test_email_field_case_and_spaces(tmp_path):
    p = tmp_path / 'e.csv'
    p.write_text('Код;Имя;EMAIL\n1;Иван;ivan@x.com\n', encoding='utf-8')
    _, emails = csv_utils.read_csv_with_email(str(p), 'код', ['Имя'], email_field=' email ')
    assert emails == {'Иван': 'ivan@x.com'}


def test_email_empty_value_last_nonempty_wins(tmp_path):
    p = tmp_path / 'e2.csv'
    p.write_text('Код;Имя;Email\n1;Иван;\n2;Иван;ivan@x.com\n', encoding='utf-8')
    _, emails = csv_utils.read_csv_with_email(str(p), 'код', ['Имя'], email_field='email')
    assert emails == {'Иван': 'ivan@x.com'}


def test_email_empty_value_does_not_overwrite_existing(tmp_path):
    p = tmp_path / 'e3.csv'
    p.write_text('Код;Имя;Email\n1;Иван;ivan@x.com\n2;Иван;\n', encoding='utf-8')
    _, emails = csv_utils.read_csv_with_email(str(p), 'код', ['Имя'], email_field='email')
    assert emails == {'Иван': 'ivan@x.com'}


def test_email_missing_column_yields_empty_string(tmp_path):
    p = tmp_path / 'e4.csv'
    p.write_text('Код;Имя\n1;Иван\n', encoding='utf-8')
    _, emails = csv_utils.read_csv_with_email(str(p), 'код', ['Имя'], email_field='email')
    assert emails == {'Иван': ''}


# --- кэш ---


def test_cache_returns_deep_clone_read_csv(tmp_path):
    p = tmp_path / 'cache.csv'
    p.write_text('Код;Имя\n1;Иван\n', encoding='utf-8')
    data1 = csv_utils.read_csv(str(p), 'код', ['Имя'])
    data1['Иван']['Код'].append('999')
    data2 = csv_utils.read_csv(str(p), 'код', ['Имя'])  # кэш-хит
    assert data2 == {'Иван': {'Код': ['1']}}


def test_cache_returns_deep_clone_read_csv_with_email(tmp_path):
    p = tmp_path / 'cache2.csv'
    p.write_text('Код;Имя;Email\n1;Иван;ivan@x.com\n', encoding='utf-8')
    data1, emails1 = csv_utils.read_csv_with_email(str(p), 'код', ['Имя'], email_field='email')
    data1['Иван']['Код'].append('999')
    emails1['Новый'] = 'x@y.com'
    data2, emails2 = csv_utils.read_csv_with_email(str(p), 'код', ['Имя'], email_field='email')
    assert data2 == {'Иван': {'Код': ['1']}}
    assert 'Новый' not in emails2


def test_cache_invalidated_on_size_change(tmp_path):
    p = tmp_path / 'size.csv'
    p.write_text('Код;Имя\n1;Иван\n', encoding='utf-8')
    assert 'Петр' not in csv_utils.read_csv(str(p), 'код', ['Имя'])
    with open(str(p), 'a', encoding='utf-8') as f:
        f.write('2;Петр\n')
    data = csv_utils.read_csv(str(p), 'код', ['Имя'])
    assert data == {'Иван': {'Код': ['1']}, 'Петр': {'Код': ['2']}}


def test_cache_invalidated_on_mtime_change(tmp_path):
    p = tmp_path / 'mtime.csv'
    p.write_text('Код;Имя\n1;Иван\n', encoding='utf-8')
    assert 'Иван' in csv_utils.read_csv(str(p), 'код', ['Имя'])

    old_stat = os.stat(str(p))
    # тот же размер, другое содержимое ('Иван' -> 'Олег' по 8 байт в UTF-8)
    p.write_text('Код;Имя\n1;Олег\n', encoding='utf-8')
    # гарантированно меняем mtime, чтобы сработал новый ключ кэша
    os.utime(str(p), ns=(old_stat.st_atime_ns, old_stat.st_mtime_ns + 5_000_000_000))

    data = csv_utils.read_csv(str(p), 'код', ['Имя'])
    assert 'Олег' in data
    assert 'Иван' not in data


def test_cache_limit_full_clear(monkeypatch, tmp_path):
    monkeypatch.setattr(csv_utils, '_CSV_CACHE_MAX_ENTRIES', 3)
    p = tmp_path / 'limit.csv'
    p.write_text('Код;Имя\n1;Иван\n', encoding='utf-8')
    for i in range(4):
        csv_utils.read_csv(str(p), 'код%d' % i, ['Имя'])
    # после 4-го вызова len достиг лимита -> полная очистка -> остался 1 элемент
    assert len(csv_utils._CSV_CACHE) == 1


def test_read_csv_with_email_empty_email_field_no_cache_collision(tmp_path):
    p = tmp_path / 'collision.csv'
    p.write_text('Код;Имя;Email\n1;Иван;ivan@x.com\n2;Петр;petr@x.com\n', encoding='utf-8')
    csv_utils.read_csv(str(p), 'код', ['Имя'])
    data, emails = csv_utils.read_csv_with_email(str(p), 'код', ['Имя'], email_field='')
    assert data == {'Иван': {'Код': ['1']}, 'Петр': {'Код': ['2']}}
    assert set(emails) == {'Иван', 'Петр'}