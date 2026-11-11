# -*- coding: utf-8 -*-
"""Детерминированный генератор датасетов для бенчмарков Python vs Rust.

Создаёт benchmarks/data/:
  csv/    — большие CSV (10k/50k строк) + CSV для сплит-бенча (50 студентов)
  qr/     — 300 QR-изображений (часть повёрнута на 90°) + codes.json
  junk/   — 300 случайных файлов (пул для copy/zip)
  pairing/— 600 файлов с именами вида 123-1.jpg (для check_pairing)
"""
import json
import os
import random
import shutil
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(ROOT, 'benchmarks', 'data')

SURNAMES = [
    'Иванов', 'Петров', 'Сидоров', 'Кузнецов', 'Смирнов', 'Васильев', 'Попов',
    'Соколов', 'Михайлов', 'Новиков', 'Фёдоров', 'Морозов', 'Волков', 'Алексеев',
    'Лебедев', 'Семёнов', 'Егоров', 'Павлов', 'Козлов', 'Степанов', 'Николаев',
    'Орлов', 'Андреев', 'Макаров', 'Никитин', 'Захаров', 'Зайцев', 'Соловьёв',
    'Борисов', 'Яковлев',
]
NAMES = [
    'Алексей', 'Мария', 'Дмитрий', 'Анна', 'Сергей', 'Елена', 'Иван', 'Ольга',
    'Пётр', 'Наталья', 'Андрей', 'Светлана', 'Николай', 'Татьяна', 'Виктор',
    'Юлия', 'Михаил', 'Екатерина', 'Артём', 'Дарья',
]
GRADES = ['9А', '9Б', '10А', '10Б', '11А', '11Б']


def make_code(i):
    if i % 2 == 0:
        return 'https://school.example.com/qr/{}'.format(100000 + i)
    return '{:013d}'.format(1234000000000 + i * 7)


def gen_students(count):
    students = []
    used = set()
    rng = random.Random(20261112)
    i = 0
    while len(students) < count:
        surname = SURNAMES[i % len(SURNAMES)]
        name = NAMES[(i // len(SURNAMES)) % len(NAMES)]
        grade = GRADES[(i // (len(SURNAMES) * len(NAMES))) % len(GRADES)]
        key = (surname, name, grade)
        if key in used:
            i += 1
            continue
        used.add(key)
        students.append(key)
        i += 1
    return students


def write_big_csv(path, rows_count, students_count):
    students = gen_students(students_count)
    rng = random.Random(rows_count)
    with open(path, 'w', encoding='utf-8', newline='') as fh:
        fh.write('Фамилия;Имя;Класс;код;email\n')
        for row_idx in range(rows_count):
            surname, name, grade = students[rng.randrange(len(students))]
            if rng.random() < 0.05:
                code = ''
            else:
                code = make_code(row_idx)
            if rng.random() < 0.10:
                email = ''
            else:
                email = '{}.{}@example.com'.format(name.lower(), surname.lower())
            fh.write('{};{};{};{};{}\n'.format(surname, name, grade, code, email))


def gen_qr():
    import qrcode
    from PIL import Image

    qr_dir = os.path.join(DATA, 'qr')
    os.makedirs(qr_dir, exist_ok=True)
    codes = {}
    for idx in range(300):
        code = make_code(idx)
        size = 300 if idx % 2 == 0 else 600
        img = qrcode.make(code, box_size=max(4, size // 33)).convert('RGB')
        if idx % 3 == 0:
            # Повёрнутые: декодер обязан пройти путь поворотов (find_best_rotation)
            img = img.transpose(Image.ROTATE_90)
        filename = 'qr_{:03d}.png'.format(idx)
        img.save(os.path.join(qr_dir, filename))
        codes[filename] = code
    with open(os.path.join(qr_dir, 'codes.json'), 'w', encoding='utf-8') as fh:
        json.dump(codes, fh, ensure_ascii=False, indent=1)


def gen_junk():
    junk_dir = os.path.join(DATA, 'junk')
    os.makedirs(junk_dir, exist_ok=True)
    rng = random.Random(4242)
    for idx in range(300):
        ext = '.jpg' if idx % 3 else '.png'
        filename = 'junk_{:03d}{}'.format(idx, ext)
        with open(os.path.join(junk_dir, filename), 'wb') as fh:
            fh.write(rng.getrandbits(8 * 40 * 1024).to_bytes(40 * 1024, 'little'))


def gen_pairing():
    pairing_dir = os.path.join(DATA, 'pairing')
    os.makedirs(pairing_dir, exist_ok=True)
    rng = random.Random(777)
    for base in range(100, 250):
        for suffix in '1234':
            if rng.random() < 0.1:
                continue  # неполные пары
            names = ['{}.jpg'.format(base + '-' + suffix)]
            if rng.random() < 0.15:
                names.append('{}({}).jpg'.format(base + '-' + suffix, 2))
            for name in names:
                with open(os.path.join(pairing_dir, name), 'wb') as fh:
                    fh.write(b'\xff\xd8\xff\xe0' + bytes(512))


def main():
    if os.path.isdir(DATA):
        shutil.rmtree(DATA)
    os.makedirs(os.path.join(DATA, 'csv'), exist_ok=True)

    write_big_csv(os.path.join(DATA, 'csv', 'big_10k.csv'), 10000, 1200)
    write_big_csv(os.path.join(DATA, 'csv', 'big_50k.csv'), 50000, 5000)

    # CSV для сплит-бенча: 50 студентов × 6 кодов; коды 0..299 совпадают с qr/
    students = gen_students(50)
    with open(os.path.join(DATA, 'csv', 'split_students.csv'), 'w',
              encoding='utf-8', newline='') as fh:
        fh.write('Фамилия;Имя;Класс;код;email\n')
        for s_idx, (surname, name, grade) in enumerate(students):
            for c_idx in range(6):
                code_idx = s_idx * 6 + c_idx
                fh.write('{};{};{};{};{}.{}@example.com\n'.format(
                    surname, name, grade, make_code(code_idx),
                    name.lower(), surname.lower()))

    gen_qr()
    gen_junk()
    gen_pairing()
    print('OK: датасеты в', DATA)


if __name__ == '__main__':
    main()
