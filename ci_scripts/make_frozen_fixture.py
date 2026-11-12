# -*- coding: utf-8 -*-
"""CI-фикстура для смоука замороженного бинаря: один QR + CSV."""
import sys

import qrcode

root = sys.argv[1]
qrcode.make('CI-FROZEN-1').save(root + '/images/q1.png')
with open(root + '/students.csv', 'w') as fh:
    fh.write('Name;Surname;code\nIvan;Ivanov;CI-FROZEN-1\n')
