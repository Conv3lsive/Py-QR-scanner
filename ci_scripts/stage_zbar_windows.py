# -*- coding: utf-8 -*-
"""CI-стейджинг zbar для Windows-бандла: libzbar-64.dll из wheel pyzbar."""
import pathlib
import shutil

import pyzbar

src = pathlib.Path(pyzbar.__file__).parent
dst = pathlib.Path('packaging/lib')
dst.mkdir(exist_ok=True, parents=True)
dlls = list(src.glob('*.dll'))
assert dlls, 'в wheel pyzbar нет libzbar-64.dll — нужен другой источник для Windows'
for d in dlls:
    shutil.copy2(d, dst / d.name)
print('bundled:', [d.name for d in dlls])
