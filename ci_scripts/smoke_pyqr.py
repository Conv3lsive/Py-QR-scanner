# -*- coding: utf-8 -*-
"""CI-смоук: pyqr_core импортируется и декодирует реальный QR (Rust-бэкенд)."""
import os
import sys
import tempfile

sys.path.insert(0, os.getcwd())

import backend_switch  # noqa: E402
import barcode_utils  # noqa: E402
import qrcode  # noqa: E402

assert backend_switch.RUST_AVAILABLE, 'pyqr_core не импортируется'
d = tempfile.mkdtemp()
qrcode.make('CI-SMOKE-1').save(os.path.join(d, 'q.png'))
found = barcode_utils.decode_image_cv(os.path.join(d, 'q.png'), None)
assert [c for c, _ in found] == ['CI-SMOKE-1'], found
print('pyqr_core: QR декодирован, бэкенд =', os.environ.get('PYQR_BACKEND', 'auto'))
