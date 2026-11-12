# -*- mode: python ; coding: utf-8 -*-


block_cipher = None


a = Analysis(
    ['frozen_entry.py'],
    pathex=[],
    binaries=[('/Users/convulsive/Py-QR-scanner/pyqr_core.so', '.'), ('/Users/convulsive/Py-QR-scanner/packaging/lib/libzbar.dylib', 'lib'), ('/Users/convulsive/Py-QR-scanner/packaging/lib/libjpeg.8.dylib', 'lib')],
    datas=[],
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)
pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='PyQR-scanner',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
app = BUNDLE(
    exe,
    name='PyQR-scanner.app',
    icon=None,
    bundle_identifier=None,
)
