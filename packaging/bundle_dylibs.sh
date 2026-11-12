#!/bin/bash
# Стейджинг системных dylib для замороженного билда (PyInstaller).
# libzbar + libjpeg с переписыванием путей на @loader_path, чтобы .app
# работал на машинах без Homebrew.
set -e
cd "$(dirname "$0")/.."

mkdir -p packaging/lib
cp /opt/homebrew/opt/zbar/lib/libzbar.0.dylib packaging/lib/libzbar.dylib
cp /opt/homebrew/opt/jpeg-turbo/lib/libjpeg.8.dylib packaging/lib/
install_name_tool -change \
  /opt/homebrew/opt/jpeg-turbo/lib/libjpeg.8.dylib \
  @loader_path/libjpeg.8.dylib packaging/lib/libzbar.dylib
codesign --force --sign - packaging/lib/libzbar.dylib packaging/lib/libjpeg.8.dylib
echo "OK: packaging/lib/ (libzbar.dylib, libjpeg.8.dylib)"
