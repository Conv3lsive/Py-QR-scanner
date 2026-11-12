#!/bin/bash
# Сборка pyqr_core.so с гарантией 8-байтового выравнивания strtab.
#
# Баг ld-27106 (новый линкер Apple): strtab символов кладётся с 4-байтовым
# выравниванием, а dyld macOS 26+ требует 8 — dlopen падает с
# "mis-aligned LINKEDIT string pool". Длина имени экспортируемого символа
# pyqr_pad* в src/lib.rs сдвигает раскладку __LINKEDIT; скрипт перебирает
# её, пока stroff не станет кратен 8.
set -e
cd "$(dirname "$0")/pyqr-core"

# Единый deployment target: иначе build-скрипты ring/libsqlite3-sys собирают
# объекты под minos хоста, а итоговый линк идёт под 11.0 (warn'ы и риск
# раскладки __LINKEDIT).
export MACOSX_DEPLOYMENT_TARGET=11.0

for attempt in 1 2 3 4 5 6 7 8; do
  if ! cargo build --release 2>&1 | grep -E '^error' ; then :; else
    echo "ошибка сборки"; exit 1
  fi
  DYLIB="target/release/libpyqr_core.dylib"
  STROFF=$(otool -l "$DYLIB" | grep -A5 LC_SYMTAB | grep stroff | tr -dc '0-9')
  MOD=$(( STROFF % 8 ))
  if [ "$MOD" -eq 0 ]; then
    cp "$DYLIB" ../../pyqr_core.so
    echo "OK: pyqr_core.so собран (stroff=$STROFF, попытка $attempt)"
    exit 0
  fi
  PAD=$(sed -nE 's/.*pub extern "C" fn (pyqr_pad[a-z0-9_]*)\(\).*/\1/p' src/lib.rs | head -1)
  NEWPAD="${PAD}x"
  sed -i '' "s/fn ${PAD}()/fn ${NEWPAD}/" src/lib.rs
  echo "stroff=$STROFF (mod 8 = $MOD) — пэд-символ: $PAD -> $NEWPAD, пересборка"
done
echo "ОШИБКА: не удалось выровнять strtab за 8 попыток"
exit 1
