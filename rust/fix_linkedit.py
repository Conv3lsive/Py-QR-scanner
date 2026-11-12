# -*- coding: utf-8 -*-
"""Выравнивание strtab в Mach-O cdylib под 8 байт.

Новый ld (Xcode 26+) кладёт таблицу строк символов (LC_SYMTAB.stroff)
с 4-байтовым выравниванием; dyld macOS 26+ требует 8 и роняет dlopen с
"mis-aligned LINKEDIT string pool". Скрипт вставляет N байт паддинга
ПЕРЕД strtab (между предыдущим блоком LINKEDIT и таблицей строк),
пропатчивает load-команды (stroff, размеры __LINKEDIT, dataoff-блоки
после вставки) и пересигнитит файл. Индексы n_strx не меняются: таблица
и её база сдвигаются на одно и то же число.

  python3 fix_linkedit.py <файл.dylib|so> [--check]
"""
import struct
import subprocess
import sys

MH_MAGIC_64 = 0xFEEDFACF
LC_SYMTAB = 0x2
LC_CODE_SIGNATURE = 0x1D
LC_SEGMENT_64 = 0x19
LC_FUNCTION_STARTS = 0x26
LC_DATA_IN_CODE = 0x29
LC_DYLD_INFO_ONLY = 0x80000022
LC_DYLD_EXPORTS_TRIE = 0x80000033
LC_DYLD_CHAINED_FIXUPS = 0x80000034


def load_commands(blob):
    """yield (offset_в_файле, cmd, cmdsize)."""
    ncmds = struct.unpack_from('<I', blob, 16)[0]
    off = 32
    for _ in range(ncmds):
        cmd, cmdsize = struct.unpack_from('<II', blob, off)
        yield off, cmd, cmdsize
        off += cmdsize


def main():
    path = sys.argv[1]
    check_only = '--check' in sys.argv
    with open(path, 'rb') as fh:
        blob = bytearray(fh.read())
    if struct.unpack_from('<I', blob, 0)[0] != MH_MAGIC_64:
        print('не Mach-O 64 LE, выходим')
        return 1

    # stroff из LC_SYMTAB
    stroff = None
    for off, cmd, cmdsize in load_commands(bytes(blob)):
        if cmd == LC_SYMTAB:
            stroff = struct.unpack_from('<I', blob, off + 8 + 8)[0]
            break
    if stroff is None:
        print('LC_SYMTAB не найден')
        return 1

    pad = (8 - stroff % 8) % 8
    print('stroff={} (mod 8 = {}), pad={}'.format(stroff, stroff % 8, pad))
    if pad == 0:
        print('уже выровнено, ок')
        return 0
    if check_only:
        print('требуется паддинг {} байт'.format(pad))
        return 2

    new = bytearray(blob[:stroff]) + bytearray(pad) + bytearray(blob[stroff:])

    for off, cmd, cmdsize in load_commands(bytes(blob)):
        if cmd == LC_SYMTAB:
            struct.pack_into('<I', new, off + 16, stroff + pad)
        elif cmd == LC_SEGMENT_64:
            name = bytes(blob[off + 8:off + 24]).split(b'\0', 1)[0]
            if name == b'__LINKEDIT':
                fileoff, filesize = struct.unpack_from('<QQ', blob, off + 32)
                vmsize = struct.unpack_from('<Q', blob, off + 24)[0]
                filesize += pad
                if vmsize < filesize:
                    vmsize = (filesize + 0x3FFF) & ~0x3FFF
                struct.pack_into('<Q', new, off + 24, vmsize)
                struct.pack_into('<Q', new, off + 40, filesize)
                assert fileoff <= stroff
        elif cmd in (LC_FUNCTION_STARTS, LC_DATA_IN_CODE, LC_CODE_SIGNATURE,
                     LC_DYLD_EXPORTS_TRIE, LC_DYLD_CHAINED_FIXUPS):
            dataoff = struct.unpack_from('<I', blob, off + 8)[0]
            if dataoff >= stroff:
                struct.pack_into('<I', new, off + 8, dataoff + pad)
        elif cmd == LC_DYLD_INFO_ONLY:
            for i in range(6):  # 6 пар (off, size)
                foff = off + 8 + i * 8
                val = struct.unpack_from('<I', blob, foff)[0]
                if val >= stroff:
                    struct.pack_into('<I', new, foff, val + pad)

    with open(path, 'wb') as fh:
        fh.write(new)
    subprocess.run(['codesign', '--force', '--sign', '-', path], check=True,
                   capture_output=True)

    # самопроверка
    out = subprocess.run(['otool', '-l', path], capture_output=True, text=True,
                         check=True).stdout
    new_stroff = None
    lines = out.splitlines()
    for i, line in enumerate(lines):
        if 'LC_SYMTAB' in line:
            for j in range(i, min(i + 8, len(lines))):
                if 'stroff' in lines[j]:
                    new_stroff = int(lines[j].split()[-1])
                    break
            break
    print('новый stroff={}, mod 8 = {}'.format(new_stroff,
                                               new_stroff % 8 if new_stroff else '?'))
    return 0 if new_stroff and new_stroff % 8 == 0 else 3


if __name__ == '__main__':
    sys.exit(main())
