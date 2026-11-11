# -*- coding: utf-8 -*-
"""Сравнение результатов бенчмарков Python vs Rust.

  ./bin/python benchmarks/compare.py benchmarks/results/python_baseline.json \
      benchmarks/results/rust.json
"""
import json
import sys


def load(path):
    with open(path, encoding='utf-8') as fh:
        return json.load(fh)


def main():
    if len(sys.argv) != 3:
        print(__doc__)
        sys.exit(1)
    py, ru = load(sys.argv[1]), load(sys.argv[2])

    print('meta python: {} | commit {}'.format(py['meta']['python'], py['meta']['commit']))
    print('meta rust:   {} | commit {} | cpu: {}'.format(
        ru['meta']['python'], ru['meta']['commit'], ru['meta']['cpu']))
    print()
    print('{:24s} {:>12s} {:>12s} {:>9s}'.format('метрика', 'python ms', 'rust ms', 'ускор.'))
    print('-' * 62)
    rows = []
    for key in sorted(py['results']):
        if key not in ru['results']:
            continue
        py_ms = py['results'][key]['median_ms']
        ru_ms = ru['results'][key]['median_ms']
        speedup = (py_ms / ru_ms) if ru_ms > 0 else float('inf')
        rows.append((key, py_ms, ru_ms, speedup))
        print('{:24s} {:>12.2f} {:>12.2f} {:>8.2f}x'.format(key, py_ms, ru_ms, speedup))
    if rows:
        geo = 1.0
        for _, _, _, s in rows:
            geo *= s
        geo **= 1.0 / len(rows)
        print('-' * 62)
        print('{:24s} {:>33.2f}x'.format('геометрическое среднее', geo))


if __name__ == '__main__':
    main()
