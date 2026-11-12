# -*- coding: utf-8 -*-
"""Микробенчмарк SMTP-клиентов: Python smtplib vs Rust lettre (pyqr_core).

Локальный SMTP-синк на 127.0.0.1 (stdlib smtpd), режим plain (без TLS),
одинаковое число одинаковых писем с обеих сторон. Меряет именно клиента
и сборку письма, а не сервер.

  ./bin/python benchmarks/bench_smtp.py --out benchmarks/results/smtp.json
"""
import argparse
import asyncore
import json
import os
import smtplib
import smtpd
import statistics
import sys
import threading
import time
from email.message import EmailMessage

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import backend_switch  # noqa: E402

if not backend_switch.RUST_AVAILABLE:
    print('pyqr_core не собран — Rust-часть пропущена')


class _Sink(smtpd.SMTPServer):
    def __init__(self, *args, **kwargs):
        smtpd.SMTPServer.__init__(self, *args, **kwargs)
        self.count = 0

    def process_message(self, peer, mailfrom, rcpttos, data, **kwargs):
        self.count += 1


def start_sink():
    sink = _Sink(('127.0.0.1', 0), None)
    thread = threading.Thread(target=asyncore.loop, kwargs={'timeout': 0.1},
                              daemon=True)
    thread.start()
    port = sink.socket.getsockname()[1]
    return sink, port


def build_python_message(i):
    msg = EmailMessage()
    msg['From'] = 'bench@example.com'
    msg['To'] = 'student{}@example.com'.format(i)
    msg['Subject'] = 'Бенчмарк письмо {}'.format(i)
    msg.set_content('Тело письма №{} — проверка скорости SMTP-клиента.\n'.format(i) * 3)
    return msg


def bench_python(port, count):
    def run():
        with smtplib.SMTP('127.0.0.1', port, timeout=30) as smtp:
            for i in range(count):
                smtp.send_message(build_python_message(i))
    return run


def bench_rust(port, count):
    send = backend_switch.pyqr_core.email.send_smtp

    def run():
        for i in range(count):
            ok, _msg_id, err = send(
                '127.0.0.1', port, '', '', 'plain',
                'bench@example.com', 'student{}@example.com'.format(i),
                'Бенчмарк письмо {}'.format(i),
                'Тело письма №{} — проверка скорости SMTP-клиента.\n'.format(i) * 3,
                None)
            if not ok:
                raise RuntimeError(err)
    return run


def timed(fn, reps):
    runs = []
    for _ in range(reps):
        t0 = time.perf_counter()
        fn()
        runs.append(round((time.perf_counter() - t0) * 1000, 3))
    return {'median_ms': round(statistics.median(runs), 3), 'runs_ms': runs}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--out', default=None)
    parser.add_argument('--count', type=int, default=50)
    parser.add_argument('--reps', type=int, default=3)
    args = parser.parse_args()

    sink, port = start_sink()
    time.sleep(0.3)

    results = {}
    results['smtp_python_50msg'] = timed(bench_python(port, args.count), args.reps)
    if backend_switch.RUST_AVAILABLE:
        results['smtp_rust_50msg'] = timed(bench_rust(port, args.count), args.reps)

    for key in results:
        print('{:24s} {:>12.2f} ms'.format(key, results[key]['median_ms']))
    print('писем принято синком:', sink.count)

    if args.out:
        payload = {'meta': {'backend': 'smtp-clients', 'count': args.count},
                   'results': results}
        os.makedirs(os.path.dirname(args.out), exist_ok=True)
        with open(args.out, 'w', encoding='utf-8') as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=1)
        print('->', args.out)


if __name__ == '__main__':
    main()
