# -*- coding: utf-8 -*-
"""Микробенчмарк SMTP-клиентов: Python smtplib vs Rust lettre (pyqr_core).

Локальный SMTP-синк на 127.0.0.1 (stdlib smtpd), режим plain (без TLS),
одинаковое число одинаковых писем с обеих сторон. Меряет именно клиента
и сборку письма, а не сервер.

  ./bin/python benchmarks/bench_smtp.py --out benchmarks/results/smtp.json
"""
import argparse
import json
import os
import smtplib
import socket
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


class SyncSink(object):
    """Минимальный синхронный SMTP-синк: accept-цикл + поток на соединение
    (asyncore с поллингом оказался узким горлышком для бенчмарка)."""

    def __init__(self):
        self.sock = socket.socket()
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(('127.0.0.1', 0))
        self.sock.listen(128)
        self.port = self.sock.getsockname()[1]
        self.count = 0
        self._lock = threading.Lock()
        threading.Thread(target=self._loop, daemon=True).start()

    def _loop(self):
        while True:
            try:
                conn, _ = self.sock.accept()
            except OSError:
                return
            threading.Thread(target=self._handle, args=(conn,), daemon=True).start()

    def _handle(self, conn):
        try:
            conn.sendall(b'220 sink ESMTP\r\n')
            buf = b''
            in_data = False
            while True:
                chunk = conn.recv(4096)
                if not chunk:
                    return
                buf += chunk
                while b'\r\n' in buf:
                    line, buf = buf.split(b'\r\n', 1)
                    if in_data:
                        if line == b'.':
                            in_data = False
                            conn.sendall(b'250 ok\r\n')
                            with self._lock:
                                self.count += 1
                        continue
                    cmd = line.upper()
                    if cmd.startswith(b'EHLO') or cmd.startswith(b'HELO'):
                        conn.sendall(b'250-sink\r\n250 8BITMIME\r\n')
                    elif cmd.startswith(b'DATA'):
                        conn.sendall(b'354 go\r\n')
                        in_data = True
                    elif cmd.startswith(b'QUIT'):
                        conn.sendall(b'221 bye\r\n')
                        conn.close()
                        return
                    else:
                        conn.sendall(b'250 ok\r\n')
        except OSError:
            pass


def start_sink():
    return SyncSink()


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

    sink = start_sink()
    port = sink.port
    time.sleep(0.2)

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
