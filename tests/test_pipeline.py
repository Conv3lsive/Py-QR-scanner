"""Тесты модуля pipeline.py.

Покрытие: run_action (action 0..4), контракт progress_callback, проброс
исключений, process_watch_batch (сигнатура, статистика, дедуп через state,
move_mode, фильтрация расширений, ошибки хэширования, пустой CSV),
WATCH_EXTENSIONS.

Нижние слои (barcode_utils, csv_utils, email_*, file_utils, zip_utils)
подменяются через monkeypatch атрибутов модулей. Реальная ФС — tmp_path.
Время не используется напрямую. Исходники приложения не изменяются.
"""

import os
import types

import pytest

import pipeline
from processing_state import ProcessingState


# ---------------------------------------------------------------------------
# Вспомогательные объекты
# ---------------------------------------------------------------------------

REQUIRED_PROGRESS_KEYS = {'action', 'done', 'total', 'unit', 'message'}


# ---------------------------------------------------------------------------
# progress_callback контракт
# ---------------------------------------------------------------------------

def test_emit_progress_dict_contract():
    captured = []
    pipeline._emit_progress(captured.append, 3, 2, None, 'файлов', 'msg')
    p = captured[0]
    assert set(p.keys()) == REQUIRED_PROGRESS_KEYS
    assert p['action'] == 3
    assert isinstance(p['done'], int) and p['done'] == 2
    assert p['total'] is None
    assert p['unit'] == 'файлов'
    assert p['message'] == 'msg'


def test_emit_progress_casts_done_and_total_to_int():
    captured = []
    pipeline._emit_progress(captured.append, 0, 5.9, 10.1, 'ед.', 'x')
    p = captured[0]
    assert p['done'] == 5 and isinstance(p['done'], int)
    assert p['total'] == 10 and isinstance(p['total'], int)


def test_emit_progress_noop_when_callback_is_none():
    assert pipeline._emit_progress(None, 1, 2, 3, 'ед.', 'x') is None


# ---------------------------------------------------------------------------
# WATCH_EXTENSIONS
# ---------------------------------------------------------------------------

def test_watch_extensions():
    assert pipeline.WATCH_EXTENSIONS == {'.jpg', '.jpeg', '.png'}


# ---------------------------------------------------------------------------
# run_action: action 0 (переименование)
# ---------------------------------------------------------------------------

def test_run_action_0_calls_find_barcodes_and_renamer(monkeypatch):
    calls = {}
    monkeypatch.setattr(
        'barcode_utils.find_barcodes',
        lambda image_folder, max_workers, progress_callback:
            calls.setdefault('find', []).append((image_folder, max_workers))
            or {'C1': ['a.jpg']},
    )
    monkeypatch.setattr(
        'barcode_utils.file_renamer',
        lambda image_folder, barcode, progress_callback:
            calls.setdefault('rename', []).append((image_folder, barcode)),
    )

    result = pipeline.run_action(0, image_folder='/tmp/img', threads=3)

    assert result['status'] == 'ok'
    assert result['message'] == 'Файлы переименованы'
    assert calls['find'] == [('/tmp/img', 3)]
    assert calls['rename'][0][0] == '/tmp/img'
    assert calls['rename'][0][1] == {'C1': ['a.jpg']}
    # Новые ключи времени/скорости присутствуют и валидны
    assert result['files'] == 1
    assert isinstance(result['elapsed_ms'], int) and result['elapsed_ms'] >= 0
    assert isinstance(result['speed'], (int, float)) and result['speed'] >= 0


def test_run_action_0_requires_image_folder():
    with pytest.raises(ValueError):
        pipeline.run_action(0)


def test_run_action_0_progress_payloads(monkeypatch):
    payloads = []

    def fake_find(image_folder, max_workers, progress_callback):
        progress_callback(3, 10, 'файлов', 'распознавание')
        return {}

    def fake_rename(image_folder, barcode, progress_callback):
        progress_callback(0, 0, 'файлов', 'переименование')

    monkeypatch.setattr('barcode_utils.find_barcodes', fake_find)
    monkeypatch.setattr('barcode_utils.file_renamer', fake_rename)

    pipeline.run_action(0, image_folder='/x', progress_callback=payloads.append)

    # 2 от нижних слоёв + 1 финальный сигнал
    assert len(payloads) == 3
    for p in payloads:
        assert set(p.keys()) == REQUIRED_PROGRESS_KEYS
        assert p['action'] == 0
        assert isinstance(p['done'], int)
        assert p['total'] is None or isinstance(p['total'], int)
        assert isinstance(p['unit'], str)
        assert isinstance(p['message'], str)
    # финальный сигнал всегда (1, 1, 'этап', ...)
    assert payloads[-1]['done'] == 1
    assert payloads[-1]['total'] == 1


# ---------------------------------------------------------------------------
# run_action: action 1 (распределение по фамилиям)
# ---------------------------------------------------------------------------

def test_run_action_1_kwargs_and_result(monkeypatch):
    calls = {}
    monkeypatch.setattr(
        'barcode_utils.find_barcodes',
        lambda image_folder, max_workers, progress_callback: {'C1': ['a.jpg']},
    )
    monkeypatch.setattr(
        'barcode_utils.split_by_student_folders',
        lambda barcodes, data, output_folder, max_workers, progress_callback:
            calls.__setitem__('split', (barcodes, data, output_folder, max_workers)),
    )
    monkeypatch.setattr(
        pipeline, 'read_csv',
        lambda csv_path, code_name, student_fields, csv_delimiter='auto':
            calls.__setitem__('read_csv', (csv_path, code_name, student_fields, csv_delimiter))
            or {'Иванов': {'код': ['C1']}},
    )
    monkeypatch.setattr(
        pipeline, 'move_unfound',
        lambda barcodes, data, output_folder, move_mode='copy':
            calls.__setitem__('unfound', (barcodes, data, output_folder, move_mode)),
    )
    monkeypatch.setattr(
        pipeline, 'move_clear',
        lambda output_folder, image_folder, found_files, move_mode='copy':
            calls.__setitem__('clear', (output_folder, image_folder, found_files, move_mode)),
    )

    result = pipeline.run_action(
        1, image_folder='/img', csv_path='/c.csv', name_fields=['ФИО'],
        output_folder='/out', move_mode='move', threads=4,
    )

    assert result['status'] == 'ok'
    assert result['students'] == 1
    assert result['decoded'] == 1
    # точные kwargs в нижние слои
    assert calls['read_csv'] == ('/c.csv', 'код', ['ФИО'], 'auto')
    assert calls['unfound'][3] == 'move'
    assert calls['clear'][3] == 'move'
    assert calls['split'][2] == '/out'
    assert calls['split'][3] == 4


def test_run_action_1_requires_all_args():
    with pytest.raises(ValueError):
        pipeline.run_action(1, image_folder='/img', csv_path='/c.csv')
    with pytest.raises(ValueError):
        pipeline.run_action(1, image_folder='/img')


# ---------------------------------------------------------------------------
# run_action: action 2 (архивация)
# ---------------------------------------------------------------------------

def test_run_action_2_kwargs(monkeypatch):
    calls = {}
    monkeypatch.setattr(
        pipeline, 'zip_student_folders',
        lambda output_folder, max_workers, progress_callback:
            calls.__setitem__('zip', (output_folder, max_workers))
            or {'Иванов': '/out/Иванов.zip'},
    )
    result = pipeline.run_action(2, output_folder='/out', threads=5)
    assert result['status'] == 'ok'
    assert result['archives'] == 1
    assert calls['zip'] == ('/out', 5)


def test_run_action_2_requires_output_folder():
    with pytest.raises(ValueError):
        pipeline.run_action(2)


# ---------------------------------------------------------------------------
# run_action: action 3 (рассылка email)
# ---------------------------------------------------------------------------

def _make_summary(**kw):
    defaults = dict(
        sent=1, permanent_failed=0, temp_failed=0, skipped_duplicate=0,
        retried=0, total_attempts=1,
    )
    defaults.update(kw)
    return types.SimpleNamespace(**defaults)


def test_run_action_3_skips_invalid_emails(monkeypatch, tmp_path):
    out = tmp_path / 'out'
    out.mkdir()
    sent = {}

    monkeypatch.setattr(
        pipeline, 'zip_student_folders',
        lambda output_folder, max_workers, progress_callback:
            {'Иванов': str(out / 'Иванов.zip'),
             'Петров': str(out / 'Петров.zip')},
    )
    monkeypatch.setattr(
        pipeline, 'read_csv_with_email',
        lambda csv_path, code_name, student_fields, email_field='email', csv_delimiter='auto':
            ({'Иванов': {}, 'Петров': {}},
             {'Иванов': 'good@example.com', 'Петров': 'bad'}),
    )
    monkeypatch.setattr(pipeline, 'get_email_config',
                        lambda: {'EMAIL_SUBJECT': 'Тема', 'EMAIL_BODY': 'Тело'})
    monkeypatch.setattr(pipeline, 'get_smtp_config',
                        lambda: {'SMTP_EMAIL': 'a@b.com', 'SMTP_PASSWORD': 'p',
                                 'SMTP_HOST': 'h', 'SMTP_PORT': 465})
    monkeypatch.setattr(pipeline, 'get_fallback_smtp_config',
                        lambda: {'FALLBACK_SMTP_EMAIL': 'fb@b.com',
                                 'FALLBACK_SMTP_PASSWORD': 'p2',
                                 'FALLBACK_SMTP_HOST': 'h2',
                                 'FALLBACK_SMTP_PORT': 587})
    monkeypatch.setattr(
        pipeline, 'is_valid_email',
        lambda email: email == 'good@example.com',
    )

    def fake_send(tasks, primary_cfg, fallback_cfg=None, store=None,
                  threads=4, progress_callback=None):
        sent['tasks'] = tasks
        sent['primary'] = primary_cfg
        sent['fallback'] = fallback_cfg
        sent['threads'] = threads
        sent['store'] = store
        return _make_summary(sent=1)

    monkeypatch.setattr(pipeline, 'send_batch_with_retry', fake_send)

    result = pipeline.run_action(
        3, csv_path='/c.csv', name_fields=['ФИО'], output_folder=str(out), threads=6,
    )

    assert result['status'] == 'ok'
    assert result['sent'] == 1
    assert result['invalid_recipients'] == 1
    assert len(sent['tasks']) == 1
    assert sent['tasks'][0].recipient == 'good@example.com'
    assert sent['primary'].name == 'primary'
    assert sent['fallback'].name == 'fallback'
    # только один валидный получатель => потоки ужимаются до 1
    assert sent['threads'] == 1
    # store создаётся в output_folder/state/email_delivery.sqlite
    assert os.path.exists(str(out / 'state' / 'email_delivery.sqlite'))


def test_run_action_3_requires_args():
    with pytest.raises(ValueError):
        pipeline.run_action(3, csv_path='/c.csv', name_fields=['ФИО'])


# ---------------------------------------------------------------------------
# run_action: action 4 (валидация email)
# ---------------------------------------------------------------------------

def test_run_action_4_kwargs(monkeypatch):
    calls = {}
    monkeypatch.setattr(
        pipeline, 'read_csv_with_email',
        lambda csv_path, code_name, student_fields, email_field='email', csv_delimiter='auto':
            ({}, {'a@b.c': 'a@b.c'}),
    )
    monkeypatch.setattr(
        pipeline, 'validate_emails',
        lambda emails, max_workers, progress_callback:
            calls.__setitem__('validate', (emails, max_workers)),
    )
    result = pipeline.run_action(4, csv_path='/c.csv', name_fields=['ФИО'], threads=7)
    assert result['status'] == 'ok'
    assert result['emails'] == 1
    assert calls['validate'] == ({'a@b.c': 'a@b.c'}, 7)


def test_run_action_4_requires_args():
    with pytest.raises(ValueError):
        pipeline.run_action(4, csv_path='/c.csv')


# ---------------------------------------------------------------------------
# run_action: прочее
# ---------------------------------------------------------------------------

def test_run_action_unknown_raises():
    with pytest.raises(ValueError):
        pipeline.run_action(99)


def test_run_action_lower_layer_exception_propagates(monkeypatch):
    def boom(*a, **k):
        raise RuntimeError('нижний слой упал')

    monkeypatch.setattr('barcode_utils.find_barcodes', boom)
    with pytest.raises(RuntimeError):
        pipeline.run_action(0, image_folder='/img')


# ---------------------------------------------------------------------------
# process_watch_batch
# ---------------------------------------------------------------------------

def test_process_watch_batch_result_keys_and_types(monkeypatch, tmp_path):
    p1 = tmp_path / 'a.jpg'
    p1.write_bytes(b'AAAA')
    out = tmp_path / 'out'
    out.mkdir()

    monkeypatch.setattr(pipeline, 'read_csv', lambda *a, **k: {'С': {'код': ['C1']}})
    monkeypatch.setattr(
        'barcode_utils.find_barcodes_in_files',
        lambda files, max_workers=4: {'C1': list(files)},
    )
    monkeypatch.setattr(pipeline, 'move_unfound', lambda *a, **k: None)
    monkeypatch.setattr(pipeline, '_move_clear_for_batch', lambda *a, **k: None)
    monkeypatch.setattr('barcode_utils.split_by_student_folders', lambda *a, **k: None)

    result = pipeline.process_watch_batch(
        [str(p1)], str(tmp_path / 'c.csv'), ['ФИО'], str(out),
    )

    assert set(result.keys()) == {'incoming', 'processed', 'duplicates', 'unrecognized'}
    assert all(isinstance(v, int) for v in result.values())
    assert result == {'incoming': 1, 'processed': 1, 'duplicates': 0, 'unrecognized': 0}


def test_process_watch_batch_filters_and_dedups(monkeypatch, tmp_path):
    p1 = tmp_path / 'a.jpg'
    p1.write_bytes(b'A')
    txt = tmp_path / 'b.txt'
    txt.write_bytes(b'T')
    ghost = str(tmp_path / 'ghost.png')

    find_calls = []
    monkeypatch.setattr(pipeline, 'read_csv', lambda *a, **k: {})
    monkeypatch.setattr(
        'barcode_utils.find_barcodes_in_files',
        lambda files, max_workers=4: find_calls.append(list(files)) or {},
    )
    monkeypatch.setattr(pipeline, 'move_unfound', lambda *a, **k: None)
    monkeypatch.setattr(pipeline, '_move_clear_for_batch', lambda *a, **k: None)
    monkeypatch.setattr('barcode_utils.split_by_student_folders', lambda *a, **k: None)

    result = pipeline.process_watch_batch(
        [str(p1), str(p1), str(txt), ghost],
        str(tmp_path / 'c.csv'), ['ФИО'], str(tmp_path / 'out'),
    )

    assert result == {'incoming': 1, 'processed': 0, 'duplicates': 0, 'unrecognized': 1}
    assert find_calls[0] == [str(p1)]


def test_process_watch_batch_move_mode_passthrough(monkeypatch, tmp_path):
    p1 = tmp_path / 'a.jpg'
    p1.write_bytes(b'AAAA')
    out = tmp_path / 'out'
    out.mkdir()
    captured = {}

    monkeypatch.setattr(pipeline, 'read_csv', lambda *a, **k: {'С': {'код': ['C1']}})
    monkeypatch.setattr(
        'barcode_utils.find_barcodes_in_files',
        lambda files, max_workers=4: {'C1': list(files)},
    )
    monkeypatch.setattr(
        pipeline, 'move_unfound',
        lambda barcodes, data, output_folder, move_mode='copy':
            captured.__setitem__('mf', move_mode),
    )
    monkeypatch.setattr(
        pipeline, '_move_clear_for_batch',
        lambda output_folder, considered, found, move_mode='copy':
            captured.__setitem__('mc', move_mode),
    )
    monkeypatch.setattr('barcode_utils.split_by_student_folders', lambda *a, **k: None)

    pipeline.process_watch_batch(
        [str(p1)], str(tmp_path / 'c.csv'), ['ФИО'], str(out), move_mode='move',
    )
    assert captured['mf'] == 'move'
    assert captured['mc'] == 'move'


def test_process_watch_batch_state_dedup_second_run(monkeypatch, tmp_path):
    p1 = tmp_path / 'a.jpg'
    p1.write_bytes(b'AAA')
    p2 = tmp_path / 'b.png'
    p2.write_bytes(b'BBB')
    out = tmp_path / 'out'
    out.mkdir()
    state = ProcessingState(str(tmp_path / 'state.sqlite'))

    mapping = {str(p1): 'C1', str(p2): 'C2'}
    monkeypatch.setattr(pipeline, 'read_csv', lambda *a, **k: {'С': {'код': ['C1']}})
    monkeypatch.setattr(
        'barcode_utils.find_barcodes_in_files',
        lambda files, max_workers=4:
            {code: [path] for path, code in mapping.items() if path in files},
    )
    monkeypatch.setattr(pipeline, 'move_unfound', lambda *a, **k: None)
    monkeypatch.setattr(pipeline, '_move_clear_for_batch', lambda *a, **k: None)
    monkeypatch.setattr('barcode_utils.split_by_student_folders', lambda *a, **k: None)

    csv = str(tmp_path / 'c.csv')
    first = pipeline.process_watch_batch([str(p1), str(p2)], csv, ['ФИО'], str(out), state=state)
    assert first == {'incoming': 2, 'processed': 2, 'duplicates': 0, 'unrecognized': 0}
    assert state.stats().get('processed') == 2

    second = pipeline.process_watch_batch([str(p1), str(p2)], csv, ['ФИО'], str(out), state=state)
    assert second == {'incoming': 2, 'processed': 0, 'duplicates': 2, 'unrecognized': 0}


def test_process_watch_batch_hash_error_recorded(monkeypatch, tmp_path):
    good = tmp_path / 'good.jpg'
    good.write_bytes(b'G')
    bad = tmp_path / 'bad.jpg'
    bad.write_bytes(b'B')
    out = tmp_path / 'out'
    out.mkdir()
    state = ProcessingState(str(tmp_path / 'state.sqlite'))

    def fake_hash(path):
        if str(path).endswith('bad.jpg'):
            raise OSError('ошибка чтения')
        return 'GOODHASH'

    monkeypatch.setattr(pipeline, '_hash_file', fake_hash)
    monkeypatch.setattr(pipeline, 'read_csv', lambda *a, **k: {'С': {'код': ['C1']}})
    monkeypatch.setattr(
        'barcode_utils.find_barcodes_in_files',
        lambda files, max_workers=4:
            {'C1': [f for f in files if f.endswith('good.jpg')]},
    )
    monkeypatch.setattr(pipeline, 'move_unfound', lambda *a, **k: None)
    monkeypatch.setattr(pipeline, '_move_clear_for_batch', lambda *a, **k: None)
    monkeypatch.setattr('barcode_utils.split_by_student_folders', lambda *a, **k: None)

    result = pipeline.process_watch_batch(
        [str(good), str(bad)], str(tmp_path / 'c.csv'), ['ФИО'], str(out), state=state,
    )
    assert result == {'incoming': 2, 'processed': 1, 'duplicates': 0, 'unrecognized': 0}
    st = state.stats()
    assert st.get('processed') == 1
    assert st.get('hash_error') == 1


def test_process_watch_batch_missing_csv_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        pipeline.process_watch_batch(
            [], str(tmp_path / 'nonexistent.csv'), ['ФИО'], str(tmp_path / 'out'),
        )


def test_process_watch_batch_empty_csv_real(monkeypatch, tmp_path):
    csv = tmp_path / 'empty.csv'
    csv.write_text('', encoding='utf-8')
    p1 = tmp_path / 'a.jpg'
    p1.write_bytes(b'AAAA')
    out = tmp_path / 'out'
    out.mkdir()

    monkeypatch.setattr(
        'barcode_utils.find_barcodes_in_files',
        lambda files, max_workers=4: {'C1': list(files)},
    )
    monkeypatch.setattr(pipeline, 'move_unfound', lambda *a, **k: None)
    monkeypatch.setattr(pipeline, '_move_clear_for_batch', lambda *a, **k: None)
    monkeypatch.setattr('barcode_utils.split_by_student_folders', lambda *a, **k: None)

    # read_csv не мокаем: реальный пустой CSV -> пустой словарь данных
    result = pipeline.process_watch_batch(
        [str(p1)], str(csv), ['ФИО'], str(out),
    )
    assert result == {'incoming': 1, 'processed': 1, 'duplicates': 0, 'unrecognized': 0}

def test_run_action_3_status_error_when_delivery_failed(monkeypatch, tmp_path):
    # Баг №10: рассылка частично/полностью провалилась (failed > 0), но
    # статус был 'ok' — GUI красил такое «завершено» зелёным.
    out = tmp_path / 'out'
    out.mkdir()

    monkeypatch.setattr(pipeline, 'zip_student_folders',
                        lambda output_folder, max_workers, progress_callback:
                        {'Иванов': str(out / 'Иванов.zip')})
    monkeypatch.setattr(
        pipeline, 'read_csv_with_email',
        lambda csv_path, code_name, student_fields, email_field='email', csv_delimiter='auto':
            ({'Иванов': {}}, {'Иванов': 'ivan@example.com'}),
    )
    monkeypatch.setattr(pipeline, 'get_email_config',
                        lambda: {'EMAIL_SUBJECT': 'Тема', 'EMAIL_BODY': 'Тело'})
    monkeypatch.setattr(pipeline, 'get_smtp_config',
                        lambda: {'SMTP_EMAIL': 'a@b.com', 'SMTP_PASSWORD': 'p',
                                 'SMTP_HOST': 'h', 'SMTP_PORT': 465})
    monkeypatch.setattr(pipeline, 'get_fallback_smtp_config', lambda: None)
    monkeypatch.setattr(pipeline, 'is_valid_email', lambda email: True)
    monkeypatch.setattr(
        pipeline, 'send_batch_with_retry',
        lambda tasks, primary_cfg, fallback_cfg=None, store=None,
                threads=4, progress_callback=None:
            _make_summary(sent=0, permanent_failed=2, temp_failed=1),
    )

    result = pipeline.run_action(
        3, csv_path='/c.csv', name_fields=['ФИО'], output_folder=str(out), threads=4,
    )

    assert result['status'] == 'error'
    assert result['failed'] == 3
    assert result['permanent_failed'] == 2
    assert result['temp_failed'] == 1
