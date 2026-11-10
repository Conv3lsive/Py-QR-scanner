import os
import zipfile

import zip_utils


def _student(output, name, files):
    base = os.path.join(output, name)
    for rel, content in files:
        p = os.path.join(base, rel)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p, 'wb') as f:
            f.write(content)
    return base


def test_zip_content_and_nested_subfolders(tmp_path):
    out = str(tmp_path)
    _student(out, 'stu', [
        ('1.jpg', b'1'),
        ('sub/nested.txt', b't'),
        ('sub/deep/2.jpg', b'2'),
    ])
    result = zip_utils.zip_student_folders(out, max_workers=2)
    assert set(result) == {'stu'}
    with zipfile.ZipFile(result['stu']) as z:
        assert set(z.namelist()) == {'1.jpg', 'sub/nested.txt', 'sub/deep/2.jpg'}


def test_excluded_archive_folders_skipped(tmp_path):
    out = str(tmp_path)
    for name in ('unsorted', 'noqrcode', 'unfound', 'state'):
        _student(out, name, [('x.jpg', b'x')])
    _student(out, 'stu', [('a.jpg', b'a')])
    result = zip_utils.zip_student_folders(out, max_workers=2)
    assert set(result) == {'stu'}
    zips = {f for f in os.listdir(out) if f.endswith('.zip')}
    assert zips == {'stu.zip'}


def test_up_to_date_zip_not_recreated(tmp_path):
    out = str(tmp_path)
    base = os.path.join(out, 'stu')
    os.makedirs(base)
    src = os.path.join(base, 'a.jpg')
    with open(src, 'wb') as f:
        f.write(b'IMG')
    old = 1_000_000_000
    os.utime(src, (old, old))
    os.utime(base, (old, old))

    zp = os.path.join(out, 'stu.zip')
    with zipfile.ZipFile(zp, 'w') as z:
        z.writestr('stub.txt', 'STUB')
    os.utime(zp, (old + 1000, old + 1000))

    result = zip_utils.zip_student_folders(out, max_workers=1)
    assert result == {'stu': zp}
    # содержимое не пересоздано: остался только stub.txt
    with zipfile.ZipFile(zp) as z:
        assert z.namelist() == ['stub.txt']
        assert z.read('stub.txt') == b'STUB'


def test_zip_error_returns_none_and_does_not_crash(tmp_path, monkeypatch):
    out = str(tmp_path)
    _student(out, 'stu', [('a.jpg', b'a')])

    def boom(*args, **kwargs):
        raise OSError('simulated archive failure')

    monkeypatch.setattr(zip_utils.zipfile, 'ZipFile', boom)
    assert zip_utils._zip_one_student(out, 'stu') == ('stu', None)
    # общий вызов не падает, но и не возвращает ошибочную папку
    assert zip_utils.zip_student_folders(out, max_workers=1) == {}


def test_progress_callback_zero_and_n_folders(tmp_path):
    out = str(tmp_path)
    calls = []
    res0 = zip_utils.zip_student_folders(
        out, progress_callback=lambda d, t, u, m: calls.append((d, t, u, m)))
    assert res0 == {}
    assert calls == [(0, 0, 'папок', 'Нет папок для архивации')]

    calls.clear()
    for name in ('a', 'b', 'c'):
        _student(out, name, [('f.jpg', b'x')])
    res = zip_utils.zip_student_folders(
        out, max_workers=2,
        progress_callback=lambda d, t, u, m: calls.append((d, t, u, m)))
    assert set(res) == {'a', 'b', 'c'}
    completed = [c[0] for c in calls]
    assert sorted(completed) == [1, 2, 3]
    assert all(c[1] == 3 for c in calls)


def test_max_workers_nonpositive(tmp_path):
    out = str(tmp_path)
    _student(out, 'a', [('f.jpg', b'x')])
    assert list(zip_utils.zip_student_folders(out, max_workers=0)) == ['a']
    assert list(zip_utils.zip_student_folders(out, max_workers=-3)) == ['a']


def test_nonexistent_output_folder(tmp_path):
    assert zip_utils.zip_student_folders(os.path.join(str(tmp_path), 'missing')) == {}