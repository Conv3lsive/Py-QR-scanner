# pyqr_core — справка для модулей (обязательна к прочтению)

Крейт: PyO3-расширение `pyqr_core`, собирается как cdylib → `pyqr_core.so`.
Python-обёртки проекта дергают его с теми же сигнатурами, что и чистый Python
(см. соответствующие `.py`-файлы в корне репозитория — они источник семантики).

## Жёсткие правила
- Трогай ТОЛЬКО свой файл (`rust/pyqr-core/src/<модуль>.rs`). Cargo.toml, lib.rs,
  чужие .rs, *.py, tests/, benchmarks/ — нельзя.
- НЕ запускай `cargo` (любые команды), `pytest`, бенчмарки, `git`, `pip`:
  сборка и замеры — централизованно. Компиляцию проверяет координатор.
- Никаких `unwrap()`/`expect()` на путях внешних данных: только `PyResult` с
  понятным сообщением на русском.
- Python-словари возвращай как `HashMap<String, ...>` (PyO3 сам конвертирует),
- Прогресс-коллбек: `Option<Bound<'_, PyAny>>`, вызывается как
  `cb(done, total, "файлов", "сообщение")` — ровно как `_emit_progress` в
  `barcode_utils.py`. Из rayon-потоков — только внутри `Python::with_gil`.
  Параллельную работу оборачивай в `py.allow_threads(...)`, чтобы не держать GIL.

## PyO3 0.22 — рабочие шаблоны
```rust
use pyo3::prelude::*;

#[pyfunction]
#[pyo3(signature = (path, max_workers=6, progress_callback=None))]
pub fn find_barcodes_in_files(
    path: String,
    max_workers: usize,
    progress_callback: Option<Bound<'_, PyAny>>,
) -> PyResult<HashMap<String, Vec<String>>> { ... }

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(find_barcodes_in_files, m)?)?;
    Ok(())
}
```

## libzbar через dlopen (как это делает Python через ctypes)
Библиотека: `/opt/homebrew/lib/libzbar.dylib` (или env `ZBAR_LIB_PATH`/`LIBZBAR_PATH`).
Используй крейт `libloading`, грузи один раз (`OnceLock`/`std::sync::OnceLock`).

Нужные символы (все extern "C"):
```c
zbar_image_scanner_t* zbar_image_scanner_create(void);
int  zbar_image_scanner_set_config(zbar_image_scanner_t*, int symbology, int config, int value);
// symbology ZBAR_QRCODE = 64 (zbar.h 0.23; НЕ 0!); config ZBAR_CFG_ENABLE = 0; value 1
zbar_image_t* zbar_image_create(void);
void zbar_image_set_format(zbar_image_t*, unsigned long fmt);   // fmt = 'Y800' fourcc: 0x30303859 (НЕ 0x59383030!)
void zbar_image_set_size(zbar_image_t*, unsigned w, unsigned h);
void zbar_image_set_data(zbar_image_t*, const void *data, unsigned long byte_len, void *cleanup /*NULL*/);
int  zbar_scan_image(zbar_image_scanner_t*, zbar_image_t*);     // >0 = найдено символов, <0 = ошибка
const zbar_symbol_t* zbar_image_first_symbol(const zbar_image_t*);
const zbar_symbol_t* zbar_symbol_next(const zbar_symbol_t*);
const char* zbar_symbol_get_data(const zbar_symbol_t*);
unsigned zbar_symbol_get_data_length(const zbar_symbol_t*);
void zbar_image_ref(zbar_image_t*, int refs);                   // -1 = destroy
void zbar_image_scanner_destroy(zbar_image_scanner_t*);
```
Важно:
- данные — 8-битный grayscale (Y800), w*h байт: `image::DynamicImage::to_luma8()`.
- буфер должен жить до конца `zbar_scan_image` (cleanup=NULL: zbar не забирает владение).
- `zbar_symbol_get_data` НЕ null-terminated: длина через `get_data_length`;
  данные — UTF-8 (QR).
- один `scanner` на поток (создавай в каждом rayon-воркере или держи thread_local).

## Повороты (паритет с barcode_utils.rotate_image/find_best_rotation)
Поддерживаются только 90/180/270 — используй точные повороты
`image::imageops::rotate90/rotate180/rotate270` (без потерь, быстрее warpAffine).
Порядок попыток декода: 0° → 90° → 180° → 270° (как `find_best_rotation`).

## Совместимость с Python 3.8
Крейт собирается с `abi3-py38` — не используй фичи PyO3 новее 0.22 API
(`Bound` — можно, всё из 0.22 — можно).
