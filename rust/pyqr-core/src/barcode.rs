//! QR/штрих-коды и файловые операции над ними: паритет с barcode_utils.py
//! (decode_image_cv, find_barcodes_in_files, file_renamer, split_by_student_folders).
//! zbar — через dlopen (см. ../NOTES.md).
use pyo3::exceptions::PyOSError;
use pyo3::prelude::*;
use rayon::prelude::*;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::ffi::{c_char, c_int, c_uint, c_ulong, c_void};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// libzbar через libloading/dlopen: грузим один раз на процесс (OnceLock).
// ---------------------------------------------------------------------------

// Значения ровно из установленного zbar 0.23 (/opt/homebrew/include/zbar.h):
// ZBAR_QRCODE = 64, ZBAR_CFG_ENABLE = 0, fourcc 'Y800' = 0x30303859.
const ZBAR_QRCODE: c_int = 64;
const ZBAR_CFG_ENABLE: c_int = 0;
const FOURCC_Y800: c_ulong = 0x30303859;

#[repr(C)]
struct zbar_image_scanner_t {
    _opaque: [u8; 0],
}
#[repr(C)]
struct zbar_image_t {
    _opaque: [u8; 0],
}
#[repr(C)]
struct zbar_symbol_t {
    _opaque: [u8; 0],
}

type ZbarScannerCreate = unsafe extern "C" fn() -> *mut zbar_image_scanner_t;
type ZbarScannerDestroy = unsafe extern "C" fn(*mut zbar_image_scanner_t);
type ZbarScannerSetConfig =
    unsafe extern "C" fn(*mut zbar_image_scanner_t, c_int, c_int, c_int) -> c_int;
type ZbarImageCreate = unsafe extern "C" fn() -> *mut zbar_image_t;
type ZbarImageRef = unsafe extern "C" fn(*mut zbar_image_t, c_int);
type ZbarImageSetFormat = unsafe extern "C" fn(*mut zbar_image_t, c_ulong);
type ZbarImageSetSize = unsafe extern "C" fn(*mut zbar_image_t, c_uint, c_uint);
type ZbarImageSetData = unsafe extern "C" fn(*mut zbar_image_t, *const c_void, c_ulong, *mut c_void);
type ZbarScanImage = unsafe extern "C" fn(*mut zbar_image_scanner_t, *mut zbar_image_t) -> c_int;
type ZbarFirstSymbol = unsafe extern "C" fn(*const zbar_image_t) -> *const zbar_symbol_t;
type ZbarSymbolNext = unsafe extern "C" fn(*const zbar_symbol_t) -> *const zbar_symbol_t;
type ZbarSymbolGetData = unsafe extern "C" fn(*const zbar_symbol_t) -> *const c_char;
type ZbarSymbolGetDataLength = unsafe extern "C" fn(*const zbar_symbol_t) -> c_uint;

struct Zbar {
    _lib: libloading::Library,
    scanner_create: ZbarScannerCreate,
    scanner_destroy: ZbarScannerDestroy,
    scanner_set_config: ZbarScannerSetConfig,
    image_create: ZbarImageCreate,
    image_ref: ZbarImageRef,
    image_set_format: ZbarImageSetFormat,
    image_set_size: ZbarImageSetSize,
    image_set_data: ZbarImageSetData,
    scan_image: ZbarScanImage,
    first_symbol: ZbarFirstSymbol,
    symbol_next: ZbarSymbolNext,
    symbol_get_data: ZbarSymbolGetData,
    symbol_get_data_length: ZbarSymbolGetDataLength,
}

static ZBAR: OnceLock<Option<Zbar>> = OnceLock::new();

fn zbar() -> Option<&'static Zbar> {
    ZBAR.get_or_init(load_zbar).as_ref()
}

/// Кандидаты библиотеки: сначала env (ZBAR_LIB_PATH, LIBZBAR_PATH), затем
/// стандартные системные пути по платформам (зеркало SYSTEM_ZBAR_LIBRARY_PATHS
/// из barcode_utils.py + имена Windows для поиска в PATH/возле exe).
fn find_zbar_library() -> Option<PathBuf> {
    for var in ["ZBAR_LIB_PATH", "LIBZBAR_PATH"] {
        if let Ok(raw) = std::env::var(var) {
            let raw = raw.trim();
            if !raw.is_empty() {
                let path = PathBuf::from(raw);
                if path.is_file() {
                    return Some(path);
                }
            }
        }
    }
    // macOS (Homebrew, Intel и Apple Silicon)
    for candidate in [
        "/opt/homebrew/lib/libzbar.dylib",
        "/opt/homebrew/opt/zbar/lib/libzbar.dylib",
        "/usr/local/lib/libzbar.dylib",
        "/usr/local/opt/zbar/lib/libzbar.dylib",
    ] {
        let path = Path::new(candidate);
        if path.is_file() {
            return Some(path.to_path_buf());
        }
    }
    // Linux (apt/dnf ставят libzbar.so.0)
    for candidate in [
        "/usr/lib/x86_64-linux-gnu/libzbar.so.0",
        "/usr/lib/aarch64-linux-gnu/libzbar.so.0",
        "/lib/x86_64-linux-gnu/libzbar.so.0",
        "/lib/aarch64-linux-gnu/libzbar.so.0",
        "/usr/lib64/libzbar.so.0",
        "/usr/lib64/libzbar.so",
        "/usr/lib/libzbar.so.0",
        "/usr/lib/libzbar.so",
        "/usr/local/lib/libzbar.so.0",
        "/usr/local/lib/libzbar.so",
    ] {
        let path = Path::new(candidate);
        if path.is_file() {
            return Some(path.to_path_buf());
        }
    }
    None
}

fn load_zbar() -> Option<Zbar> {
    // macOS/Linux — по конкретному пути; Windows — по имени (PATH, каталог
    // exe, системные каталоги: libzbar-64.dll из wheel pyzbar или своя установка).
    let lib = unsafe {
        match find_zbar_library() {
            Some(path) => libloading::Library::new(&path).ok()?,
            None => {
                if !cfg!(target_os = "windows") {
                    return None;
                }
                libloading::Library::new("libzbar-64.dll")
                    .or_else(|_| unsafe { libloading::Library::new("libzbar.dll") })
                    .ok()?
            }
        }
    };
    unsafe {
        let scanner_create = *lib
            .get::<ZbarScannerCreate>(b"zbar_image_scanner_create")
            .ok()?;
        let scanner_destroy = *lib
            .get::<ZbarScannerDestroy>(b"zbar_image_scanner_destroy")
            .ok()?;
        let scanner_set_config = *lib
            .get::<ZbarScannerSetConfig>(b"zbar_image_scanner_set_config")
            .ok()?;
        let image_create = *lib.get::<ZbarImageCreate>(b"zbar_image_create").ok()?;
        let image_ref = *lib.get::<ZbarImageRef>(b"zbar_image_ref").ok()?;
        let image_set_format = *lib
            .get::<ZbarImageSetFormat>(b"zbar_image_set_format")
            .ok()?;
        let image_set_size = *lib
            .get::<ZbarImageSetSize>(b"zbar_image_set_size")
            .ok()?;
        let image_set_data = *lib
            .get::<ZbarImageSetData>(b"zbar_image_set_data")
            .ok()?;
        let scan_image = *lib.get::<ZbarScanImage>(b"zbar_scan_image").ok()?;
        let first_symbol = *lib
            .get::<ZbarFirstSymbol>(b"zbar_image_first_symbol")
            .ok()?;
        let symbol_next = *lib.get::<ZbarSymbolNext>(b"zbar_symbol_next").ok()?;
        let symbol_get_data = *lib
            .get::<ZbarSymbolGetData>(b"zbar_symbol_get_data")
            .ok()?;
        let symbol_get_data_length = *lib
            .get::<ZbarSymbolGetDataLength>(b"zbar_symbol_get_data_length")
            .ok()?;
        Some(Zbar {
            _lib: lib,
            scanner_create,
            scanner_destroy,
            scanner_set_config,
            image_create,
            image_ref,
            image_set_format,
            image_set_size,
            image_set_data,
            scan_image,
            first_symbol,
            symbol_next,
            symbol_get_data,
            symbol_get_data_length,
        })
    }
}

/// Сканер создаётся один раз на поток и уничтожается при завершении потока.
struct ThreadScanner(*mut zbar_image_scanner_t);

impl Drop for ThreadScanner {
    fn drop(&mut self) {
        if let Some(z) = zbar() {
            unsafe { (z.scanner_destroy)(self.0) };
        }
    }
}

thread_local! {
    static SCANNER: RefCell<Option<ThreadScanner>> = const { RefCell::new(None) };
}

fn thread_scanner(z: &Zbar) -> Option<*mut zbar_image_scanner_t> {
    SCANNER.with(|cell| {
        let mut slot = cell.borrow_mut();
        if slot.is_none() {
            let scanner = unsafe { (z.scanner_create)() };
            if scanner.is_null() {
                return None;
            }
            unsafe {
                (z.scanner_set_config)(scanner, ZBAR_QRCODE, ZBAR_CFG_ENABLE, 1);
            }
            *slot = Some(ThreadScanner(scanner));
        }
        slot.as_ref().map(|sc| sc.0)
    })
}

/// Сканирует один grayscale-кадр (Y800) и возвращает уникальные UTF-8 коды
/// в порядке первого встречания (`.strip()` + пропуск пустых — как
/// `decode_image_cv`).
fn scan_gray(z: &Zbar, gray: &image::GrayImage) -> Vec<String> {
    let scanner = match thread_scanner(z) {
        Some(s) => s,
        None => return Vec::new(),
    };
    let (w, h) = gray.dimensions();
    let data = gray.as_raw();
    unsafe {
        let img = (z.image_create)();
        if img.is_null() {
            return Vec::new();
        }
        (z.image_set_format)(img, FOURCC_Y800);
        (z.image_set_size)(img, w, h);
        (z.image_set_data)(
            img,
            data.as_ptr() as *const c_void,
            data.len() as c_ulong,
            std::ptr::null_mut(),
        );
        let found = (z.scan_image)(scanner, img);
        let mut seen: HashSet<String> = HashSet::new();
        let mut out: Vec<String> = Vec::new();
        if found > 0 {
            let mut symbol = (z.first_symbol)(img);
            while !symbol.is_null() {
                let len = (z.symbol_get_data_length)(symbol) as usize;
                let ptr = (z.symbol_get_data)(symbol);
                if !ptr.is_null() {
                    let bytes = std::slice::from_raw_parts(ptr as *const u8, len);
                    if let Ok(text) = std::str::from_utf8(bytes) {
                        let text = text.trim();
                        if !text.is_empty() {
                            let owned = text.to_string();
                            if seen.insert(owned.clone()) {
                                out.push(owned);
                            }
                        }
                    }
                }
                symbol = (z.symbol_next)(symbol);
            }
        }
        (z.image_ref)(img, -1);
        out
    }
}

/// Уникальные коды одного изображения: 0° → 90° → 180° → 270°.
#[pyfunction]
pub fn decode_image(path: String) -> Vec<String> {
    let z = match zbar() {
        Some(z) => z,
        None => return Vec::new(),
    };
    let img = match image::open(&path) {
        Ok(img) => img,
        Err(_) => return Vec::new(),
    };
    let gray = img.to_luma8();

    let mut codes = scan_gray(z, &gray);
    if codes.is_empty() {
        codes = scan_gray(z, &image::imageops::rotate90(&gray));
    }
    if codes.is_empty() {
        codes = scan_gray(z, &image::imageops::rotate180(&gray));
    }
    if codes.is_empty() {
        codes = scan_gray(z, &image::imageops::rotate270(&gray));
    }
    codes
}

/// Вызывает progress_callback(done, total, unit, message) как `_emit_progress`.
fn emit_progress(
    progress_callback: &Option<Py<PyAny>>,
    done: usize,
    total: usize,
    unit: &str,
    message: &str,
) {
    if let Some(cb) = progress_callback {
        let _ = Python::with_gil(|py| cb.call1(py, (done, total, unit, message)));
    }
}

fn is_supported_image(path: &str) -> bool {
    let p = Path::new(path);
    if !p.is_file() {
        return false;
    }
    match p.extension().and_then(|e| e.to_str()) {
        Some(ext) => matches!(ext.to_ascii_lowercase().as_str(), "jpg" | "jpeg" | "png"),
        None => false,
    }
}

/// {код: [пути]} — только существующие .jpg/.jpeg/.png, дедупликация путей,
/// параллельность rayon (max(1, min(max_workers, total))), прогресс как в Python.
#[pyfunction]
#[pyo3(signature = (paths, max_workers=6, progress_callback=None))]
pub fn find_barcodes_in_files(
    paths: Vec<String>,
    max_workers: usize,
    progress_callback: Option<Bound<'_, PyAny>>,
) -> PyResult<HashMap<String, Vec<String>>> {
    // Переводим в owned-форму (Send+Sync), чтобы коллбек можно было дергать
    // из rayon-потоков внутри Python::with_gil.
    let progress_callback: Option<Py<PyAny>> = progress_callback.map(|cb| cb.unbind());

    // Фильтр «файл существует + разрешённое расширение» + дедупликация путей
    // с сохранением порядка (аналог `dict.fromkeys`).
    let mut seen_paths: HashSet<String> = HashSet::new();
    let mut image_files: Vec<String> = Vec::with_capacity(paths.len());
    for path in paths {
        if !seen_paths.insert(path.clone()) {
            continue;
        }
        if is_supported_image(&path) {
            image_files.push(path);
        }
    }

    if image_files.is_empty() {
        emit_progress(
            &progress_callback,
            0,
            0,
            "файлов",
            "Нет изображений для распознавания",
        );
        return Ok(HashMap::new());
    }

    let total = image_files.len();
    let worker_count = max_workers.max(1).min(total);

    let results: Mutex<HashMap<String, Vec<String>>> = Mutex::new(HashMap::new());
    let done = AtomicUsize::new(0);

    let process = |path: &String| {
        let codes = decode_image(path.clone());
        {
            let mut map = results.lock().unwrap_or_else(|e| e.into_inner());
            for code in codes {
                map.entry(code).or_default().push(path.clone());
            }
        }
        let completed = done.fetch_add(1, Ordering::SeqCst) + 1;
        emit_progress(
            &progress_callback,
            completed,
            total,
            "файлов",
            "Распознавание QR/штрихкодов",
        );
    };

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(worker_count)
        .build();
    Python::with_gil(|py| {
        py.allow_threads(|| {
            match &pool {
                Ok(pool) => pool.install(|| image_files.par_iter().for_each(&process)),
                Err(_) => image_files.iter().for_each(&process),
            }
        });
    });

    Ok(results.into_inner().unwrap_or_default())
}

/// Переименовать {код: [пути]} в image_folder: f"{safe_code}_{idx}{ext|.jpg}",
/// safe_code — нелегальные для имён символы ('/' '\' ':' и пр.) → '_' (как в barcode_utils.py).
/// Возвращает количество переименованных файлов.
#[pyfunction]
#[pyo3(signature = (image_folder, barcodes, progress_callback=None))]
pub fn file_renamer(
    image_folder: String,
    barcodes: HashMap<String, Vec<String>>,
    progress_callback: Option<Bound<'_, PyAny>>,
) -> PyResult<u64> {
    let progress_callback: Option<Py<PyAny>> = progress_callback.map(|cb| cb.unbind());

    let total: usize = barcodes.values().map(|paths| paths.len()).sum();

    if total == 0 {
        emit_progress(
            &progress_callback,
            0,
            0,
            "файлов",
            "Нет файлов для переименования",
        );
        return Ok(0);
    }

    let mut processed = 0usize;
    let mut renamed: u64 = 0;

    for (code, paths) in &barcodes {
        for (idx0, path) in paths.iter().enumerate() {
            let idx = idx0 + 1;
            let ext = Path::new(path)
                .extension()
                .and_then(|e| e.to_str())
                .map(|e| format!(".{}", e))
                .unwrap_or_else(|| ".jpg".to_string());
            // Нелегальные в именах файлов символы (':' на Windows даёт
            // WinError 123 и пр.) — единый набор с barcode_utils.py.
            let safe_code = code.replace(
                |ch: char| matches!(ch, '/' | '\\' | ':' | '\0' | '<' | '>' | '|' | '?' | '*'),
                "_",
            );
            let new_name = format!("{}_{}{}", safe_code, idx, ext);
            let new_path = Path::new(&image_folder).join(new_name);

            if std::fs::rename(path, &new_path).is_ok() {
                renamed += 1;
            }

            processed += 1;
            emit_progress(
                &progress_callback,
                processed,
                total,
                "файлов",
                "Переименование файлов",
            );
        }
    }

    Ok(renamed)
}

fn build_code_to_students(
    student_data: &HashMap<String, HashMap<String, Vec<String>>>,
) -> HashMap<String, HashSet<String>> {
    let mut code_to_students: HashMap<String, HashSet<String>> = HashMap::new();
    for (student, codes) in student_data {
        for code_list in codes.values() {
            for code in code_list {
                if code.is_empty() {
                    continue;
                }
                code_to_students
                    .entry(code.clone())
                    .or_default()
                    .insert(student.clone());
            }
        }
    }
    code_to_students
}

/// Копирует файл в папку студента: копия во временное имя, затем под глобальным
/// локом anti-collision (_N) и атомарный rename. Возвращает true при успехе.
fn copy_to_student_folder(src: &str, student_folder: &Path, rename_lock: &Mutex<()>) -> bool {
    let filename = match Path::new(src).file_name().and_then(|n| n.to_str()) {
        Some(name) if !name.is_empty() => name.to_string(),
        _ => return false,
    };

    let tmp_name = format!(".{}.tmp-{}", filename, Uuid::new_v4().simple());
    let tmp_path = student_folder.join(&tmp_name);

    if std::fs::copy(src, &tmp_path).is_err() {
        return false;
    }

    // exists-check + rename — сериализуем, чтобы одинаковые имена не
    // перезаписывали друг друга (как `_copy_rename_lock` в Python).
    let _guard = rename_lock.lock().unwrap_or_else(|e| e.into_inner());

    let stem = Path::new(&filename)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("");
    let ext = Path::new(&filename)
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| format!(".{}", e))
        .unwrap_or_default();

    let mut dst = student_folder.join(&filename);
    if dst.exists() {
        let mut counter = 1usize;
        loop {
            let candidate = student_folder.join(format!("{}_{}{}", stem, counter, ext));
            if !candidate.exists() {
                dst = candidate;
                break;
            }
            counter += 1;
        }
    }

    match std::fs::rename(&tmp_path, &dst) {
        Ok(_) => true,
        Err(_) => {
            let _ = std::fs::remove_file(&tmp_path);
            false
        }
    }
}

/// Раскидать файлы по папкам студентов (копия во временное имя + атомарный
/// rename, анти-коллизия _N, глобальный лок на exists-check+rename).
/// Возвращает количество скопированных файлов.
#[pyfunction]
#[pyo3(signature = (barcodes, student_data, output_folder, max_workers=6, progress_callback=None))]
pub fn split_by_student_folders(
    barcodes: HashMap<String, Vec<String>>,
    student_data: HashMap<String, HashMap<String, Vec<String>>>,
    output_folder: String,
    max_workers: usize,
    progress_callback: Option<Bound<'_, PyAny>>,
) -> PyResult<u64> {
    let progress_callback: Option<Py<PyAny>> = progress_callback.map(|cb| cb.unbind());

    if let Err(e) = std::fs::create_dir_all(&output_folder) {
        return Err(PyOSError::new_err(format!(
            "Не удалось создать папку {}: {}",
            output_folder, e
        )));
    }

    let code_to_students = build_code_to_students(&student_data);

    // Расписание заданий: для каждого кода → каждый студент → каждый путь,
    // (src_path, student_folder) дедуплицируется.
    let mut student_folders: HashMap<String, PathBuf> = HashMap::new();
    let mut scheduled: HashSet<(String, PathBuf)> = HashSet::new();
    let mut jobs: Vec<(String, PathBuf)> = Vec::new();

    for (code, paths) in &barcodes {
        let students = match code_to_students.get(code) {
            Some(students) => students,
            None => continue,
        };
        for student in students {
            let student_folder = student_folders
                .entry(student.clone())
                .or_insert_with(|| {
                    let dir = Path::new(&output_folder).join(student);
                    let _ = std::fs::create_dir_all(&dir);
                    dir
                })
                .clone();
            for src_path in paths {
                let job = (src_path.clone(), student_folder.clone());
                if scheduled.insert(job.clone()) {
                    jobs.push(job);
                }
            }
        }
    }

    let total = jobs.len();
    if total == 0 {
        emit_progress(
            &progress_callback,
            0,
            0,
            "файлов",
            "Нет файлов для распределения",
        );
        return Ok(0);
    }

    let worker_count = max_workers.max(1).min(total);
    let rename_lock = Mutex::new(());
    let copied = AtomicUsize::new(0);
    let done = AtomicUsize::new(0);

    let process = |job: &(String, PathBuf)| {
        if copy_to_student_folder(job.0.as_str(), &job.1, &rename_lock) {
            copied.fetch_add(1, Ordering::SeqCst);
        }
        let completed = done.fetch_add(1, Ordering::SeqCst) + 1;
        emit_progress(
            &progress_callback,
            completed,
            total,
            "файлов",
            "Распределение файлов по папкам",
        );
    };

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(worker_count)
        .build();
    Python::with_gil(|py| {
        py.allow_threads(|| {
            match &pool {
                Ok(pool) => pool.install(|| jobs.par_iter().for_each(&process)),
                Err(_) => jobs.iter().for_each(&process),
            }
        });
    });

    Ok(copied.load(Ordering::SeqCst) as u64)
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(decode_image, m)?)?;
    m.add_function(wrap_pyfunction!(find_barcodes_in_files, m)?)?;
    m.add_function(wrap_pyfunction!(file_renamer, m)?)?;
    m.add_function(wrap_pyfunction!(split_by_student_folders, m)?)?;
    m.add_function(wrap_pyfunction!(zbar_available, m)?)?;
    Ok(())
}

/// Загружен ли системный libzbar (для Python-обвязки: авто-бэкенд решает,
/// можно ли отдавать декодирование Rust-пути, или падать на pyzbar/OpenCV).
#[pyfunction]
pub fn zbar_available() -> bool {
    zbar().is_some()
}