//! Файловые операции: паритет с file_utils.py
//! (get_all_files, move_unfound, move_clear, check_pairing).
use pyo3::exceptions::{PyFileNotFoundError, PyOSError};
use pyo3::prelude::*;
use std::collections::{HashMap, HashSet};
use std::path::Path;

/// Нижний регистр расширения без точки ("" если расширения нет) — как
/// `os.path.splitext(name)[1].lower()`.
fn extension_lower(filename: &str) -> String {
    Path::new(filename)
        .extension()
        .map(|e| e.to_string_lossy().to_lowercase())
        .unwrap_or_default()
}

fn is_supported_image(filename: &str) -> bool {
    matches!(extension_lower(filename).as_str(), "jpg" | "jpeg" | "png")
}

/// Путь без коллизий: если `directory/filename` занят — `name_N.ext`
/// (полный паритет с `_safe_destination` из file_utils.py).
fn safe_destination(directory: &Path, filename: &str) -> String {
    let dst = directory.join(filename);
    if !dst.exists() {
        return dst.to_string_lossy().to_string();
    }
    let p = Path::new(filename);
    let name = p
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();
    let ext = p
        .extension()
        .map(|e| format!(".{}", e.to_string_lossy()))
        .unwrap_or_default();
    let mut counter: u64 = 1;
    loop {
        let candidate = directory.join(format!("{}_{}{}", name, counter, ext));
        if !candidate.exists() {
            return candidate.to_string_lossy().to_string();
        }
        counter += 1;
    }
}

/// Все известные коды из data ({студент: {колонка: [коды]}}) — как
/// `_collect_known_codes` из file_utils.py.
fn collect_known_codes(data: &HashMap<String, HashMap<String, Vec<String>>>) -> HashSet<String> {
    let mut known = HashSet::new();
    for codes in data.values() {
        for code_list in codes.values() {
            for code in code_list {
                if !code.is_empty() {
                    known.insert(code.clone());
                }
            }
        }
    }
    known
}

/// Все .jpg/.jpeg/.png в папке (полные пути; регистр расширения не важен).
#[pyfunction]
pub fn get_all_files(folder: String) -> PyResult<Vec<String>> {
    let dir = Path::new(&folder);
    let entries = std::fs::read_dir(dir).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            PyFileNotFoundError::new_err(format!("Папка не найдена: {}", folder))
        } else {
            PyOSError::new_err(format!("Не удалось прочитать папку {}: {}", folder, e))
        }
    })?;

    let mut result = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|e| {
            PyOSError::new_err(format!(
                "Ошибка при чтении содержимого папки {}: {}",
                folder, e
            ))
        })?;
        let name = entry.file_name();
        if is_supported_image(&name.to_string_lossy()) {
            result.push(entry.path().to_string_lossy().to_string());
        }
    }
    Ok(result)
}

/// Неопознанные коды → output_folder/unsorted (copy2 или move).
/// Возвращает количество перемещённых файлов.
#[pyfunction]
#[pyo3(signature = (barcodes, data, output_folder, move_mode="copy"))]
pub fn move_unfound(
    barcodes: HashMap<String, Vec<String>>,
    data: HashMap<String, HashMap<String, Vec<String>>>,
    output_folder: String,
    move_mode: &str,
) -> PyResult<u64> {
    let known = collect_known_codes(&data);

    let unfound_dir = Path::new(&output_folder).join("unsorted");
    std::fs::create_dir_all(&unfound_dir).map_err(|e| {
        PyOSError::new_err(format!(
            "Не удалось создать папку {}: {}",
            unfound_dir.display(),
            e
        ))
    })?;

    let mut count: u64 = 0;
    for (code, paths) in &barcodes {
        if known.contains(code) {
            continue;
        }
        for src in paths {
            let filename = Path::new(src)
                .file_name()
                .map(|s| s.to_string_lossy().into_owned())
                .unwrap_or_else(|| "unnamed".to_string());
            let dst = safe_destination(&unfound_dir, &filename);
            if move_mode == "move" {
                std::fs::rename(src, &dst).map_err(|e| {
                    PyOSError::new_err(format!(
                        "Не удалось переместить {} в {}: {}",
                        src, dst, e
                    ))
                })?;
            } else {
                std::fs::copy(src, &dst).map_err(|e| {
                    PyOSError::new_err(format!(
                        "Не удалось скопировать {} в {}: {}",
                        src, dst, e
                    ))
                })?;
            }
            count += 1;
        }
    }
    Ok(count)
}

/// Файлы без QR → output_folder/noqrcode. Возвращает количество.
#[pyfunction]
#[pyo3(signature = (output_folder, image_folder, found_files, move_mode="copy"))]
pub fn move_clear(
    output_folder: String,
    image_folder: String,
    found_files: Vec<String>,
    move_mode: &str,
) -> PyResult<u64> {
    let all_files = get_all_files(image_folder)?;
    let found: HashSet<&str> = found_files.iter().map(|s| s.as_str()).collect();

    let clear_dir = Path::new(&output_folder).join("noqrcode");
    std::fs::create_dir_all(&clear_dir).map_err(|e| {
        PyOSError::new_err(format!(
            "Не удалось создать папку {}: {}",
            clear_dir.display(),
            e
        ))
    })?;

    let mut count: u64 = 0;
    for src in &all_files {
        if found.contains(src.as_str()) {
            continue;
        }
        let filename = Path::new(src)
            .file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| "unnamed".to_string());
        let dst = safe_destination(&clear_dir, &filename);
        if move_mode == "move" {
            std::fs::rename(src, &dst).map_err(|e| {
                PyOSError::new_err(format!(
                    "Не удалось переместить {} в {}: {}",
                    src, dst, e
                ))
            })?;
        } else {
            std::fs::copy(src, &dst).map_err(|e| {
                PyOSError::new_err(format!(
                    "Не удалось скопировать {} в {}: {}",
                    src, dst, e
                ))
            })?;
        }
        count += 1;
    }
    Ok(count)
}

/// Список предупреждений о неполных парах (паритет с check_pairing:
/// regex r"(\d+)-([1-4])(?:\(\d+\))?\.jpg", пары 1-2 и 3-4).
#[pyfunction]
pub fn check_pairing(folder: String) -> PyResult<Vec<String>> {
    let dir = Path::new(&folder);
    let entries = std::fs::read_dir(dir).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            PyFileNotFoundError::new_err(format!("Папка не найдена: {}", folder))
        } else {
            PyOSError::new_err(format!("Не удалось прочитать папку {}: {}", folder, e))
        }
    })?;

    // `re.match` якорится на начало строки — добавляем `^`.
    let pattern = regex::Regex::new(r"^(\d+)-([1-4])(?:\(\d+\))?\.jpg")
        .map_err(|e| PyOSError::new_err(format!("Ошибка регулярного выражения: {}", e)))?;

    let mut base_order: Vec<String> = Vec::new();
    let mut base_index: HashMap<String, usize> = HashMap::new();
    let mut suffixes: Vec<HashSet<String>> = Vec::new();

    for entry in entries {
        let entry = entry.map_err(|e| {
            PyOSError::new_err(format!(
                "Ошибка при чтении содержимого папки {}: {}",
                folder, e
            ))
        })?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if !is_supported_image(&name) {
            continue;
        }
        let Some(caps) = pattern.captures(&name) else {
            continue;
        };
        let (Some(base_m), Some(suffix_m)) = (caps.get(1), caps.get(2)) else {
            continue;
        };
        let base = base_m.as_str().to_string();
        let suffix = suffix_m.as_str().to_string();

        let idx = match base_index.get(&base) {
            Some(&i) => i,
            None => {
                let i = base_order.len();
                base_order.push(base.clone());
                suffixes.push(HashSet::new());
                base_index.insert(base, i);
                i
            }
        };
        suffixes[idx].insert(suffix);
    }

    let mut warnings = Vec::new();
    for (base, sufs) in base_order.iter().zip(suffixes.iter()) {
        if sufs.contains("1") != sufs.contains("2") {
            warnings.push(format!("Неполная пара 1-2 для: {}", base));
        }
        if sufs.contains("3") != sufs.contains("4") {
            warnings.push(format!("Неполная пара 3-4 для: {}", base));
        }
    }
    Ok(warnings)
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(get_all_files, m)?)?;
    m.add_function(wrap_pyfunction!(move_unfound, m)?)?;
    m.add_function(wrap_pyfunction!(move_clear, m)?)?;
    m.add_function(wrap_pyfunction!(check_pairing, m)?)?;
    Ok(())
}