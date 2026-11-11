//! Архивация: паритет с zip_utils.py (zip_student_folders).
use pyo3::exceptions::PyOSError;
use pyo3::prelude::*;
use rayon::prelude::*;
use std::cmp::max;
use std::fs::File;
use std::io::copy;
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use zip::write::SimpleFileOptions;
use zip::{CompressionMethod, ZipWriter};

const EXCLUDED_ARCHIVE_FOLDERS: &[&str] = &["unsorted", "noqrcode", "unfound", "state"];

fn mtime_of(path: &Path) -> Option<SystemTime> {
    std::fs::metadata(path).and_then(|m| m.modified()).ok()
}

/// Новейшая mtime среди каталога и всех файлов в нём (рекурсивно) — как
/// `_latest_source_mtime` из zip_utils.py. Каталоги-симлинки не обходятся
/// (как os.walk с followlinks=False).
fn latest_source_mtime(student_dir: &Path) -> Option<SystemTime> {
    let mut latest = mtime_of(student_dir);
    let mut stack: Vec<PathBuf> = vec![student_dir.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let is_dir = entry.file_type().map(|t| t.is_dir()).unwrap_or(false);
            if is_dir {
                stack.push(path);
            } else if let Some(m) = mtime_of(&path) {
                latest = match latest {
                    Some(l) if l >= m => Some(l),
                    _ => Some(m),
                };
            }
        }
    }
    latest
}

/// Рекурсивно добавляет файлы из `dir` в архив с путями относительно `base`.
fn zip_add_tree(
    zip: &mut ZipWriter<File>,
    options: SimpleFileOptions,
    base: &Path,
    dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let is_dir = entry.file_type().map(|t| t.is_dir()).unwrap_or(false);
        if is_dir {
            zip_add_tree(zip, options, base, &path)?;
        } else {
            let rel = path.strip_prefix(base)?;
            zip.start_file_from_path(rel, options)?;
            let mut src = File::open(&path)?;
            copy(&mut src, zip)?;
        }
    }
    Ok(())
}

/// Архивация одной папки студента — как `_zip_one_student`.
/// Возвращает (студент, создан_ли_архив_в_этот_раз).
fn zip_one_student(output_folder: &Path, student: &str) -> (String, bool) {
    let student_path = output_folder.join(student);
    let zip_path = output_folder.join(format!("{}.zip", student));

    let written = (|| -> Result<bool, Box<dyn std::error::Error>> {
        let latest = latest_source_mtime(&student_path);
        if zip_path.exists() {
            if let (Some(latest), Some(zip_mtime)) = (latest, mtime_of(&zip_path)) {
                if zip_mtime >= latest {
                    // Архив уже актуален — пропускаем, не создаём.
                    return Ok(false);
                }
            }
        }
        let file = File::create(&zip_path)?;
        let mut zip = ZipWriter::new(file);
        let options = SimpleFileOptions::default()
            .compression_method(CompressionMethod::Deflated)
            .compression_level(Some(6));
        zip_add_tree(&mut zip, options, &student_path, &student_path)?;
        zip.finish()?;
        Ok(true)
    })()
    .unwrap_or(false);

    (student.to_string(), written)
}

/// Zip каждой папки студента в output_folder → <студент>.zip (deflate),
/// с учётом свежести архива (см. _latest_source_mtime в zip_utils.py).
/// Параллельность rayon. Возвращает количество созданных архивов.
#[pyfunction]
#[pyo3(signature = (output_folder, max_workers=6, progress_callback=None))]
pub fn zip_student_folders(
    output_folder: String,
    max_workers: usize,
    progress_callback: Option<Bound<'_, PyAny>>,
) -> PyResult<u64> {
    let out_path = Path::new(&output_folder);
    if !out_path.is_dir() {
        return Ok(0);
    }

    let mut students: Vec<String> = Vec::new();
    for entry in std::fs::read_dir(out_path).map_err(|e| {
        PyOSError::new_err(format!(
            "Не удалось прочитать папку {}: {}",
            output_folder, e
        ))
    })? {
        let entry = entry.map_err(|e| {
            PyOSError::new_err(format!(
                "Ошибка при чтении содержимого папки {}: {}",
                output_folder, e
            ))
        })?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if EXCLUDED_ARCHIVE_FOLDERS.contains(&name.as_str()) {
            continue;
        }
        if entry.path().is_dir() {
            students.push(name);
        }
    }

    let total = students.len();

    if total == 0 {
        if let Some(cb) = &progress_callback {
            cb.call1((0u64, 0u64, "папок", "Нет папок для архивации"))?;
        }
        return Ok(0);
    }

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(max(1, max_workers))
        .build()
        .map_err(|e| {
            PyOSError::new_err(format!("Не удалось создать пул потоков: {}", e))
        })?;

    let results: Vec<(String, bool)> = match &progress_callback {
        Some(cb) => {
            let py = cb.py();
            py.allow_threads(|| {
                pool.install(|| {
                    students
                        .par_iter()
                        .map(|s| zip_one_student(out_path, s))
                        .collect()
                })
            })
        }
        None => pool.install(|| {
            students
                .par_iter()
                .map(|s| zip_one_student(out_path, s))
                .collect()
        }),
    };

    let mut created: u64 = 0;
    let mut completed: u64 = 0;
    let total = total as u64;
    for (_, written) in &results {
        if *written {
            created += 1;
        }
        completed += 1;
        if let Some(cb) = &progress_callback {
            cb.call1((completed, total, "папок", "Архивация папок"))?;
        }
    }

    Ok(created)
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(zip_student_folders, m)?)?;
    Ok(())
}