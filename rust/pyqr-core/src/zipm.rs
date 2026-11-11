//! Архивация: паритет с zip_utils.py (zip_student_folders).
use pyo3::prelude::*;

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
    let _ = (output_folder, max_workers, progress_callback);
    todo!("агент C: реализовать по zip_utils.zip_student_folders")
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(zip_student_folders, m)?)?;
    Ok(())
}
