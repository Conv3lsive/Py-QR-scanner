//! Файловые операции: паритет с file_utils.py
//! (get_all_files, move_unfound, move_clear, check_pairing).
use pyo3::prelude::*;
use std::collections::HashMap;

/// Все .jpg/.jpeg/.png в папке (пути как Vec; порядок не важен).
#[pyfunction]
pub fn get_all_files(folder: String) -> PyResult<Vec<String>> {
    let _ = folder;
    todo!("агент C: реализовать по file_utils.get_all_files")
}

/// Неопознанные коды → output_folder/unsorted (copy2 или move).
/// Возвращает количество перемещённых файлов.
#[pyfunction]
#[pyo3(signature = (barcodes, data, output_folder, move_mode="copy"))]
pub fn move_unfound(
    barcodes: HashMap<String, Vec<String>>,
    data: HashMap<String, HashMap<String, Vec<String>>>,
    output_folder: String,
    move_mode: String,
) -> PyResult<u64> {
    let _ = (barcodes, data, output_folder, move_mode);
    todo!("агент C: реализовать по file_utils.move_unfound")
}

/// Файлы без QR → output_folder/noqrcode. Возвращает количество.
#[pyfunction]
#[pyo3(signature = (output_folder, image_folder, found_files, move_mode="copy"))]
pub fn move_clear(
    output_folder: String,
    image_folder: String,
    found_files: Vec<String>,
    move_mode: String,
) -> PyResult<u64> {
    let _ = (output_folder, image_folder, found_files, move_mode);
    todo!("агент C: реализовать по file_utils.move_clear")
}

/// Список предупреждений о неполных парах (паритет с check_pairing:
/// regex r"(\d+)-([1-4])(?:\(\d+\))?\.jpg", пары 1-2 и 3-4).
#[pyfunction]
pub fn check_pairing(folder: String) -> PyResult<Vec<String>> {
    let _ = folder;
    todo!("агент C: реализовать по file_utils.check_pairing")
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(get_all_files, m)?)?;
    m.add_function(wrap_pyfunction!(move_unfound, m)?)?;
    m.add_function(wrap_pyfunction!(move_clear, m)?)?;
    m.add_function(wrap_pyfunction!(check_pairing, m)?)?;
    Ok(())
}
