//! CSV-парсер: паритет с csv_utils.py (read_csv / read_csv_with_email).
//! Семантика: читать csv_utils.py — источник истины (BOM, авто-детект
//! разделителя, разрешения колонок, email-режим, накопление кодов).
use pyo3::prelude::*;
use std::collections::HashMap;

/// {студент: {колонка: [коды]}} — как csv_utils.read_csv.
#[pyfunction]
#[pyo3(signature = (path, code_name, student_fields, delimiter=None))]
pub fn read_csv(
    path: String,
    code_name: String,
    student_fields: Vec<String>,
    delimiter: Option<String>,
) -> PyResult<HashMap<String, HashMap<String, Vec<String>>>> {
    let _ = (path, code_name, student_fields, delimiter);
    todo!("агент A: реализовать по csv_utils.read_csv")
}

/// ({студент: {колонка: [коды]}}, {студент: email}) — как csv_utils.read_csv_with_email.
#[pyfunction]
#[pyo3(signature = (path, code_name, student_fields, email_field, delimiter=None))]
pub fn read_csv_with_email(
    path: String,
    code_name: String,
    student_fields: Vec<String>,
    email_field: String,
    delimiter: Option<String>,
) -> PyResult<(HashMap<String, HashMap<String, Vec<String>>>, HashMap<String, String>)> {
    let _ = (path, code_name, student_fields, email_field, delimiter);
    todo!("агент A: реализовать по csv_utils.read_csv_with_email")
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(read_csv, m)?)?;
    m.add_function(wrap_pyfunction!(read_csv_with_email, m)?)?;
    Ok(())
}
