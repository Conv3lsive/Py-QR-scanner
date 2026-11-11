//! SQLite-состояние: паритет с processing_state.py (класс ProcessingState).
use pyo3::prelude::*;
use std::collections::HashMap;
use std::sync::Mutex;

/// Точная схема и семантика — processing_state.py (WAL, chunked add_records,
/// get_existing_* возвращают только найденные).
#[pyclass]
struct ProcessingState {
    conn: Mutex<rusqlite::Connection>,
}

#[pymethods]
impl ProcessingState {
    #[new]
    fn new(db_path: String) -> PyResult<Self> {
        let _ = db_path;
        todo!("агент D: реализовать по processing_state.py")
    }

    fn has_file_hash(&self, file_hash: String) -> PyResult<bool> {
        let _ = file_hash;
        todo!()
    }

    fn has_qr_for_hash(&self, qr_value: String, file_hash: String) -> PyResult<bool> {
        let _ = (qr_value, file_hash);
        todo!()
    }

    fn get_existing_file_hashes(&self, file_hashes: Vec<String>) -> PyResult<Vec<String>> {
        let _ = file_hashes;
        todo!()
    }

    fn get_existing_qr_hash_pairs(
        &self,
        qr_hash_pairs: Vec<(String, String)>,
    ) -> PyResult<Vec<(String, String)>> {
        let _ = qr_hash_pairs;
        todo!()
    }

    #[pyo3(signature = (file_hash, qr_value, file_path, status, output_path=""))]
    fn add_record(
        &self,
        file_hash: String,
        qr_value: String,
        file_path: String,
        status: String,
        output_path: String,
    ) -> PyResult<()> {
        let _ = (file_hash, qr_value, file_path, status, output_path);
        todo!()
    }

    /// records: [(file_hash, qr_value, file_path, status, output_path), ...]
    fn add_records(&self, records: Vec<(String, String, String, String, String)>) -> PyResult<u64> {
        let _ = records;
        todo!()
    }

    fn stats(&self) -> PyResult<HashMap<String, u64>> {
        todo!()
    }
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<ProcessingState>()?;
    Ok(())
}
