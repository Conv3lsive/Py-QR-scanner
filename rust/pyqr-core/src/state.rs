//! SQLite-состояние: паритет с processing_state.py (класс ProcessingState).
use pyo3::exceptions::{PyOSError, PyRuntimeError};
use pyo3::prelude::*;
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

/// Лимит переменных SQLite — как SQLITE_MAX_VARIABLES в processing_state.py.
const SQLITE_MAX_VARIABLES: usize = 900;

/// Схема БД — 1:1 с ProcessingState._init_db (та же таблица, колонки, индексы).
const SCHEMA: &str = "\
CREATE TABLE IF NOT EXISTS processed_files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_hash TEXT,
    qr_value TEXT,
    file_path TEXT,
    status TEXT,
    output_path TEXT,
    created_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_file_hash ON processed_files(file_hash);
CREATE INDEX IF NOT EXISTS idx_qr_value ON processed_files(qr_value);
CREATE INDEX IF NOT EXISTS idx_file_path ON processed_files(file_path);
";

/// Точная схема и семантика — processing_state.py (WAL, chunked get_existing_*,
/// get_existing_* возвращают только найденные).
#[pyclass]
struct ProcessingState {
    conn: Mutex<rusqlite::Connection>,
}

/// Ошибка rusqlite -> PyErr с понятным сообщением.
fn sqlite_err(e: rusqlite::Error) -> PyErr {
    PyRuntimeError::new_err(format!("Ошибка SQLite: {e}"))
}

/// Блокировка соединения; отравленный мьютекс -> PyErr, а не паника.
fn lock_conn(
    conn: &Mutex<rusqlite::Connection>,
) -> PyResult<std::sync::MutexGuard<'_, rusqlite::Connection>> {
    conn.lock()
        .map_err(|_| PyRuntimeError::new_err("Внутренняя ошибка: мьютекс соединения отравлен"))
}

/// Григорианская дата из числа дней начиная с 1970-01-01 (алгоритм H. Hinnant).
fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    (if month <= 2 { year + 1 } else { year }, month, day)
}

/// UTC-метка в формате datetime.now(timezone.utc).isoformat():
/// YYYY-MM-DDTHH:MM:SS[.ffffff]+00:00 (дробная часть только при ненулевых микросекундах).
fn utc_now_isoformat() -> String {
    let dur = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let secs = dur.as_secs() as i64;
    let micros = dur.subsec_micros();
    let days = secs.div_euclid(86_400);
    let secs_of_day = secs.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let hour = secs_of_day / 3600;
    let minute = (secs_of_day % 3600) / 60;
    let second = secs_of_day % 60;
    let base = format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}");
    if micros == 0 {
        format!("{base}+00:00")
    } else {
        format!("{base}.{micros:06}+00:00")
    }
}

#[pymethods]
impl ProcessingState {
    #[new]
    fn new(db_path: String) -> PyResult<Self> {
        // Родительский каталог создаётся (os.makedirs(..., exist_ok=True)),
        // но только если dirname непустой — иначе в Python бросило бы
        // FileNotFoundError из os.makedirs('').
        if let Some(parent) = std::path::Path::new(&db_path).parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    PyOSError::new_err(format!("Не удалось создать каталог {:?}: {e}", parent))
                })?;
            }
        }

        let conn = rusqlite::Connection::open(&db_path).map_err(sqlite_err)?;
        conn.execute_batch("PRAGMA journal_mode=WAL;")
            .map_err(sqlite_err)?;
        conn.execute_batch(SCHEMA).map_err(sqlite_err)?;

        Ok(ProcessingState {
            conn: Mutex::new(conn),
        })
    }

    fn has_file_hash(&self, file_hash: String) -> PyResult<bool> {
        // Паритет: file_hash in self.get_existing_file_hashes([file_hash])
        // (включая фильтрацию пустой строки внутри get_existing_file_hashes).
        Ok(!self.get_existing_file_hashes(vec![file_hash])?.is_empty())
    }

    fn has_qr_for_hash(&self, qr_value: String, file_hash: String) -> PyResult<bool> {
        // Паритет: (qr_value, file_hash) in self.get_existing_qr_hash_pairs([...]).
        Ok(!self
            .get_existing_qr_hash_pairs(vec![(qr_value, file_hash)])?
            .is_empty())
    }

    fn get_existing_file_hashes(&self, file_hashes: Vec<String>) -> PyResult<Vec<String>> {
        // Дедупликация с сохранением порядка + фильтр пустых:
        // dict.fromkeys(file_hashes) и if file_hash — как в Python.
        let mut unique: Vec<String> = Vec::new();
        for h in file_hashes {
            if h.is_empty() {
                continue;
            }
            if !unique.contains(&h) {
                unique.push(h);
            }
        }
        if unique.is_empty() {
            return Ok(Vec::new());
        }

        let conn = lock_conn(&self.conn)?;
        let mut existing: HashSet<String> = HashSet::new();
        for chunk in unique.chunks(SQLITE_MAX_VARIABLES) {
            let placeholders = vec!["?"; chunk.len()].join(", ");
            let sql = format!(
                "SELECT DISTINCT file_hash FROM processed_files WHERE file_hash IN ({placeholders})"
            );
            let params: Vec<&str> = chunk.iter().map(|s| s.as_str()).collect();
            let mut stmt = conn.prepare(&sql).map_err(sqlite_err)?;
            let rows = stmt
                .query_map(rusqlite::params_from_iter(params), |row| {
                    row.get::<_, String>(0)
                })
                .map_err(sqlite_err)?;
            for r in rows {
                let v = r.map_err(sqlite_err)?;
                if !v.is_empty() {
                    existing.insert(v);
                }
            }
        }
        // Python вернул бы set (порядок не определён); здесь детерминированный
        // порядок входного списка, состав элементов — точно как у set.
        Ok(unique.into_iter().filter(|h| existing.contains(h)).collect())
    }

    fn get_existing_qr_hash_pairs(
        &self,
        qr_hash_pairs: Vec<(String, String)>,
    ) -> PyResult<Vec<(String, String)>> {
        // Фильтр пустых без дедупликации: if qr_value and file_hash — как в Python.
        let normalized: Vec<(String, String)> = qr_hash_pairs
            .into_iter()
            .filter(|(q, h)| !q.is_empty() && !h.is_empty())
            .collect();
        if normalized.is_empty() {
            return Ok(Vec::new());
        }

        let max_pairs = (SQLITE_MAX_VARIABLES / 2).max(1);
        let conn = lock_conn(&self.conn)?;
        let mut existing: HashSet<(String, String)> = HashSet::new();
        for chunk in normalized.chunks(max_pairs) {
            let conditions = vec!["(qr_value = ? AND file_hash = ?)"; chunk.len()].join(" OR ");
            let sql = format!(
                "SELECT DISTINCT qr_value, file_hash FROM processed_files WHERE {conditions}"
            );
            let mut params: Vec<&str> = Vec::with_capacity(chunk.len() * 2);
            for (q, h) in chunk {
                params.push(q.as_str());
                params.push(h.as_str());
            }
            let mut stmt = conn.prepare(&sql).map_err(sqlite_err)?;
            let rows = stmt
                .query_map(rusqlite::params_from_iter(params), |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })
                .map_err(sqlite_err)?;
            for r in rows {
                let pair = r.map_err(sqlite_err)?;
                if !pair.0.is_empty() && !pair.1.is_empty() {
                    existing.insert(pair);
                }
            }
        }
        Ok(normalized
            .into_iter()
            .filter(|p| existing.contains(p))
            .collect())
    }

    #[pyo3(signature = (file_hash, qr_value, file_path, status, output_path=""))]
    fn add_record(
        &self,
        file_hash: String,
        qr_value: String,
        file_path: String,
        status: String,
        output_path: &str,
    ) -> PyResult<()> {
        self.add_records(vec![(file_hash, qr_value, file_path, status, output_path.to_string())])?;
        Ok(())
    }

    /// records: [(file_hash, qr_value, file_path, status, output_path), ...]
    fn add_records(
        &self,
        records: Vec<(String, String, String, String, String)>,
    ) -> PyResult<u64> {
        // prepared_records с меткой времени на каждую запись — как в Python
        // (datetime.now(timezone.utc).isoformat() вычисляется для каждого элемента).
        let mut prepared: Vec<(String, String, String, String, String, String)> = Vec::new();
        for (file_hash, qr_value, file_path, status, output_path) in records {
            let created_at = utc_now_isoformat();
            prepared.push((file_hash, qr_value, file_path, status, output_path, created_at));
        }
        if prepared.is_empty() {
            return Ok(0);
        }

        let mut conn = lock_conn(&self.conn)?;
        // Одна транзакция на весь executemany — как в Python (_connect: commit в конце).
        let tx = conn.transaction().map_err(sqlite_err)?;
        {
            let mut stmt = tx
                .prepare(
                    "INSERT INTO processed_files(file_hash, qr_value, file_path, status, \
                     output_path, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                )
                .map_err(sqlite_err)?;
            for (file_hash, qr_value, file_path, status, output_path, created_at) in &prepared {
                stmt.execute(rusqlite::params![
                    file_hash,
                    qr_value,
                    file_path,
                    status,
                    output_path,
                    created_at
                ])
                .map_err(sqlite_err)?;
            }
        }
        tx.commit().map_err(sqlite_err)?;
        Ok(prepared.len() as u64)
    }

    fn stats(&self) -> PyResult<HashMap<String, u64>> {
        let conn = lock_conn(&self.conn)?;
        let mut stmt = conn
            .prepare("SELECT status, COUNT(*) FROM processed_files GROUP BY status")
            .map_err(sqlite_err)?;
        let rows = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })
            .map_err(sqlite_err)?;
        let mut map = HashMap::new();
        for r in rows {
            let (status, count) = r.map_err(sqlite_err)?;
            map.insert(status, count as u64);
        }
        Ok(map)
    }
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<ProcessingState>()?;
    Ok(())
}