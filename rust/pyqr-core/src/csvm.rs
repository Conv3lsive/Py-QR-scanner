//! CSV-парсер: паритет с csv_utils.py (read_csv / read_csv_with_email).
//! Семантика: читать csv_utils.py — источник истины (BOM, авто-детект
//! разделителя, разрешения колонок, email-режим, накопление кодов).
use pyo3::exceptions::{PyFileNotFoundError, PyOSError, PyValueError};
use pyo3::prelude::*;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fs;

type StudentData = HashMap<String, HashMap<String, Vec<String>>>;
type Emails = HashMap<String, String>;

/// {студент: {колонка: [коды]}} — как csv_utils.read_csv.
#[pyfunction]
#[pyo3(signature = (path, code_name, student_fields, delimiter=None))]
pub fn read_csv(
    path: String,
    code_name: String,
    student_fields: Vec<String>,
    delimiter: Option<String>,
) -> PyResult<StudentData> {
    let (data, _) = parse_csv(&path, &code_name, &student_fields, None, delimiter.as_deref())?;
    Ok(data)
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
) -> PyResult<(StudentData, Emails)> {
    let (data, emails) = parse_csv(
        &path,
        &code_name,
        &student_fields,
        Some(&email_field),
        delimiter.as_deref(),
    )?;
    // email_field всегда Some в этом пути, поэтому emails гарантированно Some.
    Ok((data, emails.unwrap_or_default()))
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(read_csv, m)?)?;
    m.add_function(wrap_pyfunction!(read_csv_with_email, m)?)?;
    Ok(())
}

/// Аналог csv_utils._normalize_delimiter. Возвращает None для авто-детекта.
fn normalize_delimiter(delimiter: Option<&str>) -> PyResult<Option<u8>> {
    let raw = match delimiter {
        None => return Ok(None),
        Some(s) => s,
    };
    // Реальный символ табуляции: trim() ниже съел бы его целиком.
    if raw == "\t" {
        return Ok(Some(b'\t'));
    }
    let value = raw.trim().to_lowercase();
    if value.is_empty() || value == "auto" {
        return Ok(None);
    }
    if value == "tab" || value == "\\t" {
        return Ok(Some(b'\t'));
    }
    if value == "semicolon" || value == ";" {
        return Ok(Some(b';'));
    }
    if value == "comma" || value == "," {
        return Ok(Some(b','));
    }
    // Иначе — строка как есть (одинарный символ-разделитель).
    let bytes = raw.as_bytes();
    if bytes.len() != 1 {
        return Err(PyValueError::new_err(format!(
            "разделитель должен быть одним символом, получено «{raw}»"
        )));
    }
    Ok(Some(bytes[0]))
}

/// Аналог csv_utils._create_reader (заголовки: trim + срез BOM).
/// `name or ''` → пустая строка остаётся пустой.
fn normalize_header(name: &str) -> String {
    name.trim().trim_start_matches('\u{feff}').to_string()
}

/// Аналог csv_utils._resolve_column_name: точное совпадение, потом подстрока.
fn resolve_column_name(headers: &[String], target: &str) -> Option<String> {
    if target.is_empty() {
        return None;
    }
    let normalized_target = target.trim().to_lowercase();
    if normalized_target.is_empty() {
        return None;
    }
    for header in headers {
        if header.trim().to_lowercase() == normalized_target {
            return Some(header.clone());
        }
    }
    for header in headers {
        if header.trim().to_lowercase().contains(&normalized_target) {
            return Some(header.clone());
        }
    }
    None
}

/// Аналог csv_utils._find_code_columns: заголовки, содержащие code_name
/// (по умолчанию 'код' при пустом, без учёта регистра).
fn find_code_columns(headers: &[String], code_name: &str) -> Vec<String> {
    let normalized = {
        let base = if code_name.is_empty() { "код" } else { code_name };
        base.trim().to_lowercase()
    };
    headers
        .iter()
        .filter(|h| !h.is_empty() && h.to_lowercase().contains(&normalized))
        .cloned()
        .collect()
}

/// Аналог csv_utils._build_student_name: join непустых student_fields по пробелу.
fn build_student_name(row: &HashMap<String, String>, student_fields: &[String]) -> String {
    let mut parts: Vec<&str> = Vec::new();
    for field in student_fields {
        if let Some(value) = row.get(field) {
            let stripped = value.trim();
            if !stripped.is_empty() {
                parts.push(stripped);
            }
        }
    }
    parts.join(" ").trim().to_string()
}

/// Собирает row по DictReader-семантике: `zip(fieldnames, values)`,
/// пропущенные значения — '' (эквивалентно None в коде csv_utils), лишние — отбрасываются.
fn row_to_map(headers: &[String], fields: &[String]) -> HashMap<String, String> {
    let mut map = HashMap::with_capacity(headers.len());
    for (i, header) in headers.iter().enumerate() {
        let value = fields.get(i).map(|s| s.as_str()).unwrap_or("");
        map.insert(header.clone(), value.to_string());
    }
    map
}

/// Вставка частоты в список (freq, count) с сохранением порядка появления
/// ключей — для воспроизведения Python-словаря charFrequency.
fn add_freq(list: &mut Vec<(i64, i64)>, freq: i64) {
    for pair in list.iter_mut() {
        if pair.0 == freq {
            pair.1 += 1;
            return;
        }
    }
    list.push((freq, 1));
}

/// Аналог csv.Sniffer._guess_delimiter(sample, delimiters=';,\t') с фолбэком ','.
/// Воспроизводит алгоритм частот/мод/consistency из Lib/csv.py. Первый проход
/// _guess_quote_and_delimiter (regex) не воспроизводится: на реальных данных
/// он не меняет исход, а поведение для патологических файлов покрывается фолбэком.
fn sniff_delimiter(sample: &str) -> u8 {
    // Порядок кандидатов, как в строке delimiters=';,\t'.
    const CANDIDATES: [char; 3] = [';', ',', '\t'];
    // csv.Sniffer.preferred = [',', '\t', ';', ' ', ':'].
    const PREFERRED: [char; 5] = [',', '\t', ';', ' ', ':'];

    let lines: Vec<&str> = sample.split('\n').filter(|l| !l.is_empty()).collect();
    if lines.is_empty() {
        return b',';
    }

    let chunk_length = std::cmp::min(10, lines.len());

    // charFrequency[char] = [(freq, count), ...] в порядке появления freq.
    let mut char_frequency: [Vec<(i64, i64)>; 3] = [Vec::new(), Vec::new(), Vec::new()];

    let mut delims: Vec<(char, (i64, i64))> = Vec::new();
    let mut start = 0usize;
    let mut end = chunk_length;
    let mut iteration = 0usize;

    while start < lines.len() {
        iteration += 1;
        let chunk_end = std::cmp::min(end, lines.len());
        for line in &lines[start..chunk_end] {
            for (i, candidate) in CANDIDATES.iter().enumerate() {
                let freq = line.matches(*candidate).count() as i64;
                add_freq(&mut char_frequency[i], freq);
            }
        }

        // Пересчёт modes (как в csv.Sniffer, каждый внешний проход).
        let mut modes: [Option<(i64, i64)>; 3] = [None, None, None];
        for i in 0..3 {
            let items = &char_frequency[i];
            if items.is_empty() {
                continue;
            }
            if items.len() == 1 && items[0].0 == 0 {
                // Символ ни разу не встретился — пропускаем.
                continue;
            }
            if items.len() > 1 {
                // max(items, key=count), при равенстве счётов — первый встреченный.
                let mut best = items[0];
                let mut best_idx = 0usize;
                for (idx, it) in items.iter().enumerate() {
                    if it.1 > best.1 {
                        best = *it;
                        best_idx = idx;
                    }
                }
                let sum_others: i64 = items
                    .iter()
                    .enumerate()
                    .filter(|(idx, _)| *idx != best_idx)
                    .map(|(_, it)| it.1)
                    .sum();
                modes[i] = Some((best.0, best.1 - sum_others));
            } else {
                modes[i] = Some(items[0]);
            }
        }

        let total = std::cmp::min(chunk_length * iteration, lines.len()) as f64;
        let threshold = 0.9f64;
        let mut consistency = 1.0f64;
        while delims.is_empty() && consistency >= threshold {
            for i in 0..3 {
                if let Some((f, c)) = modes[i] {
                    if f > 0 && c > 0 && (c as f64) / total >= consistency {
                        delims.push((CANDIDATES[i], (f, c)));
                    }
                }
            }
            consistency -= 0.01;
        }

        if delims.len() == 1 {
            return delims[0].0 as u8;
        }

        start = end;
        end += chunk_length;
    }

    if delims.is_empty() {
        return b',';
    }
    if delims.len() > 1 {
        for &d in PREFERRED.iter() {
            if delims.iter().any(|(c, _)| *c == d) {
                return d as u8;
            }
        }
    }

    // Предпочтений нет — берём символ с максимальным (freq, count).
    let mut items: Vec<((i64, i64), char)> = delims.iter().map(|&(k, v)| (v, k)).collect();
    items.sort();
    items.last().unwrap().1 as u8
}

fn parse_csv(
    path: &str,
    code_name: &str,
    student_fields: &[String],
    email_field: Option<&str>,
    delimiter: Option<&str>,
) -> PyResult<(StudentData, Option<Emails>)> {
    let bytes = fs::read(path).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            PyFileNotFoundError::new_err(format!("файл не найден: {path}"))
        } else {
            PyOSError::new_err(format!("не удалось прочитать файл CSV «{path}»: {e}"))
        }
    })?;

    // utf-8-sig: срезаем BOM, если есть.
    let bytes = if bytes.starts_with(b"\xef\xbb\xbf") {
        &bytes[3..]
    } else {
        &bytes[..]
    };
    let text = String::from_utf8(bytes.to_vec())
        .map_err(|_| PyValueError::new_err(format!("файл «{path}» не является корректным UTF-8")))?;

    let delim = match normalize_delimiter(delimiter)? {
        Some(d) => d,
        None => {
            // Авто-детект по первым 4096 символам (как file.read(4096)).
            let sample: String = text.chars().take(4096).collect();
            sniff_delimiter(&sample)
        }
    };

    let mut reader = csv::ReaderBuilder::new()
        .delimiter(delim)
        .quote(b'"')
        .double_quote(true)
        .has_headers(false)
        .flexible(true)
        .from_reader(text.as_bytes());

    let mut records = reader.records();
    let headers: Vec<String> = match records.next() {
        None => Vec::new(),
        Some(rec) => {
            let rec = rec
                .map_err(|e| PyValueError::new_err(format!("ошибка чтения CSV: {e}")))?;
            rec.iter()
                .map(|f| normalize_header(f))
                .collect()
        }
    };

    let code_columns = find_code_columns(&headers, code_name);
    let resolved_email_field: Option<String> = match email_field {
        Some(field) => resolve_column_name(&headers, field),
        None => None,
    };

    let mut data: StudentData = HashMap::new();
    let mut emails: Option<Emails> = if email_field.is_some() {
        Some(HashMap::new())
    } else {
        None
    };

    for result in records {
        let rec = result.map_err(|e| PyValueError::new_err(format!("ошибка чтения CSV: {e}")))?;
        let fields: Vec<String> = rec
            .iter()
            .map(|f| f.to_string())
            .collect();
        let row = row_to_map(&headers, &fields);
        let student_name = build_student_name(&row, student_fields);

        if let Some(emails_map) = emails.as_mut() {
            // email-режим: пустые имена пропускаются.
            if student_name.is_empty() {
                continue;
            }
            if let Some(field_name) = &resolved_email_field {
                let email_value = row
                    .get(field_name)
                    .map(|s| s.trim().to_string())
                    .unwrap_or_default();
                match emails_map.entry(student_name.clone()) {
                    Entry::Vacant(entry) => {
                        entry.insert(email_value);
                    }
                    Entry::Occupied(mut entry) => {
                        // Как в csv_utils: перезаписывается непустым значением,
                        // пустое значение существующую запись не трогает.
                        if !email_value.is_empty() {
                            entry.insert(email_value);
                        }
                    }
                }
            } else {
                emails_map.entry(student_name.clone()).or_default();
            }
        }

        if code_columns.is_empty() {
            continue;
        }

        let entry = data.entry(student_name.clone()).or_default();
        for column in &code_columns {
            let value = row.get(column).map(|s| s.trim()).unwrap_or("");
            if value.is_empty() {
                continue;
            }
            entry.entry(column.clone()).or_default().push(value.to_string());
        }
    }

    Ok((data, emails))
}