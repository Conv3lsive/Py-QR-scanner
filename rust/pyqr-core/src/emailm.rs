//! Отправка SMTP: паритет с email_utils.send_email_smtp / email_delivery._send_one
//! (одиночная отправка; retry-логика остаётся в Python).
use pyo3::prelude::*;

/// Отправить письмо (lettre). mode: "ssl" (465, implicit TLS), "starttls", "plain".
/// Возвращает (ok, message_id, error): ok=False + error-текст при неудаче,
/// message_id — из ответа сервера или сгенерированный.
/// Вложение — attachment_path (MIME, как в email_utils/_build_message).
#[pyfunction]
#[pyo3(signature = (host, port, username, password, mode, sender, recipient, subject, body, attachment_path=None))]
pub fn send_smtp(
    host: String,
    port: u16,
    username: String,
    password: String,
    mode: String,
    sender: String,
    recipient: String,
    subject: String,
    body: String,
    attachment_path: Option<String>,
) -> PyResult<(bool, String, String)> {
    let _ = (host, port, username, password, mode, sender, recipient, subject, body, attachment_path);
    todo!("агент E: реализовать по email_utils.send_email_smtp + email_delivery._send_one")
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(send_smtp, m)?)?;
    Ok(())
}
