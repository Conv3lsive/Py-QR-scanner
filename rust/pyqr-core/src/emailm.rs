//! Отправка SMTP: паритет с email_utils.send_email_smtp / email_delivery._send_one
//! (одиночная отправка; retry-логика остаётся в Python).
use std::path::Path;
use std::time::Duration;

use pyo3::prelude::*;

use lettre::message::{header::ContentType, Attachment, Mailbox, Message, MultiPart, SinglePart};
use lettre::transport::smtp::authentication::Credentials;
use lettre::transport::smtp::client::{Tls, TlsParameters};
use lettre::transport::smtp::response::Response;
use lettre::{SmtpTransport, Transport};
use uuid::Uuid;

/// Таймаут сокета (подключение + операции), как в email_delivery._SMTP_TIMEOUT_SEC.
const SMTP_TIMEOUT_SEC: u64 = 30;

/// Отправить письмо (lettre). mode: "ssl" (465, implicit TLS), "starttls", "plain".
/// Возвращает (ok, message_id, error): ok=False + error-текст при неудаче,
/// message_id — из ответа сервера или сгенерированный.
/// Вложение — attachment_path (MIME, как в email_utils/_build_message).
#[pyfunction]
#[pyo3(signature = (host, port, username, password, mode, sender, recipient, subject, body, attachment_path=None))]
pub fn send_smtp(
    py: Python<'_>,
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
    // Блокирующее сетевое I/O без GIL — иначе Python-потоки (в т.ч. локальные
    // тестовые SMTP-синки и поток наблюдения) замерзают на время отправки.
    let result = py.allow_threads(|| {
        send_smtp_inner(
            &host,
            port,
            &username,
            &password,
            &mode,
            &sender,
            &recipient,
            &subject,
            &body,
            attachment_path.as_deref(),
        )
    });
    match result {
        Ok(message_id) => Ok((true, message_id, String::new())),
        Err(error) => Ok((false, String::new(), error)),
    }
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(send_smtp, m)?)?;
    Ok(())
}

/// Собрать и отправить письмо. Ok → message_id, Err → текст ошибки.
fn send_smtp_inner(
    host: &str,
    port: u16,
    username: &str,
    password: &str,
    mode: &str,
    sender: &str,
    recipient: &str,
    subject: &str,
    body: &str,
    attachment_path: Option<&str>,
) -> Result<String, String> {
    let from: Mailbox = sender
        .parse()
        .map_err(|e| format!("Неверный адрес отправителя: {e}"))?;
    let to: Mailbox = recipient
        .parse()
        .map_err(|e| format!("Неверный адрес получателя: {e}"))?;

    // Домен для Message-ID — как в email_delivery._build_message:
    // sender_email.split('@')[-1], иначе 'localhost'.
    let domain = sender
        .rsplit_once('@')
        .map(|(_, d)| d.to_string())
        .unwrap_or_else(|| "localhost".to_string());
    let generated_id = format!("<{}@{}>", Uuid::new_v4(), domain);

    let message = build_message(from, to, subject, body, attachment_path, &generated_id)?;

    let mut transport = SmtpTransport::builder_dangerous(host)
        .port(port)
        .tls(tls_for_mode(mode, host)?)
        .timeout(Some(Duration::from_secs(SMTP_TIMEOUT_SEC)));

    if !username.is_empty() {
        transport = transport.credentials(Credentials::new(
            username.to_string(),
            password.to_string(),
        ));
    }

    let mailer = transport.build();
    let response = mailer.send(&message).map_err(|e| e.to_string())?;

    Ok(extract_message_id(&response).unwrap_or(generated_id))
}

/// Режимы TLS — паритет с email_delivery._send_one:
///   "ssl"     → SMTP_SSL (implicit TLS, порт 465)
///   "starttls"→ SMTP + starttls()
///   "plain"   → без TLS (локальный тестовый SMTP-синк)
fn tls_for_mode(mode: &str, host: &str) -> Result<Tls, String> {
    match mode.to_lowercase().as_str() {
        "ssl" => Ok(Tls::Wrapper(
            TlsParameters::new(host.to_string())
                .map_err(|e| format!("Ошибка настройки TLS: {e}"))?,
        )),
        "starttls" => Ok(Tls::Required(
            TlsParameters::new(host.to_string())
                .map_err(|e| format!("Ошибка настройки TLS: {e}"))?,
        )),
        _ => Ok(Tls::None),
    }
}

/// Собрать письмо: From/To/Subject (UTF-8), body текстом, опциональное
/// вложение MIME (application/zip, имя файла из пути) — как
/// email_utils.send_email_smtp / email_delivery._build_message.
fn build_message(
    from: Mailbox,
    to: Mailbox,
    subject: &str,
    body: &str,
    attachment_path: Option<&str>,
    message_id: &str,
) -> Result<Message, String> {
    let builder = Message::builder()
        .from(from)
        .to(to)
        .subject(subject.to_string())
        .message_id(Some(message_id.to_string()));

    match attachment_path.filter(|p| !p.trim().is_empty()) {
        Some(path) => {
            let data = std::fs::read(path)
                .map_err(|e| format!("Не удалось прочитать вложение {path}: {e}"))?;
            let filename = Path::new(path)
                .file_name()
                .map(|s| s.to_string_lossy().into_owned())
                .unwrap_or_else(|| "attachment".to_string());
            let content_type = ContentType::parse("application/zip")
                .map_err(|e| format!("Неверный MIME-тип вложения: {e}"))?;
            let attachment = Attachment::new(filename).body(data, content_type);
            let multipart = MultiPart::mixed()
                .singlepart(SinglePart::plain(body.to_string()))
                .singlepart(attachment);
            builder
                .multipart(multipart)
                .map_err(|e| format!("Ошибка сборки письма: {e}"))
        }
        None => builder
            .body(body.to_string())
            .map_err(|e| format!("Ошибка сборки письма: {e}")),
    }
}

/// Попытаться вытащить Message-ID из ответа сервера (строки вида "<...@...>").
fn extract_message_id(response: &Response) -> Option<String> {
    for line in response.message() {
        let mut rest = line;
        while let Some(rel_start) = rest.find('<') {
            let after = &rest[rel_start..];
            match after.find('>') {
                Some(rel_end) => {
                    let inner = &after[1..rel_end];
                    if valid_message_id(inner) {
                        return Some(format!("<{inner}>"));
                    }
                    rest = &after[rel_end + 1..];
                }
                None => break,
            }
        }
    }
    None
}

fn valid_message_id(inner: &str) -> bool {
    if inner.is_empty()
        || !inner.contains('@')
        || inner.chars().any(|c| c.is_whitespace() || c.is_control())
    {
        return false;
    }
    let mut parts = inner.splitn(2, '@');
    let local = parts.next().unwrap_or("");
    let domain = parts.next().unwrap_or("");
    !local.is_empty() && !domain.is_empty()
}