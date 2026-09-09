use std::{
    collections::{BTreeMap, HashMap},
    io::Read,
    sync::atomic::Ordering,
};

use anyhow::{Context, Result, bail};
use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    routing::post,
};
use flate2::read::GzDecoder;
use serde_json::{Map, Value, json};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use url::Url;
use uuid::Uuid;

use crate::{
    ingest::IngestBatch,
    model::{ProjectStatus, Signal, StoredRecord},
    server::{ApiError, AppState, unix_time_ms},
    writer::SubmitError,
};

pub const MAX_ENVELOPE_BYTES: usize = 20 * 1024 * 1024;
const MAX_ITEM_BYTES: usize = 1024 * 1024;
const BATCH_ID_NAMESPACE: Uuid = Uuid::from_u128(0x0f433f6e_39b2_4c56_b3b8_65d92cb5887c);

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route("/api/{project_id}/envelope/", post(ingest))
        .layer(DefaultBodyLimit::max(MAX_ENVELOPE_BYTES))
}

async fn ingest(
    State(state): State<AppState>,
    Path(project_id): Path<u64>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, ApiError> {
    let store = state.store.clone();
    let project = tokio::task::spawn_blocking(move || store.get_project(project_id))
        .await
        .map_err(ApiError::internal)??
        .filter(|project| project.status == ProjectStatus::Active)
        .ok_or_else(|| ApiError::new(StatusCode::FORBIDDEN, "unknown project"))?;

    let request_key = sentry_auth_key(&headers).or_else(|| query.get("sentry_key").cloned());
    if request_key.as_deref().is_some_and(|key| key != project.key) {
        reject_unauthorized(&state);
        return Err(ApiError::new(StatusCode::FORBIDDEN, "invalid DSN key"));
    }

    let encoding = headers
        .get(header::CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok());
    let decoded = decode_body(encoding, &body).map_err(|error| reject_malformed(&state, error))?;
    let received_at_ms = unix_time_ms()?;
    let parsed = parse_envelope(project_id, received_at_ms, &decoded)
        .map_err(|error| reject_malformed(&state, error))?;
    let envelope_key = envelope_key(&decoded);
    if request_key.is_none() && envelope_key.is_none() {
        reject_unauthorized(&state);
        return Err(ApiError::new(StatusCode::FORBIDDEN, "missing DSN key"));
    }
    if envelope_key
        .as_deref()
        .is_some_and(|key| key != project.key)
        || request_key
            .as_deref()
            .zip(envelope_key.as_deref())
            .is_some_and(|(left, right)| left != right)
    {
        reject_unauthorized(&state);
        return Err(ApiError::new(StatusCode::FORBIDDEN, "conflicting DSN key"));
    }
    let response_id = parsed
        .records
        .iter()
        .find_map(|record| record.source_id.clone());

    match state.writer.submit(parsed).await {
        Ok(()) => Ok(Json(json!({ "id": response_id }))),
        Err(SubmitError::Full) => {
            state.process.queue_full.fetch_add(1, Ordering::Relaxed);
            state.writer.diagnostic("queue_full");
            Err(ApiError::new(
                StatusCode::TOO_MANY_REQUESTS,
                "ingest queue is full",
            ))
        }
        Err(SubmitError::Closed) => Err(ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "ingest writer is unavailable",
        )),
        Err(SubmitError::Write(error)) => {
            Err(ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, error))
        }
    }
}

fn sentry_auth_key(headers: &HeaderMap) -> Option<String> {
    headers
        .get("x-sentry-auth")?
        .to_str()
        .ok()?
        .split([',', ' '])
        .find_map(|part| part.trim().strip_prefix("sentry_key=").map(str::to_owned))
}

fn reject_malformed(state: &AppState, error: anyhow::Error) -> ApiError {
    state.process.malformed.fetch_add(1, Ordering::Relaxed);
    state.writer.diagnostic("malformed");
    ApiError::new(StatusCode::BAD_REQUEST, error.to_string())
}

fn reject_unauthorized(state: &AppState) {
    state.process.unauthorized.fetch_add(1, Ordering::Relaxed);
    state.writer.diagnostic("unauthorized");
}

pub fn decode_body(content_encoding: Option<&str>, body: &[u8]) -> Result<Vec<u8>> {
    match content_encoding.unwrap_or("identity") {
        "" | "identity" => {
            if body.len() > MAX_ENVELOPE_BYTES {
                bail!("envelope exceeds the 20 MiB limit");
            }
            Ok(body.to_vec())
        }
        "gzip" => {
            let mut decoded = Vec::new();
            GzDecoder::new(body)
                .take(MAX_ENVELOPE_BYTES as u64 + 1)
                .read_to_end(&mut decoded)
                .context("decompress gzip envelope")?;
            if decoded.len() > MAX_ENVELOPE_BYTES {
                bail!("decompressed envelope exceeds the 20 MiB limit");
            }
            Ok(decoded)
        }
        other => bail!("unsupported content encoding {other:?}"),
    }
}

pub fn parse_envelope(project_id: u64, received_at_ms: u64, bytes: &[u8]) -> Result<IngestBatch> {
    let mut cursor = 0;
    let envelope_line = read_line(bytes, &mut cursor).context("missing envelope header")?;
    let envelope: Value =
        serde_json::from_slice(envelope_line).context("invalid envelope header")?;
    let envelope_object = envelope
        .as_object()
        .context("envelope header must be a JSON object")?;
    let mut records = Vec::new();
    let mut skipped = BTreeMap::new();
    let mut envelope_item_index = 0;

    while cursor < bytes.len() {
        let header_line = read_line(bytes, &mut cursor).context("missing item header")?;
        if header_line.is_empty() {
            bail!("empty item header");
        }
        let header: Value = serde_json::from_slice(header_line).context("invalid item header")?;
        let header = header
            .as_object()
            .context("item header must be a JSON object")?;
        let kind = header
            .get("type")
            .and_then(Value::as_str)
            .context("item header is missing type")?;
        let payload = read_payload(bytes, &mut cursor, header)?;

        match kind {
            "event" => parse_event(
                project_id,
                received_at_ms,
                envelope_object,
                payload,
                &mut records,
            )?,
            "log" => parse_batch(
                project_id,
                received_at_ms,
                envelope_object,
                header,
                payload,
                envelope_item_index,
                Signal::Log,
                &mut records,
            )?,
            "trace_metric" => parse_batch(
                project_id,
                received_at_ms,
                envelope_object,
                header,
                payload,
                envelope_item_index,
                Signal::Metric,
                &mut records,
            )?,
            other => *skipped.entry(other.to_owned()).or_default() += 1,
        }
        envelope_item_index += 1;
    }

    Ok(IngestBatch { records, skipped })
}

pub fn envelope_key(bytes: &[u8]) -> Option<String> {
    let mut cursor = 0;
    let line = read_line(bytes, &mut cursor)?;
    let header: Value = serde_json::from_slice(line).ok()?;
    header
        .get("dsn")
        .and_then(Value::as_str)
        .and_then(|dsn| Url::parse(dsn).ok())
        .map(|dsn| dsn.username().to_owned())
        .filter(|key| !key.is_empty())
}

fn parse_event(
    project_id: u64,
    received_at_ms: u64,
    envelope: &Map<String, Value>,
    payload: &[u8],
    records: &mut Vec<StoredRecord>,
) -> Result<()> {
    check_item_size(payload)?;
    let raw: Value = serde_json::from_slice(payload).context("invalid event payload")?;
    let object = raw
        .as_object()
        .context("event payload must be a JSON object")?;
    let envelope_id = envelope.get("event_id").and_then(Value::as_str);
    let payload_id = object.get("event_id").and_then(Value::as_str);
    let event_id = envelope_id
        .or(payload_id)
        .context("event is missing event_id")?;
    let event_id = Uuid::parse_str(event_id).context("event_id must be a UUID")?;
    if let Some(other) = envelope_id.zip(payload_id).map(|(_, payload)| payload)
        && Uuid::parse_str(other).context("event_id must be a UUID")? != event_id
    {
        bail!("envelope and event payload IDs do not match");
    }
    if let Some(timestamp) = object.get("timestamp") {
        validate_timestamp(timestamp)?;
    }
    let event_id = event_id.simple().to_string();
    records.push(StoredRecord {
        project_id,
        id: event_id.clone(),
        signal: Signal::Error,
        source: "sentry".into(),
        source_id: Some(event_id),
        issue_id: None,
        timestamp_unix_nano: timestamp_unix_nano(object.get("timestamp"), received_at_ms)?,
        received_at_ms,
        fields: canonical_fields(&raw, envelope, Signal::Error),
        raw,
    });
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn parse_batch(
    project_id: u64,
    received_at_ms: u64,
    envelope: &Map<String, Value>,
    header: &Map<String, Value>,
    payload: &[u8],
    envelope_item_index: usize,
    signal: Signal,
    records: &mut Vec<StoredRecord>,
) -> Result<()> {
    check_item_size(payload)?;
    let raw: Value = serde_json::from_slice(payload).context("invalid batched item payload")?;
    let items = raw
        .get("items")
        .and_then(Value::as_array)
        .context("batched item payload is missing items")?;
    let expected = header
        .get("item_count")
        .and_then(Value::as_u64)
        .context("batched item header is missing item_count")?;
    if expected != items.len() as u64 {
        bail!("item_count does not match the payload");
    }

    for (batch_item_index, item) in items.iter().enumerate() {
        validate_telemetry_item(item, signal)?;
        let id = Uuid::new_v5(
            &BATCH_ID_NAMESPACE,
            &serde_json::to_vec(&(
                envelope,
                header,
                envelope_item_index,
                batch_item_index,
                item,
            ))?,
        )
        .simple()
        .to_string();
        records.push(StoredRecord {
            project_id,
            id: id.clone(),
            signal,
            source: "sentry".into(),
            source_id: Some(id),
            issue_id: None,
            timestamp_unix_nano: timestamp_unix_nano(item.get("timestamp"), received_at_ms)?,
            received_at_ms,
            fields: canonical_fields(item, envelope, signal),
            raw: item.clone(),
        });
    }
    Ok(())
}

fn validate_telemetry_item(item: &Value, signal: Signal) -> Result<()> {
    let item = item
        .as_object()
        .context("telemetry item must be a JSON object")?;
    if let Some(timestamp) = item.get("timestamp") {
        validate_timestamp(timestamp)?;
    }
    if item.get("trace_id").and_then(Value::as_str).is_none() {
        bail!("telemetry item is missing trace_id");
    }
    match signal {
        Signal::Log => {
            let level = item.get("level").and_then(Value::as_str);
            if !matches!(
                level,
                Some("trace" | "debug" | "info" | "warn" | "error" | "fatal")
            ) {
                bail!("log item has an invalid level");
            }
            if item.get("body").and_then(Value::as_str).is_none() {
                bail!("log item is missing body");
            }
        }
        Signal::Metric => {
            if item.get("name").and_then(Value::as_str).is_none()
                || item.get("value").and_then(Value::as_f64).is_none()
                || !matches!(
                    item.get("type").and_then(Value::as_str),
                    Some("counter" | "gauge" | "distribution")
                )
            {
                bail!("metric item has an invalid name, value, or type");
            }
        }
        Signal::Error | Signal::Trace => unreachable!(),
    }
    Ok(())
}

fn timestamp_unix_nano(value: Option<&Value>, received_at_ms: u64) -> Result<u64> {
    match value {
        Some(Value::Number(number)) => Ok((number
            .as_f64()
            .filter(|value| value.is_finite() && *value >= 0.0)
            .context("timestamp must be a non-negative number")?
            * 1_000_000_000.0) as u64),
        Some(Value::String(value)) => u64::try_from(
            OffsetDateTime::parse(value, &Rfc3339)
                .context("timestamp must be RFC 3339")?
                .unix_timestamp_nanos(),
        )
        .context("timestamp is before 1970"),
        Some(_) => bail!("timestamp must be a non-negative number or RFC 3339 string"),
        None => Ok(received_at_ms.saturating_mul(1_000_000)),
    }
}

fn canonical_fields(
    raw: &Value,
    envelope: &Map<String, Value>,
    signal: Signal,
) -> BTreeMap<String, Value> {
    let mut fields = BTreeMap::new();
    for field in [
        "level",
        "logger",
        "transaction",
        "platform",
        "environment",
        "release",
        "dist",
        "trace_id",
        "span_id",
        "parent_span_id",
    ] {
        insert(&mut fields, field, raw.get(field).cloned());
    }
    insert(&mut fields, "server", raw.get("server_name").cloned());
    for (field, pointer) in [
        ("user.id", "/user/id"),
        ("user.email", "/user/email"),
        ("user.username", "/user/username"),
        ("user.ip", "/user/ip_address"),
        ("http.method", "/request/method"),
        ("http.url", "/request/url"),
        ("http.query", "/request/query_string"),
        ("http.cookies", "/request/cookies"),
        ("http.environment", "/request/env"),
        ("http.headers", "/request/headers"),
        ("http.body", "/request/data"),
        ("trace_id", "/contexts/trace/trace_id"),
        ("span_id", "/contexts/trace/span_id"),
        ("parent_span_id", "/contexts/trace/parent_span_id"),
        ("trace.op", "/contexts/trace/op"),
        ("trace.status", "/contexts/trace/status"),
    ] {
        if !fields.contains_key(field) {
            insert(&mut fields, field, raw.pointer(pointer).cloned());
        }
    }
    for field in ["trace_id", "environment", "release"] {
        if !fields.contains_key(field) {
            insert(
                &mut fields,
                field,
                envelope
                    .get("trace")
                    .and_then(|trace| trace.get(field))
                    .cloned(),
            );
        }
    }
    insert_prefixed(&mut fields, "sdk.", raw.get("sdk"));
    insert_prefixed(&mut fields, "sdk.", envelope.get("sdk"));
    insert_prefixed(&mut fields, "user.", raw.get("user"));
    insert_prefixed(&mut fields, "trace.", raw.pointer("/contexts/trace"));
    if let Some(contexts) = raw.get("contexts").and_then(Value::as_object) {
        for (name, value) in contexts {
            if name != "trace" {
                fields.insert(format!("context.{name}"), value.clone());
            }
        }
    }
    if let Some(attributes) = raw.get("attributes").and_then(Value::as_object) {
        for (name, value) in attributes {
            fields.insert(
                format!("attribute.{name}"),
                value.get("value").unwrap_or(value).clone(),
            );
        }
    }
    if let Some(tags) = raw.get("tags") {
        if let Some(tags) = tags.as_object() {
            for (name, value) in tags {
                fields.insert(format!("tag.{name}"), value.clone());
                fields.insert(format!("attribute.{name}"), value.clone());
            }
        } else if let Some(tags) = tags.as_array() {
            for tag in tags {
                if let Some(pair) = tag.as_array()
                    && let (Some(name), Some(value)) =
                        (pair.first().and_then(Value::as_str), pair.get(1))
                {
                    fields.insert(format!("tag.{name}"), value.clone());
                    fields.insert(format!("attribute.{name}"), value.clone());
                }
            }
        }
    }
    for field in [
        "level",
        "logger",
        "transaction",
        "platform",
        "environment",
        "release",
        "dist",
    ] {
        if !fields.contains_key(field) {
            let value = fields.get(&format!("attribute.sentry.{field}")).cloned();
            insert(&mut fields, field, value);
        }
    }
    for (field, attribute) in [("logger", "logger.name"), ("server", "server.address")] {
        if !fields.contains_key(field) {
            let value = fields.get(&format!("attribute.{attribute}")).cloned();
            insert(&mut fields, field, value);
        }
    }
    for field in ["name", "version"] {
        let value = raw
            .pointer(&format!("/sdk/{field}"))
            .cloned()
            .or_else(|| envelope.get("sdk")?.get(field).cloned())
            .or_else(|| {
                fields
                    .get(&format!("attribute.sentry.sdk.{field}"))
                    .cloned()
            });
        insert(&mut fields, &format!("sdk.{field}"), value);
    }

    match signal {
        Signal::Error => {
            let exception = raw
                .pointer("/exception/values")
                .and_then(Value::as_array)
                .and_then(|values| values.last());
            let message = raw
                .get("message")
                .and_then(Value::as_str)
                .or_else(|| raw.pointer("/message/formatted").and_then(Value::as_str))
                .or_else(|| raw.pointer("/logentry/formatted").and_then(Value::as_str))
                .or_else(|| raw.pointer("/logentry/message").and_then(Value::as_str))
                .or_else(|| exception?.get("value")?.as_str());
            let title = match (
                exception
                    .and_then(|value| value.get("type"))
                    .and_then(Value::as_str),
                exception
                    .and_then(|value| value.get("value"))
                    .and_then(Value::as_str),
            ) {
                (Some(kind), Some(value)) => format!("{kind}: {value}"),
                (Some(kind), None) => kind.to_owned(),
                _ => message.unwrap_or("Error").to_owned(),
            };
            insert(&mut fields, "message", message.map(|value| json!(value)));
            fields.insert("title".into(), json!(title));
            fields.insert("issue.title".into(), json!(title));
            fields.insert(
                "issue.fingerprint".into(),
                json!(issue_fingerprint(raw, exception, &title)),
            );
            insert(&mut fields, "name", raw.get("transaction").cloned());
            insert(
                &mut fields,
                "error.type",
                exception.and_then(|value| value.get("type")).cloned(),
            );
            insert(
                &mut fields,
                "error.value",
                exception.and_then(|value| value.get("value")).cloned(),
            );
            insert(
                &mut fields,
                "error.handled",
                exception
                    .and_then(|value| value.pointer("/mechanism/handled"))
                    .and_then(Value::as_bool)
                    .map(Value::Bool),
            );
            fields
                .entry("level".into())
                .or_insert_with(|| json!("error"));
            insert(&mut fields, "error.exceptions", normalized_exceptions(raw));
            insert(
                &mut fields,
                "breadcrumbs",
                raw.pointer("/breadcrumbs/values").cloned(),
            );
            for field in ["filename", "function", "module"] {
                let mut values = raw
                    .pointer("/exception/values")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(|exception| exception.pointer("/stacktrace/frames")?.as_array())
                    .flatten()
                    .filter_map(|frame| frame.get(field).cloned())
                    .collect::<Vec<_>>();
                values.extend(
                    raw.pointer("/stacktrace/frames")
                        .and_then(Value::as_array)
                        .into_iter()
                        .flatten()
                        .filter_map(|frame| frame.get(field).cloned()),
                );
                if !values.is_empty() {
                    fields.insert(format!("stack.{field}"), Value::Array(values));
                }
            }
        }
        Signal::Log => {
            insert(&mut fields, "message", raw.get("body").cloned());
            insert(&mut fields, "title", raw.get("body").cloned());
        }
        Signal::Metric => {
            for field in ["name", "unit", "value"] {
                insert(&mut fields, field, raw.get(field).cloned());
            }
            let metric_type = raw.get("type").cloned();
            if metric_type.as_ref().and_then(Value::as_str) == Some("counter") {
                fields.insert("type".into(), json!("sum"));
                fields.insert("temporality".into(), json!("delta"));
                fields.insert("monotonic".into(), json!(true));
            } else {
                insert(&mut fields, "type", metric_type);
            }
        }
        Signal::Trace => unreachable!(),
    }
    fields
}

fn insert_prefixed(fields: &mut BTreeMap<String, Value>, prefix: &str, value: Option<&Value>) {
    if let Some(object) = value.and_then(Value::as_object) {
        for (name, value) in object {
            fields
                .entry(format!("{prefix}{name}"))
                .or_insert_with(|| value.clone());
        }
    }
}

fn normalized_exceptions(raw: &Value) -> Option<Value> {
    let exceptions = if let Some(values) = raw
        .pointer("/exception/values")
        .and_then(Value::as_array)
        .filter(|values| !values.is_empty())
    {
        values.iter().rev().map(|value| (value, false)).collect()
    } else if raw
        .pointer("/stacktrace/frames")
        .and_then(Value::as_array)
        .is_some_and(|frames| !frames.is_empty())
    {
        vec![(raw, true)]
    } else {
        return None;
    };

    Some(Value::Array(
        exceptions
            .into_iter()
            .map(|(exception, top_level)| {
                let mut normalized = Map::new();
                if top_level {
                    normalized.insert("type".into(), json!("Stacktrace"));
                } else {
                    for name in ["type", "module"] {
                        if let Some(value) = exception.get(name) {
                            normalized.insert(name.into(), value.clone());
                        }
                    }
                    if let Some(value) = exception.get("value") {
                        normalized.insert("message".into(), value.clone());
                    }
                    if let Some(value) = exception.pointer("/mechanism/type") {
                        normalized.insert("mechanism".into(), value.clone());
                    }
                    if let Some(value) = exception
                        .pointer("/mechanism/handled")
                        .and_then(Value::as_bool)
                    {
                        normalized.insert("handled".into(), Value::Bool(value));
                    }
                }
                if let Some(frames) = exception
                    .pointer("/stacktrace/frames")
                    .and_then(Value::as_array)
                {
                    normalized.insert(
                        "frames".into(),
                        Value::Array(
                            frames
                                .iter()
                                .map(|frame| {
                                    let mut normalized = Map::new();
                                    for (name, source) in [
                                        ("filename", "filename"),
                                        ("path", "abs_path"),
                                        ("module", "module"),
                                        ("function", "function"),
                                        ("line", "lineno"),
                                        ("column", "colno"),
                                        ("in_app", "in_app"),
                                        ("context_line", "context_line"),
                                        ("pre_context", "pre_context"),
                                        ("post_context", "post_context"),
                                        ("variables", "vars"),
                                    ] {
                                        if let Some(value) = frame.get(source) {
                                            normalized.insert(name.into(), value.clone());
                                        }
                                    }
                                    Value::Object(normalized)
                                })
                                .collect(),
                        ),
                    );
                }
                Value::Object(normalized)
            })
            .collect(),
    ))
}

fn issue_fingerprint(raw: &Value, exception: Option<&Value>, title: &str) -> String {
    raw.get("fingerprint")
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join("\0")
        })
        .filter(|value| !value.is_empty())
        .or_else(|| {
            let exception = exception?;
            let kind = exception.get("type")?.as_str()?;
            let frames = exception.pointer("/stacktrace/frames")?.as_array()?;
            let frame = frames
                .iter()
                .rev()
                .find(|frame| frame.get("in_app").and_then(Value::as_bool) == Some(true))
                .or_else(|| frames.last())?;
            Some(format!(
                "{kind}\0{}\0{}\0{}",
                frame
                    .get("module")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
                frame
                    .get("function")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
                frame
                    .get("filename")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
            ))
        })
        .unwrap_or_else(|| {
            raw.pointer("/logentry/message")
                .or_else(|| raw.get("message"))
                .and_then(Value::as_str)
                .unwrap_or(title)
                .to_owned()
        })
}

fn insert(fields: &mut BTreeMap<String, Value>, name: &str, value: Option<Value>) {
    if let Some(value) = value {
        fields.insert(name.to_owned(), value);
    }
}

fn validate_timestamp(value: &Value) -> Result<()> {
    match value {
        Value::Number(number)
            if number
                .as_f64()
                .is_some_and(|value| value.is_finite() && value >= 0.0) =>
        {
            Ok(())
        }
        Value::String(value) => crate::query::parse_time_ms(value).map(|_| ()),
        _ => bail!("timestamp must be a non-negative number or RFC 3339 string"),
    }
}

fn check_item_size(payload: &[u8]) -> Result<()> {
    if payload.len() > MAX_ITEM_BYTES {
        bail!("supported item exceeds the 1 MiB limit");
    }
    Ok(())
}

fn read_line<'a>(bytes: &'a [u8], cursor: &mut usize) -> Option<&'a [u8]> {
    if *cursor >= bytes.len() {
        return None;
    }
    let start = *cursor;
    match bytes[start..].iter().position(|byte| *byte == b'\n') {
        Some(offset) => {
            *cursor = start + offset + 1;
            Some(&bytes[start..start + offset])
        }
        None => {
            *cursor = bytes.len();
            Some(&bytes[start..])
        }
    }
}

fn read_payload<'a>(
    bytes: &'a [u8],
    cursor: &mut usize,
    header: &Map<String, Value>,
) -> Result<&'a [u8]> {
    if let Some(length) = header.get("length") {
        let length = usize::try_from(length.as_u64().context("item length must be an integer")?)
            .context("item length is too large")?;
        let end = cursor.checked_add(length).context("item length overflow")?;
        if end > bytes.len() {
            bail!("item payload is shorter than its declared length");
        }
        let payload = &bytes[*cursor..end];
        *cursor = end;
        if *cursor < bytes.len() {
            if bytes[*cursor] != b'\n' {
                bail!("length-prefixed item is not followed by a newline");
            }
            *cursor += 1;
        }
        Ok(payload)
    } else {
        read_line(bytes, cursor).context("missing item payload")
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::store::Store;

    #[test]
    fn parses_length_prefixed_error() {
        let event = br#"{"event_id":"9ec79c33ec9942ab8353589fcb2e04dc","timestamp":1}"#;
        let envelope = format!(
            "{{\"event_id\":\"9ec79c33ec9942ab8353589fcb2e04dc\"}}\n{{\"type\":\"event\",\"length\":{}}}\n{}",
            event.len(),
            String::from_utf8_lossy(event)
        );
        let parsed = parse_envelope(1, 2, envelope.as_bytes()).unwrap();
        assert_eq!(parsed.records.len(), 1);
        assert_eq!(parsed.records[0].signal, Signal::Error);
        assert_eq!(parsed.records[0].source, "sentry");
        assert_eq!(parsed.records[0].fields["level"], "error");
        assert_eq!(parsed.records[0].timestamp_unix_nano, 1_000_000_000);
    }

    #[test]
    fn normalizes_exceptions_primary_first() {
        let raw = json!({
            "exception": {"values": [
                {
                    "type": "CauseError",
                    "value": "cause",
                    "mechanism": {"type": "chained", "handled": true}
                },
                {
                    "type": "PrimaryError",
                    "value": "failed",
                    "module": "app.errors",
                    "mechanism": {"type": "generic", "handled": false},
                    "stacktrace": {"frames": [{
                        "filename": "worker.py",
                        "abs_path": "/srv/worker.py",
                        "module": "app.worker",
                        "function": "run",
                        "lineno": 42,
                        "colno": 7,
                        "in_app": true,
                        "context_line": "raise PrimaryError()",
                        "pre_context": ["def run():"],
                        "post_context": ["return"],
                        "vars": {"job_id": "123"}
                    }]}
                }
            ]}
        });

        let fields = canonical_fields(&raw, &Map::new(), Signal::Error);
        assert_eq!(fields["error.type"], "PrimaryError");
        assert_eq!(fields["error.handled"], false);
        assert_eq!(
            fields["error.exceptions"],
            json!([
                {
                    "type": "PrimaryError",
                    "message": "failed",
                    "module": "app.errors",
                    "mechanism": "generic",
                    "handled": false,
                    "frames": [{
                        "filename": "worker.py",
                        "path": "/srv/worker.py",
                        "module": "app.worker",
                        "function": "run",
                        "line": 42,
                        "column": 7,
                        "in_app": true,
                        "context_line": "raise PrimaryError()",
                        "pre_context": ["def run():"],
                        "post_context": ["return"],
                        "variables": {"job_id": "123"}
                    }]
                },
                {
                    "type": "CauseError",
                    "message": "cause",
                    "mechanism": "chained",
                    "handled": true
                }
            ])
        );

        let fallback = canonical_fields(
            &json!({
                "exception": {"values": []},
                "stacktrace": {"frames": [{"filename": "main.py", "lineno": 3}]}
            }),
            &Map::new(),
            Signal::Error,
        );
        assert_eq!(
            fallback["error.exceptions"],
            json!([{"type": "Stacktrace", "frames": [{"filename": "main.py", "line": 3}]}])
        );
    }

    #[test]
    fn normalizes_issue_request_and_context_fields() {
        let raw = json!({
            "event_id": "9ec79c33ec9942ab8353589fcb2e04dc",
            "breadcrumbs": {"values": [{"category": "query", "message": "loaded"}]},
            "request": {
                "method": "POST",
                "url": "https://example.test/jobs",
                "query_string": "retry=1",
                "cookies": {"session": "abc"},
                "env": {"REMOTE_ADDR": "127.0.0.1"},
                "headers": [["content-type", "application/json"]],
                "data": {"job": 7}
            },
            "user": {"id": "42", "ip_address": "127.0.0.1", "role": "admin"},
            "tags": {"region": "west"},
            "sdk": {"name": "sentry.test", "version": "1.2.3", "integrations": ["logging"]},
            "contexts": {
                "trace": {"trace_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "op": "http.server"},
                "browser": {"name": "Firefox", "version": "130"}
            }
        });
        let envelope = json!({
            "event_id": "9ec79c33ec9942ab8353589fcb2e04dc"
        });
        let bytes = format!("{}\n{}\n{}", envelope, json!({"type": "event"}), raw);

        let record = &parse_envelope(1, 2, bytes.as_bytes()).unwrap().records[0];
        assert_eq!(record.raw, raw);
        assert_eq!(record.fields["breadcrumbs"], raw["breadcrumbs"]["values"]);
        assert_eq!(record.fields["http.method"], "POST");
        assert_eq!(record.fields["http.url"], "https://example.test/jobs");
        assert_eq!(record.fields["http.query"], "retry=1");
        assert_eq!(record.fields["http.cookies"], json!({"session": "abc"}));
        assert_eq!(
            record.fields["http.environment"],
            json!({"REMOTE_ADDR": "127.0.0.1"})
        );
        assert_eq!(
            record.fields["http.headers"],
            json!([["content-type", "application/json"]])
        );
        assert_eq!(record.fields["http.body"], json!({"job": 7}));
        assert_eq!(record.fields["user.id"], "42");
        assert_eq!(record.fields["user.ip"], "127.0.0.1");
        assert_eq!(record.fields["user.ip_address"], "127.0.0.1");
        assert_eq!(record.fields["user.role"], "admin");
        assert_eq!(record.fields["tag.region"], "west");
        assert_eq!(record.fields["attribute.region"], "west");
        assert_eq!(record.fields["sdk.name"], "sentry.test");
        assert_eq!(record.fields["sdk.integrations"], json!(["logging"]));
        assert_eq!(record.fields["trace.op"], "http.server");
        assert_eq!(
            record.fields["context.browser"],
            json!({"name": "Firefox", "version": "130"})
        );
        assert!(!record.fields.contains_key("context.trace"));
    }

    #[test]
    fn rejects_incorrect_batch_count() {
        let payload = r#"{"items":[]}"#;
        let envelope = format!(
            "{{}}\n{{\"type\":\"log\",\"item_count\":1,\"length\":{}}}\n{}",
            payload.len(),
            payload
        );
        assert!(parse_envelope(1, 2, envelope.as_bytes()).is_err());
    }

    #[test]
    fn batched_retries_are_deterministic_without_collapsing_items() {
        let directory = tempdir().unwrap();
        let store = Store::open(&directory.path().join("db")).unwrap();
        let project = store.add_project("demo").unwrap();
        let log = json!({
            "timestamp": 1,
            "level": "info",
            "body": "same",
            "trace_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        });
        let metric = json!({
            "timestamp": 1,
            "name": "same",
            "type": "gauge",
            "value": 1,
            "trace_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        });
        let envelope = format!(
            "{}\n{}\n{}\n{}\n{}",
            json!({"sent_at": "2026-09-08T00:00:00Z"}),
            json!({"type": "log", "item_count": 2}),
            json!({"items": [log.clone(), log]}),
            json!({"type": "trace_metric", "item_count": 2}),
            json!({"items": [metric.clone(), metric]}),
        );

        let first = parse_envelope(project.id, 2, envelope.as_bytes()).unwrap();
        let retry = parse_envelope(project.id, 3, envelope.as_bytes()).unwrap();
        let first_ids = first
            .records
            .iter()
            .map(|record| record.id.clone())
            .collect::<Vec<_>>();
        assert_eq!(
            first_ids,
            retry
                .records
                .iter()
                .map(|record| record.id.clone())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            first_ids
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            4
        );
        assert!(
            first
                .records
                .iter()
                .all(|record| record.source_id.as_deref() == Some(record.id.as_str()))
        );
        assert_eq!(
            store
                .write_records(first.records, BTreeMap::new(), Vec::new(), true)
                .unwrap()
                .accepted,
            4
        );
        assert_eq!(
            store
                .write_records(retry.records, BTreeMap::new(), Vec::new(), true)
                .unwrap()
                .accepted,
            0
        );
    }

    #[test]
    fn parses_python_sdk_2_68_1_fixtures() {
        let fixtures: &[(&[u8], Signal, Option<&str>)] = &[
            (
                include_bytes!("../../tests/fixtures/sentry-sdk-2.68.1/error.envelope"),
                Signal::Error,
                None,
            ),
            (
                include_bytes!("../../tests/fixtures/sentry-sdk-2.68.1/log.envelope"),
                Signal::Log,
                None,
            ),
            (
                include_bytes!("../../tests/fixtures/sentry-sdk-2.68.1/counter.envelope"),
                Signal::Metric,
                Some("fixture.count"),
            ),
            (
                include_bytes!("../../tests/fixtures/sentry-sdk-2.68.1/gauge.envelope"),
                Signal::Metric,
                Some("fixture.gauge"),
            ),
            (
                include_bytes!("../../tests/fixtures/sentry-sdk-2.68.1/distribution.envelope"),
                Signal::Metric,
                Some("fixture.distribution"),
            ),
        ];
        for (fixture, signal, name) in fixtures {
            let parsed = parse_envelope(1, 0, fixture).unwrap();
            assert_eq!(parsed.records.len(), 1);
            assert_eq!(parsed.records[0].signal, *signal);
            if let Some(name) = name {
                assert_eq!(parsed.records[0].raw["name"], *name);
            }
        }
        let counter = parse_envelope(1, 0, fixtures[2].0).unwrap();
        assert_eq!(counter.records[0].fields["type"], "sum");
        assert_eq!(counter.records[0].fields["temporality"], "delta");
        assert_eq!(counter.records[0].fields["monotonic"], true);
        assert_eq!(
            parse_envelope(1, 0, fixtures[3].0).unwrap().records[0].fields["type"],
            "gauge"
        );
        assert_eq!(
            parse_envelope(1, 0, fixtures[4].0).unwrap().records[0].fields["type"],
            "distribution"
        );
        assert_eq!(timestamp_unix_nano(None, 42).unwrap(), 42_000_000);
    }
}
