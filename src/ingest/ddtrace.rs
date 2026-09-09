use std::{collections::BTreeMap, sync::atomic::Ordering};

use anyhow::{Result, bail};
use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Path, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, put},
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Number, Value, json};

use crate::{
    ingest::IngestBatch,
    model::{ProjectStatus, Signal, StoredRecord},
    server::{AppState, unix_time_ms},
    writer::SubmitError,
};

const MAX_REQUEST_BYTES: usize = 64 * 1024 * 1024;

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route("/{project_id}/{project_key}/info", get(info))
        .route(
            "/{project_id}/{project_key}/v0.4/traces",
            put(traces_v04).post(traces_v04),
        )
        .route(
            "/{project_id}/{project_key}/v0.5/traces",
            put(traces_v05).post(traces_v05),
        )
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BYTES))
}

async fn info(
    State(state): State<AppState>,
    Path((project_id, project_key)): Path<(u64, String)>,
) -> Result<Json<Value>, Response> {
    authorize(&state, project_id, &project_key).await?;
    Ok(Json(json!({
        "version": env!("CARGO_PKG_VERSION"),
        "endpoints": ["/v0.4/traces", "/v0.5/traces"],
        "client_drop_p0s": false,
        "span_meta_structs": false,
        "long_running_spans": false,
        "span_events": false,
        "config": { "statsd_port": 0 },
        "peer_tags": [],
    })))
}

async fn traces_v04(
    State(state): State<AppState>,
    Path((project_id, project_key)): Path<(u64, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, Response> {
    authorize_request(&state, project_id, &project_key, &headers).await?;
    let traces: Vec<Vec<DatadogSpan>> = rmp_serde::from_slice(&body)
        .map_err(|error| malformed(&state, format!("invalid v0.4 MessagePack: {error}")))?;
    submit(&state, parse_traces(project_id, traces)).await?;
    Ok(sampling_response())
}

async fn traces_v05(
    State(state): State<AppState>,
    Path((project_id, project_key)): Path<(u64, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, Response> {
    authorize_request(&state, project_id, &project_key, &headers).await?;
    let (dictionary, traces): (Vec<String>, Vec<Vec<V05Span>>) = rmp_serde::from_slice(&body)
        .map_err(|error| malformed(&state, format!("invalid v0.5 MessagePack: {error}")))?;
    let traces = traces
        .into_iter()
        .map(|trace| {
            trace
                .into_iter()
                .map(|span| span.expand(&dictionary))
                .collect::<Result<Vec<_>>>()
        })
        .collect::<Result<Vec<_>>>();
    let batch = match traces {
        Ok(traces) => parse_traces(project_id, traces),
        Err(error) => {
            return Err(malformed(
                &state,
                format!("invalid v0.5 dictionary: {error}"),
            ));
        }
    };
    submit(&state, batch).await?;
    Ok(sampling_response())
}

async fn authorize_request(
    state: &AppState,
    project_id: u64,
    project_key: &str,
    headers: &HeaderMap,
) -> Result<(), Response> {
    if !headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/msgpack"))
    {
        return Err(error_response(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "Content-Type must be application/msgpack",
        ));
    }
    if headers
        .get(header::CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| !value.trim().eq_ignore_ascii_case("identity"))
    {
        return Err(error_response(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "Content-Encoding must be identity",
        ));
    }
    authorize(state, project_id, project_key).await
}

async fn authorize(state: &AppState, project_id: u64, project_key: &str) -> Result<(), Response> {
    let store = state.store.clone();
    let project = tokio::task::spawn_blocking(move || store.get_project(project_id))
        .await
        .map_err(internal)?
        .map_err(internal)?;
    if project
        .as_ref()
        .is_none_or(|project| project.status != ProjectStatus::Active || project.key != project_key)
    {
        state.process.unauthorized.fetch_add(1, Ordering::Relaxed);
        state.writer.diagnostic("unauthorized");
        return Err(error_response(
            StatusCode::FORBIDDEN,
            "invalid project or project key",
        ));
    }
    Ok(())
}

async fn submit(state: &AppState, batch: IngestBatch) -> Result<(), Response> {
    match state.writer.submit(batch).await {
        Ok(()) => Ok(()),
        Err(SubmitError::Full) => {
            state.process.queue_full.fetch_add(1, Ordering::Relaxed);
            state.writer.diagnostic("queue_full");
            let mut response =
                error_response(StatusCode::TOO_MANY_REQUESTS, "ingest queue is full");
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, "1".parse().unwrap());
            Err(response)
        }
        Err(SubmitError::Closed) => Err(error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "ingest writer is unavailable",
        )),
        Err(SubmitError::Write(error)) => Err(internal(error)),
    }
}

fn sampling_response() -> Json<Value> {
    Json(json!({ "rate_by_service": {} }))
}

fn malformed(state: &AppState, message: String) -> Response {
    state.process.malformed.fetch_add(1, Ordering::Relaxed);
    state.writer.diagnostic("malformed");
    error_response(StatusCode::BAD_REQUEST, message)
}

fn internal(error: impl std::fmt::Display) -> Response {
    tracing::error!(%error, "ddtrace request failed");
    error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal server error")
}

fn error_response(status: StatusCode, message: impl Into<String>) -> Response {
    (status, message.into()).into_response()
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
struct DatadogSpan {
    service: String,
    name: String,
    resource: String,
    trace_id: u64,
    span_id: u64,
    parent_id: u64,
    start: i64,
    duration: i64,
    error: i32,
    meta: BTreeMap<String, String>,
    metrics: BTreeMap<String, f64>,
    #[serde(rename = "type")]
    span_type: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct V05Span(
    u32,
    u32,
    u32,
    u64,
    u64,
    u64,
    i64,
    i64,
    i32,
    BTreeMap<u32, u32>,
    BTreeMap<u32, f64>,
    u32,
);

impl V05Span {
    fn expand(self, dictionary: &[String]) -> Result<DatadogSpan> {
        let text = |index: u32| {
            dictionary
                .get(index as usize)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("string index {index} is out of bounds"))
        };
        Ok(DatadogSpan {
            service: text(self.0)?,
            name: text(self.1)?,
            resource: text(self.2)?,
            trace_id: self.3,
            span_id: self.4,
            parent_id: self.5,
            start: self.6,
            duration: self.7,
            error: self.8,
            meta: self
                .9
                .into_iter()
                .map(|(key, value)| Ok((text(key)?, text(value)?)))
                .collect::<Result<_>>()?,
            metrics: self
                .10
                .into_iter()
                .map(|(key, value)| Ok((text(key)?, value)))
                .collect::<Result<_>>()?,
            span_type: text(self.11)?,
        })
    }
}

fn parse_traces(project_id: u64, traces: Vec<Vec<DatadogSpan>>) -> IngestBatch {
    let received_at_ms = unix_time_ms().unwrap_or(0);
    let mut records = Vec::new();
    let mut rejected = 0;
    for span in traces.into_iter().flatten() {
        match span_records(project_id, received_at_ms, span) {
            Ok(mut parsed) => records.append(&mut parsed),
            Err(_) => rejected += 1,
        }
    }
    IngestBatch {
        records,
        skipped: if rejected == 0 {
            BTreeMap::new()
        } else {
            BTreeMap::from([("ddtrace.invalid_span".into(), rejected)])
        },
    }
}

fn span_records(
    project_id: u64,
    received_at_ms: u64,
    span: DatadogSpan,
) -> Result<Vec<StoredRecord>> {
    if span.trace_id == 0 || span.span_id == 0 {
        bail!("trace and span IDs must be nonzero");
    }
    if span.name.is_empty() {
        bail!("span name is empty");
    }
    let timestamp_unix_nano = u64::try_from(span.start)?;
    let duration_ns = u64::try_from(span.duration)?;
    let trace_id = trace_id(&span);
    let span_id = format!("{:016x}", span.span_id);
    let source_id = format!("{trace_id}-{span_id}");
    let raw = raw_span(&span);
    let mut fields = BTreeMap::from([
        ("name".into(), json!(span.name)),
        (
            "message".into(),
            json!(if span.resource.is_empty() {
                &span.name
            } else {
                &span.resource
            }),
        ),
        ("trace_id".into(), json!(trace_id)),
        ("span_id".into(), json!(span_id)),
        (
            "span.status".into(),
            json!(if span.error == 0 { "unset" } else { "error" }),
        ),
        ("duration_ns".into(), json!(duration_ns)),
    ]);
    if span.parent_id != 0 {
        fields.insert(
            "parent_span_id".into(),
            json!(format!("{:016x}", span.parent_id)),
        );
    }
    if !span.service.is_empty() {
        fields.insert("resource.service.name".into(), json!(span.service));
    }
    if !span.resource.is_empty() {
        fields.insert("resource.name".into(), json!(span.resource));
    }
    if let Some(environment) = span.meta.get("env") {
        fields.insert("environment".into(), json!(environment));
    }
    if let Some(version) = span.meta.get("version") {
        fields.insert("resource.service.version".into(), json!(version));
    }
    fields.insert(
        "span.kind".into(),
        json!(
            span.meta
                .get("span.kind")
                .map_or("unspecified", String::as_str)
        ),
    );
    if !span.span_type.is_empty() {
        fields.insert("attribute.datadog.type".into(), json!(span.span_type));
    }
    for (name, value) in &span.meta {
        fields.insert(format!("attribute.{name}"), json!(value));
    }
    for (name, value) in &span.metrics {
        if Number::from_f64(*value).is_some() {
            fields.insert(format!("attribute.{name}"), json!(value));
        }
    }
    if let Some(message) = span.meta.get("error.msg")
        && span.error != 0
    {
        fields.insert("span.status_message".into(), json!(message));
    }
    let trace_record = StoredRecord {
        project_id,
        id: source_id.clone(),
        signal: Signal::Trace,
        source: "ddtrace".into(),
        source_id: Some(source_id.clone()),
        issue_id: None,
        timestamp_unix_nano,
        received_at_ms,
        fields: fields.clone(),
        raw: raw.clone(),
    };
    let mut records = vec![trace_record];
    if span.error != 0 {
        let error_type = span.meta.get("error.type").map_or("Error", String::as_str);
        let error_value = span
            .meta
            .get("error.msg")
            .map_or(span.resource.as_str(), String::as_str);
        let title = if error_value.is_empty() {
            error_type.to_owned()
        } else {
            format!("{error_type}: {error_value}")
        };
        fields.extend([
            ("source_record_id".into(), json!(source_id)),
            ("error.type".into(), json!(error_type)),
            ("error.value".into(), json!(error_value)),
            ("level".into(), json!("error")),
            ("message".into(), json!(error_value)),
            ("title".into(), json!(title)),
            ("issue.title".into(), json!(title)),
            (
                "issue.fingerprint".into(),
                json!(format!("ddtrace:{error_type}")),
            ),
        ]);
        if let Some(stacktrace) = span.meta.get("error.stack") {
            fields.insert("error.stacktrace".into(), json!(stacktrace));
        }
        let error_source_id = format!("{source_id}-error");
        records.push(StoredRecord {
            project_id,
            id: error_source_id.clone(),
            signal: Signal::Error,
            source: "ddtrace".into(),
            source_id: Some(error_source_id),
            issue_id: None,
            timestamp_unix_nano,
            received_at_ms,
            fields,
            raw: json!({ "source_record_id": source_id, "span": raw }),
        });
    }
    Ok(records)
}

fn trace_id(span: &DatadogSpan) -> String {
    let low = format!("{:016x}", span.trace_id);
    span.meta
        .get("_dd.p.tid")
        .filter(|high| high.len() == 16 && high.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .map_or(low.clone(), |high| {
            format!("{}{low}", high.to_ascii_lowercase())
        })
}

fn raw_span(span: &DatadogSpan) -> Value {
    let metrics = span
        .metrics
        .iter()
        .map(|(name, value)| {
            (
                name.clone(),
                Number::from_f64(*value).map_or(Value::Null, Value::Number),
            )
        })
        .collect::<Map<_, _>>();
    json!({
        "service": span.service,
        "name": span.name,
        "resource": span.resource,
        "trace_id": span.trace_id.to_string(),
        "span_id": span.span_id.to_string(),
        "parent_id": span.parent_id.to_string(),
        "start": span.start,
        "duration": span.duration,
        "error": span.error,
        "meta": span.meta,
        "metrics": metrics,
        "type": span.span_type,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::{body::Body, http::Request};
    use tempfile::tempdir;
    use tower::ServiceExt;

    use super::*;
    use crate::{model::ProcessStats, store::Store, writer::Durability};

    fn span() -> DatadogSpan {
        DatadogSpan {
            service: "checkout".into(),
            name: "web.request".into(),
            resource: "GET /orders".into(),
            trace_id: 0x12,
            span_id: 0x34,
            parent_id: 0,
            start: 1_700_000_000_000_000_000,
            duration: 2_000_000,
            error: 1,
            meta: BTreeMap::from([
                ("_dd.p.tid".into(), "0123456789ABCDEF".into()),
                ("env".into(), "test".into()),
                ("error.type".into(), "ValueError".into()),
                ("error.msg".into(), "bad order".into()),
                ("error.stack".into(), "stack".into()),
                ("span.kind".into(), "server".into()),
                ("version".into(), "1.2.3".into()),
            ]),
            metrics: BTreeMap::from([("process_id".into(), 42.0)]),
            span_type: "web".into(),
        }
    }

    #[test]
    fn converts_v04_spans_and_errors() {
        let batch = parse_traces(7, vec![vec![span()]]);
        assert!(batch.skipped.is_empty());
        assert_eq!(batch.records.len(), 2);
        let trace = &batch.records[0];
        assert_eq!(trace.source, "ddtrace");
        assert_eq!(trace.fields["trace_id"], "0123456789abcdef0000000000000012");
        assert_eq!(trace.fields["span_id"], "0000000000000034");
        assert_eq!(trace.fields["resource.service.name"], "checkout");
        assert_eq!(trace.fields["resource.service.version"], "1.2.3");
        assert_eq!(trace.fields["environment"], "test");
        assert_eq!(trace.fields["span.kind"], "server");
        assert_eq!(trace.fields["span.status"], "error");
        assert_eq!(trace.fields["attribute.process_id"], 42.0);

        let error = &batch.records[1];
        assert_eq!(error.signal, Signal::Error);
        assert_eq!(error.fields["error.type"], "ValueError");
        assert_eq!(error.fields["error.value"], "bad order");
        assert_eq!(error.fields["error.stacktrace"], "stack");
        assert_eq!(error.fields["issue.fingerprint"], "ddtrace:ValueError");
    }

    #[test]
    fn decodes_v05_dictionary_spans() {
        let dictionary: Vec<String> = vec![
            "".into(),
            "checkout".into(),
            "web.request".into(),
            "GET /orders".into(),
            "env".into(),
            "test".into(),
            "process_id".into(),
            "web".into(),
        ];
        let encoded = rmp_serde::to_vec(&(
            dictionary,
            vec![vec![V05Span(
                1,
                2,
                3,
                0x12,
                0x34,
                0,
                1_700_000_000_000_000_000,
                2_000_000,
                0,
                BTreeMap::from([(4, 5)]),
                BTreeMap::from([(6, 42.0)]),
                7,
            )]],
        ))
        .unwrap();
        let (dictionary, traces): (Vec<String>, Vec<Vec<V05Span>>) =
            rmp_serde::from_slice(&encoded).unwrap();
        let traces = traces
            .into_iter()
            .map(|trace| {
                trace
                    .into_iter()
                    .map(|span| span.expand(&dictionary).unwrap())
                    .collect()
            })
            .collect();
        let batch = parse_traces(7, traces);
        assert_eq!(batch.records.len(), 1);
        assert_eq!(batch.records[0].fields["attribute.env"], "test");
        assert_eq!(batch.records[0].fields["attribute.process_id"], 42.0);
    }

    #[test]
    fn invalid_spans_are_counted() {
        let mut invalid = span();
        invalid.span_id = 0;
        let batch = parse_traces(7, vec![vec![invalid]]);
        assert!(batch.records.is_empty());
        assert_eq!(batch.skipped["ddtrace.invalid_span"], 1);
    }

    #[tokio::test]
    async fn router_authenticates_and_ingests_v04() {
        let directory = tempdir().unwrap();
        let store = Store::open(&directory.path().join("db")).unwrap();
        let project = store.add_project("test").unwrap();
        let process = Arc::new(ProcessStats::default());
        let (changes, _) = tokio::sync::broadcast::channel(1);
        let (writer, writer_task) = crate::writer::IngestWriter::start(
            store.clone(),
            Durability::Safe,
            changes,
            process.clone(),
        );
        let app = router().with_state(AppState::for_test(store.clone(), writer.clone(), process));
        let body = rmp_serde::to_vec_named(&vec![vec![span()]]).unwrap();

        let forbidden = app
            .clone()
            .oneshot(
                Request::put(format!("/{}/wrong/v0.4/traces", project.id))
                    .header(header::CONTENT_TYPE, "application/msgpack")
                    .body(Body::from(body.clone()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(forbidden.status(), StatusCode::FORBIDDEN);

        let response = app
            .oneshot(
                Request::post(format!("/{}/{}/v0.4/traces", project.id, project.key))
                    .header(header::CONTENT_TYPE, "application/msgpack")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(store.status().unwrap().1, 2);

        writer.shutdown().await;
        writer_task.await.unwrap();
    }
}
