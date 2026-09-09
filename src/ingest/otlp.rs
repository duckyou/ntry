use std::{collections::BTreeMap, io::Read, sync::atomic::Ordering};

use anyhow::{Result as AnyResult, bail};
use axum::{
    Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Path, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::post,
};
use flate2::read::GzDecoder;
use opentelemetry_proto::tonic::{
    collector::{
        logs::v1::{ExportLogsPartialSuccess, ExportLogsServiceRequest, ExportLogsServiceResponse},
        metrics::v1::{
            ExportMetricsPartialSuccess, ExportMetricsServiceRequest, ExportMetricsServiceResponse,
        },
        trace::v1::{
            ExportTracePartialSuccess, ExportTraceServiceRequest, ExportTraceServiceResponse,
        },
    },
    common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value},
    logs::v1::LogRecord,
    metrics::v1::{
        AggregationTemporality, Exemplar, ExponentialHistogramDataPoint, HistogramDataPoint,
        Metric, NumberDataPoint, SummaryDataPoint, exemplar, metric, number_data_point,
    },
    resource::v1::Resource,
    trace::v1::{Span, span, status},
};
use prost::Message;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{
    ingest::IngestBatch,
    model::{ProjectStatus, Signal, StoredRecord},
    server::{AppState, unix_time_ms},
    writer::SubmitError,
};

const MAX_REQUEST_BYTES: usize = 64 * 1024 * 1024;
const ID_NAMESPACE: Uuid = Uuid::from_u128(0x42f8d624_1587_49f8_a155_aa005376c405);

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route("/{project_id}/v1/traces", post(traces))
        .route("/{project_id}/v1/logs", post(logs))
        .route("/{project_id}/v1/metrics", post(metrics))
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BYTES))
}

async fn traces(
    State(state): State<AppState>,
    Path(project_id): Path<u64>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, Response> {
    authorize(&state, project_id, &headers).await?;
    let body = request_body(&headers, &body)?;
    let request = ExportTraceServiceRequest::decode(body.as_slice())
        .map_err(|error| bad_request(&state, format!("invalid trace protobuf: {error}")))?;
    let (batch, rejected) = parse_traces(
        project_id,
        unix_time_ms().map_err(IntoResponse::into_response)?,
        request,
    );
    submit(&state, batch).await?;
    Ok(protobuf(
        ExportTraceServiceResponse {
            partial_success: partial(rejected, "spans").map(|(count, error_message)| {
                ExportTracePartialSuccess {
                    rejected_spans: count,
                    error_message,
                }
            }),
        }
        .encode_to_vec(),
    ))
}

async fn logs(
    State(state): State<AppState>,
    Path(project_id): Path<u64>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, Response> {
    authorize(&state, project_id, &headers).await?;
    let body = request_body(&headers, &body)?;
    let request = ExportLogsServiceRequest::decode(body.as_slice())
        .map_err(|error| bad_request(&state, format!("invalid logs protobuf: {error}")))?;
    let (batch, rejected) = parse_logs(
        project_id,
        unix_time_ms().map_err(IntoResponse::into_response)?,
        request,
    );
    submit(&state, batch).await?;
    Ok(protobuf(
        ExportLogsServiceResponse {
            partial_success: partial(rejected, "log records").map(|(count, error_message)| {
                ExportLogsPartialSuccess {
                    rejected_log_records: count,
                    error_message,
                }
            }),
        }
        .encode_to_vec(),
    ))
}

async fn metrics(
    State(state): State<AppState>,
    Path(project_id): Path<u64>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, Response> {
    authorize(&state, project_id, &headers).await?;
    let body = request_body(&headers, &body)?;
    let request = ExportMetricsServiceRequest::decode(body.as_slice())
        .map_err(|error| bad_request(&state, format!("invalid metrics protobuf: {error}")))?;
    let (batch, rejected) = parse_metrics(
        project_id,
        unix_time_ms().map_err(IntoResponse::into_response)?,
        request,
    );
    submit(&state, batch).await?;
    Ok(protobuf(
        ExportMetricsServiceResponse {
            partial_success: partial(rejected, "data points").map(|(count, error_message)| {
                ExportMetricsPartialSuccess {
                    rejected_data_points: count,
                    error_message,
                }
            }),
        }
        .encode_to_vec(),
    ))
}

async fn authorize(state: &AppState, project_id: u64, headers: &HeaderMap) -> Result<(), Response> {
    if !headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/x-protobuf"))
    {
        return Err(error_response(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "Content-Type must be application/x-protobuf",
        ));
    }
    let mut authorization = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .into_iter()
        .flat_map(str::split_ascii_whitespace);
    let bearer = match (
        authorization.next(),
        authorization.next(),
        authorization.next(),
    ) {
        (Some(scheme), Some(key), None) if scheme.eq_ignore_ascii_case("bearer") => Some(key),
        _ => None,
    };
    let store = state.store.clone();
    let project = tokio::task::spawn_blocking(move || store.get_project(project_id))
        .await
        .map_err(internal)?
        .map_err(internal)?;
    if project.as_ref().is_none_or(|project| {
        project.status != ProjectStatus::Active || bearer != Some(project.key.as_str())
    }) {
        state.process.unauthorized.fetch_add(1, Ordering::Relaxed);
        state.writer.diagnostic("unauthorized");
        return Err(error_response(
            StatusCode::UNAUTHORIZED,
            "invalid project or bearer token",
        ));
    }
    Ok(())
}

#[allow(clippy::result_large_err)]
fn request_body(headers: &HeaderMap, body: &[u8]) -> Result<Vec<u8>, Response> {
    if body.len() > MAX_REQUEST_BYTES {
        return Err(error_response(
            StatusCode::PAYLOAD_TOO_LARGE,
            "request exceeds the 64 MiB limit",
        ));
    }
    let encoding = headers
        .get(header::CONTENT_ENCODING)
        .map(|value| value.to_str())
        .transpose()
        .map_err(|_| {
            error_response(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "Content-Encoding must be identity or gzip",
            )
        })?
        .unwrap_or("identity")
        .trim();
    if encoding.is_empty() || encoding.eq_ignore_ascii_case("identity") {
        Ok(body.to_vec())
    } else if encoding.eq_ignore_ascii_case("gzip") {
        let mut decoded = Vec::new();
        GzDecoder::new(body)
            .take(MAX_REQUEST_BYTES as u64 + 1)
            .read_to_end(&mut decoded)
            .map_err(|error| {
                error_response(
                    StatusCode::BAD_REQUEST,
                    format!("invalid gzip body: {error}"),
                )
            })?;
        if decoded.len() > MAX_REQUEST_BYTES {
            return Err(error_response(
                StatusCode::PAYLOAD_TOO_LARGE,
                "decompressed request exceeds the 64 MiB limit",
            ));
        }
        Ok(decoded)
    } else {
        Err(error_response(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "Content-Encoding must be identity or gzip",
        ))
    }
}

async fn submit(state: &AppState, batch: IngestBatch) -> Result<(), Response> {
    if batch.records.is_empty() && batch.skipped.is_empty() {
        return Ok(());
    }
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

fn protobuf(body: Vec<u8>) -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/x-protobuf")],
        body,
    )
        .into_response()
}

fn partial(rejected: usize, item: &str) -> Option<(i64, String)> {
    (rejected > 0).then(|| {
        (
            i64::try_from(rejected).unwrap_or(i64::MAX),
            format!("rejected {rejected} invalid {item}"),
        )
    })
}

fn bad_request(state: &AppState, message: String) -> Response {
    state.process.malformed.fetch_add(1, Ordering::Relaxed);
    state.writer.diagnostic("malformed");
    error_response(StatusCode::BAD_REQUEST, message)
}

fn internal(error: impl std::fmt::Display) -> Response {
    tracing::error!(%error, "OTLP request failed");
    error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal server error")
}

fn error_response(status: StatusCode, message: impl Into<String>) -> Response {
    (status, message.into()).into_response()
}

fn parse_traces(
    project_id: u64,
    received_at_ms: u64,
    request: ExportTraceServiceRequest,
) -> (IngestBatch, usize) {
    let mut records = Vec::new();
    let mut rejected = 0;
    for resource_spans in request.resource_spans {
        let resource = resource_spans.resource.as_ref();
        for scope_spans in resource_spans.scope_spans {
            let scope = scope_spans.scope.as_ref();
            for span in scope_spans.spans {
                match span_records(
                    project_id,
                    received_at_ms,
                    resource,
                    &resource_spans.schema_url,
                    scope,
                    &scope_spans.schema_url,
                    &span,
                ) {
                    Ok(mut parsed) => records.append(&mut parsed),
                    Err(_) => rejected += 1,
                }
            }
        }
    }
    (batch(records, rejected, "otlp.invalid_span"), rejected)
}

fn span_records(
    project_id: u64,
    received_at_ms: u64,
    resource: Option<&Resource>,
    resource_schema_url: &str,
    scope: Option<&InstrumentationScope>,
    scope_schema_url: &str,
    span: &Span,
) -> AnyResult<Vec<StoredRecord>> {
    validate_id(&span.trace_id, 16, "trace")?;
    validate_id(&span.span_id, 8, "span")?;
    if span.name.is_empty() {
        bail!("span name is empty");
    }
    if span.end_time_unix_nano < span.start_time_unix_nano {
        bail!("span ends before it starts");
    }
    if !span.parent_span_id.is_empty() {
        validate_id(&span.parent_span_id, 8, "parent span")?;
    }

    let trace_id = hex(&span.trace_id);
    let span_id = hex(&span.span_id);
    let source_id = format!("{trace_id}-{span_id}");
    let mut fields = context_fields(resource, resource_schema_url, scope, scope_schema_url);
    fields.insert("name".into(), json!(span.name));
    fields.insert("message".into(), json!(span.name));
    fields.insert("trace_id".into(), json!(trace_id));
    fields.insert("span_id".into(), json!(span_id));
    if !span.parent_span_id.is_empty() {
        fields.insert("parent_span_id".into(), json!(hex(&span.parent_span_id)));
    }
    fields.insert("span.kind".into(), json!(span_kind(span.kind)));
    fields.insert(
        "span.status".into(),
        json!(
            span.status
                .as_ref()
                .map_or("unset", |status| status_code(status.code))
        ),
    );
    if let Some(status) = &span.status
        && !status.message.is_empty()
    {
        fields.insert("span.status_message".into(), json!(status.message));
    }
    fields.insert(
        "duration_ns".into(),
        json!(span.end_time_unix_nano - span.start_time_unix_nano),
    );
    insert_attributes(&mut fields, "attribute.", &span.attributes);
    let raw = json!({
        "resource": resource_json(resource, resource_schema_url),
        "scope": scope_json(scope, scope_schema_url),
        "span": span_json(span),
    });
    let mut records = vec![StoredRecord {
        project_id,
        id: source_id.clone(),
        signal: Signal::Trace,
        source: "otlp".into(),
        source_id: Some(source_id.clone()),
        issue_id: None,
        timestamp_unix_nano: span.start_time_unix_nano,
        received_at_ms,
        fields,
        raw,
    }];

    for (index, event) in span.events.iter().enumerate() {
        let attributes = attributes_map(&event.attributes);
        if event.name == "exception" || attributes.contains_key("exception.type") {
            let error_type = attributes
                .get("exception.type")
                .and_then(Value::as_str)
                .unwrap_or("Exception");
            let error_value = attributes
                .get("exception.message")
                .and_then(Value::as_str)
                .unwrap_or("");
            let title = if error_value.is_empty() {
                error_type.to_owned()
            } else {
                format!("{error_type}: {error_value}")
            };
            let error_source_id = format!("{source_id}-exception-{}-{index}", event.time_unix_nano);
            let mut error_fields =
                context_fields(resource, resource_schema_url, scope, scope_schema_url);
            insert_attributes(&mut error_fields, "attribute.", &span.attributes);
            insert_attributes(&mut error_fields, "attribute.", &event.attributes);
            error_fields.extend([
                ("source_record_id".into(), json!(source_id)),
                ("trace_id".into(), json!(trace_id)),
                ("span_id".into(), json!(span_id)),
                ("error.type".into(), json!(error_type)),
                ("error.value".into(), json!(error_value)),
                ("level".into(), json!("error")),
                ("message".into(), json!(error_value)),
                ("title".into(), json!(title)),
                ("issue.title".into(), json!(title)),
                (
                    "issue.fingerprint".into(),
                    json!(format!("otlp:{error_type}")),
                ),
            ]);
            insert_exception_details(&mut error_fields, &attributes, error_type, error_value);
            if let Some(stacktrace) = attributes.get("exception.stacktrace") {
                error_fields.insert("error.stacktrace".into(), stacktrace.clone());
            }
            records.push(StoredRecord {
                project_id,
                id: error_source_id.clone(),
                signal: Signal::Error,
                source: "otlp".into(),
                source_id: Some(error_source_id),
                issue_id: None,
                timestamp_unix_nano: if event.time_unix_nano == 0 {
                    span.start_time_unix_nano
                } else {
                    event.time_unix_nano
                },
                received_at_ms,
                fields: error_fields,
                raw: json!({
                    "resource": resource_json(resource, resource_schema_url),
                    "scope": scope_json(scope, scope_schema_url),
                    "source_record_id": source_id,
                    "event": event_json(event),
                }),
            });
        }
    }
    Ok(records)
}

fn parse_logs(
    project_id: u64,
    received_at_ms: u64,
    request: ExportLogsServiceRequest,
) -> (IngestBatch, usize) {
    let mut records = Vec::new();
    let mut rejected = 0;
    for resource_logs in request.resource_logs {
        let resource = resource_logs.resource.as_ref();
        for scope_logs in resource_logs.scope_logs {
            let scope = scope_logs.scope.as_ref();
            for log in scope_logs.log_records {
                match log_records(
                    project_id,
                    received_at_ms,
                    resource,
                    &resource_logs.schema_url,
                    scope,
                    &scope_logs.schema_url,
                    &log,
                ) {
                    Ok(mut parsed) => records.append(&mut parsed),
                    Err(_) => rejected += 1,
                }
            }
        }
    }
    (batch(records, rejected, "otlp.invalid_log"), rejected)
}

fn log_records(
    project_id: u64,
    received_at_ms: u64,
    resource: Option<&Resource>,
    resource_schema_url: &str,
    scope: Option<&InstrumentationScope>,
    scope_schema_url: &str,
    log: &LogRecord,
) -> AnyResult<Vec<StoredRecord>> {
    if !log.trace_id.is_empty() {
        validate_id(&log.trace_id, 16, "trace")?;
    }
    if !log.span_id.is_empty() {
        validate_id(&log.span_id, 8, "span")?;
        if log.trace_id.is_empty() {
            bail!("log span ID has no trace ID");
        }
    }
    if !(0..=24).contains(&log.severity_number) {
        bail!("invalid log severity");
    }
    let timestamp = [log.time_unix_nano, log.observed_time_unix_nano]
        .into_iter()
        .find(|timestamp| *timestamp != 0)
        .unwrap_or(received_at_ms.saturating_mul(1_000_000));
    let message = log
        .body
        .as_ref()
        .map(any_value_json)
        .map_or_else(String::new, |value| {
            value
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| value.to_string())
        });
    let raw = json!({
        "resource": resource_json(resource, resource_schema_url),
        "scope": scope_json(scope, scope_schema_url),
        "log_record": log_json(log),
    });
    let source_id = stable_id(&raw);
    let mut fields = context_fields(resource, resource_schema_url, scope, scope_schema_url);
    fields.insert("message".into(), json!(message));
    fields.insert(
        "level".into(),
        json!(severity(log.severity_number, &log.severity_text)),
    );
    if !log.severity_text.is_empty() {
        fields.insert("severity_text".into(), json!(log.severity_text));
    }
    if !log.event_name.is_empty() {
        fields.insert("event.name".into(), json!(log.event_name));
    }
    if !log.trace_id.is_empty() {
        fields.insert("trace_id".into(), json!(hex(&log.trace_id)));
    }
    if !log.span_id.is_empty() {
        fields.insert("span_id".into(), json!(hex(&log.span_id)));
    }
    insert_attributes(&mut fields, "attribute.", &log.attributes);
    let mut records = vec![StoredRecord {
        project_id,
        id: source_id.clone(),
        signal: Signal::Log,
        source: "otlp".into(),
        source_id: Some(source_id.clone()),
        issue_id: None,
        timestamp_unix_nano: timestamp,
        received_at_ms,
        fields,
        raw: raw.clone(),
    }];
    let attributes = attributes_map(&log.attributes);
    if log.event_name == "exception" || attributes.contains_key("exception.type") {
        let error_type = attributes
            .get("exception.type")
            .and_then(Value::as_str)
            .unwrap_or("Exception");
        let error_value = attributes
            .get("exception.message")
            .and_then(Value::as_str)
            .unwrap_or("");
        let title = if error_value.is_empty() {
            error_type.to_owned()
        } else {
            format!("{error_type}: {error_value}")
        };
        let error_source_id = format!("{source_id}-exception");
        let mut error_fields =
            context_fields(resource, resource_schema_url, scope, scope_schema_url);
        insert_attributes(&mut error_fields, "attribute.", &log.attributes);
        error_fields.extend([
            ("source_record_id".into(), json!(source_id)),
            ("error.type".into(), json!(error_type)),
            ("error.value".into(), json!(error_value)),
            ("level".into(), json!("error")),
            ("message".into(), json!(error_value)),
            ("title".into(), json!(title)),
            ("issue.title".into(), json!(title)),
            (
                "issue.fingerprint".into(),
                json!(format!("otlp:{error_type}")),
            ),
        ]);
        insert_exception_details(&mut error_fields, &attributes, error_type, error_value);
        if !log.trace_id.is_empty() {
            error_fields.insert("trace_id".into(), json!(hex(&log.trace_id)));
        }
        if !log.span_id.is_empty() {
            error_fields.insert("span_id".into(), json!(hex(&log.span_id)));
        }
        if let Some(stacktrace) = attributes.get("exception.stacktrace") {
            error_fields.insert("error.stacktrace".into(), stacktrace.clone());
        }
        records.push(StoredRecord {
            project_id,
            id: error_source_id.clone(),
            signal: Signal::Error,
            source: "otlp".into(),
            source_id: Some(error_source_id),
            issue_id: None,
            timestamp_unix_nano: timestamp,
            received_at_ms,
            fields: error_fields,
            raw,
        });
    }
    Ok(records)
}

fn parse_metrics(
    project_id: u64,
    received_at_ms: u64,
    request: ExportMetricsServiceRequest,
) -> (IngestBatch, usize) {
    let mut records = Vec::new();
    let mut rejected = 0;
    for resource_metrics in request.resource_metrics {
        let resource = resource_metrics.resource.as_ref();
        for scope_metrics in resource_metrics.scope_metrics {
            let scope = scope_metrics.scope.as_ref();
            for metric in scope_metrics.metrics {
                metric_records(
                    project_id,
                    received_at_ms,
                    resource,
                    &resource_metrics.schema_url,
                    scope,
                    &scope_metrics.schema_url,
                    &metric,
                    &mut records,
                    &mut rejected,
                );
            }
        }
    }
    (
        batch(records, rejected, "otlp.invalid_metric_point"),
        rejected,
    )
}

#[allow(clippy::too_many_arguments)]
fn metric_records(
    project_id: u64,
    received_at_ms: u64,
    resource: Option<&Resource>,
    resource_schema_url: &str,
    scope: Option<&InstrumentationScope>,
    scope_schema_url: &str,
    metric: &Metric,
    records: &mut Vec<StoredRecord>,
    rejected: &mut usize,
) {
    let Some(data) = &metric.data else { return };
    match data {
        metric::Data::Gauge(data) => {
            for point in &data.data_points {
                push_metric(
                    project_id,
                    received_at_ms,
                    resource,
                    resource_schema_url,
                    scope,
                    scope_schema_url,
                    metric,
                    "gauge",
                    None,
                    None,
                    point.start_time_unix_nano,
                    point.time_unix_nano,
                    &point.attributes,
                    point.value.as_ref().and_then(number_value),
                    number_point_json(point),
                    records,
                    rejected,
                );
            }
        }
        metric::Data::Sum(data) => {
            for point in &data.data_points {
                push_metric(
                    project_id,
                    received_at_ms,
                    resource,
                    resource_schema_url,
                    scope,
                    scope_schema_url,
                    metric,
                    "sum",
                    Some(data.aggregation_temporality),
                    Some(data.is_monotonic),
                    point.start_time_unix_nano,
                    if valid_temporality(data.aggregation_temporality) {
                        point.time_unix_nano
                    } else {
                        0
                    },
                    &point.attributes,
                    point.value.as_ref().and_then(number_value),
                    number_point_json(point),
                    records,
                    rejected,
                );
            }
        }
        metric::Data::Histogram(data) => {
            for point in &data.data_points {
                let valid = valid_temporality(data.aggregation_temporality)
                    && (point.bucket_counts.is_empty() && point.explicit_bounds.is_empty()
                        || point.bucket_counts.len() == point.explicit_bounds.len() + 1
                            && point
                                .bucket_counts
                                .iter()
                                .fold(0_u64, |sum, count| sum.saturating_add(*count))
                                == point.count
                            && point.explicit_bounds.iter().all(|bound| bound.is_finite())
                            && point
                                .explicit_bounds
                                .windows(2)
                                .all(|pair| pair[0] < pair[1]));
                push_metric(
                    project_id,
                    received_at_ms,
                    resource,
                    resource_schema_url,
                    scope,
                    scope_schema_url,
                    metric,
                    "histogram",
                    Some(data.aggregation_temporality),
                    None,
                    point.start_time_unix_nano,
                    if valid { point.time_unix_nano } else { 0 },
                    &point.attributes,
                    None,
                    histogram_point_json(point),
                    records,
                    rejected,
                );
            }
        }
        metric::Data::ExponentialHistogram(data) => {
            for point in &data.data_points {
                let bucket_count = point
                    .positive
                    .iter()
                    .chain(point.negative.iter())
                    .flat_map(|buckets| &buckets.bucket_counts)
                    .fold(point.zero_count, |sum, count| sum.saturating_add(*count));
                push_metric(
                    project_id,
                    received_at_ms,
                    resource,
                    resource_schema_url,
                    scope,
                    scope_schema_url,
                    metric,
                    "exponential_histogram",
                    Some(data.aggregation_temporality),
                    None,
                    point.start_time_unix_nano,
                    if valid_temporality(data.aggregation_temporality)
                        && bucket_count == point.count
                    {
                        point.time_unix_nano
                    } else {
                        0
                    },
                    &point.attributes,
                    None,
                    exponential_histogram_point_json(point),
                    records,
                    rejected,
                );
            }
        }
        metric::Data::Summary(data) => {
            for point in &data.data_points {
                let valid = point.quantile_values.iter().all(|value| {
                    value.quantile.is_finite()
                        && (0.0..=1.0).contains(&value.quantile)
                        && value.value.is_finite()
                }) && point
                    .quantile_values
                    .windows(2)
                    .all(|pair| pair[0].quantile < pair[1].quantile);
                push_metric(
                    project_id,
                    received_at_ms,
                    resource,
                    resource_schema_url,
                    scope,
                    scope_schema_url,
                    metric,
                    "summary",
                    None,
                    None,
                    point.start_time_unix_nano,
                    if valid { point.time_unix_nano } else { 0 },
                    &point.attributes,
                    None,
                    summary_point_json(point),
                    records,
                    rejected,
                );
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn push_metric(
    project_id: u64,
    received_at_ms: u64,
    resource: Option<&Resource>,
    resource_schema_url: &str,
    scope: Option<&InstrumentationScope>,
    scope_schema_url: &str,
    metric: &Metric,
    metric_type: &str,
    temporality: Option<i32>,
    monotonic: Option<bool>,
    start_time: u64,
    time: u64,
    attributes: &[KeyValue],
    value: Option<f64>,
    point_raw: Value,
    records: &mut Vec<StoredRecord>,
    rejected: &mut usize,
) {
    if metric.name.is_empty()
        || time == 0
        || start_time > time
        || matches!(metric_type, "gauge" | "sum") && value.is_none_or(|value| !value.is_finite())
    {
        *rejected += 1;
        return;
    }
    let mut fields = context_fields(resource, resource_schema_url, scope, scope_schema_url);
    fields.insert("name".into(), json!(metric.name));
    fields.insert("type".into(), json!(metric_type));
    if !metric.unit.is_empty() {
        fields.insert("unit".into(), json!(metric.unit));
    }
    if !metric.description.is_empty() {
        fields.insert("description".into(), json!(metric.description));
    }
    if start_time != 0 {
        fields.insert("start_timestamp_unix_nano".into(), json!(start_time));
    }
    if let Some(temporality) = temporality {
        fields.insert("temporality".into(), json!(temporality_name(temporality)));
    }
    if let Some(monotonic) = monotonic {
        fields.insert("monotonic".into(), json!(monotonic));
    }
    if let Some(value) = value {
        fields.insert("value".into(), finite_number(value));
    }
    insert_attributes(&mut fields, "attribute.", attributes);

    let stream_identity = json!({
        "name": metric.name,
        "type": metric_type,
        "unit": metric.unit,
        "temporality": temporality,
        "monotonic": monotonic,
        "resource": resource_json(resource, resource_schema_url),
        "scope": scope_json(scope, scope_schema_url),
        "attributes": attributes_json(attributes),
    });
    let stream_id = stable_id(&stream_identity);
    let source_id = format!("{stream_id}-{metric_type}-{start_time}-{time}");
    fields.insert("stream_id".into(), json!(stream_id));
    records.push(StoredRecord {
        project_id,
        id: source_id.clone(),
        signal: Signal::Metric,
        source: "otlp".into(),
        source_id: Some(source_id),
        issue_id: None,
        timestamp_unix_nano: time,
        received_at_ms,
        fields,
        raw: json!({
            "resource": resource_json(resource, resource_schema_url),
            "scope": scope_json(scope, scope_schema_url),
            "metric": {
                "name": metric.name,
                "description": metric.description,
                "unit": metric.unit,
                "metadata": attributes_json(&metric.metadata),
                "type": metric_type,
                "temporality": temporality.map(temporality_name),
                "monotonic": monotonic,
            },
            "data_point": point_raw,
        }),
    });
}

fn batch(records: Vec<StoredRecord>, rejected: usize, reason: &str) -> IngestBatch {
    IngestBatch {
        records,
        skipped: if rejected > 0 {
            BTreeMap::from([(reason.to_owned(), rejected as u64)])
        } else {
            BTreeMap::new()
        },
    }
}

fn context_fields(
    resource: Option<&Resource>,
    resource_schema_url: &str,
    scope: Option<&InstrumentationScope>,
    scope_schema_url: &str,
) -> BTreeMap<String, Value> {
    let mut fields = BTreeMap::new();
    if let Some(resource) = resource {
        insert_attributes(&mut fields, "resource.", &resource.attributes);
    }
    if !resource_schema_url.is_empty() {
        fields.insert("resource.schema_url".into(), json!(resource_schema_url));
    }
    if let Some(scope) = scope {
        if !scope.name.is_empty() {
            fields.insert("scope.name".into(), json!(scope.name));
        }
        if !scope.version.is_empty() {
            fields.insert("scope.version".into(), json!(scope.version));
        }
        insert_attributes(&mut fields, "scope.attribute.", &scope.attributes);
    }
    if !scope_schema_url.is_empty() {
        fields.insert("scope.schema_url".into(), json!(scope_schema_url));
    }
    fields
}

fn insert_attributes(fields: &mut BTreeMap<String, Value>, prefix: &str, attributes: &[KeyValue]) {
    for attribute in attributes {
        if !attribute.key.is_empty() {
            fields.insert(
                format!("{prefix}{}", attribute.key),
                attribute
                    .value
                    .as_ref()
                    .map(any_value_json)
                    .unwrap_or(Value::Null),
            );
        }
    }
}

fn attributes_map(attributes: &[KeyValue]) -> BTreeMap<String, Value> {
    attributes
        .iter()
        .filter(|attribute| !attribute.key.is_empty())
        .map(|attribute| {
            (
                attribute.key.clone(),
                attribute
                    .value
                    .as_ref()
                    .map(any_value_json)
                    .unwrap_or(Value::Null),
            )
        })
        .collect()
}

fn insert_exception_details(
    fields: &mut BTreeMap<String, Value>,
    attributes: &BTreeMap<String, Value>,
    error_type: &str,
    error_message: &str,
) {
    let mut exception = json!({
        "type": error_type,
        "message": error_message,
    });
    if let Some(stacktrace) = attributes
        .get("exception.stacktrace")
        .and_then(Value::as_str)
    {
        exception["stacktrace"] = json!(stacktrace);
    }
    if let Some(escaped) = attributes.get("exception.escaped").and_then(Value::as_bool) {
        fields.insert("error.handled".into(), json!(!escaped));
        exception["handled"] = json!(!escaped);
    }
    fields.insert("error.exceptions".into(), json!([exception]));
}

fn attributes_json(attributes: &[KeyValue]) -> Value {
    Value::Array(
        attributes
            .iter()
            .map(|attribute| {
                json!({
                    "key": attribute.key,
                    "value": attribute.value.as_ref().map(any_value_json).unwrap_or(Value::Null),
                })
            })
            .collect(),
    )
}

fn any_value_json(value: &AnyValue) -> Value {
    match &value.value {
        Some(any_value::Value::StringValue(value)) => json!(value),
        Some(any_value::Value::BoolValue(value)) => json!(value),
        Some(any_value::Value::IntValue(value)) => json!(value),
        Some(any_value::Value::DoubleValue(value)) => finite_number(*value),
        Some(any_value::Value::ArrayValue(value)) => {
            Value::Array(value.values.iter().map(any_value_json).collect())
        }
        Some(any_value::Value::KvlistValue(value)) => attributes_json(&value.values),
        Some(any_value::Value::BytesValue(value)) => json!(hex(value)),
        Some(any_value::Value::StringValueStrindex(value)) => {
            json!({ "string_value_strindex": value })
        }
        None => Value::Null,
    }
}

fn resource_json(resource: Option<&Resource>, schema_url: &str) -> Value {
    json!({
        "attributes": resource.map(|resource| attributes_json(&resource.attributes)),
        "dropped_attributes_count": resource.map(|resource| resource.dropped_attributes_count),
        "entity_refs": resource.map(|resource| resource.entity_refs.iter().map(|entity| json!({
                "schema_url": entity.schema_url,
                "type": entity.r#type,
                "id_keys": entity.id_keys,
                "description_keys": entity.description_keys,
            })).collect::<Vec<_>>()),
        "schema_url": schema_url,
    })
}

fn scope_json(scope: Option<&InstrumentationScope>, schema_url: &str) -> Value {
    json!({
        "name": scope.map(|scope| &scope.name),
        "version": scope.map(|scope| &scope.version),
        "attributes": scope.map(|scope| attributes_json(&scope.attributes)),
        "dropped_attributes_count": scope.map(|scope| scope.dropped_attributes_count),
        "schema_url": schema_url,
    })
}

fn span_json(span: &Span) -> Value {
    json!({
        "trace_id": hex(&span.trace_id),
        "span_id": hex(&span.span_id),
        "trace_state": span.trace_state,
        "parent_span_id": hex(&span.parent_span_id),
        "flags": span.flags,
        "name": span.name,
        "kind": span.kind,
        "kind_name": span_kind(span.kind),
        "start_time_unix_nano": span.start_time_unix_nano,
        "end_time_unix_nano": span.end_time_unix_nano,
        "attributes": attributes_json(&span.attributes),
        "dropped_attributes_count": span.dropped_attributes_count,
        "events": span.events.iter().map(event_json).collect::<Vec<_>>(),
        "dropped_events_count": span.dropped_events_count,
        "links": span.links.iter().map(|link| json!({
            "trace_id": hex(&link.trace_id),
            "span_id": hex(&link.span_id),
            "trace_state": link.trace_state,
            "attributes": attributes_json(&link.attributes),
            "dropped_attributes_count": link.dropped_attributes_count,
            "flags": link.flags,
        })).collect::<Vec<_>>(),
        "dropped_links_count": span.dropped_links_count,
        "status": span.status.as_ref().map(|status| json!({
            "code": status.code,
            "code_name": status_code(status.code),
            "message": status.message,
        })),
    })
}

fn event_json(event: &span::Event) -> Value {
    json!({
        "time_unix_nano": event.time_unix_nano,
        "name": event.name,
        "attributes": attributes_json(&event.attributes),
        "dropped_attributes_count": event.dropped_attributes_count,
    })
}

fn log_json(log: &LogRecord) -> Value {
    json!({
        "time_unix_nano": log.time_unix_nano,
        "observed_time_unix_nano": log.observed_time_unix_nano,
        "severity_number": log.severity_number,
        "severity_text": log.severity_text,
        "body": log.body.as_ref().map(any_value_json),
        "attributes": attributes_json(&log.attributes),
        "dropped_attributes_count": log.dropped_attributes_count,
        "flags": log.flags,
        "trace_id": hex(&log.trace_id),
        "span_id": hex(&log.span_id),
        "event_name": log.event_name,
    })
}

fn number_point_json(point: &NumberDataPoint) -> Value {
    json!({
        "attributes": attributes_json(&point.attributes),
        "start_time_unix_nano": point.start_time_unix_nano,
        "time_unix_nano": point.time_unix_nano,
        "exemplars": point.exemplars.iter().map(exemplar_json).collect::<Vec<_>>(),
        "flags": point.flags,
        "value": point.value.as_ref().map(number_value_json),
    })
}

fn histogram_point_json(point: &HistogramDataPoint) -> Value {
    json!({
        "attributes": attributes_json(&point.attributes),
        "start_time_unix_nano": point.start_time_unix_nano,
        "time_unix_nano": point.time_unix_nano,
        "count": point.count,
        "sum": point.sum.map(finite_number),
        "bucket_counts": point.bucket_counts,
        "explicit_bounds": point.explicit_bounds.iter().map(|value| finite_number(*value)).collect::<Vec<_>>(),
        "exemplars": point.exemplars.iter().map(exemplar_json).collect::<Vec<_>>(),
        "flags": point.flags,
        "min": point.min.map(finite_number),
        "max": point.max.map(finite_number),
    })
}

fn exponential_histogram_point_json(point: &ExponentialHistogramDataPoint) -> Value {
    json!({
        "attributes": attributes_json(&point.attributes),
        "start_time_unix_nano": point.start_time_unix_nano,
        "time_unix_nano": point.time_unix_nano,
        "count": point.count,
        "sum": point.sum.map(finite_number),
        "scale": point.scale,
        "zero_count": point.zero_count,
        "positive": point.positive.as_ref().map(|buckets| json!({
            "offset": buckets.offset,
            "bucket_counts": buckets.bucket_counts,
        })),
        "negative": point.negative.as_ref().map(|buckets| json!({
            "offset": buckets.offset,
            "bucket_counts": buckets.bucket_counts,
        })),
        "flags": point.flags,
        "exemplars": point.exemplars.iter().map(exemplar_json).collect::<Vec<_>>(),
        "min": point.min.map(finite_number),
        "max": point.max.map(finite_number),
        "zero_threshold": finite_number(point.zero_threshold),
    })
}

fn summary_point_json(point: &SummaryDataPoint) -> Value {
    json!({
        "attributes": attributes_json(&point.attributes),
        "start_time_unix_nano": point.start_time_unix_nano,
        "time_unix_nano": point.time_unix_nano,
        "count": point.count,
        "sum": finite_number(point.sum),
        "quantile_values": point.quantile_values.iter().map(|value| json!({
            "quantile": finite_number(value.quantile),
            "value": finite_number(value.value),
        })).collect::<Vec<_>>(),
        "flags": point.flags,
    })
}

fn exemplar_json(exemplar: &Exemplar) -> Value {
    json!({
        "filtered_attributes": attributes_json(&exemplar.filtered_attributes),
        "time_unix_nano": exemplar.time_unix_nano,
        "span_id": hex(&exemplar.span_id),
        "trace_id": hex(&exemplar.trace_id),
        "value": exemplar.value.as_ref().map(|value| match value {
            exemplar::Value::AsDouble(value) => finite_number(*value),
            exemplar::Value::AsInt(value) => json!(value),
        }),
    })
}

fn number_value(value: &number_data_point::Value) -> Option<f64> {
    Some(match value {
        number_data_point::Value::AsDouble(value) => *value,
        number_data_point::Value::AsInt(value) => *value as f64,
    })
}

fn number_value_json(value: &number_data_point::Value) -> Value {
    match value {
        number_data_point::Value::AsDouble(value) => finite_number(*value),
        number_data_point::Value::AsInt(value) => json!(value),
    }
}

fn finite_number(value: f64) -> Value {
    serde_json::Number::from_f64(value).map_or_else(
        || {
            json!(if value.is_nan() {
                "NaN"
            } else if value.is_sign_positive() {
                "Infinity"
            } else {
                "-Infinity"
            })
        },
        Value::Number,
    )
}

fn validate_id(id: &[u8], length: usize, name: &str) -> AnyResult<()> {
    if id.len() != length || id.iter().all(|byte| *byte == 0) {
        bail!("invalid {name} ID");
    }
    Ok(())
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(DIGITS[(byte >> 4) as usize] as char);
        output.push(DIGITS[(byte & 0xf) as usize] as char);
    }
    output
}

fn stable_id(value: &Value) -> String {
    Uuid::new_v5(
        &ID_NAMESPACE,
        &serde_json::to_vec(value).expect("JSON values serialize"),
    )
    .simple()
    .to_string()
}

fn span_kind(kind: i32) -> &'static str {
    match kind {
        1 => "internal",
        2 => "server",
        3 => "client",
        4 => "producer",
        5 => "consumer",
        _ => "unspecified",
    }
}

fn status_code(code: i32) -> &'static str {
    match code {
        value if value == status::StatusCode::Ok as i32 => "ok",
        value if value == status::StatusCode::Error as i32 => "error",
        _ => "unset",
    }
}

fn severity(number: i32, text: &str) -> &'static str {
    match number {
        1..=4 => "trace",
        5..=8 => "debug",
        13..=16 => "warn",
        17..=20 => "error",
        21..=24 => "fatal",
        9..=12 => "info",
        _ if text.eq_ignore_ascii_case("trace") => "trace",
        _ if text.eq_ignore_ascii_case("debug") => "debug",
        _ if text.eq_ignore_ascii_case("warn") || text.eq_ignore_ascii_case("warning") => "warn",
        _ if text.eq_ignore_ascii_case("error") => "error",
        _ if text.eq_ignore_ascii_case("fatal") => "fatal",
        _ => "info",
    }
}

fn temporality_name(temporality: i32) -> &'static str {
    match temporality {
        value if value == AggregationTemporality::Delta as i32 => "delta",
        value if value == AggregationTemporality::Cumulative as i32 => "cumulative",
        _ => "unspecified",
    }
}

fn valid_temporality(temporality: i32) -> bool {
    matches!(temporality, 1 | 2)
}

#[cfg(test)]
mod tests {
    use std::{io::Write, sync::Arc};

    use axum::{
        body::{Body, to_bytes},
        http::Request,
    };
    use flate2::{Compression, write::GzEncoder};
    use opentelemetry_proto::tonic::{
        common::v1::{AnyValue, KeyValue, any_value},
        logs::v1::{LogRecord, ResourceLogs, ScopeLogs, SeverityNumber},
        metrics::v1::{
            ExponentialHistogram, Gauge, Histogram, NumberDataPoint, ResourceMetrics, ScopeMetrics,
            Sum, Summary, SummaryDataPoint, exponential_histogram_data_point, metric,
            number_data_point,
        },
        trace::v1::{ResourceSpans, ScopeSpans, Span, span},
    };
    use tempfile::tempdir;
    use tower::ServiceExt;

    use super::*;
    use crate::{model::ProcessStats, store::Store, writer::Durability};

    fn attribute(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.into(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(value.into())),
            }),
            ..Default::default()
        }
    }

    #[test]
    fn request_body_accepts_gzip_and_enforces_limit() {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(b"protobuf").unwrap();
        let compressed = encoder.finish().unwrap();
        let headers = HeaderMap::from_iter([(header::CONTENT_ENCODING, "gzip".parse().unwrap())]);
        assert_eq!(request_body(&headers, &compressed).unwrap(), b"protobuf");

        let response = request_body(&HeaderMap::new(), &vec![0; MAX_REQUEST_BYTES + 1])
            .expect_err("oversized body must be rejected");
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[test]
    fn raw_attributes_preserve_order_and_duplicates() {
        assert_eq!(
            attributes_json(&[attribute("same", "one"), attribute("same", "two")]),
            json!([
                {"key": "same", "value": "one"},
                {"key": "same", "value": "two"}
            ])
        );
    }

    #[test]
    fn trace_and_log_exceptions_create_errors() {
        let trace = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                scope_spans: vec![ScopeSpans {
                    spans: vec![Span {
                        trace_id: vec![1; 16],
                        span_id: vec![2; 8],
                        name: "GET /".into(),
                        start_time_unix_nano: 10,
                        end_time_unix_nano: 20,
                        attributes: vec![attribute("http.route", "/")],
                        events: vec![span::Event {
                            time_unix_nano: 15,
                            name: "exception".into(),
                            attributes: vec![
                                attribute("exception.type", "Boom"),
                                attribute("exception.message", "span failed"),
                                attribute("exception.stacktrace", "span stack"),
                                attribute("code.function.name", "handler"),
                                KeyValue {
                                    key: "exception.escaped".into(),
                                    value: Some(AnyValue {
                                        value: Some(any_value::Value::BoolValue(true)),
                                    }),
                                    ..Default::default()
                                },
                            ],
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let (batch, rejected) = parse_traces(7, 1, trace);
        assert_eq!(rejected, 0);
        assert_eq!(batch.records.len(), 2);
        assert_eq!(batch.records[0].signal, Signal::Trace);
        let source = &batch.records[0];
        let error = &batch.records[1];
        assert_eq!(error.signal, Signal::Error);
        assert_eq!(error.fields["title"], "Boom: span failed");
        assert_eq!(error.fields["source_record_id"], source.id);
        assert_eq!(error.fields["trace_id"], hex(&[1; 16]));
        assert_eq!(error.fields["span_id"], hex(&[2; 8]));
        assert_eq!(error.fields["error.type"], "Boom");
        assert_eq!(error.fields["error.value"], "span failed");
        assert_eq!(error.fields["error.handled"], false);
        assert_eq!(error.fields["error.stacktrace"], "span stack");
        assert_eq!(
            error.fields["error.exceptions"],
            json!([{
                "type": "Boom",
                "message": "span failed",
                "stacktrace": "span stack",
                "handled": false,
            }])
        );
        assert_eq!(error.fields["attribute.http.route"], "/");
        assert_eq!(error.fields["attribute.code.function.name"], "handler");
        assert_eq!(error.fields["attribute.exception.escaped"], true);
        assert_eq!(error.fields["issue.fingerprint"], "otlp:Boom");
        assert_eq!(error.fields["message"], "span failed");

        let log = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        time_unix_nano: 10,
                        severity_number: SeverityNumber::Error as i32,
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("failed".into())),
                        }),
                        event_name: "exception".into(),
                        trace_id: vec![3; 16],
                        span_id: vec![4; 8],
                        attributes: vec![
                            attribute("exception.message", "log failed"),
                            attribute("exception.stacktrace", "log stack"),
                            attribute("log.source", "worker"),
                            KeyValue {
                                key: "exception.escaped".into(),
                                value: Some(AnyValue {
                                    value: Some(any_value::Value::BoolValue(false)),
                                }),
                                ..Default::default()
                            },
                        ],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let (batch, rejected) = parse_logs(7, 1, log);
        assert_eq!(rejected, 0);
        assert_eq!(batch.records.len(), 2);
        assert_eq!(batch.records[0].fields["level"], "error");
        let source = &batch.records[0];
        let error = &batch.records[1];
        assert_eq!(error.signal, Signal::Error);
        assert_eq!(error.fields["title"], "Exception: log failed");
        assert_eq!(error.fields["source_record_id"], source.id);
        assert_eq!(error.fields["trace_id"], hex(&[3; 16]));
        assert_eq!(error.fields["span_id"], hex(&[4; 8]));
        assert_eq!(error.fields["error.type"], "Exception");
        assert_eq!(error.fields["error.value"], "log failed");
        assert_eq!(error.fields["error.handled"], true);
        assert_eq!(error.fields["error.stacktrace"], "log stack");
        assert_eq!(
            error.fields["error.exceptions"],
            json!([{
                "type": "Exception",
                "message": "log failed",
                "stacktrace": "log stack",
                "handled": true,
            }])
        );
        assert_eq!(error.fields["attribute.log.source"], "worker");
        assert_eq!(error.fields["attribute.exception.escaped"], false);
        assert_eq!(error.fields["issue.fingerprint"], "otlp:Exception");
        assert_eq!(error.fields["message"], "log failed");
    }

    #[test]
    fn parses_all_metric_point_kinds_and_rejects_invalid_point() {
        let number = |value| NumberDataPoint {
            time_unix_nano: 10,
            value: Some(number_data_point::Value::AsDouble(value)),
            ..Default::default()
        };
        let metrics = vec![
            Metric {
                name: "g".into(),
                data: Some(metric::Data::Gauge(Gauge {
                    data_points: vec![number(1.0), NumberDataPoint::default()],
                })),
                ..Default::default()
            },
            Metric {
                name: "s".into(),
                data: Some(metric::Data::Sum(Sum {
                    data_points: vec![number(2.0)],
                    aggregation_temporality: AggregationTemporality::Delta as i32,
                    is_monotonic: true,
                })),
                ..Default::default()
            },
            Metric {
                name: "h".into(),
                data: Some(metric::Data::Histogram(Histogram {
                    data_points: vec![HistogramDataPoint {
                        time_unix_nano: 10,
                        count: 1,
                        bucket_counts: vec![0, 1],
                        explicit_bounds: vec![1.0],
                        ..Default::default()
                    }],
                    aggregation_temporality: AggregationTemporality::Cumulative as i32,
                })),
                ..Default::default()
            },
            Metric {
                name: "eh".into(),
                data: Some(metric::Data::ExponentialHistogram(ExponentialHistogram {
                    data_points: vec![ExponentialHistogramDataPoint {
                        time_unix_nano: 10,
                        count: 1,
                        positive: Some(exponential_histogram_data_point::Buckets {
                            bucket_counts: vec![1],
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                    aggregation_temporality: AggregationTemporality::Delta as i32,
                })),
                ..Default::default()
            },
            Metric {
                name: "summary".into(),
                data: Some(metric::Data::Summary(Summary {
                    data_points: vec![SummaryDataPoint {
                        time_unix_nano: 10,
                        count: 1,
                        sum: 1.0,
                        ..Default::default()
                    }],
                })),
                ..Default::default()
            },
        ];
        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics,
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let (batch, rejected) = parse_metrics(7, 1, request);
        assert_eq!(rejected, 1);
        assert_eq!(batch.records.len(), 5);
        assert_eq!(
            batch
                .records
                .iter()
                .map(|record| record.fields["type"].as_str().unwrap())
                .collect::<Vec<_>>(),
            [
                "gauge",
                "sum",
                "histogram",
                "exponential_histogram",
                "summary"
            ]
        );
    }

    #[test]
    fn protobuf_requests_round_trip() {
        let request = ExportTraceServiceRequest::default();
        let decoded =
            ExportTraceServiceRequest::decode(request.encode_to_vec().as_slice()).unwrap();
        assert!(decoded.resource_spans.is_empty());
    }

    #[tokio::test]
    async fn router_requires_matching_bearer_and_returns_partial_success() {
        let directory = tempdir().unwrap();
        let store = Store::open(&directory.path().join("db")).unwrap();
        let project = store.add_project("test").unwrap();
        let process = Arc::new(ProcessStats::default());
        let (writer, writer_task) = crate::writer::IngestWriter::start(
            store.clone(),
            Durability::Safe,
            {
                let (changes, _) = tokio::sync::broadcast::channel(1);
                changes
            },
            process.clone(),
        );
        let state = AppState::for_test(store, writer.clone(), process);
        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        name: "invalid".into(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint::default()],
                        })),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
        .encode_to_vec();

        let response = router()
            .with_state(state.clone())
            .oneshot(
                Request::post(format!("/{}/v1/metrics", project.id + 1))
                    .header(header::CONTENT_TYPE, "application/x-protobuf")
                    .header(header::AUTHORIZATION, format!("Bearer {}", project.key))
                    .body(Body::from(request.clone()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let response = router()
            .with_state(state)
            .oneshot(
                Request::post(format!("/{}/v1/metrics", project.id))
                    .header(header::CONTENT_TYPE, "application/x-protobuf")
                    .header(header::AUTHORIZATION, format!("Bearer {}", project.key))
                    .body(Body::from(request))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 1024).await.unwrap();
        let response = ExportMetricsServiceResponse::decode(body).unwrap();
        assert_eq!(response.partial_success.unwrap().rejected_data_points, 1);

        writer.shutdown().await;
        writer_task.await.unwrap();
    }
}
