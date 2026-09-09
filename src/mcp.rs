use std::time::UNIX_EPOCH;

use anyhow::{Context, Result, bail};
use axum::{
    Router,
    extract::Request,
    http::{HeaderMap, StatusCode, header},
    middleware::{self, Next},
    response::Response,
};
use rmcp::{
    ServiceExt,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::CallToolResult,
    schemars,
    schemars::JsonSchema,
    tool, tool_handler, tool_router,
    transport::{
        stdio,
        streamable_http_server::{
            StreamableHttpServerConfig, session::local::LocalSessionManager,
            tower::StreamableHttpService,
        },
    },
};
use serde::Deserialize;
use serde_json::{Value, json};
use url::Url;

use crate::{
    client::Client,
    model::{MetricQueryRequest, Signal, StoredRecord},
    query,
};

const MAX_SEARCH_RESULTS: usize = 100;
const MAX_METRIC_POINTS: usize = 10_000;
const MAX_RAW_BYTES: usize = 64 * 1_024;

#[derive(Clone)]
struct NtryMcp {
    client: Client,
    tool_router: ToolRouter<Self>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct SearchInput {
    /// Project name or numeric ID. May be omitted when exactly one project exists.
    project: Option<String>,
    /// One of: issues, errors, logs, traces.
    #[serde(default = "default_search_signal")]
    signal: String,
    /// Sentry-style search expression.
    #[serde(default)]
    query: String,
    /// Maximum results, from 1 to 100. Defaults to 20.
    limit: Option<usize>,
    /// Relative duration such as 24h. Defaults to 1h and conflicts with start_ms.
    since: Option<String>,
    /// Inclusive Unix timestamp in milliseconds. Conflicts with since.
    start_ms: Option<u64>,
    /// Inclusive Unix timestamp in milliseconds. Defaults to now.
    end_ms: Option<u64>,
    /// Include the original item JSON.
    #[serde(default)]
    raw: bool,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct GetInput {
    /// Ntry record or issue ID.
    id: String,
    /// Project name or numeric ID. May be omitted when exactly one project exists.
    project: Option<String>,
    /// Include the original item JSON.
    #[serde(default)]
    raw: bool,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct MetricInput {
    /// Project name or numeric ID. May be omitted when exactly one project exists.
    project: Option<String>,
    name: String,
    aggregate: String,
    #[serde(default)]
    query: String,
    #[serde(default)]
    group_by: Vec<String>,
    #[serde(rename = "type")]
    metric_type: Option<String>,
    unit: Option<String>,
    start_ms: Option<u64>,
    end_ms: Option<u64>,
    interval_ms: Option<u64>,
    group_limit: Option<usize>,
}

#[tool_router(router = tool_router)]
impl NtryMcp {
    fn new(client: Client) -> Self {
        Self {
            client,
            tool_router: Self::tool_router(),
        }
    }

    #[tool(description = "Search Ntry issues, errors, logs, or traces")]
    async fn search(&self, Parameters(input): Parameters<SearchInput>) -> CallToolResult {
        tool_result(
            async {
                let project = self
                    .client
                    .resolve_project(input.project.as_deref())
                    .await?;
                let limit = search_limit(input.limit)?;
                let (start_ms, end_ms) =
                    search_time_range(input.since.as_deref(), input.start_ms, input.end_ms)?;
                let value = match input.signal.as_str() {
                    "issue" | "issues" => json!({
                        "project": project.name,
                        "start_ms": start_ms,
                        "end_ms": end_ms,
                        "results": self.client.search_issues(
                            project.id, &input.query, start_ms, end_ms, limit,
                        ).await?,
                    }),
                    "error" | "errors" | "log" | "logs" | "trace" | "traces" => {
                        let signal = if input.signal.starts_with("error") {
                            Signal::Error
                        } else if input.signal.starts_with("trace") {
                            Signal::Trace
                        } else {
                            Signal::Log
                        };
                        let records = self
                            .client
                            .search_records(
                                project.id,
                                signal,
                                &input.query,
                                start_ms,
                                end_ms,
                                limit,
                            )
                            .await?;
                        json!({
                            "project": project.name,
                            "start_ms": start_ms,
                            "end_ms": end_ms,
                            "results": records.into_iter()
                                .map(|record| record_json(record, input.raw))
                                .collect::<Vec<_>>(),
                        })
                    }
                    _ => bail!("signal must be issues, errors, logs, or traces"),
                };
                Ok(value)
            }
            .await,
        )
    }

    #[tool(description = "Get one Ntry issue, error, log, or trace by ID")]
    async fn get(&self, Parameters(input): Parameters<GetInput>) -> CallToolResult {
        tool_result(
            async {
                let project = self
                    .client
                    .resolve_project(input.project.as_deref())
                    .await?;
                if let Some(record) = self.client.find_record(project.id, &input.id).await? {
                    return Ok(json!({
                        "project": project.name,
                        "record": record_json(record, input.raw),
                    }));
                }
                let query = format!("issue:{}", serde_json::to_string(&input.id)?);
                let issue = self
                    .client
                    .search_issues(project.id, &query, 0, u64::MAX, 1)
                    .await?
                    .into_iter()
                    .find(|issue| issue.id == input.id)
                    .with_context(|| format!("record or issue {:?} not found", input.id))?;
                let mut record = match issue.latest_record_id.as_deref() {
                    Some(id) => self.client.find_record(project.id, id).await?,
                    None => None,
                };
                if record.is_none() && issue.retained_events > 0 {
                    record = self
                        .client
                        .search_records(project.id, Signal::Error, &query, 0, u64::MAX, 1)
                        .await?
                        .into_iter()
                        .next();
                }
                Ok(json!({
                    "project": project.name,
                    "issue": issue,
                    "record": record.map(|record| record_json(record, input.raw)),
                }))
            }
            .await,
        )
    }

    #[tool(description = "Query and aggregate Ntry metrics")]
    async fn query_metrics(&self, Parameters(input): Parameters<MetricInput>) -> CallToolResult {
        tool_result(
            async {
                let project = self
                    .client
                    .resolve_project(input.project.as_deref())
                    .await?;
                let request = MetricQueryRequest {
                    project_id: project.id,
                    name: input.name,
                    aggregate: input.aggregate,
                    query: input.query,
                    group_by: input.group_by,
                    metric_type: input.metric_type,
                    unit: input.unit,
                    start_ms: input.start_ms,
                    end_ms: input.end_ms,
                    interval_ms: input.interval_ms,
                    group_limit: input.group_limit,
                };
                let mut response = self.client.metric_query(&request).await?;
                let original_points: usize = response
                    .series
                    .iter()
                    .map(|series| series.points.len())
                    .sum();
                let mut remaining = MAX_METRIC_POINTS;
                for series in &mut response.series {
                    series.points.truncate(remaining);
                    remaining = remaining.saturating_sub(series.points.len());
                }
                response.series.retain(|series| !series.points.is_empty());
                Ok(json!({
                    "project": project.name,
                    "result": response,
                    "truncated": original_points > MAX_METRIC_POINTS,
                }))
            }
            .await,
        )
    }

    #[tool(description = "Get Ntry daemon health and storage counters")]
    async fn status(&self) -> CallToolResult {
        tool_result(
            self.client
                .status()
                .await
                .map(|status| json!({ "status": status })),
        )
    }
}

#[tool_handler(router = self.tool_router)]
impl rmcp::ServerHandler for NtryMcp {}

pub async fn serve_stdio(client: Client) -> Result<()> {
    NtryMcp::new(client).serve(stdio()).await?.waiting().await?;
    Ok(())
}

pub fn http_router<S>(client: Client) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    let service = StreamableHttpService::new(
        move || Ok(NtryMcp::new(client.clone())),
        LocalSessionManager::default().into(),
        StreamableHttpServerConfig::default(),
    );
    Router::new()
        .nest_service("/mcp", service)
        .layer(middleware::from_fn(require_valid_origin))
}

async fn require_valid_origin(
    headers: HeaderMap,
    request: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    if !valid_origin(&headers) {
        return Err(StatusCode::FORBIDDEN);
    }
    Ok(next.run(request).await)
}

fn valid_origin(headers: &HeaderMap) -> bool {
    let Some(origin) = headers.get(header::ORIGIN) else {
        return true;
    };
    let (Ok(origin), Some(host)) = (
        origin
            .to_str()
            .ok()
            .and_then(|value| Url::parse(value).ok())
            .ok_or(()),
        headers
            .get(header::HOST)
            .and_then(|value| value.to_str().ok()),
    ) else {
        return false;
    };
    let Ok(expected) = Url::parse(&format!("http://{host}")) else {
        return false;
    };
    matches!(origin.scheme(), "http" | "https")
        && origin.username().is_empty()
        && origin.password().is_none()
        && origin.path() == "/"
        && origin.query().is_none()
        && origin.fragment().is_none()
        && origin.host_str() == expected.host_str()
        && origin.port_or_known_default() == expected.port_or_known_default()
}

fn tool_result(result: Result<Value>) -> CallToolResult {
    match result {
        Ok(value) => CallToolResult::structured(value),
        Err(error) => CallToolResult::structured_error(json!({ "error": error.to_string() })),
    }
}

fn search_limit(limit: Option<usize>) -> Result<usize> {
    let limit = limit.unwrap_or(20);
    if !(1..=MAX_SEARCH_RESULTS).contains(&limit) {
        bail!("limit must be between 1 and {MAX_SEARCH_RESULTS}");
    }
    Ok(limit)
}

fn search_time_range(
    since: Option<&str>,
    start_ms: Option<u64>,
    end_ms: Option<u64>,
) -> Result<(u64, u64)> {
    if since.is_some() && start_ms.is_some() {
        bail!("since conflicts with start_ms");
    }
    let end_ms = end_ms.unwrap_or(
        std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before 1970")?
            .as_millis() as u64,
    );
    let duration_ms = query::parse_duration_ms(since.unwrap_or("1h"))?;
    let start_ms = start_ms.unwrap_or_else(|| end_ms.saturating_sub(duration_ms));
    if start_ms > end_ms {
        bail!("start_ms must not be after end_ms");
    }
    Ok((start_ms, end_ms))
}

fn default_search_signal() -> String {
    "errors".to_owned()
}

fn record_json(record: StoredRecord, raw: bool) -> Value {
    let mut value = json!({
        "fields": query::normalized_fields(&record),
        "signal": record.signal,
    });
    if raw {
        let encoded = serde_json::to_string(&record.raw).expect("JSON values serialize");
        if encoded.len() <= MAX_RAW_BYTES {
            value["raw"] = record.raw;
            value["raw_truncated"] = Value::Bool(false);
        } else {
            let mut end = MAX_RAW_BYTES;
            while !encoded.is_char_boundary(end) {
                end -= 1;
            }
            value["raw"] = Value::String(encoded[..end].to_owned());
            value["raw_truncated"] = Value::Bool(true);
        }
    }
    value
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enforces_mcp_boundaries() {
        assert_eq!(search_limit(None).unwrap(), 20);
        assert!(search_limit(Some(101)).is_err());
        assert_eq!(
            search_time_range(Some("2s"), None, Some(10_000)).unwrap(),
            (8_000, 10_000)
        );
        assert!(search_time_range(Some("1h"), Some(0), None).is_err());

        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, "127.0.0.1:8910".parse().unwrap());
        headers.insert(header::ORIGIN, "http://127.0.0.1:8910".parse().unwrap());
        assert!(valid_origin(&headers));
        headers.insert(header::ORIGIN, "http://attacker.test".parse().unwrap());
        assert!(!valid_origin(&headers));

        let record = StoredRecord {
            project_id: 1,
            id: "id".into(),
            signal: Signal::Error,
            source: "sentry".into(),
            source_id: None,
            issue_id: None,
            timestamp_unix_nano: 1_000_000,
            received_at_ms: 1,
            fields: Default::default(),
            raw: json!({ "message": "x".repeat(MAX_RAW_BYTES) }),
        };
        let output = record_json(record, true);
        assert_eq!(output["raw_truncated"], true);
        assert!(output["raw"].as_str().unwrap().len() <= MAX_RAW_BYTES);
    }
}
