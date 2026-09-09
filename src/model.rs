use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicU64, Ordering},
};

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Project {
    pub id: u64,
    pub name: String,
    pub key: String,
    pub status: ProjectStatus,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProjectStatus {
    Active,
    Deleting,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct AddProject {
    pub name: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum Signal {
    Error,
    Log,
    Metric,
    Trace,
}

impl Signal {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Error => "error",
            Self::Log => "log",
            Self::Metric => "metric",
            Self::Trace => "trace",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StoredRecord {
    pub project_id: u64,
    pub id: String,
    pub signal: Signal,
    pub source: String,
    pub source_id: Option<String>,
    pub issue_id: Option<String>,
    pub timestamp_unix_nano: u64,
    pub received_at_ms: u64,
    pub fields: BTreeMap<String, Value>,
    pub raw: Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Issue {
    pub id: String,
    pub project_id: u64,
    pub title: String,
    pub level: String,
    pub first_seen_ms: u64,
    pub last_seen_ms: u64,
    pub total_seen: u64,
    pub retained_events: u64,
    pub latest_record_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SearchRequest {
    pub project_id: u64,
    #[serde(default)]
    pub query: String,
    #[serde(default = "default_limit")]
    pub limit: usize,
    pub start_ms: Option<u64>,
    pub end_ms: Option<u64>,
    pub cursor: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SearchCursor {
    pub start_ms: u64,
    pub end_ms: u64,
    pub timestamp_unix_nano: u64,
    pub id: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MetricQueryRequest {
    pub project_id: u64,
    pub name: String,
    pub aggregate: String,
    #[serde(default)]
    pub query: String,
    #[serde(default)]
    pub group_by: Vec<String>,
    #[serde(rename = "type")]
    pub metric_type: Option<String>,
    pub unit: Option<String>,
    pub start_ms: Option<u64>,
    pub end_ms: Option<u64>,
    pub interval_ms: Option<u64>,
    pub group_limit: Option<usize>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MetricQueryResponse {
    pub name: String,
    #[serde(rename = "type")]
    pub metric_type: String,
    pub unit: Option<String>,
    pub aggregate: String,
    pub interval_ms: u64,
    pub series: Vec<MetricSeries>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MetricSeries {
    pub group: BTreeMap<String, Value>,
    pub points: Vec<MetricPoint>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MetricPoint {
    pub timestamp_ms: u64,
    pub value: f64,
}

fn default_limit() -> usize {
    50
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Change {
    pub project_id: u64,
    pub signal: Signal,
    pub sequence: u64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Status {
    pub projects: usize,
    pub records: usize,
    pub disk_bytes: u64,
    pub logical_bytes: u64,
    pub storage_target_bytes: u64,
    pub storage_warning: bool,
    pub listeners: ListenerStatus,
    pub persisted: BTreeMap<String, u64>,
    pub process: ProcessStatsSnapshot,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ListenerStatus {
    pub web_api: u16,
    pub sentry: Option<u16>,
    pub otlp: Option<u16>,
}

#[derive(Default)]
pub struct ProcessStats {
    pub accepted: AtomicU64,
    pub skipped: AtomicU64,
    pub malformed: AtomicU64,
    pub unauthorized: AtomicU64,
    pub queue_full: AtomicU64,
    pub failed: AtomicU64,
}

impl ProcessStats {
    pub fn snapshot(&self) -> ProcessStatsSnapshot {
        ProcessStatsSnapshot {
            accepted: self.accepted.load(Ordering::Relaxed),
            skipped: self.skipped.load(Ordering::Relaxed),
            malformed: self.malformed.load(Ordering::Relaxed),
            unauthorized: self.unauthorized.load(Ordering::Relaxed),
            queue_full: self.queue_full.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ProcessStatsSnapshot {
    pub accepted: u64,
    pub skipped: u64,
    pub malformed: u64,
    pub unauthorized: u64,
    pub queue_full: u64,
    pub failed: u64,
}
