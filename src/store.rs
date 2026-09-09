use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result, bail};
use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    archive::Archive,
    model::{
        Change, Issue, MetricQueryRequest, MetricQueryResponse, Page, Project, ProjectStatus,
        SearchCursor, Signal, StoredRecord,
    },
    query::{Expr, IndexHint, aggregate_metrics, event_time_ms, field_values, index_hint, matches},
};

const SCHEMA_VERSION: &str = "3";

pub struct WriteOutcome {
    pub changed: Vec<(u64, Signal)>,
    pub accepted: u64,
    pub skipped: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ArchiveManifest {
    id: String,
    path: PathBuf,
    project_id: u64,
    signal: Signal,
    receive_date: String,
    min_event_ms: u64,
    max_event_ms: u64,
    min_received_ms: u64,
    max_received_ms: u64,
    records: usize,
    logical_bytes: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
struct CumulativeSumState {
    last_timestamp_unix_nano: u64,
    start_timestamp_unix_nano: u64,
    previous_value: f64,
}

#[derive(Clone)]
pub struct Store {
    db: Database,
    archive: Archive,
    metadata: Keyspace,
    projects: Keyspace,
    project_names: Keyspace,
    records: Keyspace,
    record_indexes: Keyspace,
    record_ids: Keyspace,
    source_ids: Keyspace,
    issues: Keyspace,
    archive_manifests: Keyspace,
    archive_ids: Keyspace,
    diagnostics: Keyspace,
    cumulative_sums: Keyspace,
    mutations: Arc<Mutex<()>>,
    path: PathBuf,
}

impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        let db = Database::builder(path)
            .open()
            .context("open Fjall database")?;
        let store = Self {
            archive: Archive::open(path.parent().unwrap_or(path).join("archive"))?,
            metadata: db.keyspace("metadata", KeyspaceCreateOptions::default)?,
            projects: db.keyspace("projects", KeyspaceCreateOptions::default)?,
            project_names: db.keyspace("project_names", KeyspaceCreateOptions::default)?,
            records: db.keyspace("records", KeyspaceCreateOptions::default)?,
            record_indexes: db.keyspace("record_indexes", KeyspaceCreateOptions::default)?,
            record_ids: db.keyspace("record_ids", KeyspaceCreateOptions::default)?,
            source_ids: db.keyspace("source_ids", KeyspaceCreateOptions::default)?,
            issues: db.keyspace("issues", KeyspaceCreateOptions::default)?,
            archive_manifests: db.keyspace("archive_manifests", KeyspaceCreateOptions::default)?,
            archive_ids: db.keyspace("archive_ids", KeyspaceCreateOptions::default)?,
            diagnostics: db.keyspace("diagnostics", KeyspaceCreateOptions::default)?,
            cumulative_sums: db.keyspace("cumulative_sums", KeyspaceCreateOptions::default)?,
            db,
            mutations: Arc::new(Mutex::new(())),
            path: path.to_path_buf(),
        };

        match store.metadata.get("schema_version")? {
            Some(version) if version.as_ref() != SCHEMA_VERSION.as_bytes() => bail!(
                "unsupported data schema {}; remove {} explicitly to recreate it",
                String::from_utf8_lossy(&version),
                path.display()
            ),
            Some(_) => {}
            None => {
                store.metadata.insert("schema_version", SCHEMA_VERSION)?;
                store.db.persist(PersistMode::SyncAll)?;
            }
        }

        store.resume_deletions()?;
        store.archive.cleanup(
            &store
                .manifests()?
                .into_iter()
                .map(|manifest| manifest.path)
                .collect(),
        )?;
        Ok(store)
    }

    pub fn add_project(&self, name: &str) -> Result<Project> {
        validate_project_name(name)?;
        let _guard = self
            .mutations
            .lock()
            .expect("project mutation lock poisoned");
        if self.project_names.get(name)?.is_some() {
            bail!("project {name:?} already exists");
        }

        let id = self
            .metadata
            .get("next_project_id")?
            .map(|value| String::from_utf8_lossy(&value).parse())
            .transpose()?
            .unwrap_or(1);
        let project = Project {
            id,
            name: name.to_owned(),
            key: Uuid::new_v4().simple().to_string(),
            status: ProjectStatus::Active,
        };
        let mut batch = self.db.batch().durability(Some(PersistMode::SyncAll));
        batch.insert(
            &self.projects,
            project_key(id),
            serde_json::to_vec(&project)?,
        );
        batch.insert(&self.project_names, name, id.to_string());
        batch.insert(&self.metadata, "next_project_id", (id + 1).to_string());
        batch.commit()?;
        Ok(project)
    }

    pub fn list_projects(&self) -> Result<Vec<Project>> {
        self.projects
            .iter()
            .map(|item| {
                let (_, value) = item.into_inner()?;
                Ok(serde_json::from_slice(&value)?)
            })
            .collect()
    }

    pub fn get_project(&self, id: u64) -> Result<Option<Project>> {
        self.projects
            .get(project_key(id))?
            .map(|value| serde_json::from_slice(&value).context("decode project"))
            .transpose()
    }

    pub fn remove_project(&self, name: &str) -> Result<bool> {
        let _guard = self
            .mutations
            .lock()
            .expect("project mutation lock poisoned");
        let Some(id) = self.project_id_by_name(name)? else {
            return Ok(false);
        };
        let mut project = self
            .get_project(id)?
            .context("project name index points to a missing project")?;
        project.status = ProjectStatus::Deleting;
        self.projects
            .insert(project_key(id), serde_json::to_vec(&project)?)?;
        self.db.persist(PersistMode::SyncAll)?;
        self.finish_project_removal(&project)?;
        Ok(true)
    }

    pub fn write_records(
        &self,
        mut records: Vec<StoredRecord>,
        skipped: BTreeMap<String, u64>,
        mut diagnostics: Vec<String>,
        safe: bool,
    ) -> Result<WriteOutcome> {
        let _guard = self.mutations.lock().expect("store mutation lock poisoned");
        records.sort_by_key(|record| record.timestamp_unix_nano);
        let mut seen_source_ids = BTreeSet::new();
        let mut cumulative_sum_updates = BTreeMap::<String, CumulativeSumState>::new();
        let mut accepted = BTreeMap::<(u64, Signal), u64>::new();
        let mut batch = self.db.batch().durability(Some(if safe {
            PersistMode::SyncAll
        } else {
            PersistMode::Buffer
        }));

        let mut issue_updates = BTreeMap::<String, Issue>::new();
        for mut record in records {
            if self
                .get_project(record.project_id)?
                .is_none_or(|project| project.status != ProjectStatus::Active)
            {
                continue;
            }
            if let Some(source_id) = &record.source_id {
                let key = source_id_key(record.project_id, &record.source, source_id);
                if self.source_ids.get(&key)?.is_some() || !seen_source_ids.insert(key.clone()) {
                    continue;
                }
                batch.insert(&self.source_ids, key, record.id.as_bytes());
            }
            if record.signal == Signal::Metric
                && record
                    .fields
                    .get("type")
                    .and_then(serde_json::Value::as_str)
                    == Some("sum")
                && record
                    .fields
                    .get("temporality")
                    .and_then(serde_json::Value::as_str)
                    == Some("cumulative")
            {
                let source_value = record.fields.remove("value");
                if let Some(source_value) = source_value {
                    let current = source_value.as_f64();
                    record
                        .fields
                        .insert("cumulative_value".into(), source_value);
                    if let (Some(current), Some(stream_id)) = (
                        current,
                        record
                            .fields
                            .get("stream_id")
                            .and_then(serde_json::Value::as_str),
                    ) {
                        let key = cumulative_sum_key(record.project_id, &record.source, stream_id);
                        let previous = if let Some(state) = cumulative_sum_updates.get(&key) {
                            Some(*state)
                        } else {
                            self.cumulative_sums
                                .get(&key)?
                                .map(|value| serde_json::from_slice(&value))
                                .transpose()?
                        };
                        let start_timestamp_unix_nano = record
                            .fields
                            .get("start_timestamp_unix_nano")
                            .and_then(serde_json::Value::as_u64)
                            .unwrap_or_default();
                        let state = CumulativeSumState {
                            last_timestamp_unix_nano: record.timestamp_unix_nano,
                            start_timestamp_unix_nano,
                            previous_value: current,
                        };
                        if previous.is_none_or(|previous| {
                            record.timestamp_unix_nano > previous.last_timestamp_unix_nano
                        }) {
                            let mut update_state = true;
                            if let Some(previous) = previous {
                                if start_timestamp_unix_nano != previous.start_timestamp_unix_nano {
                                    record
                                        .fields
                                        .insert("value".into(), serde_json::json!(current));
                                } else if record
                                    .fields
                                    .get("monotonic")
                                    .and_then(serde_json::Value::as_bool)
                                    == Some(true)
                                    && current < previous.previous_value
                                {
                                    diagnostics
                                        .push("metric.cumulative_sum_monotonic_decrease".into());
                                    update_state = false;
                                } else {
                                    record.fields.insert(
                                        "value".into(),
                                        serde_json::json!(current - previous.previous_value),
                                    );
                                }
                            }
                            if update_state {
                                batch.insert(
                                    &self.cumulative_sums,
                                    &key,
                                    serde_json::to_vec(&state)?,
                                );
                                cumulative_sum_updates.insert(key, state);
                            }
                        }
                    }
                }
            }
            if record.signal == Signal::Error {
                let (issue_id, title) = issue_identity(&record);
                record.issue_id = Some(issue_id.clone());
                let key = issue_key(record.project_id, &issue_id);
                let issue = if let Some(issue) = issue_updates.get_mut(&key) {
                    issue
                } else {
                    let existing: Option<Issue> = self
                        .issues
                        .get(&key)?
                        .map(|value| serde_json::from_slice(&value))
                        .transpose()?;
                    if let Some(issue) = &existing {
                        batch.remove(
                            &self.record_indexes,
                            issue_time_index_key(issue.project_id, issue.last_seen_ms, &issue.id),
                        );
                    }
                    let existing = existing.unwrap_or_else(|| Issue {
                        id: issue_id,
                        project_id: record.project_id,
                        title: title.clone(),
                        level: record
                            .fields
                            .get("level")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or("error")
                            .to_owned(),
                        first_seen_ms: event_time_ms(&record),
                        last_seen_ms: event_time_ms(&record),
                        total_seen: 0,
                        retained_events: 0,
                        latest_record_id: None,
                    });
                    issue_updates.entry(key.clone()).or_insert(existing)
                };
                let timestamp = event_time_ms(&record);
                issue.first_seen_ms = issue.first_seen_ms.min(timestamp);
                if timestamp >= issue.last_seen_ms {
                    issue.last_seen_ms = timestamp;
                    issue.latest_record_id = Some(record.id.clone());
                    issue.title = title;
                }
                issue.total_seen += 1;
                issue.retained_events += 1;
            }
            let key = record_key(&record);
            let value = serde_json::to_vec(&record)?;
            let timestamp = event_time_ms(&record);
            for index in record_index_keys(&record, timestamp) {
                batch.insert(&self.record_indexes, index, key.as_bytes());
            }
            batch.insert(
                &self.record_ids,
                record_id_key(record.project_id, &record.id),
                key.as_bytes(),
            );
            batch.insert(&self.records, key, value);
            *accepted
                .entry((record.project_id, record.signal))
                .or_default() += 1;
        }

        for (key, issue) in issue_updates {
            batch.insert(
                &self.record_indexes,
                issue_time_index_key(issue.project_id, issue.last_seen_ms, &issue.id),
                key.as_bytes(),
            );
            batch.insert(&self.issues, key, serde_json::to_vec(&issue)?);
        }

        let skipped_total = skipped.values().sum();
        let mut diagnostic_changes = skipped
            .into_iter()
            .map(|(kind, count)| (format!("skipped.{kind}"), count))
            .collect::<BTreeMap<_, _>>();
        for ((_, signal), count) in &accepted {
            *diagnostic_changes.entry("accepted".into()).or_default() += count;
            *diagnostic_changes
                .entry(format!("accepted.{}", signal.as_str()))
                .or_default() += count;
        }
        if skipped_total > 0 {
            *diagnostic_changes.entry("skipped".into()).or_default() += skipped_total;
        }
        for name in diagnostics {
            *diagnostic_changes.entry(name).or_default() += 1;
        }
        for (key, amount) in diagnostic_changes {
            let current = self.diagnostic_value(&key)?;
            batch.insert(&self.diagnostics, key, (current + amount).to_string());
        }
        batch.commit()?;
        Ok(WriteOutcome {
            accepted: accepted.values().sum(),
            changed: accepted.into_keys().collect(),
            skipped: skipped_total,
        })
    }

    pub fn increment_diagnostic(&self, name: &str, safe: bool) -> Result<()> {
        let current = self.diagnostic_value(name)?;
        let mut batch = self.db.batch().durability(Some(if safe {
            PersistMode::SyncAll
        } else {
            PersistMode::Buffer
        }));
        batch.insert(&self.diagnostics, name, (current + 1).to_string());
        batch.commit()?;
        Ok(())
    }

    pub fn persist(&self) -> Result<()> {
        self.db.persist(PersistMode::SyncAll)?;
        Ok(())
    }

    pub fn archive_before(&self, cutoff_ms: u64) -> Result<usize> {
        let _guard = self.mutations.lock().expect("store mutation lock poisoned");
        let mut groups = BTreeMap::<(u64, Signal, String), Vec<StoredRecord>>::new();
        for item in self.records.iter() {
            let (_, value) = item.into_inner()?;
            let record: StoredRecord = serde_json::from_slice(&value)?;
            if record.received_at_ms >= cutoff_ms {
                continue;
            }
            let timestamp = i64::try_from(record.received_at_ms / 1_000)?;
            let date = time::OffsetDateTime::from_unix_timestamp(timestamp)?
                .date()
                .to_string();
            groups
                .entry((record.project_id, record.signal, date))
                .or_default()
                .push(record);
        }

        let mut archived = 0;
        for ((project_id, signal, receive_date), records) in groups {
            let id = Uuid::new_v4().simple().to_string();
            let path = PathBuf::from(format!(
                "project={project_id}/signal={}/date={receive_date}/part-{id}.parquet",
                signal.as_str()
            ));
            self.archive.write(&path, records.clone())?;
            let manifest = ArchiveManifest {
                id: id.clone(),
                path,
                project_id,
                signal,
                receive_date,
                min_event_ms: records.iter().map(event_time_ms).min().unwrap_or(0),
                max_event_ms: records.iter().map(event_time_ms).max().unwrap_or(0),
                min_received_ms: records
                    .iter()
                    .map(|record| record.received_at_ms)
                    .min()
                    .unwrap_or(0),
                max_received_ms: records
                    .iter()
                    .map(|record| record.received_at_ms)
                    .max()
                    .unwrap_or(0),
                records: records.len(),
                logical_bytes: records
                    .iter()
                    .map(|record| serde_json::to_vec(record).map(|value| value.len() as u64))
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .sum(),
            };
            let mut batch = self.db.batch().durability(Some(PersistMode::SyncAll));
            for record in &records {
                batch.remove(&self.records, record_key(record));
                batch.remove(
                    &self.record_ids,
                    record_id_key(record.project_id, &record.id),
                );
                for key in record_index_keys(record, event_time_ms(record)) {
                    batch.remove(&self.record_indexes, key);
                }
                batch.insert(
                    &self.archive_ids,
                    record_id_key(record.project_id, &record.id),
                    id.as_bytes(),
                );
            }
            batch.insert(
                &self.archive_manifests,
                id.as_bytes(),
                serde_json::to_vec(&manifest)?,
            );
            batch.commit()?;
            archived += records.len();
        }
        Ok(archived)
    }

    fn manifests(&self) -> Result<Vec<ArchiveManifest>> {
        self.archive_manifests
            .iter()
            .map(|item| {
                let (_, value) = item.into_inner()?;
                Ok(serde_json::from_slice(&value)?)
            })
            .collect()
    }

    fn archive_paths(
        &self,
        project_id: u64,
        signal: Signal,
        start_ms: u64,
        end_ms: u64,
    ) -> Result<Vec<PathBuf>> {
        Ok(self
            .manifests()?
            .into_iter()
            .filter(|manifest| {
                manifest.project_id == project_id
                    && manifest.signal == signal
                    && manifest.max_event_ms >= start_ms
                    && manifest.min_event_ms <= end_ms
            })
            .map(|manifest| manifest.path)
            .collect())
    }

    pub fn logical_bytes(&self) -> Result<u64> {
        let hot = self
            .records
            .iter()
            .map(|item| item.into_inner().map(|(_, value)| value.len() as u64))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .sum::<u64>();
        let issues = self
            .issues
            .iter()
            .map(|item| item.into_inner().map(|(_, value)| value.len() as u64))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .sum::<u64>();
        let cumulative_sums = self
            .cumulative_sums
            .iter()
            .map(|item| item.into_inner().map(|(_, value)| value.len() as u64))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .sum::<u64>();
        Ok(hot
            + issues
            + cumulative_sums
            + self
                .manifests()?
                .iter()
                .map(|manifest| manifest.logical_bytes)
                .sum::<u64>())
    }

    pub fn enforce_storage_target(&self, target_bytes: u64) -> Result<u64> {
        if target_bytes == 0 {
            bail!("storage target must be greater than zero");
        }
        let _guard = self.mutations.lock().expect("store mutation lock poisoned");
        let mut logical = self.logical_bytes()?;
        if logical >= target_bytes.saturating_mul(4) / 5 {
            tracing::warn!(
                logical,
                target_bytes,
                "ntry storage is above 80% of its target"
            );
        }
        if logical <= target_bytes {
            return Ok(logical);
        }

        let mut manifests = self.manifests()?;
        manifests.sort_by_key(|manifest| {
            (
                manifest.min_received_ms,
                manifest.project_id,
                manifest.signal,
                manifest.id.clone(),
            )
        });
        for manifest in manifests {
            if logical <= target_bytes {
                break;
            }
            let records = self.archive.query(
                vec![manifest.path.clone()],
                manifest.project_id,
                manifest.signal,
                manifest.min_event_ms,
                manifest.max_event_ms,
                None,
            )?;
            let mut batch = self.db.batch().durability(Some(PersistMode::SyncAll));
            let mut issue_updates = BTreeMap::new();
            for record in &records {
                batch.remove(
                    &self.archive_ids,
                    record_id_key(record.project_id, &record.id),
                );
                if let Some(source_id) = &record.source_id {
                    batch.remove(
                        &self.source_ids,
                        source_id_key(record.project_id, &record.source, source_id),
                    );
                }
                self.decrement_retained_issue(record, &mut issue_updates)?;
            }
            for (key, issue) in issue_updates {
                batch.insert(&self.issues, key, serde_json::to_vec(&issue)?);
            }
            batch.remove(&self.archive_manifests, manifest.id.as_bytes());
            batch.commit()?;
            self.archive.remove(&manifest.path)?;
            logical = logical.saturating_sub(manifest.logical_bytes);
        }

        if logical > target_bytes {
            let mut hot = self
                .records
                .iter()
                .map(|item| {
                    let (key, value) = item.into_inner()?;
                    let record: StoredRecord = serde_json::from_slice(&value)?;
                    Ok((record.received_at_ms, record.id.clone(), key, value, record))
                })
                .collect::<Result<Vec<_>>>()?;
            hot.sort_by(|left, right| (left.0, &left.1).cmp(&(right.0, &right.1)));
            for (_, _, key, value, record) in hot {
                if logical <= target_bytes {
                    break;
                }
                let mut batch = self.db.batch().durability(Some(PersistMode::SyncAll));
                batch.remove(&self.records, key);
                batch.remove(
                    &self.record_ids,
                    record_id_key(record.project_id, &record.id),
                );
                if let Some(source_id) = &record.source_id {
                    batch.remove(
                        &self.source_ids,
                        source_id_key(record.project_id, &record.source, source_id),
                    );
                }
                for index in record_index_keys(&record, event_time_ms(&record)) {
                    batch.remove(&self.record_indexes, index);
                }
                let mut issue_updates = BTreeMap::new();
                self.decrement_retained_issue(&record, &mut issue_updates)?;
                for (key, issue) in issue_updates {
                    batch.insert(&self.issues, key, serde_json::to_vec(&issue)?);
                }
                batch.commit()?;
                logical = logical.saturating_sub(value.len() as u64);
            }
        }

        if logical > target_bytes {
            let mut issues = self
                .issues
                .iter()
                .map(|item| {
                    let (key, value) = item.into_inner()?;
                    let issue: Issue = serde_json::from_slice(&value)?;
                    Ok((issue.first_seen_ms, issue.id.clone(), key, value, issue))
                })
                .collect::<Result<Vec<_>>>()?;
            issues.sort_by(|left, right| (left.0, &left.1).cmp(&(right.0, &right.1)));
            for (_, _, key, value, issue) in issues {
                if logical <= target_bytes {
                    break;
                }
                let mut batch = self.db.batch().durability(Some(PersistMode::SyncAll));
                batch.remove(&self.issues, key);
                batch.remove(
                    &self.record_indexes,
                    issue_time_index_key(issue.project_id, issue.last_seen_ms, &issue.id),
                );
                batch.commit()?;
                logical = logical.saturating_sub(value.len() as u64);
            }
        }
        self.logical_bytes()
    }

    fn decrement_retained_issue(
        &self,
        record: &StoredRecord,
        updates: &mut BTreeMap<String, Issue>,
    ) -> Result<()> {
        let Some(issue_id) = &record.issue_id else {
            return Ok(());
        };
        let key = issue_key(record.project_id, issue_id);
        if !updates.contains_key(&key) {
            let Some(value) = self.issues.get(&key)? else {
                return Ok(());
            };
            updates.insert(key.clone(), serde_json::from_slice(&value)?);
        }
        let issue = updates.get_mut(&key).expect("issue was inserted");
        issue.retained_events = issue.retained_events.saturating_sub(1);
        if issue.latest_record_id.as_deref() == Some(&record.id) {
            issue.latest_record_id = None;
        }
        Ok(())
    }

    #[allow(clippy::type_complexity)]
    pub fn status(&self) -> Result<(usize, usize, u64, u64, BTreeMap<String, u64>)> {
        let diagnostics = self
            .diagnostics
            .iter()
            .map(|item| {
                let (key, value) = item.into_inner()?;
                Ok((
                    String::from_utf8_lossy(&key).into_owned(),
                    String::from_utf8_lossy(&value).parse()?,
                ))
            })
            .collect::<Result<_, anyhow::Error>>()?;
        let manifests = self.manifests()?;
        Ok((
            self.projects.iter().count(),
            self.records.iter().count()
                + manifests
                    .iter()
                    .map(|manifest| manifest.records)
                    .sum::<usize>(),
            self.db.disk_space()? + self.archive.disk_space()?,
            self.logical_bytes()?,
            diagnostics,
        ))
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    #[allow(clippy::too_many_arguments)]
    pub fn search_records(
        &self,
        project_id: u64,
        signal: Signal,
        expression: &Expr,
        start_ms: u64,
        end_ms: u64,
        limit: usize,
        cursor: Option<&SearchCursor>,
    ) -> Result<Page<StoredRecord>> {
        if !(1..=500).contains(&limit) {
            bail!("limit must be between 1 and 500");
        }
        if start_ms > end_ms {
            bail!("query start must not be after its end");
        }
        let prefix = match index_hint(expression) {
            Some(IndexHint::Level(value)) => value_index_prefix(project_id, signal, b'l', &value),
            Some(IndexHint::Issue(value)) => value_index_prefix(project_id, signal, b'i', &value),
            Some(IndexHint::Trace(value)) => value_index_prefix(project_id, signal, b't', &value),
            None => index_prefix(project_id, signal, b'e'),
        };
        let mut records = Vec::new();
        let mut boundary_ms = None;
        for item in self.record_indexes.prefix(prefix).rev() {
            let (_, record_key) = item.into_inner()?;
            let Some(value) = self.records.get(record_key)? else {
                continue;
            };
            let record: StoredRecord = serde_json::from_slice(&value)?;
            let timestamp = event_time_ms(&record);
            if timestamp < start_ms {
                break;
            }
            if boundary_ms.is_some_and(|boundary| timestamp < boundary) {
                break;
            }
            if timestamp <= end_ms
                && cursor.is_none_or(|cursor| {
                    (record.timestamp_unix_nano, record.id.as_str())
                        < (cursor.timestamp_unix_nano, cursor.id.as_str())
                })
                && matches(&record, expression)
            {
                records.push(record);
                if records.len() > limit {
                    boundary_ms = Some(timestamp);
                }
            }
        }
        records.sort_by(|left, right| {
            (right.timestamp_unix_nano, right.id.as_str())
                .cmp(&(left.timestamp_unix_nano, left.id.as_str()))
        });
        let hot_has_more = records.len() > limit;
        records.truncate(limit);
        records.extend(self.archive.query(
            self.archive_paths(project_id, signal, start_ms, end_ms)?,
            project_id,
            signal,
            start_ms,
            end_ms,
            None,
        )?);
        records.retain(|record| {
            cursor.is_none_or(|cursor| {
                (record.timestamp_unix_nano, record.id.as_str())
                    < (cursor.timestamp_unix_nano, cursor.id.as_str())
            }) && matches(record, expression)
        });
        records.sort_by(|left, right| {
            (right.timestamp_unix_nano, right.id.as_str())
                .cmp(&(left.timestamp_unix_nano, left.id.as_str()))
        });
        records.dedup_by(|left, right| left.id == right.id);
        let has_more = hot_has_more || records.len() > limit;
        records.truncate(limit);
        let next_cursor = has_more.then(|| {
            let record = records.last().expect("a page with more rows is not empty");
            crate::query::encode_cursor(&SearchCursor {
                start_ms,
                end_ms,
                timestamp_unix_nano: record.timestamp_unix_nano,
                id: record.id.clone(),
            })
        });
        Ok(Page {
            items: records,
            next_cursor,
        })
    }

    pub fn get_record(&self, project_id: u64, id: &str) -> Result<Option<StoredRecord>> {
        let key = record_id_key(project_id, id);
        if let Some(record_key) = self.record_ids.get(&key)? {
            return self
                .records
                .get(record_key)?
                .map(|value| serde_json::from_slice(&value).context("decode stored record"))
                .transpose();
        }
        let Some(manifest_id) = self.archive_ids.get(&key)? else {
            return Ok(None);
        };
        let Some(manifest) = self.archive_manifests.get(manifest_id)? else {
            return Ok(None);
        };
        let manifest: ArchiveManifest = serde_json::from_slice(&manifest)?;
        let start_ms = manifest.min_event_ms;
        let end_ms = manifest.max_event_ms;
        Ok(self
            .archive
            .query(
                vec![manifest.path],
                project_id,
                manifest.signal,
                start_ms,
                end_ms,
                Some(id),
            )?
            .into_iter()
            .next())
    }

    pub fn metric_query(
        &self,
        request: &MetricQueryRequest,
        expression: &Expr,
    ) -> Result<MetricQueryResponse> {
        let prefix = value_index_prefix(request.project_id, Signal::Metric, b'm', &request.name);
        let start_ms = request
            .start_ms
            .context("metric query is missing start_ms")?;
        let end_ms = request.end_ms.context("metric query is missing end_ms")?;
        let mut records = Vec::new();
        for item in self.record_indexes.prefix(prefix).rev() {
            let (_, record_key) = item.into_inner()?;
            let Some(value) = self.records.get(record_key)? else {
                continue;
            };
            let record: StoredRecord = serde_json::from_slice(&value)?;
            let timestamp = event_time_ms(&record);
            if timestamp < start_ms {
                break;
            }
            if timestamp <= end_ms {
                records.push(record);
            }
        }
        records.extend(self.archive.query(
            self.archive_paths(request.project_id, Signal::Metric, start_ms, end_ms)?,
            request.project_id,
            Signal::Metric,
            start_ms,
            end_ms,
            None,
        )?);
        aggregate_metrics(records, request, expression)
    }

    pub fn search_issues(
        &self,
        project_id: u64,
        expression: &Expr,
        start_ms: u64,
        end_ms: u64,
        limit: usize,
        cursor: Option<&SearchCursor>,
    ) -> Result<Page<Issue>> {
        if !(1..=500).contains(&limit) {
            bail!("limit must be between 1 and 500");
        }
        if start_ms > end_ms {
            bail!("query start must not be after its end");
        }
        let prefix = index_prefix(project_id, Signal::Error, b'u');
        let issues = self
            .record_indexes
            .prefix(prefix)
            .rev()
            .map(|item| {
                let (_, issue_key) = item.into_inner()?;
                let value = self
                    .issues
                    .get(issue_key)?
                    .context("issue index points to a missing issue")?;
                let issue: Issue = serde_json::from_slice(&value)?;
                let record = issue_as_record(&issue);
                Ok((issue, record))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut issues = issues
            .into_iter()
            .filter(|(issue, record)| {
                issue.last_seen_ms >= start_ms
                    && issue.last_seen_ms <= end_ms
                    && cursor.is_none_or(|cursor| {
                        (
                            issue.last_seen_ms.saturating_mul(1_000_000),
                            issue.id.as_str(),
                        ) < (cursor.timestamp_unix_nano, cursor.id.as_str())
                    })
                    && matches(record, expression)
            })
            .take(limit + 1)
            .map(|(issue, _)| issue)
            .collect::<Vec<_>>();
        let has_more = issues.len() > limit;
        issues.truncate(limit);
        let next_cursor = has_more.then(|| {
            let issue = issues.last().expect("a page with more rows is not empty");
            crate::query::encode_cursor(&SearchCursor {
                start_ms,
                end_ms,
                timestamp_unix_nano: issue.last_seen_ms.saturating_mul(1_000_000),
                id: issue.id.clone(),
            })
        });
        Ok(Page {
            items: issues,
            next_cursor,
        })
    }

    fn diagnostic_value(&self, name: &str) -> Result<u64> {
        Ok(self
            .diagnostics
            .get(name)?
            .map(|value| String::from_utf8_lossy(&value).parse())
            .transpose()?
            .unwrap_or_default())
    }

    fn project_id_by_name(&self, name: &str) -> Result<Option<u64>> {
        self.project_names
            .get(name)?
            .map(|value| {
                String::from_utf8_lossy(&value)
                    .parse()
                    .context("decode project ID")
            })
            .transpose()
    }

    fn resume_deletions(&self) -> Result<()> {
        let deleting = self
            .list_projects()?
            .into_iter()
            .filter(|project| project.status == ProjectStatus::Deleting)
            .collect::<Vec<_>>();
        for project in deleting {
            self.finish_project_removal(&project)?;
        }
        Ok(())
    }

    fn finish_project_removal(&self, project: &Project) -> Result<()> {
        let archive_manifests = self
            .manifests()?
            .into_iter()
            .filter(|manifest| manifest.project_id == project.id)
            .collect::<Vec<_>>();
        self.remove_prefix(&self.records, format!("{:020}/", project.id).as_bytes())?;
        self.remove_prefix(&self.record_indexes, &project.id.to_be_bytes())?;
        self.remove_prefix(&self.record_ids, format!("{:020}/", project.id).as_bytes())?;
        self.remove_prefix(&self.source_ids, format!("{:020}/", project.id).as_bytes())?;
        self.remove_prefix(
            &self.cumulative_sums,
            format!("{:020}/", project.id).as_bytes(),
        )?;
        self.remove_prefix(&self.issues, format!("{:020}/", project.id).as_bytes())?;
        self.remove_prefix(&self.archive_ids, format!("{:020}/", project.id).as_bytes())?;
        let mut batch = self.db.batch().durability(Some(PersistMode::SyncAll));
        for manifest in &archive_manifests {
            batch.remove(&self.archive_manifests, manifest.id.as_bytes());
        }
        batch.remove(&self.project_names, project.name.as_bytes());
        batch.remove(&self.projects, project_key(project.id));
        batch.commit()?;
        for manifest in archive_manifests {
            self.archive.remove(&manifest.path)?;
        }
        Ok(())
    }

    fn remove_prefix(&self, keyspace: &Keyspace, prefix: &[u8]) -> Result<()> {
        loop {
            let keys = keyspace
                .prefix(prefix)
                .take(1_000)
                .map(|item| item.into_inner().map(|(key, _)| key))
                .collect::<Result<Vec<_>, _>>()?;
            if keys.is_empty() {
                return Ok(());
            }
            let mut batch = self.db.batch().durability(Some(PersistMode::Buffer));
            for key in keys {
                batch.remove(keyspace, key);
            }
            batch.commit()?;
        }
    }
}

fn validate_project_name(name: &str) -> Result<()> {
    if name.is_empty()
        || name.len() > 64
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        bail!("project name must be 1-64 ASCII letters, numbers, '.', '-', or '_'");
    }
    Ok(())
}

fn project_key(id: u64) -> String {
    format!("{id:020}")
}

fn record_key(record: &StoredRecord) -> String {
    format!(
        "{:020}/{}/{:020}/{}",
        record.project_id,
        record.signal.as_str(),
        record.received_at_ms,
        record.id
    )
}

fn record_index_keys(record: &StoredRecord, timestamp_ms: u64) -> Vec<Vec<u8>> {
    let mut keys = vec![timed_index_key(
        index_prefix(record.project_id, record.signal, b'e'),
        timestamp_ms,
        &record.id,
    )];
    for (kind, field) in [(b'l', "level"), (b'i', "issue"), (b't', "trace_id")] {
        if let Some(value) = field_values(record, field)
            .into_iter()
            .next()
            .and_then(|value| value.as_str().map(str::to_owned))
        {
            keys.push(timed_index_key(
                value_index_prefix(record.project_id, record.signal, kind, &value),
                timestamp_ms,
                &record.id,
            ));
        }
    }
    if record.signal == Signal::Metric
        && let Some(name) = record
            .fields
            .get("name")
            .and_then(serde_json::Value::as_str)
    {
        let mut key = timed_index_key(
            value_index_prefix(record.project_id, record.signal, b'm', name),
            timestamp_ms,
            &record.id,
        );
        push_component(
            &mut key,
            record
                .fields
                .get("type")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default(),
        );
        push_component(
            &mut key,
            record
                .fields
                .get("unit")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default(),
        );
        keys.push(key);
    }
    keys
}

fn index_prefix(project_id: u64, signal: Signal, kind: u8) -> Vec<u8> {
    let mut key = project_id.to_be_bytes().to_vec();
    key.push(match signal {
        Signal::Error => 0,
        Signal::Log => 1,
        Signal::Metric => 2,
        Signal::Trace => 3,
    });
    key.push(kind);
    key
}

fn value_index_prefix(project_id: u64, signal: Signal, kind: u8, value: &str) -> Vec<u8> {
    let mut key = index_prefix(project_id, signal, kind);
    push_component(&mut key, value);
    key
}

fn timed_index_key(mut prefix: Vec<u8>, timestamp_ms: u64, id: &str) -> Vec<u8> {
    prefix.extend_from_slice(&timestamp_ms.to_be_bytes());
    push_component(&mut prefix, id);
    prefix
}

fn issue_time_index_key(project_id: u64, timestamp_ms: u64, id: &str) -> Vec<u8> {
    timed_index_key(
        index_prefix(project_id, Signal::Error, b'u'),
        timestamp_ms,
        id,
    )
}

fn push_component(key: &mut Vec<u8>, value: &str) {
    key.extend_from_slice(&(value.len() as u32).to_be_bytes());
    key.extend_from_slice(value.as_bytes());
}

fn source_id_key(project_id: u64, source: &str, source_id: &str) -> String {
    format!("{project_id:020}/{source}/{source_id}")
}

fn cumulative_sum_key(project_id: u64, source: &str, stream_id: &str) -> String {
    format!("{project_id:020}/{source}/{stream_id}")
}

fn record_id_key(project_id: u64, id: &str) -> String {
    format!("{project_id:020}/{id}")
}

fn issue_key(project_id: u64, issue_id: &str) -> String {
    format!("{project_id:020}/{issue_id}")
}

fn issue_identity(record: &StoredRecord) -> (String, String) {
    let title = record
        .fields
        .get("issue.title")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("Error")
        .to_owned();
    let fingerprint = record
        .fields
        .get("issue.fingerprint")
        .and_then(serde_json::Value::as_str)
        .unwrap_or(&title);
    const NAMESPACE: uuid::Uuid = uuid::Uuid::from_u128(0x584b5a7e_983f_47f0_8de1_f9483dd695b7);
    (
        uuid::Uuid::new_v5(&NAMESPACE, fingerprint.as_bytes())
            .simple()
            .to_string(),
        title,
    )
}

fn issue_as_record(issue: &Issue) -> StoredRecord {
    StoredRecord {
        project_id: issue.project_id,
        id: issue.id.clone(),
        signal: Signal::Error,
        source: "ntry".into(),
        source_id: None,
        issue_id: Some(issue.id.clone()),
        timestamp_unix_nano: issue.last_seen_ms.saturating_mul(1_000_000),
        received_at_ms: issue.last_seen_ms,
        fields: BTreeMap::from([
            ("title".into(), serde_json::json!(issue.title)),
            ("message".into(), serde_json::json!(issue.title)),
            ("level".into(), serde_json::json!(issue.level)),
            ("firstSeen".into(), serde_json::json!(issue.first_seen_ms)),
            ("lastSeen".into(), serde_json::json!(issue.last_seen_ms)),
            ("timesSeen".into(), serde_json::json!(issue.total_seen)),
        ]),
        raw: serde_json::json!({}),
    }
}

pub fn changes_with_sequence(
    changed: Vec<(u64, Signal)>,
    next_sequence: impl Fn() -> u64,
) -> Vec<Change> {
    changed
        .into_iter()
        .map(|(project_id, signal)| Change {
            project_id,
            signal,
            sequence: next_sequence(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tempfile::tempdir;

    use super::*;

    fn cumulative_sum(
        project_id: u64,
        id: &str,
        stream_id: &str,
        timestamp_unix_nano: u64,
        start_timestamp_unix_nano: u64,
        value: f64,
        monotonic: bool,
    ) -> StoredRecord {
        StoredRecord {
            project_id,
            id: id.into(),
            signal: Signal::Metric,
            source: "otlp".into(),
            source_id: Some(id.into()),
            issue_id: None,
            timestamp_unix_nano,
            received_at_ms: timestamp_unix_nano / 1_000_000,
            fields: BTreeMap::from([
                ("name".into(), json!("requests")),
                ("type".into(), json!("sum")),
                ("temporality".into(), json!("cumulative")),
                ("monotonic".into(), json!(monotonic)),
                ("stream_id".into(), json!(stream_id)),
                (
                    "start_timestamp_unix_nano".into(),
                    json!(start_timestamp_unix_nano),
                ),
                ("value".into(), json!(value)),
            ]),
            raw: json!({}),
        }
    }

    #[test]
    fn cumulative_sums_are_differenced_and_reset() {
        let directory = tempdir().unwrap();
        let store = Store::open(&directory.path().join("db")).unwrap();
        let project = store.add_project("demo").unwrap();
        let second = 2_000_000_000;
        let reset_start = 2_500_000_000;
        store
            .write_records(
                vec![
                    cumulative_sum(project.id, "delta", "monotonic", second, 1, 15.0, true),
                    cumulative_sum(
                        project.id,
                        "baseline",
                        "monotonic",
                        1_000_000_000,
                        1,
                        10.0,
                        true,
                    ),
                    cumulative_sum(
                        project.id,
                        "reset",
                        "monotonic",
                        3_000_000_000,
                        reset_start,
                        4.0,
                        true,
                    ),
                    cumulative_sum(
                        project.id,
                        "invalid",
                        "monotonic",
                        4_000_000_000,
                        reset_start,
                        3.0,
                        true,
                    ),
                    cumulative_sum(
                        project.id,
                        "after-invalid",
                        "monotonic",
                        5_000_000_000,
                        reset_start,
                        5.0,
                        true,
                    ),
                    cumulative_sum(
                        project.id,
                        "nonmonotonic-baseline",
                        "nonmonotonic",
                        1_000_000_000,
                        1,
                        10.0,
                        false,
                    ),
                    cumulative_sum(
                        project.id,
                        "nonmonotonic-decrease",
                        "nonmonotonic",
                        second,
                        1,
                        6.0,
                        false,
                    ),
                ],
                BTreeMap::new(),
                Vec::new(),
                true,
            )
            .unwrap();

        let stored = |id| store.get_record(project.id, id).unwrap().unwrap();
        let baseline = stored("baseline");
        assert_eq!(baseline.fields["cumulative_value"].as_f64(), Some(10.0));
        assert!(!baseline.fields.contains_key("value"));
        assert_eq!(stored("delta").fields["value"].as_f64(), Some(5.0));
        assert_eq!(stored("reset").fields["value"].as_f64(), Some(4.0));
        assert!(!stored("invalid").fields.contains_key("value"));
        assert_eq!(stored("after-invalid").fields["value"].as_f64(), Some(1.0));
        assert_eq!(
            stored("nonmonotonic-decrease").fields["value"].as_f64(),
            Some(-4.0)
        );
        assert_eq!(
            store.status().unwrap().4["metric.cumulative_sum_monotonic_decrease"],
            1
        );
    }

    #[test]
    fn cumulative_sum_state_survives_reopen() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("db");
        let store = Store::open(&path).unwrap();
        let project = store.add_project("demo").unwrap();
        store
            .write_records(
                vec![cumulative_sum(
                    project.id, "first", "stream", 1, 1, 7.0, true,
                )],
                BTreeMap::new(),
                Vec::new(),
                true,
            )
            .unwrap();
        drop(store);

        let store = Store::open(&path).unwrap();
        store
            .write_records(
                vec![cumulative_sum(
                    project.id, "second", "stream", 2, 1, 9.0, true,
                )],
                BTreeMap::new(),
                Vec::new(),
                true,
            )
            .unwrap();
        assert_eq!(
            store
                .get_record(project.id, "second")
                .unwrap()
                .unwrap()
                .fields["value"]
                .as_f64(),
            Some(2.0)
        );
        let record_bytes = store
            .records
            .iter()
            .map(|item| item.into_inner().unwrap().1.len() as u64)
            .sum::<u64>();
        let state_bytes = store
            .cumulative_sums
            .iter()
            .map(|item| item.into_inner().unwrap().1.len() as u64)
            .sum::<u64>();
        assert_eq!(store.logical_bytes().unwrap(), record_bytes + state_bytes);
        assert!(store.remove_project("demo").unwrap());
        assert_eq!(store.cumulative_sums.iter().count(), 0);
    }

    #[test]
    fn project_records_are_deduplicated_and_removed() {
        let directory = tempdir().unwrap();
        let store = Store::open(&directory.path().join("db")).unwrap();
        let project = store.add_project("demo").unwrap();
        let record = StoredRecord {
            project_id: project.id,
            id: "9ec79c33ec9942ab8353589fcb2e04dc".into(),
            signal: Signal::Error,
            source: "sentry".into(),
            source_id: Some("9ec79c33ec9942ab8353589fcb2e04dc".into()),
            issue_id: None,
            timestamp_unix_nano: 1_000_000_000,
            received_at_ms: 2,
            fields: BTreeMap::from([
                ("message".into(), json!("boom")),
                ("issue.title".into(), json!("boom")),
                ("issue.fingerprint".into(), json!("boom")),
            ]),
            raw: json!({"message": "boom"}),
        };

        store
            .write_records(
                vec![record.clone(), record],
                BTreeMap::new(),
                Vec::new(),
                true,
            )
            .unwrap();
        let records = store
            .search_records(project.id, Signal::Error, &Expr::All, 0, u64::MAX, 50, None)
            .unwrap();
        assert_eq!(records.items.len(), 1);
        assert_eq!(
            store
                .get_record(project.id, &records.items[0].id)
                .unwrap()
                .unwrap()
                .raw["message"],
            "boom"
        );

        let mut second = records.items[0].clone();
        second.id = "12c2d058d58442709aa2eca08bf20986".into();
        second.source = "other".into();
        store
            .write_records(vec![second], BTreeMap::new(), Vec::new(), true)
            .unwrap();
        let issues = store
            .search_issues(project.id, &Expr::All, 0, u64::MAX, 50, None)
            .unwrap();
        assert_eq!(issues.items.len(), 1);
        assert_eq!(issues.items[0].total_seen, 2);

        assert_eq!(store.archive_before(3).unwrap(), 2);
        let archived = store
            .search_records(project.id, Signal::Error, &Expr::All, 0, u64::MAX, 50, None)
            .unwrap();
        assert_eq!(archived.items.len(), 2);
        assert!(
            store
                .get_record(project.id, &archived.items[0].id)
                .unwrap()
                .is_some()
        );
        assert_eq!(store.status().unwrap().1, 2);
        assert!(store.enforce_storage_target(1).unwrap() <= 1);
        assert_eq!(store.status().unwrap().1, 0);

        assert!(store.remove_project("demo").unwrap());
        assert!(store.list_projects().unwrap().is_empty());
        assert!(
            store
                .search_records(project.id, Signal::Error, &Expr::All, 0, u64::MAX, 50, None,)
                .unwrap()
                .items
                .is_empty()
        );
    }

    #[test]
    fn cursor_uses_nanoseconds_within_millisecond() {
        let directory = tempdir().unwrap();
        let store = Store::open(&directory.path().join("db")).unwrap();
        let project = store.add_project("demo").unwrap();
        let records = [("a", 300), ("z", 200), ("m", 200), ("y", 100)]
            .into_iter()
            .map(|(id, nanos)| StoredRecord {
                project_id: project.id,
                id: id.into(),
                signal: Signal::Log,
                source: "sentry".into(),
                source_id: None,
                issue_id: None,
                timestamp_unix_nano: 10_000_000_000 + nanos,
                received_at_ms: 10_000,
                fields: BTreeMap::from([
                    ("message".into(), json!(id)),
                    ("level".into(), json!("info")),
                ]),
                raw: json!({"body": id, "level": "info"}),
            })
            .collect();
        store
            .write_records(records, BTreeMap::new(), Vec::new(), true)
            .unwrap();

        let first = store
            .search_records(project.id, Signal::Log, &Expr::All, 0, 20_000, 2, None)
            .unwrap();
        assert_eq!(
            first
                .items
                .iter()
                .map(|record| record.id.as_str())
                .collect::<Vec<_>>(),
            ["a", "z"]
        );
        let cursor = crate::query::decode_cursor(first.next_cursor.as_deref())
            .unwrap()
            .unwrap();
        assert_eq!(cursor.timestamp_unix_nano, 10_000_000_200);
        let second = store
            .search_records(
                project.id,
                Signal::Log,
                &Expr::All,
                0,
                20_000,
                2,
                Some(&cursor),
            )
            .unwrap();
        assert_eq!(
            second
                .items
                .iter()
                .map(|record| record.id.as_str())
                .collect::<Vec<_>>(),
            ["m", "y"]
        );
        assert!(second.next_cursor.is_none());
    }

    #[test]
    fn issue_groups_and_projects_stay_isolated() {
        let directory = tempdir().unwrap();
        let store = Store::open(&directory.path().join("db")).unwrap();
        let first = store.add_project("first").unwrap();
        let second = store.add_project("second").unwrap();
        let record =
            |project_id, id: &str, function: &str, fingerprint: Option<&str>| StoredRecord {
                project_id,
                id: id.into(),
                signal: Signal::Error,
                source: "sentry".into(),
                source_id: Some(id.into()),
                issue_id: None,
                timestamp_unix_nano: 1_000_000_000,
                received_at_ms: 1_000,
                fields: BTreeMap::from([
                    ("issue.title".into(), json!("ValueError: boom")),
                    (
                        "issue.fingerprint".into(),
                        json!(
                            fingerprint
                                .map(str::to_owned)
                                .unwrap_or_else(|| format!("ValueError\0\0{function}\0app.py"))
                        ),
                    ),
                ]),
                raw: json!({
                    "fingerprint": fingerprint.map(|value| vec![value]),
                    "exception": {"values": [{
                        "type": "ValueError",
                        "value": "boom",
                        "stacktrace": {"frames": [{
                            "filename": "app.py",
                            "function": function,
                            "in_app": true
                        }]}
                    }]}
                }),
            };
        store
            .write_records(
                vec![
                    record(first.id, "a", "run", None),
                    record(first.id, "b", "run", None),
                    record(first.id, "c", "other", None),
                    record(first.id, "d", "run", Some("explicit")),
                    record(second.id, "e", "run", None),
                ],
                BTreeMap::new(),
                Vec::new(),
                true,
            )
            .unwrap();
        let first_issues = store
            .search_issues(first.id, &Expr::All, 1_000, 1_000, 50, None)
            .unwrap();
        assert_eq!(first_issues.items.len(), 3);
        assert_eq!(
            first_issues
                .items
                .iter()
                .map(|issue| issue.total_seen)
                .sum::<u64>(),
            4
        );
        assert_eq!(
            store
                .search_issues(second.id, &Expr::All, 1_000, 1_000, 50, None)
                .unwrap()
                .items
                .len(),
            1
        );
    }

    #[test]
    fn storage_pressure_and_project_deletion_are_restart_safe() {
        let directory = tempdir().unwrap();
        let store = Store::open(&directory.path().join("db")).unwrap();
        let first = store.add_project("first").unwrap();
        let second = store.add_project("second").unwrap();
        let record = |project_id, id: &str, received_at_ms| StoredRecord {
            project_id,
            id: id.into(),
            signal: Signal::Log,
            source: "sentry".into(),
            source_id: None,
            issue_id: None,
            timestamp_unix_nano: received_at_ms * 1_000_000,
            received_at_ms,
            fields: BTreeMap::from([
                ("message".into(), json!("retention")),
                ("level".into(), json!("info")),
            ]),
            raw: json!({"body": "retention", "level": "info"}),
        };
        store
            .write_records(
                vec![
                    record(first.id, "first-record", 1_000),
                    record(second.id, "second-record", 2_000),
                ],
                BTreeMap::new(),
                Vec::new(),
                true,
            )
            .unwrap();
        assert_eq!(store.archive_before(3_000).unwrap(), 2);
        let oldest_bytes = store
            .manifests()
            .unwrap()
            .into_iter()
            .min_by_key(|manifest| (manifest.min_received_ms, manifest.project_id))
            .unwrap()
            .logical_bytes;
        let target = store.logical_bytes().unwrap() - oldest_bytes;
        store.enforce_storage_target(target).unwrap();
        assert!(
            store
                .get_record(first.id, "first-record")
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .get_record(second.id, "second-record")
                .unwrap()
                .is_some()
        );

        assert!(store.remove_project("second").unwrap());
        let third = store.add_project("third").unwrap();
        let mut deleting = third.clone();
        deleting.status = ProjectStatus::Deleting;
        store
            .projects
            .insert(
                project_key(third.id),
                serde_json::to_vec(&deleting).unwrap(),
            )
            .unwrap();
        store.db.persist(PersistMode::SyncAll).unwrap();
        drop(store);

        let reopened = Store::open(&directory.path().join("db")).unwrap();
        assert_eq!(
            reopened
                .list_projects()
                .unwrap()
                .into_iter()
                .map(|project| project.name)
                .collect::<Vec<_>>(),
            ["first"]
        );
    }
}
