use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use clap::ValueEnum;
use tokio::sync::{broadcast, mpsc, oneshot};

use crate::{
    ingest::IngestBatch,
    model::{Change, ProcessStats, StoredRecord},
    store::{Store, changes_with_sequence},
};

const QUEUE_CAPACITY: usize = 1_024;
const MAX_BATCH_MESSAGES: usize = 100;

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum Durability {
    Safe,
    Fast,
}

#[derive(Clone)]
pub struct IngestWriter {
    sender: mpsc::Sender<Message>,
    diagnostics: mpsc::UnboundedSender<String>,
    durability: Durability,
}

enum Message {
    Write {
        batch: IngestBatch,
        ack: Option<oneshot::Sender<Result<(), String>>>,
    },
    Shutdown(oneshot::Sender<()>),
}

#[derive(Debug)]
pub enum SubmitError {
    Full,
    Closed,
    Write(String),
}

impl IngestWriter {
    pub fn start(
        store: Store,
        durability: Durability,
        changes: broadcast::Sender<Change>,
        process: Arc<ProcessStats>,
    ) -> (Self, tokio::task::JoinHandle<()>) {
        let (sender, receiver) = mpsc::channel(QUEUE_CAPACITY);
        let (diagnostics, diagnostic_receiver) = mpsc::unbounded_channel();
        let task = tokio::spawn(run_writer(
            store,
            durability,
            changes,
            process,
            receiver,
            diagnostic_receiver,
        ));
        (
            Self {
                sender,
                diagnostics,
                durability,
            },
            task,
        )
    }

    pub async fn submit(&self, batch: IngestBatch) -> Result<(), SubmitError> {
        let (ack, receiver) = match self.durability {
            Durability::Safe => {
                let (sender, receiver) = oneshot::channel();
                (Some(sender), Some(receiver))
            }
            Durability::Fast => (None, None),
        };
        self.sender
            .try_send(Message::Write { batch, ack })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => SubmitError::Full,
                mpsc::error::TrySendError::Closed(_) => SubmitError::Closed,
            })?;
        if let Some(receiver) = receiver {
            receiver
                .await
                .map_err(|_| SubmitError::Closed)?
                .map_err(SubmitError::Write)?;
        }
        Ok(())
    }

    pub fn diagnostic(&self, name: impl Into<String>) {
        let _ = self.diagnostics.send(name.into());
    }

    pub async fn shutdown(&self) {
        let (sender, receiver) = oneshot::channel();
        if self.sender.send(Message::Shutdown(sender)).await.is_ok() {
            let _ = receiver.await;
        }
    }
}

async fn run_writer(
    store: Store,
    durability: Durability,
    changes: broadcast::Sender<Change>,
    process: Arc<ProcessStats>,
    mut receiver: mpsc::Receiver<Message>,
    mut diagnostic_receiver: mpsc::UnboundedReceiver<String>,
) {
    let sequence = AtomicU64::new(1);
    loop {
        let message = tokio::select! {
            message = receiver.recv() => message.map(Work::Message),
            diagnostic = diagnostic_receiver.recv() => diagnostic.map(Work::Diagnostic),
        };
        let Some(message) = message else {
            break;
        };
        let mut messages = vec![message];
        tokio::time::sleep(Duration::from_millis(5)).await;
        while messages.len() < MAX_BATCH_MESSAGES {
            match receiver.try_recv() {
                Ok(message) => messages.push(Work::Message(message)),
                Err(_) => break,
            }
        }
        while messages.len() < MAX_BATCH_MESSAGES {
            match diagnostic_receiver.try_recv() {
                Ok(name) => messages.push(Work::Diagnostic(name)),
                Err(_) => break,
            }
        }

        let mut records = Vec::<StoredRecord>::new();
        let mut skipped = BTreeMap::<String, u64>::new();
        let mut diagnostics = Vec::new();
        let mut acknowledgements = Vec::new();
        let mut shutdown = None;
        for message in messages {
            match message {
                Work::Message(Message::Write { batch, ack }) => {
                    records.extend(batch.records);
                    for (kind, count) in batch.skipped {
                        *skipped.entry(kind).or_default() += count;
                    }
                    if let Some(ack) = ack {
                        acknowledgements.push(ack);
                    }
                }
                Work::Message(Message::Shutdown(sender)) => shutdown = Some(sender),
                Work::Diagnostic(name) => diagnostics.push(name),
            }
        }

        let safe = matches!(durability, Durability::Safe);
        let blocking_store = store.clone();
        let result = tokio::task::spawn_blocking(move || {
            blocking_store.write_records(records, skipped, diagnostics, safe)
        })
        .await;

        match result {
            Ok(Ok(outcome)) => {
                process
                    .accepted
                    .fetch_add(outcome.accepted, Ordering::Relaxed);
                process
                    .skipped
                    .fetch_add(outcome.skipped, Ordering::Relaxed);
                for change in changes_with_sequence(outcome.changed, || {
                    sequence.fetch_add(1, Ordering::Relaxed)
                }) {
                    let _ = changes.send(change);
                }
                for ack in acknowledgements {
                    let _ = ack.send(Ok(()));
                }
            }
            error => {
                let message = match error {
                    Ok(Err(error)) => error.to_string(),
                    Err(error) => error.to_string(),
                    Ok(Ok(_)) => unreachable!(),
                };
                process.failed.fetch_add(1, Ordering::Relaxed);
                let diagnostic_store = store.clone();
                let _ = tokio::task::spawn_blocking(move || {
                    diagnostic_store.increment_diagnostic("failed", safe)
                })
                .await;
                tracing::error!(%message, "ingest batch failed");
                for ack in acknowledgements {
                    let _ = ack.send(Err(message.clone()));
                }
            }
        }

        if let Some(shutdown) = shutdown {
            if let Err(error) = store.persist() {
                tracing::error!(%error, "final persistence failed");
            }
            let _ = shutdown.send(());
            return;
        }
    }
}

enum Work {
    Message(Message),
    Diagnostic(String),
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;
    use tempfile::tempdir;

    use super::*;
    use crate::{
        ingest::IngestBatch,
        model::{Signal, StoredRecord},
        store::Store,
    };

    #[tokio::test]
    async fn persists_actual_counts_and_one_change_per_signal() {
        let directory = tempdir().unwrap();
        let store = Store::open(&directory.path().join("db")).unwrap();
        let project = store.add_project("demo").unwrap();
        let record = |id: &str| StoredRecord {
            project_id: project.id,
            id: id.into(),
            signal: Signal::Log,
            source: "sentry".into(),
            source_id: None,
            issue_id: None,
            timestamp_unix_nano: 1_000_000_000,
            received_at_ms: 1_000,
            fields: BTreeMap::from([
                ("message".into(), json!(id)),
                ("level".into(), json!("info")),
            ]),
            raw: json!({"body": id, "level": "info"}),
        };
        let (changes, mut receiver) = broadcast::channel(8);
        let process = Arc::new(ProcessStats::default());
        let (writer, task) =
            IngestWriter::start(store.clone(), Durability::Safe, changes, process.clone());
        writer.diagnostic("malformed");
        writer
            .submit(IngestBatch {
                records: vec![record("a"), record("b")],
                skipped: BTreeMap::from([("attachment".into(), 1)]),
            })
            .await
            .unwrap();
        writer.shutdown().await;
        task.await.unwrap();

        assert_eq!(receiver.recv().await.unwrap().signal, Signal::Log);
        assert!(receiver.try_recv().is_err());
        let persisted = store.status().unwrap().4;
        assert_eq!(persisted["accepted"], 2);
        assert_eq!(persisted["skipped"], 1);
        assert_eq!(persisted["malformed"], 1);
        assert_eq!(process.accepted.load(Ordering::Relaxed), 2);
        assert_eq!(process.skipped.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn safe_and_fast_writes_survive_normal_restart() {
        for durability in [Durability::Safe, Durability::Fast] {
            let directory = tempdir().unwrap();
            let (project_id, record_id) = {
                let store = Store::open(&directory.path().join("db")).unwrap();
                let project = store.add_project("demo").unwrap();
                let record_id = format!("record-{durability:?}");
                let (changes, _) = broadcast::channel(1);
                let (writer, task) = IngestWriter::start(
                    store,
                    durability,
                    changes,
                    Arc::new(ProcessStats::default()),
                );
                writer
                    .submit(IngestBatch {
                        records: vec![StoredRecord {
                            project_id: project.id,
                            id: record_id.clone(),
                            signal: Signal::Log,
                            source: "sentry".into(),
                            source_id: None,
                            issue_id: None,
                            timestamp_unix_nano: 1_000_000_000,
                            received_at_ms: 1_000,
                            fields: BTreeMap::from([
                                ("message".into(), json!("restart")),
                                ("level".into(), json!("info")),
                            ]),
                            raw: json!({"body": "restart", "level": "info"}),
                        }],
                        skipped: BTreeMap::new(),
                    })
                    .await
                    .unwrap();
                writer.shutdown().await;
                task.await.unwrap();
                (project.id, record_id)
            };
            let reopened = Store::open(&directory.path().join("db")).unwrap();
            assert!(
                reopened
                    .get_record(project_id, &record_id)
                    .unwrap()
                    .is_some()
            );
        }
    }
}
