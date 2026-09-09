use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    sync::{Arc, mpsc},
    thread,
    time::Duration,
};

use anyhow::{Context, Result};
use duckdb::{Connection, params};

use crate::model::{Signal, StoredRecord};

#[derive(Clone)]
pub struct Archive {
    root: Arc<PathBuf>,
    sender: mpsc::Sender<Command>,
}

enum Command {
    Write {
        path: PathBuf,
        records: Vec<StoredRecord>,
        reply: mpsc::SyncSender<Result<(), String>>,
    },
    Query {
        paths: Vec<PathBuf>,
        project_id: u64,
        signal: Signal,
        start_ms: u64,
        end_ms: u64,
        id: Option<String>,
        reply: mpsc::SyncSender<Result<Vec<StoredRecord>, String>>,
    },
}

impl Archive {
    pub fn open(root: PathBuf) -> Result<Self> {
        fs::create_dir_all(&root).with_context(|| format!("create {}", root.display()))?;
        let (sender, receiver) = mpsc::channel();
        thread::Builder::new()
            .name("ntry-duckdb".into())
            .spawn(move || run(receiver))
            .context("start DuckDB worker")?;
        Ok(Self {
            root: Arc::new(root),
            sender,
        })
    }

    pub fn write(&self, path: &Path, records: Vec<StoredRecord>) -> Result<()> {
        let path = self.root.join(path);
        let (reply, response) = mpsc::sync_channel(1);
        self.sender
            .send(Command::Write {
                path,
                records,
                reply,
            })
            .context("DuckDB worker stopped")?;
        response
            .recv()
            .context("DuckDB worker stopped")?
            .map_err(anyhow::Error::msg)
    }

    pub fn query(
        &self,
        paths: Vec<PathBuf>,
        project_id: u64,
        signal: Signal,
        start_ms: u64,
        end_ms: u64,
        id: Option<&str>,
    ) -> Result<Vec<StoredRecord>> {
        if paths.is_empty() {
            return Ok(Vec::new());
        }
        let paths = paths.into_iter().map(|path| self.root.join(path)).collect();
        let (reply, response) = mpsc::sync_channel(1);
        self.sender
            .send(Command::Query {
                paths,
                project_id,
                signal,
                start_ms,
                end_ms,
                id: id.map(str::to_owned),
                reply,
            })
            .context("DuckDB worker stopped")?;
        response
            .recv()
            .context("DuckDB worker stopped")?
            .map_err(anyhow::Error::msg)
    }

    pub fn cleanup(&self, published: &BTreeSet<PathBuf>) -> Result<()> {
        for path in files_below(&self.root)? {
            let relative = path.strip_prefix(self.root.as_ref())?;
            if path.extension().is_some_and(|extension| extension == "tmp")
                || (path
                    .extension()
                    .is_some_and(|extension| extension == "parquet")
                    && !published.contains(relative))
            {
                fs::remove_file(&path)
                    .with_context(|| format!("remove orphan {}", path.display()))?;
            }
        }
        Ok(())
    }

    pub fn remove(&self, path: &Path) -> Result<()> {
        let path = self.root.join(path);
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error).with_context(|| format!("remove {}", path.display())),
        }
    }

    pub fn disk_space(&self) -> Result<u64> {
        files_below(&self.root)?
            .into_iter()
            .map(|path| Ok(fs::metadata(path)?.len()))
            .sum()
    }
}

fn run(receiver: mpsc::Receiver<Command>) {
    let connection = match Connection::open_in_memory() {
        Ok(connection) => connection,
        Err(error) => {
            tracing::error!(%error, "open DuckDB worker");
            return;
        }
    };
    for command in receiver {
        match command {
            Command::Write {
                path,
                records,
                reply,
            } => {
                let _ = reply.send(write(&connection, &path, &records).map_err(|e| e.to_string()));
            }
            Command::Query {
                paths,
                project_id,
                signal,
                start_ms,
                end_ms,
                id,
                reply,
            } => {
                let result = query(
                    &connection,
                    &paths,
                    project_id,
                    signal,
                    start_ms,
                    end_ms,
                    id.as_deref(),
                );
                let _ = reply.send(result.map_err(|e| e.to_string()));
            }
        }
    }
}

fn write(connection: &Connection, path: &Path, records: &[StoredRecord]) -> Result<()> {
    let parent = path.parent().context("archive path has no parent")?;
    fs::create_dir_all(parent)?;
    let temporary = path.with_extension("parquet.tmp");
    connection.execute_batch(
        "DROP TABLE IF EXISTS ntry_archive;
         CREATE TABLE ntry_archive (
            project_id BIGINT NOT NULL,
            signal VARCHAR NOT NULL,
            received_at_ms BIGINT NOT NULL,
            event_time_ms BIGINT NOT NULL,
            id VARCHAR NOT NULL,
            payload JSON NOT NULL
         );",
    )?;
    {
        let mut appender = connection.appender("ntry_archive")?;
        for record in records {
            appender.append_row(params![
                i64::try_from(record.project_id)?,
                record.signal.as_str(),
                i64::try_from(record.received_at_ms)?,
                i64::try_from(crate::query::event_time_ms(record))?,
                record.id,
                serde_json::to_string(record)?,
            ])?;
        }
        appender.flush()?;
    }
    connection.execute(
        "COPY ntry_archive TO ? (FORMAT PARQUET, COMPRESSION ZSTD)",
        [temporary.to_string_lossy().as_ref()],
    )?;
    fs::File::open(&temporary)?.sync_all()?;
    fs::rename(&temporary, path)?;
    fs::File::open(parent)?.sync_all()?;
    Ok(())
}

fn query(
    connection: &Connection,
    paths: &[PathBuf],
    project_id: u64,
    signal: Signal,
    start_ms: u64,
    end_ms: u64,
    id: Option<&str>,
) -> Result<Vec<StoredRecord>> {
    let files = paths
        .iter()
        .map(|path| format!("'{}'", path.to_string_lossy().replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(",");
    if files.is_empty() {
        return Ok(Vec::new());
    }
    let id_filter = id.map_or(String::new(), |_| " AND id = ?".into());
    let sql = format!(
        "SELECT payload::VARCHAR FROM read_parquet([{files}], union_by_name = true)
         WHERE project_id = ? AND signal = ? AND event_time_ms >= ? AND event_time_ms <= ?{id_filter}"
    );
    let interrupt = connection.interrupt_handle();
    let (cancel, cancelled) = mpsc::channel();
    let watchdog = thread::spawn(move || {
        if cancelled.recv_timeout(Duration::from_secs(30)).is_err() {
            interrupt.interrupt();
        }
    });
    let result = (|| {
        let mut statement = connection.prepare(&sql)?;
        let mut rows = match id {
            Some(id) => statement.query(params![
                i64::try_from(project_id)?,
                signal.as_str(),
                i64::try_from(start_ms).unwrap_or(i64::MAX),
                i64::try_from(end_ms).unwrap_or(i64::MAX),
                id,
            ])?,
            None => statement.query(params![
                i64::try_from(project_id)?,
                signal.as_str(),
                i64::try_from(start_ms).unwrap_or(i64::MAX),
                i64::try_from(end_ms).unwrap_or(i64::MAX),
            ])?,
        };
        let mut records = Vec::new();
        while let Some(row) = rows.next()? {
            let payload: String = row.get(0)?;
            records.push(serde_json::from_str(&payload)?);
        }
        Result::<_>::Ok(records)
    })();
    let _ = cancel.send(());
    watchdog
        .join()
        .map_err(|_| anyhow::anyhow!("DuckDB timeout worker panicked"))?;
    result
}

fn files_below(root: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in fs::read_dir(root)? {
        let path = entry?.path();
        if path.is_dir() {
            files.extend(files_below(&path)?);
        } else {
            files.push(path);
        }
    }
    Ok(files)
}
