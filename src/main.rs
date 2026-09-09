mod archive;
mod client;
mod ingest;
mod mcp;
mod model;
mod query;
mod server;
mod store;
mod writer;

use std::{
    fs,
    net::SocketAddr,
    os::unix::fs::{DirBuilderExt, PermissionsExt},
    path::{Path, PathBuf},
};

use crate::{
    client::Client,
    model::{MetricQueryRequest, Signal, StoredRecord},
    store::Store,
    writer::Durability,
};
use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[arg(long, global = true, env = "NTRY_DATA_DIR")]
    data_dir: Option<PathBuf>,

    #[arg(
        long,
        global = true,
        env = "NTRY_URL",
        default_value = "http://127.0.0.1:8910"
    )]
    url: String,

    #[arg(long, global = true)]
    json: bool,

    #[arg(long, global = true)]
    no_color: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Serve(Serve),
    Project {
        #[command(subcommand)]
        command: ProjectCommand,
    },
    Status,
    Issues(ListArgs),
    Errors(ListArgs),
    Logs(ListArgs),
    Traces(ListArgs),
    Metrics(MetricArgs),
    Get(GetArgs),
    Mcp,
}

#[derive(Args)]
struct Serve {
    #[arg(long, env = "NTRY_BIND", default_value = "127.0.0.1:8910")]
    bind: SocketAddr,

    #[arg(long, env = "NTRY_SENTRY_BIND")]
    sentry_bind: Option<SocketAddr>,

    #[arg(long, env = "NTRY_DDTRACE_BIND")]
    ddtrace_bind: Option<SocketAddr>,

    #[arg(long, env = "NTRY_OTLP_BIND")]
    otlp_bind: Option<SocketAddr>,

    #[arg(long)]
    allow_remote: bool,

    #[arg(short, long, help = "Print structured server logs")]
    verbose: bool,

    #[arg(long, env = "NTRY_DURABILITY", value_enum, default_value = "safe")]
    durability: Durability,

    #[arg(long, env = "NTRY_HOT_WINDOW", default_value = "24h")]
    hot_window: String,

    #[arg(long, env = "NTRY_ARCHIVE_INTERVAL", default_value = "15m")]
    archive_interval: String,

    #[arg(
        long,
        env = "NTRY_STORAGE_TARGET_BYTES",
        default_value_t = 10 * 1_024 * 1_024 * 1_024_u64
    )]
    storage_target_bytes: u64,
}

#[derive(Args)]
struct ListArgs {
    query: Option<String>,

    #[arg(long)]
    project: Option<String>,

    #[arg(long, default_value_t = 50)]
    limit: usize,

    #[arg(long)]
    cursor: Option<String>,

    #[arg(long, conflicts_with = "start")]
    since: Option<String>,

    #[arg(long)]
    start: Option<String>,

    #[arg(long)]
    end: Option<String>,
}

#[derive(Args)]
struct GetArgs {
    id: String,

    #[arg(long)]
    project: Option<String>,
}

#[derive(Args)]
struct MetricArgs {
    name: String,

    #[arg(long)]
    aggregate: String,

    #[arg(long)]
    query: Option<String>,

    #[arg(long)]
    project: Option<String>,

    #[arg(long = "type")]
    metric_type: Option<String>,

    #[arg(long)]
    unit: Option<String>,

    #[arg(long, action = clap::ArgAction::Append)]
    group_by: Vec<String>,

    #[arg(long, default_value_t = 20)]
    group_limit: usize,

    #[arg(long, conflicts_with = "start")]
    since: Option<String>,

    #[arg(long)]
    start: Option<String>,

    #[arg(long)]
    end: Option<String>,

    #[arg(long)]
    interval: Option<String>,
}

#[derive(Subcommand)]
enum ProjectCommand {
    Add { name: String },
    List,
    Remove { name: String },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let default_filter = match &cli.command {
        Command::Serve(args) if args.verbose => "ntry=info",
        _ => "ntry=warn",
    };
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| default_filter.into()),
        )
        .with_writer(std::io::stderr)
        .init();
    let data_dir = match cli.data_dir {
        Some(data_dir) => data_dir,
        None => default_data_dir()?,
    };

    match cli.command {
        Command::Serve(args) => {
            server::validate_bind(args.bind, args.allow_remote)?;
            if let Some(bind) = args.sentry_bind {
                server::validate_bind(bind, args.allow_remote)?;
            }
            if let Some(bind) = args.ddtrace_bind {
                server::validate_bind(bind, args.allow_remote)?;
            }
            if let Some(bind) = args.otlp_bind {
                server::validate_bind(bind, args.allow_remote)?;
            }
            ensure_data_dir(&data_dir)?;
            let store = Store::open(&data_dir.join("db"))?;
            server::serve(
                args.bind,
                args.sentry_bind,
                args.ddtrace_bind,
                args.otlp_bind,
                store,
                args.durability,
                query::parse_duration_ms(&args.hot_window)?,
                query::parse_duration_ms(&args.archive_interval)?,
                args.storage_target_bytes,
            )
            .await
        }
        command => {
            let client = Client::new(&cli.url)?;
            run_client(command, client, cli.json).await
        }
    }
}

async fn run_client(command: Command, client: Client, json: bool) -> Result<()> {
    match command {
        Command::Project { command } => match command {
            ProjectCommand::Add { name } => {
                let project = client.add_project(name).await?;
                let listeners = client.status().await?.listeners;
                let dsn = client.dsn(&project, listeners.sentry)?;
                let ddtrace_agent_url = client.ddtrace_agent_url(&project, listeners.ddtrace)?;
                let otlp_endpoint = client.otlp_endpoint(&project, listeners.otlp)?;
                let otlp_authorization = otlp_endpoint
                    .as_ref()
                    .map(|_| format!("Bearer {}", project.key));
                if json {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&serde_json::json!({
                            "id": project.id,
                            "name": project.name,
                            "status": project.status,
                            "dsn": dsn,
                            "ddtrace_agent_url": ddtrace_agent_url,
                            "otlp_endpoint": otlp_endpoint,
                            "otlp_authorization": otlp_authorization,
                        }))?
                    );
                } else {
                    println!("project: {}", project.name);
                    println!("id:      {}", project.id);
                    println!("sentry dsn: {}", dsn.as_deref().unwrap_or("disabled"));
                    println!(
                        "ddtrace agent url: {}",
                        ddtrace_agent_url.as_deref().unwrap_or("disabled")
                    );
                    if let Some(endpoint) = otlp_endpoint {
                        println!("otlp endpoint: {endpoint}");
                        println!("otlp header: Authorization=Bearer {}", project.key);
                    } else {
                        println!("otlp: disabled");
                    }
                }
            }
            ProjectCommand::List => {
                let projects = client.list_projects().await?;
                let listeners = client.status().await?.listeners;
                if json {
                    let projects = projects
                        .iter()
                        .map(|project| -> Result<_> {
                            let ddtrace_agent_url =
                                client.ddtrace_agent_url(project, listeners.ddtrace)?;
                            let otlp_endpoint = client.otlp_endpoint(project, listeners.otlp)?;
                            Ok(serde_json::json!({
                                "id": project.id,
                                "name": project.name,
                                "status": project.status,
                                "dsn": client.dsn(project, listeners.sentry)?,
                                "ddtrace_agent_url": ddtrace_agent_url,
                                "otlp_endpoint": otlp_endpoint,
                                "otlp_authorization": otlp_endpoint
                                    .as_ref()
                                    .map(|_| format!("Bearer {}", project.key)),
                            }))
                        })
                        .collect::<Result<Vec<_>>>()?;
                    println!("{}", serde_json::to_string_pretty(&projects)?);
                } else {
                    for project in projects {
                        let dsn = client.dsn(&project, listeners.sentry)?;
                        let ddtrace_agent_url =
                            client.ddtrace_agent_url(&project, listeners.ddtrace)?;
                        let otlp_endpoint = client.otlp_endpoint(&project, listeners.otlp)?;
                        println!("{}\t{}", project.id, project.name);
                        println!("  sentry dsn: {}", dsn.as_deref().unwrap_or("disabled"));
                        println!(
                            "  ddtrace agent url: {}",
                            ddtrace_agent_url.as_deref().unwrap_or("disabled")
                        );
                        if let Some(endpoint) = otlp_endpoint {
                            println!("  otlp endpoint: {endpoint}");
                            println!("  otlp header: Authorization=Bearer {}", project.key);
                        } else {
                            println!("  otlp: disabled");
                        }
                    }
                }
            }
            ProjectCommand::Remove { name } => {
                client.remove_project(&name).await?;
                if json {
                    println!("{{\"removed\":{}}}", serde_json::to_string(&name)?);
                } else {
                    println!("removed project {name}");
                }
            }
        },
        Command::Status => {
            let status = client.status().await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&status)?);
            } else {
                println!("projects:   {}", status.projects);
                println!("records:    {}", status.records);
                println!("disk bytes: {}", status.disk_bytes);
                println!("logical bytes: {}", status.logical_bytes);
                println!("storage target: {}", status.storage_target_bytes);
                println!("web_api: {}", status.listeners.web_api);
                println!(
                    "sentry: {}",
                    status
                        .listeners
                        .sentry
                        .map_or_else(|| "disabled".into(), |port| port.to_string())
                );
                println!(
                    "ddtrace: {}",
                    status
                        .listeners
                        .ddtrace
                        .map_or_else(|| "disabled".into(), |port| port.to_string())
                );
                println!(
                    "otlp: {}",
                    status
                        .listeners
                        .otlp
                        .map_or_else(|| "disabled".into(), |port| port.to_string())
                );
                for (name, value) in status.persisted {
                    println!("{name}: {value}");
                }
            }
        }
        Command::Errors(args) => list_records(&client, Signal::Error, args, json).await?,
        Command::Logs(args) => list_records(&client, Signal::Log, args, json).await?,
        Command::Traces(args) => list_records(&client, Signal::Trace, args, json).await?,
        Command::Issues(args) => {
            let project = client.resolve_project(args.project.as_deref()).await?;
            let (start_ms, end_ms) = time_range(&args)?;
            let page = client
                .search_issues_page(
                    project.id,
                    args.query.as_deref().unwrap_or_default(),
                    start_ms,
                    end_ms,
                    args.limit,
                    args.cursor.as_deref(),
                )
                .await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&page)?);
            } else {
                for issue in page.items {
                    println!(
                        "{}\t{}\t{}\t{}",
                        issue.last_seen_ms, issue.id, issue.total_seen, issue.title
                    );
                }
                if let Some(cursor) = page.next_cursor {
                    eprintln!("next cursor: {cursor}");
                }
            }
        }
        Command::Metrics(args) => {
            let project = client.resolve_project(args.project.as_deref()).await?;
            let (start_ms, end_ms) = resolve_time_range(
                args.since.as_deref(),
                args.start.as_deref(),
                args.end.as_deref(),
            )?;
            let request = MetricQueryRequest {
                project_id: project.id,
                name: args.name,
                aggregate: args.aggregate,
                query: args.query.unwrap_or_default(),
                group_by: args.group_by,
                metric_type: args.metric_type,
                unit: args.unit,
                start_ms: Some(start_ms),
                end_ms: Some(end_ms),
                interval_ms: args
                    .interval
                    .as_deref()
                    .map(query::parse_duration_ms)
                    .transpose()?,
                group_limit: Some(args.group_limit),
            };
            let response = client.metric_query(&request).await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&response)?);
            } else {
                for series in response.series {
                    let group = serde_json::to_string(&series.group)?;
                    for point in series.points {
                        println!("{}\t{}\t{}", point.timestamp_ms, point.value, group);
                    }
                }
            }
        }
        Command::Get(args) => {
            let project = client.resolve_project(args.project.as_deref()).await?;
            let record = client.get_record(project.id, &args.id).await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&record)?);
            } else {
                println!("id:       {}", record.id);
                println!("signal:   {}", record.signal.as_str());
                println!("received: {}", record.received_at_ms);
                println!("{}", serde_json::to_string_pretty(&record.raw)?);
            }
        }
        Command::Mcp => mcp::serve_stdio(client).await?,
        Command::Serve(_) => unreachable!(),
    }
    Ok(())
}

async fn list_records(client: &Client, signal: Signal, args: ListArgs, json: bool) -> Result<()> {
    let project = client.resolve_project(args.project.as_deref()).await?;
    let (start_ms, end_ms) = time_range(&args)?;
    let page = client
        .search_records_page(
            project.id,
            signal,
            args.query.as_deref().unwrap_or_default(),
            start_ms,
            end_ms,
            args.limit,
            args.cursor.as_deref(),
        )
        .await?;
    if json {
        println!("{}", serde_json::to_string_pretty(&page)?);
    } else {
        for record in page.items {
            println!(
                "{}\t{}\t{}",
                display_time(&record),
                record.id,
                record_summary(&record)
            );
        }
        if let Some(cursor) = page.next_cursor {
            eprintln!("next cursor: {cursor}");
        }
    }
    Ok(())
}

fn time_range(args: &ListArgs) -> Result<(u64, u64)> {
    resolve_time_range(
        args.since.as_deref(),
        args.start.as_deref(),
        args.end.as_deref(),
    )
}

fn resolve_time_range(
    since: Option<&str>,
    start: Option<&str>,
    end: Option<&str>,
) -> Result<(u64, u64)> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis() as u64;
    let end = end.map(query::parse_time_ms).transpose()?.unwrap_or(now);
    let start = match start {
        Some(start) => query::parse_time_ms(start)?,
        None => end.saturating_sub(query::parse_duration_ms(since.unwrap_or("1h"))?),
    };
    if start > end {
        bail!("query start must not be after its end");
    }
    Ok((start, end))
}

fn display_time(record: &StoredRecord) -> String {
    OffsetDateTime::from_unix_timestamp_nanos(record.timestamp_unix_nano.into())
        .ok()
        .and_then(|timestamp| timestamp.format(&Rfc3339).ok())
        .unwrap_or_else(|| format!("{}s", record.timestamp_unix_nano / 1_000_000_000))
}

fn record_summary(record: &StoredRecord) -> &str {
    match record.signal {
        Signal::Error => record
            .fields
            .get("title")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("error"),
        Signal::Log => record
            .fields
            .get("message")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("log"),
        Signal::Metric => record
            .fields
            .get("name")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("metric"),
        Signal::Trace => record
            .fields
            .get("name")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("trace"),
    }
}

fn default_data_dir() -> Result<PathBuf> {
    std::env::var_os("HOME")
        .filter(|home| !home.is_empty())
        .map(|home| PathBuf::from(home).join(".config/ntry"))
        .context("HOME is not set; use --data-dir")
}

fn ensure_data_dir(path: &Path) -> Result<()> {
    if !path.exists() {
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(path)
            .with_context(|| format!("create {}", path.display()))?;
    }
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    Ok(())
}
