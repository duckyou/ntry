use std::{
    convert::Infallible,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    panic::AssertUnwindSafe,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::{
    Json, Router,
    extract::{ConnectInfo, Path, Query, State},
    http::{StatusCode, header},
    response::{
        Html, IntoResponse, Response, Sse,
        sse::{Event, KeepAlive},
    },
    routing::{delete, get, post},
};
use futures_util::{FutureExt, Stream, StreamExt};
use serde_json::json;
use tokio::sync::{broadcast, watch};
use tokio_stream::wrappers::BroadcastStream;

use crate::{
    client::Client,
    model::{
        AddProject, Change, Issue, ListenerStatus, MetricQueryRequest, MetricQueryResponse, Page,
        ProcessStats, Project, SearchRequest, Signal, Status, StoredRecord,
    },
    query,
    store::Store,
    writer::{Durability, IngestWriter},
};

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) store: Store,
    pub(crate) writer: IngestWriter,
    changes: broadcast::Sender<Change>,
    pub(crate) process: Arc<ProcessStats>,
    storage_target_bytes: u64,
    listeners: ListenerStatus,
    allow_remote: bool,
    shutdown: watch::Receiver<bool>,
}

#[allow(clippy::too_many_arguments)]
pub async fn serve(
    bind: SocketAddr,
    sentry_bind: Option<SocketAddr>,
    otlp_bind: Option<SocketAddr>,
    store: Store,
    durability: Durability,
    hot_window_ms: u64,
    archive_interval_ms: u64,
    storage_target_bytes: u64,
) -> anyhow::Result<()> {
    if archive_interval_ms == 0 {
        anyhow::bail!("archive interval must be greater than zero");
    }
    let control_listener = tokio::net::TcpListener::bind(bind).await?;
    let control_addr = control_listener.local_addr()?;
    let sentry_listener = match sentry_bind {
        Some(bind) => Some(tokio::net::TcpListener::bind(bind).await?),
        None => None,
    };
    let sentry_addr = sentry_listener
        .as_ref()
        .map(tokio::net::TcpListener::local_addr)
        .transpose()?;
    let otlp_listener = match otlp_bind {
        Some(bind) => Some(tokio::net::TcpListener::bind(bind).await?),
        None => None,
    };
    let otlp_addr = otlp_listener
        .as_ref()
        .map(tokio::net::TcpListener::local_addr)
        .transpose()?;
    let data_path = store.path().display().to_string();
    let archive_store = store.clone();
    tokio::task::spawn_blocking(move || {
        archive_store.archive_before(current_time_ms()?.saturating_sub(hot_window_ms))?;
        archive_store.enforce_storage_target(storage_target_bytes)
    })
    .await??;
    let process = Arc::new(ProcessStats::default());
    let (changes, _) = broadcast::channel(256);
    let (shutdown, shutdown_receiver) = watch::channel(false);
    let (writer, writer_task) =
        IngestWriter::start(store.clone(), durability, changes.clone(), process.clone());
    let periodic_store = store.clone();
    let archive_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(archive_interval_ms));
        interval.tick().await;
        loop {
            interval.tick().await;
            let store = periodic_store.clone();
            if let Err(error) = tokio::task::spawn_blocking(move || {
                store.archive_before(current_time_ms()?.saturating_sub(hot_window_ms))?;
                store.enforce_storage_target(storage_target_bytes)
            })
            .await
            .map_err(anyhow::Error::from)
            .and_then(|result| result)
            {
                tracing::error!(%error, "archive task failed");
            }
        }
    });
    let state = AppState {
        store,
        writer: writer.clone(),
        changes,
        process,
        storage_target_bytes,
        listeners: ListenerStatus {
            web_api: control_addr.port(),
            sentry: sentry_addr.map(|address| address.port()),
            otlp: otlp_addr.map(|address| address.port()),
        },
        allow_remote: !is_loopback(bind.ip()),
        shutdown: shutdown_receiver.clone(),
    };
    let client_bind = client_address(control_addr);
    let web_url = format!("http://{client_bind}/");
    let control_app = control_router(
        state.clone(),
        Client::new(&format!("http://{client_bind}"))?,
    );
    let (server_done_sender, server_done) = tokio::sync::mpsc::unbounded_channel();
    let mut server_tasks = vec![spawn_http(
        "web/API",
        control_listener,
        control_app,
        shutdown_receiver.clone(),
        server_done_sender.clone(),
    )];
    if let Some(listener) = sentry_listener {
        server_tasks.push(spawn_http(
            "Sentry",
            listener,
            crate::ingest::sentry::router().with_state(state.clone()),
            shutdown_receiver.clone(),
            server_done_sender.clone(),
        ));
    }
    if let Some(listener) = otlp_listener {
        server_tasks.push(spawn_http(
            "OTLP",
            listener,
            crate::ingest::otlp::router().with_state(state),
            shutdown_receiver,
            server_done_sender,
        ));
    }
    eprintln!(
        "{}",
        startup_message(&web_url, &data_path, sentry_addr, otlp_addr)
    );
    tracing::info!(data_path, "opened ntry data");
    tracing::info!(bind = %control_addr, %web_url, "web/API listener started");
    if let Some(address) = sentry_addr {
        tracing::info!(bind = %address, "Sentry listener started");
    }
    if let Some(address) = otlp_addr {
        tracing::info!(bind = %address, "OTLP/HTTP listener started");
    }
    supervise_servers(
        shutdown,
        server_done,
        server_tasks,
        archive_task,
        writer,
        writer_task,
    )
    .await
}

fn client_address(address: SocketAddr) -> SocketAddr {
    if !address.ip().is_unspecified() {
        return address;
    }
    SocketAddr::new(
        if address.is_ipv4() {
            IpAddr::V4(Ipv4Addr::LOCALHOST)
        } else {
            IpAddr::V6(Ipv6Addr::LOCALHOST)
        },
        address.port(),
    )
}

fn startup_message(
    web_url: &str,
    data_path: &str,
    sentry_addr: Option<SocketAddr>,
    otlp_addr: Option<SocketAddr>,
) -> String {
    let sentry = sentry_addr.map_or_else(
        || "disabled (use --sentry-bind 127.0.0.1:8911)".into(),
        |address| format!("http://{}", client_address(address)),
    );
    let otlp = otlp_addr.map_or_else(
        || "disabled (use --otlp-bind 127.0.0.1:8918)".into(),
        |address| format!("http://{}", client_address(address)),
    );
    format!(
        "\n[ NTRY ] telemetry server\n\n  Web UI     {web_url}\n  Data       {data_path}\n\n  INGESTORS\n  Sentry     {sentry}\n  OTLP/HTTP  {otlp}\n\n  Press Ctrl-C to stop\n"
    )
}

fn control_router(state: AppState, client: Client) -> Router {
    Router::new()
        .merge(crate::mcp::http_router::<AppState>(client))
        .route("/", get(web_index))
        .route("/settings", get(web_index))
        .route("/docs", get(web_index))
        .route("/projects/{project_id}/{view}", get(web_index))
        .route("/projects/{project_id}/{view}/{item_id}", get(web_index))
        .route("/assets/app.js", get(web_js))
        .route("/assets/app.css", get(web_css))
        .route("/api/v1/projects", post(add_project).get(list_projects))
        .route("/api/v1/projects/{name}", delete(remove_project))
        .route("/api/v1/status", get(status))
        .route("/api/v1/issues", get(search_issues))
        .route("/api/v1/records/{signal}", get(recent_records))
        .route("/api/v1/records/{project_id}/{id}", get(get_record))
        .route("/api/v1/metrics/query", post(metric_query))
        .route("/api/v1/live", get(live))
        .with_state(state)
}

async fn supervise_servers(
    shutdown: watch::Sender<bool>,
    mut server_done: tokio::sync::mpsc::UnboundedReceiver<(&'static str, Result<(), String>)>,
    server_tasks: Vec<tokio::task::JoinHandle<()>>,
    archive_task: tokio::task::JoinHandle<()>,
    writer: IngestWriter,
    writer_task: tokio::task::JoinHandle<()>,
) -> anyhow::Result<()> {
    let server_error = tokio::select! {
        _ = shutdown_signal() => None,
        result = server_done.recv() => result.map(|(name, result)| match result {
            Ok(()) => anyhow::anyhow!("{name} listener stopped unexpectedly"),
            Err(error) => anyhow::anyhow!("{name} listener failed: {error}"),
        }),
    };
    let _ = shutdown.send(true);
    let mut task_error = None;
    for task in server_tasks {
        if let Err(error) = task.await {
            task_error.get_or_insert_with(|| anyhow::anyhow!("listener task failed: {error}"));
        }
    }
    archive_task.abort();
    writer.shutdown().await;
    writer_task.await?;
    if let Some(error) = server_error.or(task_error) {
        return Err(error);
    }
    Ok(())
}

fn spawn_http(
    name: &'static str,
    listener: tokio::net::TcpListener,
    app: Router,
    shutdown: watch::Receiver<bool>,
    done: tokio::sync::mpsc::UnboundedSender<(&'static str, Result<(), String>)>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let result = AssertUnwindSafe(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .with_graceful_shutdown(wait_for_shutdown(shutdown))
            .await
            .map_err(|error| error.to_string())
        })
        .catch_unwind()
        .await
        .map_err(|_| "listener task panicked".to_owned())
        .and_then(|result| result);
        let _ = done.send((name, result));
    })
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    if !*shutdown.borrow() {
        let _ = shutdown.changed().await;
    }
}

pub fn validate_bind(bind: SocketAddr, allow_remote: bool) -> anyhow::Result<()> {
    if !is_loopback(bind.ip()) && !allow_remote {
        anyhow::bail!("non-loopback bind {bind} requires --allow-remote; v1 has no TLS");
    }
    Ok(())
}

async fn web_index() -> Html<&'static str> {
    Html(include_str!("../web/dist/index.html"))
}

async fn web_js() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/javascript; charset=utf-8")],
        include_str!("../web/dist/assets/app.js"),
    )
}

async fn web_css() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/css; charset=utf-8")],
        include_str!("../web/dist/assets/app.css"),
    )
}

async fn add_project(
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    State(state): State<AppState>,
    Json(input): Json<AddProject>,
) -> Result<Json<Project>, ApiError> {
    authorize_local(peer, state.allow_remote)?;
    let store = state.store.clone();
    let project = tokio::task::spawn_blocking(move || store.add_project(&input.name))
        .await
        .map_err(ApiError::internal)??;
    Ok(Json(project))
}

async fn list_projects(
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    State(state): State<AppState>,
) -> Result<Json<Vec<Project>>, ApiError> {
    authorize_local(peer, state.allow_remote)?;
    let store = state.store.clone();
    let projects = tokio::task::spawn_blocking(move || store.list_projects())
        .await
        .map_err(ApiError::internal)??;
    Ok(Json(projects))
}

async fn remove_project(
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<StatusCode, ApiError> {
    authorize_local(peer, state.allow_remote)?;
    let store = state.store.clone();
    let removed = tokio::task::spawn_blocking(move || store.remove_project(&name))
        .await
        .map_err(ApiError::internal)??;
    if removed {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::new(StatusCode::NOT_FOUND, "project not found"))
    }
}

fn authorize_local(peer: SocketAddr, allow_remote: bool) -> Result<(), ApiError> {
    if allow_remote || is_loopback(peer.ip()) {
        Ok(())
    } else {
        Err(ApiError::new(
            StatusCode::FORBIDDEN,
            "remote project management requires --allow-remote",
        ))
    }
}

async fn status(State(state): State<AppState>) -> Result<Json<Status>, ApiError> {
    let store = state.store.clone();
    let (projects, records, disk_bytes, logical_bytes, persisted) =
        tokio::task::spawn_blocking(move || store.status())
            .await
            .map_err(ApiError::internal)??;
    Ok(Json(Status {
        projects,
        records,
        disk_bytes,
        logical_bytes,
        storage_target_bytes: state.storage_target_bytes,
        storage_warning: logical_bytes >= state.storage_target_bytes.saturating_mul(4) / 5,
        listeners: state.listeners.clone(),
        persisted,
        process: state.process.snapshot(),
    }))
}

async fn recent_records(
    State(state): State<AppState>,
    Path(signal): Path<Signal>,
    Query(request): Query<SearchRequest>,
) -> Result<Json<Page<StoredRecord>>, ApiError> {
    let expression = query::parse(&request.query)?;
    query::validate_fields(&expression)?;
    let cursor = query::decode_cursor(request.cursor.as_deref())?;
    let now = unix_time_ms()?;
    let end_ms = cursor
        .as_ref()
        .map(|cursor| cursor.end_ms)
        .or(request.end_ms)
        .unwrap_or(now);
    if request
        .end_ms
        .zip(cursor.as_ref())
        .is_some_and(|(end, cursor)| end != cursor.end_ms)
    {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "end_ms does not match cursor",
        ));
    }
    let start_ms = cursor
        .as_ref()
        .map(|cursor| cursor.start_ms)
        .or(request.start_ms)
        .unwrap_or_else(|| end_ms.saturating_sub(60 * 60 * 1_000));
    if request
        .start_ms
        .zip(cursor.as_ref())
        .is_some_and(|(start, cursor)| start != cursor.start_ms)
    {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "start_ms does not match cursor",
        ));
    }
    let store = state.store.clone();
    let records = tokio::task::spawn_blocking(move || {
        store.search_records(
            request.project_id,
            signal,
            &expression,
            start_ms,
            end_ms,
            request.limit,
            cursor.as_ref(),
        )
    })
    .await
    .map_err(ApiError::internal)??;
    Ok(Json(records))
}

async fn search_issues(
    State(state): State<AppState>,
    Query(request): Query<SearchRequest>,
) -> Result<Json<Page<Issue>>, ApiError> {
    let expression = query::parse(&request.query)?;
    query::validate_fields(&expression)?;
    let cursor = query::decode_cursor(request.cursor.as_deref())?;
    let now = unix_time_ms()?;
    let end_ms = cursor
        .as_ref()
        .map(|cursor| cursor.end_ms)
        .or(request.end_ms)
        .unwrap_or(now);
    if request
        .end_ms
        .zip(cursor.as_ref())
        .is_some_and(|(end, cursor)| end != cursor.end_ms)
    {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "end_ms does not match cursor",
        ));
    }
    let start_ms = cursor
        .as_ref()
        .map(|cursor| cursor.start_ms)
        .or(request.start_ms)
        .unwrap_or_else(|| end_ms.saturating_sub(60 * 60 * 1_000));
    if request
        .start_ms
        .zip(cursor.as_ref())
        .is_some_and(|(start, cursor)| start != cursor.start_ms)
    {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "start_ms does not match cursor",
        ));
    }
    let store = state.store.clone();
    let issues = tokio::task::spawn_blocking(move || {
        store.search_issues(
            request.project_id,
            &expression,
            start_ms,
            end_ms,
            request.limit,
            cursor.as_ref(),
        )
    })
    .await
    .map_err(ApiError::internal)??;
    Ok(Json(issues))
}

async fn get_record(
    State(state): State<AppState>,
    Path((project_id, id)): Path<(u64, String)>,
) -> Result<Json<StoredRecord>, ApiError> {
    let store = state.store.clone();
    let record = tokio::task::spawn_blocking(move || store.get_record(project_id, &id))
        .await
        .map_err(ApiError::internal)??
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "record not found"))?;
    Ok(Json(record))
}

async fn metric_query(
    State(state): State<AppState>,
    Json(mut request): Json<MetricQueryRequest>,
) -> Result<Json<MetricQueryResponse>, ApiError> {
    let expression = query::parse(&request.query)?;
    query::validate_fields(&expression)?;
    for field in &request.group_by {
        query::validate_field(field)?;
    }
    let now = unix_time_ms()?;
    let end_ms = request.end_ms.unwrap_or(now);
    request.start_ms = Some(
        request
            .start_ms
            .unwrap_or_else(|| end_ms.saturating_sub(60 * 60 * 1_000)),
    );
    request.end_ms = Some(end_ms);
    let store = state.store.clone();
    let response = tokio::task::spawn_blocking(move || store.metric_query(&request, &expression))
        .await
        .map_err(ApiError::internal)??;
    Ok(Json(response))
}

async fn live(
    State(state): State<AppState>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ApiError> {
    Ok(Sse::new(live_events(
        state.changes.subscribe(),
        state.shutdown.clone(),
    ))
    .keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keep-alive"),
    ))
}

fn live_events(
    changes: broadcast::Receiver<Change>,
    mut shutdown: watch::Receiver<bool>,
) -> impl Stream<Item = Result<Event, Infallible>> {
    let shutdown = async move {
        if !*shutdown.borrow() {
            let _ = shutdown.changed().await;
        }
    };
    BroadcastStream::new(changes)
        .scan((), |_, message| async move {
            match message {
                Ok(change) => Event::default().json_data(change).ok().map(Ok),
                Err(_) => None,
            }
        })
        .take_until(shutdown)
}

fn is_loopback(ip: IpAddr) -> bool {
    ip.is_loopback()
}

pub(crate) fn unix_time_ms() -> Result<u64, ApiError> {
    current_time_ms().map_err(ApiError::internal)
}

fn current_time_ms() -> anyhow::Result<u64> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64)
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c()
        .await
        .expect("install Ctrl-C handler");
}

pub(crate) struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    pub(crate) fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }

    pub(crate) fn internal(error: impl std::fmt::Display) -> Self {
        tracing::error!(%error, "request failed");
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "internal server error")
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(error: anyhow::Error) -> Self {
        Self::new(StatusCode::BAD_REQUEST, error.to_string())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut response = (self.status, Json(json!({ "error": self.message }))).into_response();
        if self.status == StatusCode::TOO_MANY_REQUESTS {
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, "1".parse().unwrap());
        }
        response
    }
}

#[cfg(test)]
impl AppState {
    pub(crate) fn for_test(store: Store, writer: IngestWriter, process: Arc<ProcessStats>) -> Self {
        let (changes, _) = broadcast::channel(1);
        let (_, shutdown) = watch::channel(false);
        Self {
            store,
            writer,
            changes,
            process,
            storage_target_bytes: u64::MAX,
            listeners: ListenerStatus {
                web_api: 0,
                sentry: None,
                otlp: None,
            },
            allow_remote: false,
            shutdown,
        }
    }
}

#[cfg(test)]
mod tests {
    use axum::{body::Body, http::Request};
    use opentelemetry_proto::tonic::collector::{
        logs::v1::ExportLogsServiceRequest, metrics::v1::ExportMetricsServiceRequest,
        trace::v1::ExportTraceServiceRequest,
    };
    use prost::Message;
    use tempfile::tempdir;
    use tower::ServiceExt;

    use super::*;
    use crate::writer::Durability;

    #[test]
    fn remote_project_management_requires_flag() {
        assert!(authorize_local("127.0.0.1:1".parse().unwrap(), false).is_ok());
        assert!(authorize_local("[::1]:1".parse().unwrap(), false).is_ok());
        assert!(authorize_local("192.0.2.1:1".parse().unwrap(), false).is_err());
        assert!(authorize_local("192.0.2.1:1".parse().unwrap(), true).is_ok());
    }

    #[test]
    fn startup_message_lists_web_ui_and_ingestors() {
        let message = startup_message(
            "http://127.0.0.1:8910/",
            "/tmp/ntry/db",
            Some("0.0.0.0:8911".parse().unwrap()),
            None,
        );
        assert!(message.contains("Web UI     http://127.0.0.1:8910/"));
        assert!(message.contains("Sentry     http://127.0.0.1:8911"));
        assert!(message.contains("OTLP/HTTP  disabled (use --otlp-bind 127.0.0.1:8918)"));
    }

    #[tokio::test]
    async fn live_stream_stops_on_shutdown() {
        let (changes, receiver) = broadcast::channel(1);
        let (shutdown, shutdown_receiver) = watch::channel(false);
        let mut stream = Box::pin(live_events(receiver, shutdown_receiver));

        shutdown.send(true).unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(100), stream.next())
                .await
                .unwrap()
                .is_none()
        );
        drop(changes);
    }

    #[tokio::test]
    async fn listener_routers_are_isolated() {
        let directory = tempdir().unwrap();
        let store = Store::open(&directory.path().join("db")).unwrap();
        let project = store.add_project("test").unwrap();
        let process = Arc::new(ProcessStats::default());
        let (changes, _) = broadcast::channel(1);
        let (writer, writer_task) =
            IngestWriter::start(store.clone(), Durability::Safe, changes, process.clone());
        let state = AppState::for_test(store, writer.clone(), process);
        let control = control_router(state.clone(), Client::new("http://127.0.0.1:1").unwrap());
        let sentry = crate::ingest::sentry::router().with_state(state.clone());
        let otlp = crate::ingest::otlp::router().with_state(state);

        for (router, expected) in [
            (control.clone(), StatusCode::OK),
            (sentry.clone(), StatusCode::NOT_FOUND),
            (otlp.clone(), StatusCode::NOT_FOUND),
        ] {
            let response = router
                .oneshot(Request::get("/api/v1/status").body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), expected);
        }

        for (router, expected) in [
            (control.clone(), StatusCode::NOT_FOUND),
            (sentry.clone(), StatusCode::OK),
            (otlp.clone(), StatusCode::NOT_FOUND),
        ] {
            let response =
                router
                    .oneshot(
                        Request::post(format!("/api/{}/envelope/", project.id))
                            .header(
                                "x-sentry-auth",
                                format!("Sentry sentry_key={}", project.key),
                            )
                            .body(Body::from(
                                &include_bytes!(
                                    "../tests/fixtures/sentry-sdk-2.68.1/error.envelope"
                                )[..],
                            ))
                            .unwrap(),
                    )
                    .await
                    .unwrap();
            assert_eq!(response.status(), expected);
        }

        for (path, body) in [
            (
                "v1/traces",
                ExportTraceServiceRequest::default().encode_to_vec(),
            ),
            (
                "v1/logs",
                ExportLogsServiceRequest::default().encode_to_vec(),
            ),
            (
                "v1/metrics",
                ExportMetricsServiceRequest::default().encode_to_vec(),
            ),
        ] {
            for (router, expected) in [
                (control.clone(), StatusCode::NOT_FOUND),
                (sentry.clone(), StatusCode::NOT_FOUND),
                (otlp.clone(), StatusCode::OK),
            ] {
                let response = router
                    .oneshot(
                        Request::post(format!("/{}/{path}", project.id))
                            .header(header::CONTENT_TYPE, "application/x-protobuf")
                            .header(header::AUTHORIZATION, format!("Bearer {}", project.key))
                            .body(Body::from(body.clone()))
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                assert_eq!(response.status(), expected);
            }
        }

        writer.shutdown().await;
        writer_task.await.unwrap();
    }

    #[tokio::test]
    async fn listener_failure_shuts_down_and_flushes_writer() {
        let directory = tempdir().unwrap();
        let store = Store::open(&directory.path().join("db")).unwrap();
        let process = Arc::new(ProcessStats::default());
        let (changes, _) = broadcast::channel(1);
        let (writer, writer_task) =
            IngestWriter::start(store.clone(), Durability::Fast, changes, process);
        writer.diagnostic("listener_failure_flush");
        let (shutdown, shutdown_receiver) = watch::channel(false);
        let (server_done, server_done_receiver) = tokio::sync::mpsc::unbounded_channel();
        server_done
            .send(("Sentry", Err("simulated failure".into())))
            .unwrap();
        let archive_task = tokio::spawn(std::future::pending());
        let (observed_shutdown, mut observed_shutdown_receiver) = tokio::sync::mpsc::channel(2);
        let server_tasks = (0..2)
            .map(|_| {
                let shutdown = shutdown_receiver.clone();
                let observed_shutdown = observed_shutdown.clone();
                tokio::spawn(async move {
                    wait_for_shutdown(shutdown).await;
                    observed_shutdown.send(()).await.unwrap();
                })
            })
            .collect();

        let error = supervise_servers(
            shutdown,
            server_done_receiver,
            server_tasks,
            archive_task,
            writer,
            writer_task,
        )
        .await
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "Sentry listener failed: simulated failure"
        );
        assert!(*shutdown_receiver.borrow());
        assert!(observed_shutdown_receiver.recv().await.is_some());
        assert!(observed_shutdown_receiver.recv().await.is_some());
        assert_eq!(store.status().unwrap().4["listener_failure_flush"], 1);
    }
}
