use anyhow::{Context, Result, bail};
use reqwest::{Client as HttpClient, Response, StatusCode, Url};
use serde::de::DeserializeOwned;

use crate::model::{
    AddProject, Issue, MetricQueryRequest, MetricQueryResponse, Page, Project, Signal, Status,
    StoredRecord,
};

#[derive(Clone)]
pub struct Client {
    http: HttpClient,
    base: Url,
}

impl Client {
    pub fn new(base: &str) -> Result<Self> {
        Ok(Self {
            http: HttpClient::new(),
            base: Url::parse(base).context("invalid NTRY_URL")?,
        })
    }

    pub async fn add_project(&self, name: String) -> Result<Project> {
        let response = self
            .http
            .post(self.url(&["api", "v1", "projects"])?)
            .json(&AddProject { name })
            .send()
            .await
            .context("connect to ntry daemon; start it with `ntry serve`")?;
        decode(response).await
    }

    pub async fn list_projects(&self) -> Result<Vec<Project>> {
        let response = self
            .http
            .get(self.url(&["api", "v1", "projects"])?)
            .send()
            .await
            .context("connect to ntry daemon; start it with `ntry serve`")?;
        decode(response).await
    }

    pub async fn remove_project(&self, name: &str) -> Result<()> {
        let response = self
            .http
            .delete(self.url(&["api", "v1", "projects", name])?)
            .send()
            .await
            .context("connect to ntry daemon; start it with `ntry serve`")?;
        ensure_success(response).await.map(|_| ())
    }

    pub async fn status(&self) -> Result<Status> {
        let response = self
            .http
            .get(self.url(&["api", "v1", "status"])?)
            .send()
            .await
            .context("connect to ntry daemon; start it with `ntry serve`")?;
        decode(response).await
    }

    #[allow(dead_code)]
    pub async fn live_response(&self) -> Result<Response> {
        let response = self
            .http
            .get(self.url(&["api", "v1", "live"])?)
            .send()
            .await
            .context("connect to ntry daemon; start it with `ntry serve`")?;
        ensure_success(response).await
    }

    pub async fn search_records(
        &self,
        project_id: u64,
        signal: Signal,
        query: &str,
        start_ms: u64,
        end_ms: u64,
        limit: usize,
    ) -> Result<Vec<StoredRecord>> {
        Ok(self
            .search_records_page(project_id, signal, query, start_ms, end_ms, limit, None)
            .await?
            .items)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn search_records_page(
        &self,
        project_id: u64,
        signal: Signal,
        query: &str,
        start_ms: u64,
        end_ms: u64,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<Page<StoredRecord>> {
        let mut url = self.url(&["api", "v1", "records", signal.as_str()])?;
        url.query_pairs_mut()
            .append_pair("project_id", &project_id.to_string())
            .append_pair("query", query)
            .append_pair("limit", &limit.to_string());
        if let Some(cursor) = cursor {
            url.query_pairs_mut().append_pair("cursor", cursor);
        } else {
            url.query_pairs_mut()
                .append_pair("start_ms", &start_ms.to_string())
                .append_pair("end_ms", &end_ms.to_string());
        }
        let response = self
            .http
            .get(url)
            .send()
            .await
            .context("connect to ntry daemon; start it with `ntry serve`")?;
        decode(response).await
    }

    pub async fn search_issues(
        &self,
        project_id: u64,
        query: &str,
        start_ms: u64,
        end_ms: u64,
        limit: usize,
    ) -> Result<Vec<Issue>> {
        Ok(self
            .search_issues_page(project_id, query, start_ms, end_ms, limit, None)
            .await?
            .items)
    }

    pub async fn search_issues_page(
        &self,
        project_id: u64,
        query: &str,
        start_ms: u64,
        end_ms: u64,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<Page<Issue>> {
        let mut url = self.url(&["api", "v1", "issues"])?;
        url.query_pairs_mut()
            .append_pair("project_id", &project_id.to_string())
            .append_pair("query", query)
            .append_pair("limit", &limit.to_string());
        if let Some(cursor) = cursor {
            url.query_pairs_mut().append_pair("cursor", cursor);
        } else {
            url.query_pairs_mut()
                .append_pair("start_ms", &start_ms.to_string())
                .append_pair("end_ms", &end_ms.to_string());
        }
        let response = self
            .http
            .get(url)
            .send()
            .await
            .context("connect to ntry daemon; start it with `ntry serve`")?;
        decode(response).await
    }

    pub async fn get_record(&self, project_id: u64, id: &str) -> Result<StoredRecord> {
        self.find_record(project_id, id)
            .await?
            .context("record not found")
    }

    pub async fn find_record(&self, project_id: u64, id: &str) -> Result<Option<StoredRecord>> {
        let project_id = project_id.to_string();
        let response = self
            .http
            .get(self.url(&["api", "v1", "records", &project_id, id])?)
            .send()
            .await
            .context("connect to ntry daemon; start it with `ntry serve`")?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        decode(response).await.map(Some)
    }

    pub async fn metric_query(&self, request: &MetricQueryRequest) -> Result<MetricQueryResponse> {
        let response = self
            .http
            .post(self.url(&["api", "v1", "metrics", "query"])?)
            .json(request)
            .send()
            .await
            .context("connect to ntry daemon; start it with `ntry serve`")?;
        decode(response).await
    }

    pub async fn resolve_project(&self, selector: Option<&str>) -> Result<Project> {
        select_project(self.list_projects().await?, selector)
    }

    pub fn dsn(&self, project: &Project, port: Option<u16>) -> Result<Option<String>> {
        let Some(port) = port else {
            return Ok(None);
        };
        let mut dsn = self.base.clone();
        dsn.set_port(Some(port))
            .map_err(|_| anyhow::anyhow!("cannot set Sentry port"))?;
        dsn.set_username(&project.key)
            .map_err(|_| anyhow::anyhow!("cannot encode DSN key"))?;
        dsn.set_password(None)
            .map_err(|_| anyhow::anyhow!("cannot clear DSN password"))?;
        dsn.set_path(&project.id.to_string());
        dsn.set_query(None);
        dsn.set_fragment(None);
        Ok(Some(dsn.to_string().trim_end_matches('/').to_owned()))
    }

    pub fn otlp_endpoint(&self, project: &Project, port: Option<u16>) -> Result<Option<String>> {
        let Some(port) = port else {
            return Ok(None);
        };
        let mut endpoint = self.base.clone();
        endpoint
            .set_port(Some(port))
            .map_err(|_| anyhow::anyhow!("cannot set OTLP port"))?;
        endpoint
            .set_username("")
            .map_err(|_| anyhow::anyhow!("cannot clear OTLP username"))?;
        endpoint
            .set_password(None)
            .map_err(|_| anyhow::anyhow!("cannot clear OTLP password"))?;
        endpoint.set_path(&format!("{}/", project.id));
        endpoint.set_query(None);
        endpoint.set_fragment(None);
        Ok(Some(endpoint.to_string()))
    }

    pub fn ddtrace_agent_url(
        &self,
        project: &Project,
        port: Option<u16>,
    ) -> Result<Option<String>> {
        let Some(port) = port else {
            return Ok(None);
        };
        let mut endpoint = self.base.clone();
        endpoint
            .set_port(Some(port))
            .map_err(|_| anyhow::anyhow!("cannot set ddtrace port"))?;
        endpoint
            .set_username("")
            .map_err(|_| anyhow::anyhow!("cannot clear ddtrace username"))?;
        endpoint
            .set_password(None)
            .map_err(|_| anyhow::anyhow!("cannot clear ddtrace password"))?;
        endpoint.set_path(&format!("/{}/{}/", project.id, project.key));
        endpoint.set_query(None);
        endpoint.set_fragment(None);
        Ok(Some(endpoint.to_string()))
    }

    fn url(&self, segments: &[&str]) -> Result<Url> {
        let mut url = self.base.clone();
        url.set_query(None);
        url.set_fragment(None);
        url.path_segments_mut()
            .map_err(|_| anyhow::anyhow!("NTRY_URL cannot be a base URL"))?
            .clear()
            .extend(segments);
        Ok(url)
    }
}

fn select_project(projects: Vec<Project>, selector: Option<&str>) -> Result<Project> {
    if projects.is_empty() {
        bail!("no projects; create one with `ntry project add NAME`");
    }
    if let Some(selector) = selector {
        if let Some(project) = projects
            .iter()
            .find(|project| project.name == selector)
            .or_else(|| {
                selector
                    .parse::<u64>()
                    .ok()
                    .and_then(|id| projects.iter().find(|project| project.id == id))
            })
        {
            return Ok(project.clone());
        }
    } else if projects.len() == 1 {
        return Ok(projects.into_iter().next().expect("one project exists"));
    }

    let choices = projects
        .iter()
        .map(|project| format!("{} ({})", project.name, project.id))
        .collect::<Vec<_>>()
        .join(", ");
    match selector {
        Some(selector) => bail!("project {selector:?} not found; available projects: {choices}"),
        None => bail!("multiple projects exist; select one by name or ID: {choices}"),
    }
}

async fn decode<T: DeserializeOwned>(response: Response) -> Result<T> {
    ensure_success(response)
        .await?
        .json()
        .await
        .context("decode daemon response")
}

async fn ensure_success(response: Response) -> Result<Response> {
    if response.status().is_success() {
        return Ok(response);
    }
    let status = response.status();
    let message = response.text().await.unwrap_or_default();
    bail!("daemon returned {status}: {message}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::ProjectStatus;

    fn project(id: u64, name: &str) -> Project {
        Project {
            id,
            name: name.into(),
            key: String::new(),
            status: ProjectStatus::Active,
        }
    }

    #[test]
    fn selects_projects_by_name_or_id() {
        let projects = vec![project(1, "1"), project(2, "demo")];
        assert_eq!(
            select_project(projects.clone(), Some("demo")).unwrap().id,
            2
        );
        assert_eq!(select_project(projects.clone(), Some("2")).unwrap().id, 2);
        assert_eq!(select_project(projects.clone(), Some("1")).unwrap().id, 1);
        assert!(
            select_project(projects, None)
                .unwrap_err()
                .to_string()
                .contains("demo (2)")
        );
    }

    #[test]
    fn builds_endpoints_from_the_control_url_and_advertised_ports() {
        let client =
            Client::new("https://old:secret@example.com:8910/control?debug=1#top").unwrap();
        let mut project = project(42, "demo");
        project.key = "project-key".into();

        assert_eq!(
            client.dsn(&project, Some(8911)).unwrap().as_deref(),
            Some("https://project-key@example.com:8911/42")
        );
        assert_eq!(
            client
                .ddtrace_agent_url(&project, Some(8112))
                .unwrap()
                .as_deref(),
            Some("https://example.com:8112/42/project-key/")
        );
        assert_eq!(
            client
                .otlp_endpoint(&project, Some(8918))
                .unwrap()
                .as_deref(),
            Some("https://example.com:8918/42/")
        );
    }

    #[test]
    fn disabled_listeners_have_no_endpoints() {
        let client = Client::new("http://127.0.0.1:8910").unwrap();
        let project = project(1, "demo");

        assert_eq!(client.dsn(&project, None).unwrap(), None);
        assert_eq!(client.ddtrace_agent_url(&project, None).unwrap(), None);
        assert_eq!(client.otlp_endpoint(&project, None).unwrap(), None);
    }
}
