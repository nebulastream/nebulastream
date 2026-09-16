/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

use anyhow::{Context, Result};
use model::query::{self, QueryWithFragments};
use model::statement::{Statement, StatementResult};
use model::worker;
use reqwest::blocking::Client as HttpClient;
use reqwest::{Method, StatusCode};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

pub use reqwest::Url;

#[derive(Debug, Clone, serde::Deserialize)]
pub struct ErrorBody {
    pub code: u16,
    pub error: String,
    pub message: String,
}

#[derive(Debug)]
pub enum ClientError {
    Api { status: StatusCode, body: ErrorBody },
    Http { status: StatusCode, text: String },
    Transport { url: Url, message: String },
    Deadline { what: String },
    Cancelled,
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Api { body, .. } => write!(f, "{}: {}", body.error, body.message),
            Self::Http { status, text } => write!(f, "the coordinator answered {status}: {text}"),
            Self::Transport { url, message } => {
                write!(f, "cannot reach the coordinator at {url}: {message}")
            }
            Self::Deadline { what } => write!(f, "timed out waiting for {what}"),
            Self::Cancelled => write!(f, "the wait was cancelled"),
        }
    }
}

impl std::error::Error for ClientError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitKind {
    Started,
    Running,
    Completed,
    Terminated,
}

impl WaitKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Started => "started",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Terminated => "terminated",
        }
    }
}

fn wait_params(
    wait: WaitKind,
    deadline: Option<Instant>,
    slice: Option<Duration>,
) -> Vec<(&'static str, String)> {
    let mut params = vec![("wait", wait.as_str().to_owned())];
    let remaining = deadline.map(|deadline| deadline.saturating_duration_since(Instant::now()));
    if let Some(hold) = [remaining, slice].into_iter().flatten().min() {
        params.push(("timeout_ms", hold.as_millis().to_string()));
    }
    params
}

pub struct Client {
    base: Url,
    http: HttpClient,
    slice: Option<Duration>,
    cancelled: AtomicBool,
}

impl Client {
    pub fn new(mut base: Url) -> Result<Self> {
        if !base.path().ends_with('/') {
            let path = format!("{}/", base.path());
            base.set_path(&path);
        }
        let http = HttpClient::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(None)
            .build()
            .context("failed to build the HTTP client")?;
        Ok(Self {
            base,
            http,
            slice: None,
            cancelled: AtomicBool::new(false),
        })
    }

    #[must_use]
    pub fn with_poll_slice(mut self, slice: Duration) -> Self {
        self.slice = Some(slice);
        self
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }

    fn check_wait(&self, deadline: Option<Instant>, what: impl FnOnce() -> String) -> Result<()> {
        if self.cancelled.load(Ordering::Relaxed) {
            return Err(ClientError::Cancelled.into());
        }
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Err(ClientError::Deadline { what: what() }.into());
        }
        Ok(())
    }

    fn params_for(&self, wait: WaitKind, deadline: Option<Instant>) -> Vec<(&'static str, String)> {
        wait_params(wait, deadline, self.slice)
    }

    fn send<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, String)],
        body: Option<&Value>,
    ) -> Result<(StatusCode, T)> {
        let url = self
            .base
            .join(path)
            .with_context(|| format!("invalid coordinator path {path}"))?;
        let mut request = self.http.request(method, url.clone()).query(query);
        if let Some(body) = body {
            request = request.json(body);
        }
        let response = request.send().map_err(|error| self.transport(&error))?;
        let status = response.status();
        let text = response.text().map_err(|error| self.transport(&error))?;
        if !status.is_success() {
            return Err(match serde_json::from_str::<ErrorBody>(&text) {
                Ok(body) => ClientError::Api { status, body },
                Err(_) => ClientError::Http { status, text },
            }
            .into());
        }
        let value = serde_json::from_str(&text)
            .with_context(|| format!("unexpected answer from {url}: {text}"))?;
        Ok((status, value))
    }

    fn transport(&self, error: &reqwest::Error) -> ClientError {
        let mut message = error.to_string();
        let mut source = std::error::Error::source(error);
        while let Some(cause) = source {
            message.push_str(": ");
            message.push_str(&cause.to_string());
            source = cause.source();
        }
        ClientError::Transport {
            url: self.base.clone(),
            message,
        }
    }

    pub fn create<T: DeserializeOwned>(&self, path: &str, body: &impl Serialize) -> Result<T> {
        let body = serde_json::to_value(body)?;
        Ok(self.send(Method::POST, path, &[], Some(&body))?.1)
    }

    pub fn list<T: DeserializeOwned>(&self, path: &str, query: &[(&str, String)]) -> Result<T> {
        Ok(self.send(Method::GET, path, query, None)?.1)
    }

    pub fn workers(&self) -> Result<Vec<worker::Model>> {
        self.list("v1/workers", &[])
    }

    pub fn queries(&self, ids: &[i64], with_fragments: bool) -> Result<Vec<QueryWithFragments>> {
        let mut query = vec![("with_fragments", with_fragments.to_string())];
        if !ids.is_empty() {
            let ids = ids
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(",");
            query.push(("ids", ids));
        }
        self.list("v1/queries", &query)
    }

    pub fn explain(&self, sql: &str) -> Result<String> {
        #[derive(serde::Deserialize)]
        struct Explanation {
            explanation: String,
        }
        let (_, explained): (_, Explanation) = self.send(
            Method::POST,
            "v1/queries/explain",
            &[],
            Some(&json!({ "sql": sql })),
        )?;
        Ok(explained.explanation)
    }

    pub fn sql(&self, sql: &str) -> Result<StatementResult> {
        Ok(self
            .send(
                Method::POST,
                "v1/statements",
                &[],
                Some(&json!({ "sql": sql })),
            )?
            .1)
    }

    pub fn statement(&self, statement: &Statement) -> Result<StatementResult> {
        let body = json!({ "statement": statement });
        Ok(self
            .send(Method::POST, "v1/statements", &[], Some(&body))?
            .1)
    }

    pub fn sql_wait(
        &self,
        sql: &str,
        wait: WaitKind,
        deadline: Option<Instant>,
    ) -> Result<StatementResult> {
        self.run_wait(&json!({ "sql": sql }), wait, deadline)
    }

    pub fn statement_wait(
        &self,
        statement: &Statement,
        wait: WaitKind,
        deadline: Option<Instant>,
    ) -> Result<StatementResult> {
        self.run_wait(&json!({ "statement": statement }), wait, deadline)
    }

    fn run_wait(
        &self,
        body: &Value,
        wait: WaitKind,
        deadline: Option<Instant>,
    ) -> Result<StatementResult> {
        self.check_wait(None, String::new)?;
        let (status, result): (_, StatementResult) = self.send(
            Method::POST,
            "v1/statements",
            &self.params_for(wait, deadline),
            Some(body),
        )?;
        if status != StatusCode::ACCEPTED {
            return Ok(result);
        }
        Ok(match result {
            StatementResult::CreatedQuery(row) => {
                StatementResult::CreatedQuery(self.wait_query(*row.query.id, wait, deadline)?)
            }
            StatementResult::Queries(rows) => {
                let ids: Vec<i64> = rows.iter().map(|row| *row.query.id).collect();
                StatementResult::Queries(self.wait_queries(&ids, wait, deadline)?)
            }
            StatementResult::DroppedQueries(queries) => {
                let ids: Vec<i64> = queries.iter().map(|query| *query.id).collect();
                let rows = self.wait_queries(&ids, WaitKind::Terminated, deadline)?;
                StatementResult::DroppedQueries(rows.into_iter().map(|row| row.query).collect())
            }
            other => other,
        })
    }

    pub fn submit_query(
        &self,
        sql: &str,
        wait: WaitKind,
        deadline: Option<Instant>,
    ) -> Result<QueryWithFragments> {
        let (status, row): (_, QueryWithFragments) = self.send(
            Method::POST,
            "v1/queries",
            &self.params_for(wait, deadline),
            Some(&json!({ "sql": sql })),
        )?;
        if status == StatusCode::ACCEPTED {
            self.wait_query(*row.query.id, wait, deadline)
        } else {
            Ok(row)
        }
    }

    pub fn wait_query(
        &self,
        id: i64,
        wait: WaitKind,
        deadline: Option<Instant>,
    ) -> Result<QueryWithFragments> {
        loop {
            self.check_wait(deadline, || format!("query {id} to be {}", wait.as_str()))?;
            let mut params = self.params_for(wait, deadline);
            params.push(("with_fragments", "true".to_owned()));
            let (status, row): (_, QueryWithFragments) =
                self.send(Method::GET, &format!("v1/queries/{id}"), &params, None)?;
            if status != StatusCode::ACCEPTED {
                return Ok(row);
            }
        }
    }

    pub fn wait_queries(
        &self,
        ids: &[i64],
        wait: WaitKind,
        deadline: Option<Instant>,
    ) -> Result<Vec<QueryWithFragments>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let ids = ids
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(",");
        loop {
            self.check_wait(deadline, || {
                format!("queries {ids} to be {}", wait.as_str())
            })?;
            let mut params = self.params_for(wait, deadline);
            params.push(("ids", ids.clone()));
            params.push(("with_fragments", "true".to_owned()));
            let (status, rows): (_, Vec<QueryWithFragments>) =
                self.send(Method::GET, "v1/queries", &params, None)?;
            if status != StatusCode::ACCEPTED {
                return Ok(rows);
            }
        }
    }

    pub fn drop_query(&self, id: i64, deadline: Option<Instant>) -> Result<query::Model> {
        let (status, row): (_, query::Model) = self.send(
            Method::DELETE,
            &format!("v1/queries/{id}"),
            &self.params_for(WaitKind::Terminated, deadline),
            None,
        )?;
        if status == StatusCode::ACCEPTED {
            Ok(self.wait_query(id, WaitKind::Terminated, deadline)?.query)
        } else {
            Ok(row)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_wait_carries_the_time_left_until_the_deadline() {
        let params = wait_params(WaitKind::Running, None, None);
        assert_eq!(params, vec![("wait", "running".to_owned())]);

        let params = wait_params(
            WaitKind::Completed,
            Some(Instant::now() + Duration::from_secs(10)),
            None,
        );
        assert_eq!(params[0], ("wait", "completed".to_owned()));
        let timeout_ms: u64 = params[1].1.parse().unwrap();
        assert!(timeout_ms > 9_000 && timeout_ms <= 10_000, "{timeout_ms}");

        let params = wait_params(
            WaitKind::Terminated,
            Some(Instant::now() - Duration::from_secs(1)),
            None,
        );
        assert_eq!(params[1], ("timeout_ms", "0".to_owned()));
    }

    #[test]
    fn a_poll_slice_bounds_every_held_request() {
        let slice = Some(Duration::from_secs(1));
        let params = wait_params(WaitKind::Started, None, slice);
        assert_eq!(params[1], ("timeout_ms", "1000".to_owned()));

        let far = Some(Instant::now() + Duration::from_secs(10));
        let params = wait_params(WaitKind::Completed, far, slice);
        assert_eq!(params[1], ("timeout_ms", "1000".to_owned()));

        let near = Some(Instant::now() + Duration::from_millis(500));
        let params = wait_params(WaitKind::Completed, near, slice);
        let timeout_ms: u64 = params[1].1.parse().unwrap();
        assert!(timeout_ms <= 500, "{timeout_ms}");
    }

    #[test]
    fn a_cancelled_client_refuses_every_wait() {
        let client = Client::new("http://127.0.0.1:8081".parse().unwrap()).unwrap();
        assert!(client.check_wait(None, String::new).is_ok());
        client.cancel();
        let error = client.check_wait(None, String::new).unwrap_err();
        assert!(matches!(
            error.downcast_ref::<ClientError>(),
            Some(ClientError::Cancelled)
        ));
    }

    #[test]
    fn the_coordinators_error_body_is_what_the_user_reads() {
        let body: ErrorBody = serde_json::from_str(
            r#"{"code":2302,"error":"UnknownWorker","message":"worker x not found"}"#,
        )
        .unwrap();
        let error = ClientError::Api {
            status: StatusCode::NOT_FOUND,
            body,
        };
        assert_eq!(error.to_string(), "UnknownWorker: worker x not found");
    }

    #[test]
    fn the_base_url_always_ends_with_a_slash() {
        let client = Client::new("http://127.0.0.1:8081".parse().unwrap()).unwrap();
        assert_eq!(client.base.as_str(), "http://127.0.0.1:8081/");
        let client = Client::new("http://127.0.0.1:8081/api/".parse().unwrap()).unwrap();
        assert_eq!(
            client.base.join("v1/workers").unwrap().as_str(),
            "http://127.0.0.1:8081/api/v1/workers"
        );
    }
}
