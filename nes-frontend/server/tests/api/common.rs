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

use axum::Router;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use http_body_util::BodyExt;
use model::database::Database;
use model::query::query_fragment::{self, QueryFragmentState};
use sea_orm::sea_query::Expr;
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
use serde_json::{Value, json};
use server::Config;
use std::time::Duration;
use tokio::task::JoinHandle;
use tower::ServiceExt;

pub const HOST: &str = "127.0.0.1:1";

pub struct TestServer {
    pub router: Router,
    pub db: Database,
    coordinator: JoinHandle<()>,
}

impl TestServer {
    pub async fn start() -> Self {
        Self::with_config(Config::default()).await
    }

    pub async fn with_cap(wait_cap: Duration) -> Self {
        Self::with_config(Config {
            wait_cap,
            ..Config::default()
        })
        .await
    }

    pub async fn with_config(config: Config) -> Self {
        let db = Database::for_test().await;
        let (router, coordinator) = server::start(db.clone(), None, None, config);
        Self {
            router,
            db,
            coordinator,
        }
    }

    pub async fn send(&self, request: Request<Body>) -> (StatusCode, Value) {
        let response = self.router.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let body = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes)
                .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()))
        };
        (status, body)
    }

    pub async fn request(
        &self,
        method: Method,
        uri: &str,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let request = Request::builder().method(method).uri(uri);
        let request = match body {
            Some(body) => request
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body.to_string())),
            None => request.body(Body::empty()),
        }
        .unwrap();
        self.send(request).await
    }

    pub async fn get(&self, uri: &str) -> (StatusCode, Value) {
        self.request(Method::GET, uri, None).await
    }

    pub async fn post(&self, uri: &str, body: Value) -> (StatusCode, Value) {
        self.request(Method::POST, uri, Some(body)).await
    }

    pub async fn delete(&self, uri: &str) -> (StatusCode, Value) {
        self.request(Method::DELETE, uri, None).await
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.coordinator.abort();
    }
}

pub fn worker(host: &str) -> Value {
    json!({
        "host_addr": host,
        "data_addr": "127.0.0.1:2",
        "max_operators": 100,
        "peers": [],
        "config": {},
    })
}

pub async fn register_worker(server: &TestServer) {
    let (status, _) = server.post("/v1/workers", worker(HOST)).await;
    assert_eq!(status, StatusCode::CREATED);
}

pub async fn register_query(server: &TestServer) -> i64 {
    register_worker(server).await;
    let (status, _) = server
        .post("/v1/sources/logical", json!({"name": "src", "schema": []}))
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, source) = server
        .post(
            "/v1/sources/physical",
            json!({"logical_source": "src", "host_addr": HOST, "source_type": "Generator"}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{source}");
    let (status, sink) = server
        .post(
            "/v1/sinks",
            json!({"name": "snk", "host_addr": HOST, "sink_type": "Print", "schema": []}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{sink}");
    let statement = json!({
        "statement": {
            "tag": "CreateQuery",
            "sql": "SELECT * FROM src INTO snk",
            "fragments": [{"host_addr": HOST, "plan": [], "num_operators": 1, "has_source": true}],
            "source_ids": [source["id"]],
            "sink_ids": [sink["id"]],
        }
    });
    let (status, created) = server.post("/v1/statements", statement).await;
    assert_eq!(status, StatusCode::OK, "{created}");
    created["CreatedQuery"]["query"]["id"].as_i64().unwrap()
}

pub async fn transition_fragments(db: &Database, target: QueryFragmentState) {
    let steps = match target {
        QueryFragmentState::Started => vec![QueryFragmentState::Started],
        QueryFragmentState::Running => {
            vec![QueryFragmentState::Started, QueryFragmentState::Running]
        }
        QueryFragmentState::Completed => vec![
            QueryFragmentState::Started,
            QueryFragmentState::Running,
            QueryFragmentState::Completed,
        ],
        QueryFragmentState::Stopped => {
            vec![QueryFragmentState::Started, QueryFragmentState::Stopped]
        }
        QueryFragmentState::Failed => vec![QueryFragmentState::Failed],
        QueryFragmentState::Pending => vec![],
    };
    for step in steps {
        query_fragment::Entity::update_many()
            .col_expr(query_fragment::Column::CurrentState, Expr::value(step))
            .filter(query_fragment::Column::CurrentState.ne(step))
            .exec(db)
            .await
            .unwrap();
    }
}
