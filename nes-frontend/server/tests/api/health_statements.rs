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

use crate::common::TestServer;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use serde_json::json;

#[tokio::test]
async fn health_answers_ok() {
    let server = TestServer::start().await;
    let (status, body) = server.get("/v1/health").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({"status": "ok"}));
}

#[tokio::test]
async fn a_parsed_statement_answers_with_the_tagged_result() {
    let server = TestServer::start().await;
    let (status, body) = server
        .post("/v1/statements", json!({"statement": {"tag": "GetWorker"}}))
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({"Workers": []}));
}

#[tokio::test]
async fn sql_needs_a_planner() {
    let server = TestServer::start().await;
    let (status, body) = server
        .post("/v1/statements", json!({"sql": "SELECT 1"}))
        .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(body["code"], 9999);
    assert_eq!(body["error"], "UnknownException");
    assert!(
        body["message"]
            .as_str()
            .unwrap()
            .contains("no SQL planner configured")
    );
}

#[tokio::test]
async fn the_body_needs_exactly_one_input() {
    let server = TestServer::start().await;
    for body in [
        json!({}),
        json!({"sql": "SELECT 1", "statement": {"tag": "GetWorker"}}),
        json!({"query": "SELECT 1"}),
        json!({"statement": {"tag": "NoSuchStatement"}}),
    ] {
        let (status, answer) = server.post("/v1/statements", body.clone()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(answer["code"], 2029);
        assert_eq!(answer["error"], "InvalidStatement");
    }
}

#[tokio::test]
async fn a_body_without_a_json_content_type_is_refused() {
    let server = TestServer::start().await;
    let request = Request::builder()
        .method(Method::POST)
        .uri("/v1/statements")
        .body(Body::from(r#"{"sql": "SELECT 1"}"#))
        .unwrap();
    let (status, body) = server.send(request).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], 2029);
}
