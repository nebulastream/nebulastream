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

use crate::common::{HOST, TestServer, register_worker, worker};
use axum::http::StatusCode;
use serde_json::json;

#[tokio::test]
async fn workers_are_created_listed_fetched_and_dropped() {
    let server = TestServer::start().await;

    let (status, created) = server.post("/v1/workers", worker(HOST)).await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    assert_eq!(created["host_addr"], HOST);

    let (status, duplicate) = server.post("/v1/workers", worker(HOST)).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(duplicate["code"], 2305);
    assert_eq!(duplicate["error"], "WorkerAlreadyExists");

    let (status, list) = server.get("/v1/workers").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(list.as_array().unwrap().len(), 1);

    let (status, filtered) = server.get("/v1/workers?host_addr=127.0.0.1:9").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(filtered, json!([]));

    let (status, one) = server.get(&format!("/v1/workers/{HOST}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(one["host_addr"], HOST);

    let (status, missing) = server.get("/v1/workers/127.0.0.1:9").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(missing["code"], 2302);

    let (status, dropped) = server.delete(&format!("/v1/workers/{HOST}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(dropped["desired_state"], "Removed");

    let (status, _) = server.delete("/v1/workers/127.0.0.1:9").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn a_worker_status_lists_its_fragments() {
    let server = TestServer::start().await;
    register_worker(&server).await;

    let (status, body) = server.get(&format!("/v1/workers/{HOST}/status")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["worker"]["host_addr"], HOST);
    assert_eq!(body["fragments"], json!([]));

    let (status, body) = server.get("/v1/workers/127.0.0.1:9/status").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["code"], 2302);
}

#[tokio::test]
async fn versions_of_no_workers_is_an_empty_list() {
    let server = TestServer::start().await;
    let (status, body) = server.get("/v1/workers/versions").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body, json!([]));
}

#[tokio::test]
async fn a_malformed_worker_is_refused() {
    let server = TestServer::start().await;
    let (status, body) = server
        .post("/v1/workers", json!({"host_addr": "not an address"}))
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], 2029);
    let (status, _) = server.get("/v1/workers/not-an-address").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}
