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

use crate::common::{TestServer, register_query, transition_fragments};
use axum::http::StatusCode;
use model::query::query_fragment::QueryFragmentState;
use std::time::Duration;

#[tokio::test]
async fn a_capped_wait_answers_accepted_with_the_current_row() {
    let server = TestServer::with_cap(Duration::from_millis(50)).await;
    let id = register_query(&server).await;

    let (status, row) = server
        .delete(&format!(
            "/v1/queries/{id}?wait=terminated&timeout_ms=60000"
        ))
        .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{row}");
    assert_eq!(row["id"], id);
    assert_eq!(row["state"], "Pending");

    transition_fragments(&server.db, QueryFragmentState::Stopped).await;
    let (status, row) = server
        .get(&format!("/v1/queries/{id}?wait=terminated"))
        .await;
    assert_eq!(status, StatusCode::OK, "{row}");
    assert_eq!(row["query"]["state"], "Stopped");
}

#[tokio::test]
async fn a_read_can_wait_for_a_state() {
    let server = TestServer::start().await;
    let id = register_query(&server).await;
    transition_fragments(&server.db, QueryFragmentState::Running).await;

    let (status, row) = server.get(&format!("/v1/queries/{id}?wait=running")).await;
    assert_eq!(status, StatusCode::OK, "{row}");
    assert_eq!(row["query"]["state"], "Running");

    let (status, row) = server.get(&format!("/v1/queries/{id}?wait=started")).await;
    assert_eq!(status, StatusCode::OK, "{row}");
}

#[tokio::test]
async fn a_short_client_timeout_ends_the_wait_early() {
    let server = TestServer::start().await;
    let id = register_query(&server).await;
    let (status, row) = server
        .get(&format!("/v1/queries/{id}?wait=terminated&timeout_ms=50"))
        .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{row}");
    assert_eq!(row["query"]["state"], "Pending");
}

#[tokio::test]
async fn an_unknown_wait_is_refused() {
    let server = TestServer::start().await;
    let (status, body) = server.get("/v1/queries?wait=soon").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], 2029);
    let (status, _) = server.get("/v1/queries?timeout_ms=abc").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}
