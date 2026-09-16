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

use crate::common::{TestServer, register_query};
use axum::http::StatusCode;
use serde_json::json;

#[tokio::test]
async fn a_query_is_created_read_filtered_and_dropped() {
    let server = TestServer::start().await;
    let id = register_query(&server).await;

    let (status, row) = server
        .get(&format!("/v1/queries/{id}?with_fragments=true"))
        .await;
    assert_eq!(status, StatusCode::OK, "{row}");
    assert_eq!(row["query"]["id"], id);
    assert_eq!(row["query"]["state"], "Pending");
    assert_eq!(row["fragments"].as_array().unwrap().len(), 1);

    let (_, row) = server.get(&format!("/v1/queries/{id}")).await;
    assert_eq!(row["fragments"], json!([]));

    let (status, list) = server
        .get(&format!("/v1/queries?ids={id}&state=Pending"))
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(list.as_array().unwrap().len(), 1);

    let (status, list) = server.get("/v1/queries?state=Running").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(list, json!([]));

    let (status, body) = server.get("/v1/queries?ids=x").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], 2029);

    let (status, missing) = server.get("/v1/queries/999").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(missing["code"], 5000);
    assert_eq!(missing["error"], "QueryNotFound");

    let (status, dropped) = server.delete(&format!("/v1/queries/{id}")).await;
    assert_eq!(status, StatusCode::OK, "{dropped}");
    assert_eq!(dropped["id"], id);

    let (status, _) = server.delete("/v1/queries/999").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn dropping_the_collection_answers_with_every_match() {
    let server = TestServer::start().await;
    let id = register_query(&server).await;
    let (status, dropped) = server.delete("/v1/queries?state=Pending").await;
    assert_eq!(status, StatusCode::OK, "{dropped}");
    assert_eq!(dropped[0]["id"], id);
    let (status, dropped) = server.delete("/v1/queries?state=Completed").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(dropped, json!([]));
}

#[tokio::test]
async fn only_a_select_creates_a_query() {
    let server = TestServer::start().await;

    let (status, body) = server
        .post("/v1/queries", json!({"sql": "CREATE SINK snk TYPE Print"}))
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], 2029);
    let (_, list) = server.get("/v1/queries").await;
    assert_eq!(list, json!([]));

    let (status, body) = server
        .post("/v1/queries", json!({"sql": "SELECT * FROM src INTO snk"}))
        .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(body["code"], 9999);

    let (status, body) = server
        .post(
            "/v1/queries/explain",
            json!({"sql": "SELECT * FROM src INTO snk"}),
        )
        .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(body["code"], 9999);
}
