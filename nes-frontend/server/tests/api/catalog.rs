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

use crate::common::{HOST, TestServer, register_worker};
use axum::http::StatusCode;
use serde_json::json;

#[tokio::test]
async fn logical_sources_round_trip() {
    let server = TestServer::start().await;
    let source = json!({"name": "src", "schema": [{"name": "value", "dataType": [8, false]}]});

    let (status, created) = server.post("/v1/sources/logical", source.clone()).await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    assert_eq!(created["name"], "src");

    let (status, duplicate) = server.post("/v1/sources/logical", source).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(duplicate["code"], 2031);

    let (status, list) = server.get("/v1/sources/logical").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(list.as_array().unwrap().len(), 1);

    let (status, one) = server.get("/v1/sources/logical/src").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(one["name"], "src");

    let (status, missing) = server.get("/v1/sources/logical/nope").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(missing["code"], 2030);

    let (status, dropped) = server.delete("/v1/sources/logical/src").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(dropped["name"], "src");

    let (status, _) = server.delete("/v1/sources/logical/src").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn physical_sources_and_sinks_hang_off_a_worker() {
    let server = TestServer::start().await;
    register_worker(&server).await;
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
    let source_id = source["id"].as_i64().unwrap();

    let (status, list) = server.get("/v1/sources/physical?logical_source=src").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(list.as_array().unwrap().len(), 1);

    let (status, one) = server
        .get(&format!("/v1/sources/physical/{source_id}"))
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(one["id"], source_id);

    let (status, dangling) = server
        .post(
            "/v1/sources/physical",
            json!({"logical_source": "nope", "host_addr": HOST, "source_type": "Generator"}),
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{dangling}");
    assert_eq!(dangling["code"], 2030);

    let (status, dropped) = server
        .delete(&format!("/v1/sources/physical/{source_id}"))
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(dropped["id"], source_id);
    let (status, _) = server
        .get(&format!("/v1/sources/physical/{source_id}"))
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, sink) = server
        .post(
            "/v1/sinks",
            json!({"name": "snk", "host_addr": HOST, "sink_type": "Print", "schema": []}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{sink}");
    let sink_id = sink["id"].as_i64().unwrap();

    let (status, list) = server.get("/v1/sinks?name=snk").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(list.as_array().unwrap().len(), 1);

    let (status, one) = server.get(&format!("/v1/sinks/{sink_id}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(one["name"], "snk");

    let (status, dangling) = server
        .post(
            "/v1/sinks",
            json!({"name": "lost", "host_addr": "127.0.0.1:9", "sink_type": "Print", "schema": []}),
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{dangling}");
    assert_eq!(dangling["code"], 2302);

    let (status, dropped) = server.delete(&format!("/v1/sinks/{sink_id}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(dropped["id"], sink_id);
    let (status, missing) = server.get(&format!("/v1/sinks/{sink_id}")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(missing["code"], 2033);
}

#[tokio::test]
async fn models_round_trip() {
    let server = TestServer::start().await;
    let model =
        json!({"name": "m", "path": "/models/m.onnx", "input_schema": [], "output_schema": []});

    let (status, created) = server.post("/v1/models", model.clone()).await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    assert_eq!(created["name"], "m");

    let (status, duplicate) = server.post("/v1/models", model).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(duplicate["code"], 2041);

    let (status, list) = server.get("/v1/models").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(list.as_array().unwrap().len(), 1);

    let (status, one) = server.get("/v1/models/m").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(one["path"], "/models/m.onnx");

    let (status, dropped) = server.delete("/v1/models/m").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(dropped["name"], "m");

    let (status, missing) = server.get("/v1/models/m").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(missing["code"], 2040);
}
