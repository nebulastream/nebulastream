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
use axum::http::StatusCode;
use model::source::logical::CreateLogicalSource;
use model::statement::Statement;
use serde_json::json;

fn logical_source(name: &str) -> Statement {
    Statement::CreateLogicalSource(CreateLogicalSource {
        name: name.to_owned(),
        schema: json!([]),
        if_not_exists: false,
    })
}

#[tokio::test]
async fn bootstrap_statements_are_applied_before_the_router_is_handed_out() {
    let server = TestServer::with_bootstrap(vec![logical_source("src")])
        .await
        .unwrap();
    let (status, list) = server.get("/v1/sources/logical").await;
    assert_eq!(status, StatusCode::OK, "{list}");
    assert_eq!(list.as_array().unwrap().len(), 1);
    assert_eq!(list[0]["name"], "src");
}

#[tokio::test]
async fn a_failing_bootstrap_statement_fails_the_start() {
    let error = TestServer::with_bootstrap(vec![logical_source("src"), logical_source("src")])
        .await
        .err()
        .unwrap();
    let chain = format!("{error:#}");
    assert!(chain.contains("bootstrap statement failed"), "{chain}");
    assert!(chain.contains("SourceAlreadyExists"), "{chain}");
}
