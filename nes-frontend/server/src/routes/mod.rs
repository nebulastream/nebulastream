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

macro_rules! expect {
    ($result:expr, $variant:path) => {
        match $result {
            $variant(inner) => inner,
            other => return Err($crate::error::ApiError::unexpected(&other)),
        }
    };
}

mod models;
mod queries;
mod sinks;
mod sources;
mod statements;
mod workers;

use crate::error::ApiError;
use crate::extract::Json;
use crate::state::{AppState, Outcome};
use axum::Router;
use axum::extract::Request;
use axum::http::StatusCode;
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::routing::get;
use model::error::ErrorCode;
use model::request::{StatementInput, Wait};
use model::statement::{Statement, StatementResult};
use std::fmt::Display;
use std::time::Instant;
use tracing::info;

#[must_use]
pub fn router(state: AppState) -> Router {
    Router::new()
        .nest("/v1", api())
        .layer(middleware::from_fn(log_request))
        .with_state(state)
}

fn api() -> Router<AppState> {
    Router::new()
        .route("/health", get(health))
        .merge(statements::routes())
        .merge(workers::routes())
        .merge(queries::routes())
        .merge(sources::routes())
        .merge(sinks::routes())
        .merge(models::routes())
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "ok" }))
}

async fn log_request(request: Request, next: Next) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_owned();
    let started = Instant::now();
    let response = next.run(request).await;
    info!(
        %method,
        path,
        status = response.status().as_u16(),
        elapsed = ?started.elapsed(),
        "handled"
    );
    response
}

async fn run(state: &AppState, statement: Statement) -> Result<StatementResult, ApiError> {
    let outcome = state
        .submit(StatementInput::Parsed(statement), Wait::None)
        .await?;
    Ok(outcome.split(StatusCode::OK).1)
}

async fn run_with(
    state: &AppState,
    input: StatementInput,
    wait: Wait,
    success: StatusCode,
) -> Result<(StatusCode, StatementResult), ApiError> {
    let outcome: Outcome = state.submit(input, wait).await?;
    Ok(outcome.split(success))
}

fn single<T>(rows: Vec<T>, code: ErrorCode, what: impl Display) -> Result<T, ApiError> {
    rows.into_iter()
        .next()
        .ok_or_else(|| ApiError::not_found(code, format!("{what} not found")))
}
