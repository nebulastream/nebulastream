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

use super::{run, single};
use crate::error::ApiError;
use crate::extract::{Json, Path, Query};
use crate::state::AppState;
use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use model::error::ErrorCode;
use model::query::query_fragment;
use model::statement::{Statement, StatementResult};
use model::worker::endpoint::NetworkAddr;
use model::worker::{
    self, CreateWorker, DropWorker, GetWorker, GetWorkerStatus, GetWorkerVersion, WorkerVersion,
};
use serde::Serialize;

pub(super) fn routes() -> Router<AppState> {
    Router::new()
        .route("/workers", get(list).post(create))
        .route("/workers/versions", get(versions))
        .route("/workers/{host_addr}", get(fetch).delete(remove))
        .route("/workers/{host_addr}/status", get(status))
}

async fn create(
    State(state): State<AppState>,
    Json(body): Json<CreateWorker>,
) -> Result<(StatusCode, Json<worker::Model>), ApiError> {
    let created = expect!(
        run(&state, Statement::CreateWorker(body)).await?,
        StatementResult::CreatedWorker
    );
    Ok((StatusCode::CREATED, Json(created)))
}

async fn list(
    State(state): State<AppState>,
    Query(filter): Query<GetWorker>,
) -> Result<Json<Vec<worker::Model>>, ApiError> {
    let workers = expect!(
        run(&state, Statement::GetWorker(filter)).await?,
        StatementResult::Workers
    );
    Ok(Json(workers))
}

async fn fetch(
    State(state): State<AppState>,
    Path(host_addr): Path<NetworkAddr>,
) -> Result<Json<worker::Model>, ApiError> {
    let filter = GetWorker::all().with_host_addr(host_addr.clone());
    let workers = expect!(
        run(&state, Statement::GetWorker(filter)).await?,
        StatementResult::Workers
    );
    Ok(Json(single(
        workers,
        ErrorCode::UnknownWorker,
        format!("worker {host_addr}"),
    )?))
}

async fn remove(
    State(state): State<AppState>,
    Path(host_addr): Path<NetworkAddr>,
) -> Result<Json<worker::Model>, ApiError> {
    let dropped = expect!(
        run(
            &state,
            Statement::DropWorker(DropWorker::new(host_addr.clone()))
        )
        .await?,
        StatementResult::DroppedWorker
    );
    let dropped = dropped.ok_or_else(|| {
        ApiError::not_found(
            ErrorCode::UnknownWorker,
            format!("worker {host_addr} not found"),
        )
    })?;
    Ok(Json(dropped))
}

#[derive(Debug, Serialize)]
struct WorkerStatus {
    worker: worker::Model,
    fragments: Vec<query_fragment::Model>,
}

async fn status(
    State(state): State<AppState>,
    Path(host_addr): Path<NetworkAddr>,
) -> Result<Json<WorkerStatus>, ApiError> {
    let statement = Statement::WorkerStatus(GetWorkerStatus::new(host_addr));
    match run(&state, statement).await? {
        StatementResult::WorkerStatus(worker, fragments) => {
            Ok(Json(WorkerStatus { worker, fragments }))
        }
        other => Err(ApiError::unexpected(&other)),
    }
}

async fn versions(
    State(state): State<AppState>,
    Query(filter): Query<GetWorkerVersion>,
) -> Result<Json<Vec<WorkerVersion>>, ApiError> {
    let versions = expect!(
        run(&state, Statement::GetWorkerVersion(filter)).await?,
        StatementResult::WorkerVersions
    );
    Ok(Json(versions))
}
