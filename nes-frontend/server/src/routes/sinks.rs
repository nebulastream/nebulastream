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
use model::identifier::SinkId;
use model::sink::{self, CreateSink, DropSink, GetSink};
use model::statement::{Statement, StatementResult};

pub(super) fn routes() -> Router<AppState> {
    Router::new()
        .route("/sinks", get(list).post(create).delete(remove_many))
        .route("/sinks/{id}", get(fetch).delete(remove))
}

async fn create(
    State(state): State<AppState>,
    Json(body): Json<CreateSink>,
) -> Result<(StatusCode, Json<sink::Model>), ApiError> {
    let created = expect!(
        run(&state, Statement::CreateSink(body)).await?,
        StatementResult::CreatedSink
    );
    Ok((StatusCode::CREATED, Json(created)))
}

async fn list(
    State(state): State<AppState>,
    Query(filter): Query<GetSink>,
) -> Result<Json<Vec<sink::Model>>, ApiError> {
    let sinks = expect!(
        run(&state, Statement::GetSink(filter)).await?,
        StatementResult::Sinks
    );
    Ok(Json(sinks))
}

async fn fetch(
    State(state): State<AppState>,
    Path(id): Path<SinkId>,
) -> Result<Json<sink::Model>, ApiError> {
    let sinks = expect!(
        run(&state, Statement::GetSink(GetSink::all().with_id(id))).await?,
        StatementResult::Sinks
    );
    Ok(Json(single(
        sinks,
        ErrorCode::UnknownSinkName,
        format!("sink {id}"),
    )?))
}

async fn remove(
    State(state): State<AppState>,
    Path(id): Path<SinkId>,
) -> Result<Json<sink::Model>, ApiError> {
    let filter = DropSink {
        id: Some(id),
        name: None,
    };
    let dropped = expect!(
        run(&state, Statement::DropSink(filter)).await?,
        StatementResult::DroppedSinks
    );
    Ok(Json(single(
        dropped,
        ErrorCode::UnknownSinkName,
        format!("sink {id}"),
    )?))
}

async fn remove_many(
    State(state): State<AppState>,
    Query(filter): Query<DropSink>,
) -> Result<Json<Vec<sink::Model>>, ApiError> {
    let dropped = expect!(
        run(&state, Statement::DropSink(filter)).await?,
        StatementResult::DroppedSinks
    );
    Ok(Json(dropped))
}
