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
use model::identifier::SourceId;
use model::source::logical::{self, CreateLogicalSource, DropLogicalSource, GetLogicalSource};
use model::source::physical::{self, CreatePhysicalSource, DropPhysicalSource, GetPhysicalSource};
use model::statement::{Statement, StatementResult};

pub(super) fn routes() -> Router<AppState> {
    Router::new()
        .route(
            "/sources/logical",
            get(list_logical)
                .post(create_logical)
                .delete(remove_logical_many),
        )
        .route(
            "/sources/logical/{name}",
            get(fetch_logical).delete(remove_logical),
        )
        .route(
            "/sources/physical",
            get(list_physical)
                .post(create_physical)
                .delete(remove_physical_many),
        )
        .route(
            "/sources/physical/{id}",
            get(fetch_physical).delete(remove_physical),
        )
}

async fn create_logical(
    State(state): State<AppState>,
    Json(body): Json<CreateLogicalSource>,
) -> Result<(StatusCode, Json<logical::Model>), ApiError> {
    let created = expect!(
        run(&state, Statement::CreateLogicalSource(body)).await?,
        StatementResult::CreatedLogicalSource
    );
    Ok((StatusCode::CREATED, Json(created)))
}

async fn list_logical(
    State(state): State<AppState>,
    Query(filter): Query<GetLogicalSource>,
) -> Result<Json<Vec<logical::Model>>, ApiError> {
    let sources = expect!(
        run(&state, Statement::GetLogicalSource(filter)).await?,
        StatementResult::LogicalSource
    );
    Ok(Json(sources))
}

async fn fetch_logical(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<logical::Model>, ApiError> {
    let filter = GetLogicalSource::all().with_name(name.clone());
    let sources = expect!(
        run(&state, Statement::GetLogicalSource(filter)).await?,
        StatementResult::LogicalSource
    );
    Ok(Json(single(
        sources,
        ErrorCode::UnknownSourceName,
        format!("logical source {name}"),
    )?))
}

async fn remove_logical(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<logical::Model>, ApiError> {
    let filter = DropLogicalSource::all().with_name(name.clone());
    let dropped = expect!(
        run(&state, Statement::DropLogicalSource(filter)).await?,
        StatementResult::DroppedLogicalSources
    );
    Ok(Json(single(
        dropped,
        ErrorCode::UnknownSourceName,
        format!("logical source {name}"),
    )?))
}

async fn remove_logical_many(
    State(state): State<AppState>,
    Query(filter): Query<DropLogicalSource>,
) -> Result<Json<Vec<logical::Model>>, ApiError> {
    let dropped = expect!(
        run(&state, Statement::DropLogicalSource(filter)).await?,
        StatementResult::DroppedLogicalSources
    );
    Ok(Json(dropped))
}

async fn create_physical(
    State(state): State<AppState>,
    Json(body): Json<CreatePhysicalSource>,
) -> Result<(StatusCode, Json<physical::Model>), ApiError> {
    let created = expect!(
        run(&state, Statement::CreatePhysicalSource(body)).await?,
        StatementResult::CreatedPhysicalSource
    );
    Ok((StatusCode::CREATED, Json(created)))
}

async fn list_physical(
    State(state): State<AppState>,
    Query(filter): Query<GetPhysicalSource>,
) -> Result<Json<Vec<physical::Model>>, ApiError> {
    let sources = expect!(
        run(&state, Statement::GetPhysicalSource(filter)).await?,
        StatementResult::PhysicalSources
    );
    Ok(Json(sources))
}

async fn fetch_physical(
    State(state): State<AppState>,
    Path(id): Path<SourceId>,
) -> Result<Json<physical::Model>, ApiError> {
    let filter = GetPhysicalSource::all().with_id(id);
    let sources = expect!(
        run(&state, Statement::GetPhysicalSource(filter)).await?,
        StatementResult::PhysicalSources
    );
    Ok(Json(single(
        sources,
        ErrorCode::UnknownSourceName,
        format!("physical source {id}"),
    )?))
}

async fn remove_physical(
    State(state): State<AppState>,
    Path(id): Path<SourceId>,
) -> Result<Json<physical::Model>, ApiError> {
    let filter = DropPhysicalSource::all().with_id(id);
    let dropped = expect!(
        run(&state, Statement::DropPhysicalSource(filter)).await?,
        StatementResult::DroppedPhysicalSources
    );
    Ok(Json(single(
        dropped,
        ErrorCode::UnknownSourceName,
        format!("physical source {id}"),
    )?))
}

async fn remove_physical_many(
    State(state): State<AppState>,
    Query(filter): Query<DropPhysicalSource>,
) -> Result<Json<Vec<physical::Model>>, ApiError> {
    let dropped = expect!(
        run(&state, Statement::DropPhysicalSource(filter)).await?,
        StatementResult::DroppedPhysicalSources
    );
    Ok(Json(dropped))
}
