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
use model::ml_model::{self, CreateMlModel, DropMlModel, GetMlModel};
use model::statement::{Statement, StatementResult};

pub(super) fn routes() -> Router<AppState> {
    Router::new()
        .route("/models", get(list).post(create).delete(remove_many))
        .route("/models/{name}", get(fetch).delete(remove))
}

async fn create(
    State(state): State<AppState>,
    Json(body): Json<CreateMlModel>,
) -> Result<(StatusCode, Json<ml_model::Model>), ApiError> {
    let created = expect!(
        run(&state, Statement::CreateMlModel(body)).await?,
        StatementResult::CreatedMlModel
    );
    Ok((StatusCode::CREATED, Json(created)))
}

async fn list(
    State(state): State<AppState>,
    Query(filter): Query<GetMlModel>,
) -> Result<Json<Vec<ml_model::Model>>, ApiError> {
    let models = expect!(
        run(&state, Statement::GetMlModel(filter)).await?,
        StatementResult::MlModels
    );
    Ok(Json(models))
}

async fn fetch(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<ml_model::Model>, ApiError> {
    let filter = GetMlModel::all().with_name(name.clone());
    let models = expect!(
        run(&state, Statement::GetMlModel(filter)).await?,
        StatementResult::MlModels
    );
    Ok(Json(single(
        models,
        ErrorCode::UnknownModelName,
        format!("model {name}"),
    )?))
}

async fn remove(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<ml_model::Model>, ApiError> {
    let filter = DropMlModel::all().with_name(name.clone());
    let dropped = expect!(
        run(&state, Statement::DropMlModel(filter)).await?,
        StatementResult::DroppedMlModels
    );
    Ok(Json(single(
        dropped,
        ErrorCode::UnknownModelName,
        format!("model {name}"),
    )?))
}

async fn remove_many(
    State(state): State<AppState>,
    Query(filter): Query<DropMlModel>,
) -> Result<Json<Vec<ml_model::Model>>, ApiError> {
    let dropped = expect!(
        run(&state, Statement::DropMlModel(filter)).await?,
        StatementResult::DroppedMlModels
    );
    Ok(Json(dropped))
}
