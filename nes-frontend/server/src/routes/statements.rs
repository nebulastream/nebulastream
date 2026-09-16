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

use super::run_with;
use crate::error::ApiError;
use crate::extract::{Json, Query};
use crate::state::AppState;
use crate::wait::WaitParams;
use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use model::request::StatementInput;
use model::statement::{Statement, StatementResult};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct StatementBody {
    sql: Option<String>,
    statement: Option<Statement>,
}

impl StatementBody {
    fn into_input(self) -> Result<StatementInput, ApiError> {
        match (self.sql, self.statement) {
            (Some(sql), None) => Ok(StatementInput::Sql(sql)),
            (None, Some(statement)) => Ok(StatementInput::Parsed(statement)),
            _ => Err(ApiError::bad_request(
                "the body needs exactly one of `sql` and `statement`",
            )),
        }
    }
}

pub(super) fn routes() -> Router<AppState> {
    Router::new().route("/statements", post(submit))
}

async fn submit(
    State(state): State<AppState>,
    Query(wait): Query<WaitParams>,
    Json(body): Json<StatementBody>,
) -> Result<(StatusCode, Json<StatementResult>), ApiError> {
    let input = body.into_input()?;
    let (status, result) = run_with(
        &state,
        input,
        wait.resolve(state.wait_cap()),
        StatusCode::OK,
    )
    .await?;
    Ok((status, Json(result)))
}
