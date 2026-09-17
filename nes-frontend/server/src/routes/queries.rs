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

use super::{run_with, single};
use crate::error::ApiError;
use crate::extract::{Json, Path, Query};
use crate::state::AppState;
use crate::wait::WaitParams;
use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use model::error::ErrorCode;
use model::identifier::QueryId;
use model::query::query_state::QueryState;
use model::query::{self, DropQuery, GetQuery, QueryWithFragments};
use model::request::{StatementInput, Wait};
use model::statement::{Statement, StatementResult};
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Deserialize)]
struct QueryFilter {
    ids: Option<String>,
    name: Option<String>,
    state: Option<QueryState>,
    #[serde(default)]
    with_fragments: bool,
}

impl QueryFilter {
    fn into_get(self) -> Result<GetQuery, ApiError> {
        let ids =
            self.ids
                .map(|ids| {
                    ids.split(',')
                        .map(|id| {
                            id.trim().parse::<i64>().map(QueryId::new).map_err(|_| {
                                ApiError::bad_request(format!("invalid query id '{id}'"))
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()
                })
                .transpose()?;
        Ok(GetQuery {
            ids,
            name: self.name,
            state: self.state,
            with_fragments: self.with_fragments,
        })
    }
}

#[derive(Debug, Default, Deserialize)]
struct SingleParams {
    #[serde(default)]
    with_fragments: bool,
}

#[derive(Debug, Deserialize)]
struct SqlBody {
    sql: String,
}

#[derive(Debug, Serialize)]
struct Explanation {
    explanation: String,
}

fn is_query(sql: &str) -> bool {
    sql.split_whitespace()
        .next()
        .is_some_and(|word| word.eq_ignore_ascii_case("select"))
}

fn explained(sql: &str) -> String {
    let already = sql
        .split_whitespace()
        .next()
        .is_some_and(|word| word.eq_ignore_ascii_case("explain"));
    if already {
        sql.to_owned()
    } else {
        format!("EXPLAIN {sql}")
    }
}

pub(super) fn routes() -> Router<AppState> {
    Router::new()
        .route("/queries", get(list).post(create).delete(remove_many))
        .route("/queries/explain", post(explain))
        .route("/queries/{id}", get(fetch).delete(remove))
}

async fn create(
    State(state): State<AppState>,
    Query(wait): Query<WaitParams>,
    Json(body): Json<SqlBody>,
) -> Result<(StatusCode, Json<QueryWithFragments>), ApiError> {
    if !is_query(&body.sql) {
        return Err(ApiError::bad_request(
            "only a SELECT statement creates a query; submit other SQL to /v1/statements",
        ));
    }
    let (status, result) = run_with(
        &state,
        StatementInput::Sql(body.sql),
        wait.resolve(state.wait_cap()),
        StatusCode::CREATED,
    )
    .await?;
    let created = expect!(result, StatementResult::CreatedQuery);
    Ok((status, Json(created)))
}

async fn list(
    State(state): State<AppState>,
    Query(filter): Query<QueryFilter>,
    Query(wait): Query<WaitParams>,
) -> Result<(StatusCode, Json<Vec<QueryWithFragments>>), ApiError> {
    let statement = Statement::GetQuery(filter.into_get()?);
    let (status, result) = run_with(
        &state,
        StatementInput::Parsed(statement),
        wait.resolve(state.wait_cap()),
        StatusCode::OK,
    )
    .await?;
    let rows = expect!(result, StatementResult::Queries);
    Ok((status, Json(rows)))
}

async fn fetch(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Query(params): Query<SingleParams>,
    Query(wait): Query<WaitParams>,
) -> Result<(StatusCode, Json<QueryWithFragments>), ApiError> {
    let mut filter = GetQuery::all().with_id(QueryId::new(id));
    filter.with_fragments = params.with_fragments;
    let (status, result) = run_with(
        &state,
        StatementInput::Parsed(Statement::GetQuery(filter)),
        wait.resolve(state.wait_cap()),
        StatusCode::OK,
    )
    .await?;
    let rows = expect!(result, StatementResult::Queries);
    let row = single(rows, ErrorCode::QueryNotFound, format!("query {id}"))?;
    Ok((status, Json(row)))
}

async fn remove(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Query(wait): Query<WaitParams>,
) -> Result<(StatusCode, Json<query::Model>), ApiError> {
    let filter = GetQuery::all().with_id(QueryId::new(id));
    let statement = Statement::DropQuery(DropQuery::all().with_filters(filter));
    let (status, result) = run_with(
        &state,
        StatementInput::Parsed(statement),
        wait.resolve(state.wait_cap()),
        StatusCode::OK,
    )
    .await?;
    let rows = expect!(result, StatementResult::DroppedQueries);
    let row = single(rows, ErrorCode::QueryNotFound, format!("query {id}"))?;
    Ok((status, Json(row)))
}

async fn remove_many(
    State(state): State<AppState>,
    Query(filter): Query<QueryFilter>,
    Query(wait): Query<WaitParams>,
) -> Result<(StatusCode, Json<Vec<query::Model>>), ApiError> {
    let statement = Statement::DropQuery(DropQuery::all().with_filters(filter.into_get()?));
    let (status, result) = run_with(
        &state,
        StatementInput::Parsed(statement),
        wait.resolve(state.wait_cap()),
        StatusCode::OK,
    )
    .await?;
    let rows = expect!(result, StatementResult::DroppedQueries);
    Ok((status, Json(rows)))
}

async fn explain(
    State(state): State<AppState>,
    Json(body): Json<SqlBody>,
) -> Result<Json<Explanation>, ApiError> {
    let (_, result) = run_with(
        &state,
        StatementInput::Sql(explained(&body.sql)),
        Wait::None,
        StatusCode::OK,
    )
    .await?;
    let explanation = expect!(result, StatementResult::ExplainedQuery);
    Ok(Json(Explanation { explanation }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_are_parsed_from_a_comma_separated_list() {
        let filter = QueryFilter {
            ids: Some("1, 2,3".to_owned()),
            ..QueryFilter::default()
        };
        let get = filter.into_get().unwrap();
        assert_eq!(
            get.ids,
            Some(vec![QueryId::new(1), QueryId::new(2), QueryId::new(3)])
        );
        let filter = QueryFilter {
            ids: Some("1,x".to_owned()),
            ..QueryFilter::default()
        };
        assert_eq!(
            filter.into_get().err().unwrap().status,
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn only_a_select_is_a_query() {
        assert!(is_query("SELECT * FROM src INTO snk"));
        assert!(is_query("  select a from src into snk"));
        assert!(!is_query("CREATE SINK snk TYPE Print"));
        assert!(!is_query(""));
    }

    #[test]
    fn explain_is_added_once() {
        assert_eq!(explained("SELECT 1"), "EXPLAIN SELECT 1");
        assert_eq!(explained("explain SELECT 1"), "explain SELECT 1");
    }
}
