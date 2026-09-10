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

//! Coordinator statements and their typed results.

use crate::Execute;
use crate::database::Database;
use crate::ml_model;
use crate::ml_model::{CreateMlModel, DropMlModel, GetMlModel};
use crate::query;
use crate::query::query_fragment;
use crate::query::{CreateQuery, DropQuery, GetQuery, QueryWithFragments, StopQuery};
use crate::request::Watched;
use crate::sink;
use crate::sink::{CreateSink, DropSink, GetSink};
use crate::source::logical;
use crate::source::logical::{CreateLogicalSource, DropLogicalSource, GetLogicalSource};
use crate::source::physical;
use crate::source::physical::{CreatePhysicalSource, DropPhysicalSource, GetPhysicalSource};
use crate::worker;
use crate::worker::{
    CreateWorker, DropWorker, GetWorker, GetWorkerStatus, GetWorkerVersion, WorkerVersion,
};
use anyhow::Result;
use sea_orm::ConnectionTrait;

/// A CRUD operation that a client issues to the coordinator.
/// Running a variant yields the matching `StatementResult` variant.
#[derive(Clone, Debug, serde::Deserialize)]
#[serde(tag = "tag")]
pub enum ParsedStatement {
    CreateWorker(CreateWorker),
    GetWorker(GetWorker),
    WorkerStatus(GetWorkerStatus),
    /// NOTE: Running it against the catalog returns an error, because only the workers know their versions.
    GetWorkerVersion(GetWorkerVersion),
    DropWorker(DropWorker),
    CreateQuery(CreateQuery),
    /// Already holds its text, so running it does not touch the catalog.
    ExplainQuery {
        explanation: String,
    },
    GetQuery(GetQuery),
    StopQuery(StopQuery),
    DropQuery(DropQuery),
    CreateLogicalSource(CreateLogicalSource),
    GetLogicalSource(GetLogicalSource),
    DropLogicalSource(DropLogicalSource),
    CreatePhysicalSource(CreatePhysicalSource),
    GetPhysicalSource(GetPhysicalSource),
    DropPhysicalSource(DropPhysicalSource),
    CreateSink(CreateSink),
    GetSink(GetSink),
    DropSink(DropSink),
    CreateMlModel(CreateMlModel),
    GetMlModel(GetMlModel),
    DropMlModel(DropMlModel),
}

/// The result of running a statement.
#[derive(Clone, Debug, serde::Serialize)]
pub enum StatementResult {
    CreatedLogicalSource(logical::Model),
    CreatedPhysicalSource(physical::Model),
    CreatedSink(sink::Model),
    CreatedQuery(QueryWithFragments),
    CreatedWorker(worker::Model),
    DroppedLogicalSources(Vec<logical::Model>),
    DroppedPhysicalSources(Vec<physical::Model>),
    DroppedSinks(Vec<sink::Model>),
    StoppedQueries(Vec<query::Model>),
    DroppedQueries(Vec<query::Model>),
    DroppedWorker(Option<worker::Model>),
    LogicalSource(Vec<logical::Model>),
    PhysicalSources(Vec<physical::Model>),
    Sinks(Vec<sink::Model>),
    ExplainedQuery(String),
    Queries(Vec<QueryWithFragments>),
    Watched(Watched),
    Workers(Vec<worker::Model>),
    WorkerStatus(worker::Model, Vec<query_fragment::Model>),
    WorkerVersions(Vec<WorkerVersion>),
    CreatedMlModel(ml_model::Model),
    DroppedMlModels(Vec<ml_model::Model>),
    MlModels(Vec<ml_model::Model>),
}

impl ParsedStatement {
    /// Runs the statement and the triggers that it fires in one transaction.
    /// A statement can write several rows: creating a query writes the query, its fragments and their links,
    /// and takes operator slots on the workers.
    /// A failure part way through leaves none of these rows behind,
    /// and no concurrent statement changes the same slots in between.
    pub async fn execute_with(self, db: &Database) -> Result<StatementResult> {
        let txn = db.begin().await?;
        let response = self.execute_on(&txn).await?;
        txn.commit().await?;
        Ok(response)
    }

    /// Runs the statement on the given connection without opening a transaction of its own.
    pub async fn execute_on(self, conn: &impl ConnectionTrait) -> Result<StatementResult> {
        match self {
            ParsedStatement::CreateWorker(stmt) => {
                Ok(StatementResult::CreatedWorker(stmt.execute(conn).await?))
            }
            ParsedStatement::GetWorker(stmt) => {
                Ok(StatementResult::Workers(stmt.execute(conn).await?))
            }
            ParsedStatement::WorkerStatus(stmt) => {
                let (worker, fragments) = stmt.execute(conn).await?;
                Ok(StatementResult::WorkerStatus(worker, fragments))
            }
            ParsedStatement::GetWorkerVersion(_) => {
                anyhow::bail!("a worker version request cannot be answered from the catalog")
            }
            ParsedStatement::DropWorker(drop) => {
                Ok(StatementResult::DroppedWorker(drop.execute(conn).await?))
            }
            ParsedStatement::CreateQuery(create) => {
                Ok(StatementResult::CreatedQuery(create.execute(conn).await?))
            }
            ParsedStatement::ExplainQuery { explanation } => {
                Ok(StatementResult::ExplainedQuery(explanation))
            }
            ParsedStatement::GetQuery(get) => {
                Ok(StatementResult::Queries(get.execute(conn).await?))
            }
            ParsedStatement::StopQuery(stop) => {
                Ok(StatementResult::StoppedQueries(stop.execute(conn).await?))
            }
            ParsedStatement::DropQuery(drop) => {
                Ok(StatementResult::DroppedQueries(drop.execute(conn).await?))
            }
            ParsedStatement::CreateLogicalSource(create) => Ok(
                StatementResult::CreatedLogicalSource(create.execute(conn).await?),
            ),
            ParsedStatement::GetLogicalSource(get) => {
                Ok(StatementResult::LogicalSource(get.execute(conn).await?))
            }
            ParsedStatement::DropLogicalSource(drop) => Ok(StatementResult::DroppedLogicalSources(
                drop.execute(conn).await?,
            )),
            ParsedStatement::CreatePhysicalSource(create) => Ok(
                StatementResult::CreatedPhysicalSource(create.execute(conn).await?),
            ),
            ParsedStatement::GetPhysicalSource(get) => {
                Ok(StatementResult::PhysicalSources(get.execute(conn).await?))
            }
            ParsedStatement::DropPhysicalSource(drop) => Ok(
                StatementResult::DroppedPhysicalSources(drop.execute(conn).await?),
            ),
            ParsedStatement::CreateSink(create) => {
                Ok(StatementResult::CreatedSink(create.execute(conn).await?))
            }
            ParsedStatement::GetSink(get) => Ok(StatementResult::Sinks(get.execute(conn).await?)),
            ParsedStatement::DropSink(drop) => {
                Ok(StatementResult::DroppedSinks(drop.execute(conn).await?))
            }
            ParsedStatement::CreateMlModel(create) => {
                Ok(StatementResult::CreatedMlModel(create.execute(conn).await?))
            }
            ParsedStatement::GetMlModel(get) => {
                Ok(StatementResult::MlModels(get.execute(conn).await?))
            }
            ParsedStatement::DropMlModel(drop) => {
                Ok(StatementResult::DroppedMlModels(drop.execute(conn).await?))
            }
        }
    }
}
