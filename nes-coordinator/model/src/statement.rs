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

use crate::Execute;
use crate::database::Database;
use crate::query;
use crate::query::fragment;
use crate::query::{CreateQuery, DropQuery, GetQuery};
use crate::sink;
use crate::sink::{CreateSink, DropSink, GetSink};
use crate::source::logical;
use crate::source::logical::{CreateLogicalSource, DropLogicalSource, GetLogicalSource};
use crate::source::physical;
use crate::source::physical::{CreatePhysicalSource, DropPhysicalSource, GetPhysicalSource};
use crate::worker;
use crate::worker::{CreateWorker, DropWorker, GetWorker, GetWorkerStatus};
use anyhow::Result;
use sea_orm::ConnectionTrait;

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(tag = "tag")]
pub enum Statement {
    CreateWorker(CreateWorker),
    GetWorker(GetWorker),
    WorkerStatus(GetWorkerStatus),
    DropWorker(DropWorker),
    CreateQuery(CreateQuery),
    ExplainQuery(String),
    GetQuery(GetQuery),
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
}

#[derive(Clone, Debug, serde::Serialize)]
pub enum StatementResult {
    CreatedLogicalSource(logical::Model),
    CreatedPhysicalSource(physical::Model),
    CreatedSink(sink::Model),
    CreatedQuery(query::Model, Vec<fragment::Model>),
    CreatedWorker(worker::Model),
    DroppedLogicalSources(Vec<logical::Model>),
    DroppedPhysicalSources(Vec<physical::Model>),
    DroppedSinks(Vec<sink::Model>),
    DroppedQueries(Vec<query::Model>),
    DroppedWorker(Option<worker::Model>),
    LogicalSource(Vec<logical::Model>),
    PhysicalSources(Vec<physical::Model>),
    Sinks(Vec<sink::Model>),
    ExplainedQuery(String),
    Queries(Vec<(query::Model, Vec<fragment::Model>)>),
    Workers(Vec<worker::Model>),
    WorkerStatus(worker::Model, Vec<fragment::Model>),
}

impl Statement {
    pub async fn execute_on(self, conn: &impl ConnectionTrait) -> Result<StatementResult> {
        match self {
            Statement::CreateWorker(req) => {
                Ok(StatementResult::CreatedWorker(req.execute(conn).await?))
            }
            Statement::GetWorker(req) => Ok(StatementResult::Workers(req.execute(conn).await?)),
            Statement::WorkerStatus(req) => {
                let (worker, fragments) = req.execute(conn).await?;
                Ok(StatementResult::WorkerStatus(worker, fragments))
            }
            Statement::DropWorker(req) => {
                Ok(StatementResult::DroppedWorker(req.execute(conn).await?))
            }
            Statement::CreateQuery(req) => {
                let (query, fragments) = req.execute(conn).await?;
                Ok(StatementResult::CreatedQuery(query, fragments))
            }
            Statement::ExplainQuery(explanation) => {
                Ok(StatementResult::ExplainedQuery(explanation))
            }
            Statement::GetQuery(req) => Ok(StatementResult::Queries(req.execute(conn).await?)),
            Statement::DropQuery(req) => {
                Ok(StatementResult::DroppedQueries(req.execute(conn).await?))
            }
            Statement::CreateLogicalSource(req) => Ok(StatementResult::CreatedLogicalSource(
                req.execute(conn).await?,
            )),
            Statement::GetLogicalSource(req) => {
                Ok(StatementResult::LogicalSource(req.execute(conn).await?))
            }
            Statement::DropLogicalSource(req) => Ok(StatementResult::DroppedLogicalSources(
                req.execute(conn).await?,
            )),
            Statement::CreatePhysicalSource(req) => Ok(StatementResult::CreatedPhysicalSource(
                req.execute(conn).await?,
            )),
            Statement::GetPhysicalSource(req) => {
                Ok(StatementResult::PhysicalSources(req.execute(conn).await?))
            }
            Statement::DropPhysicalSource(req) => Ok(StatementResult::DroppedPhysicalSources(
                req.execute(conn).await?,
            )),
            Statement::CreateSink(req) => {
                Ok(StatementResult::CreatedSink(req.execute(conn).await?))
            }
            Statement::GetSink(req) => Ok(StatementResult::Sinks(req.execute(conn).await?)),
            Statement::DropSink(req) => Ok(StatementResult::DroppedSinks(req.execute(conn).await?)),
        }
    }

    pub async fn execute_with(self, db: &Database) -> Result<StatementResult> {
        let txn = db.begin().await?;
        let response = self.execute_on(&txn).await?;
        txn.commit().await?;
        Ok(response)
    }
}
