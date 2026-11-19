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

//! Data model of the coordinator catalog.
//! It stores the state of a NebulaStream deployment:
//! workers, sources, sinks, queries, and the query fragments placed on workers.
//!
//! # Data model
//!
//! Each table has a SeaORM entity in its own module.
//! The main entities and their relationships:
//!
//! ```mermaid
//! erDiagram
//!     logical_source  |o--o{ physical_source : binds
//!     worker          ||--o{ physical_source : hosts
//!     worker          ||--o{ sink            : hosts
//!     worker          ||--o{ query_fragment  : hosts
//!     query           ||--o{ query_fragment  : has
//!     query           ||--o{ query_source    : references
//!     query           ||--o{ query_sink      : references
//!     physical_source ||--o{ query_source    : "referenced by"
//!     sink            ||--o{ query_sink      : "referenced by"
//!     query           |o--o{ physical_source : owns
//!     query           |o--o{ sink            : owns
//!     worker          ||..o{ network_link    : "endpoint (no FK)"
//! ```
//!
//! The diagram is not exhaustive.
//! `ml_model`, for example, is a standalone entity with no relationships.
//!
//! # Statements and requests
//!
//! [`statement::ParsedStatement`] is the enum of typed coordinator operations.
//! [`statement::ParsedStatement::execute_with`] runs one operation inside a transaction
//! and returns a [`statement::StatementResult`].
//! A [`request::Request`] holds a [`request::Statement`], which is an operation or raw SQL.
//! Statements on queries can also state when the request should reply.
//!
//! # CRUD operations
//!
//! Each entity module holds the request types that create, read, and drop its rows.
//! Every request implements [`Execute`], which runs it within a connection or transaction.
//! Most read and delete requests also implement [`IntoCondition`] to build their row filter.
//!
//! # Triggers
//!
//! SQL triggers in the `migration` crate enforce the invariants that span rows:
//!
//! - validate query and fragment state transitions (illegal ones abort);
//! - derive a query's state, its start and stop timestamps, and the error of its first failed fragment;
//! - debit a worker's `max_operators` when a fragment is placed,
//!   and credit it back when the fragment ends;
//! - stop a query's fragments when it fails, fail a removed worker's fragments,
//!   and delete a dropped query once all of its fragments have terminated;
//! - reject the drop of a source or sink that a non-terminal query uses.

pub mod database;
pub mod error;
mod format;
pub mod identifier;
pub mod ml_model;
pub mod name;
pub mod query;
pub mod request;
pub mod sink;
pub mod source;
pub mod statement;
pub mod worker;

use anyhow::Result;
use sea_orm::entity::prelude::*;
use sea_orm::{Condition, ConnectionTrait};
use serde::{Deserialize, Serialize};
use strum::Display;

/// Builds the row filter that read and delete requests apply.
/// Each optional field becomes one predicate that is AND-ed into the condition,
/// so an empty request matches all rows, except that a drop of sources or sinks matches only Shared ones.
pub trait IntoCondition {
    fn to_condition(&self) -> Condition;
}

/// Runs a filtered read for any entity with a request that builds a condition.
pub(crate) async fn get_all<E>(
    filter: &impl IntoCondition,
    conn: &impl ConnectionTrait,
) -> Result<Vec<E::Model>, DbErr>
where
    E: EntityTrait,
{
    E::find().filter(filter.to_condition()).all(conn).await
}

/// Deletes every row that matches a request's condition and returns the deleted rows.
pub(crate) async fn drop_all<E>(
    filter: &impl IntoCondition,
    conn: &impl ConnectionTrait,
) -> Result<Vec<E::Model>, DbErr>
where
    E: EntityTrait,
{
    E::delete_many()
        .filter(filter.to_condition())
        .exec_with_returning(conn)
        .await
}

/// Single entry point for every catalog request.
///
/// NOTE: Atomicity is the caller's responsibility.
/// Multistep implementations issue several inserts in sequence and leave partial state on failure if given a raw connection.
/// Callers that need atomicity must pass a transaction handle.
/// Several requests can share one:
/// ```ignore
/// let txn = db.begin().await?;
/// first.execute(&txn).await?;
/// second.execute(&txn).await?;
/// txn.commit().await?;
/// ```
pub trait Execute {
    type Response;
    fn execute(&self, conn: &impl ConnectionTrait) -> impl Future<Output = Result<Self::Response>>;
}

/// Ownership model for a source or sink.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Display, EnumIter, DeriveActiveEnum, Serialize, Deserialize,
)]
#[sea_orm(rs_type = "String", db_type = "Text", rename_all = "PascalCase")]
pub enum ConnectorKind {
    /// User-managed and outlives any single query.
    Shared,
    /// Created for one query and deleted together with it.
    Anonymous,
    /// Like anonymous, but reserved for system-generated connectors such as `NetworkSource`.
    Internal,
}
