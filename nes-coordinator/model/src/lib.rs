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

//! Data model of the coordinator catalog. It stores the state of a
//! NebulaStream deployment: workers, sources, sinks, queries, and the query
//! fragments placed on workers.
//!
//! # Data model
//!
//! Each entity is one module under `src/`, with a SeaORM entity in its
//! `mod.rs`. The main entities and their relationships:
//!
//! ```mermaid
//! erDiagram
//!     logical_source  ||--o{ physical_source : binds
//!     worker          ||--o{ physical_source : hosts
//!     worker          ||--o{ sink            : hosts
//!     worker          ||--o{ query_fragment  : hosts
//!     query           ||--o{ query_fragment  : has
//!     query           ||--o{ query_source    : references
//!     query           ||--o{ query_sink      : references
//!     physical_source ||--o{ query_source    : "referenced by"
//!     sink            ||--o{ query_sink      : "referenced by"
//!     worker          ||..o{ network_link    : "endpoint (no FK)"
//! ```
//!
//! The diagram is not exhaustive: `ml_model`, for example, is a standalone
//! entity with no relationships, and more catalog objects will be added over
//! time.
//!
//! # Statements and requests
//!
//! [`statement::Statement`] is the top-level enum of catalog operations. Each
//! variant wraps a typed request. [`statement::Statement::execute_with`] runs
//! one statement inside a transaction and returns a
//! [`statement::StatementResult`]. A [`request::Request`] wraps a statement (or
//! raw SQL) together with the wait options the coordinator applies before it
//! replies.
//!
//! # CRUD operations
//!
//! Each entity module has sibling `create.rs` / `get.rs` / `drop.rs` files with
//! its request types. Every request implements [`Execute`], which runs it
//! against a connection or transaction. Read and delete requests also implement
//! [`IntoCondition`] to build their row filter, so an empty request matches all
//! rows.
//!
//! # Triggers
//!
//! Invariants that span rows are enforced in the database, by SQL triggers
//! defined in the `migration` crate. They:
//!
//! - validate query and query-fragment state transitions (illegal ones abort);
//! - derive a query's state, start/stop timestamps, and per-host errors from
//!   its fragments;
//! - debit a worker's `max_operators` when a fragment is placed and credit it
//!   back when the fragment ends;
//! - stop a query's fragments when it fails, fail a removed worker's fragments,
//!   and delete query-owned sources and sinks when their last query goes away.
//!
//! Worker state is not validated: the controller writes the observed state and
//! the model trusts it.

pub mod database;
pub mod error;
mod format;
pub mod identifier;
pub mod ml_model;
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

/// Builds the row-filter applied by read and delete requests. Each
/// optional field becomes one predicate AND-ed into the condition, so
/// an empty request matches all rows.
pub trait IntoCondition {
    fn to_condition(&self) -> Condition;
}

/// Runs a filtered read for any entity whose request builds a condition.
/// Collapses the otherwise-identical read bodies in the entity modules
/// into one place, so cross-cutting changes live here.
pub(crate) async fn get_all<E>(
    filter: &impl IntoCondition,
    conn: &impl ConnectionTrait,
) -> Result<Vec<E::Model>, DbErr>
where
    E: EntityTrait,
{
    E::find().filter(filter.to_condition()).all(conn).await
}

/// Reads and then deletes every row matching a request's condition,
/// returning the deleted rows. Delete-with-returning is not exposed by the
/// ORM, so this is two statements; centralizing them keeps the drop paths
/// identical and makes a single-statement version a local change.
pub(crate) async fn drop_all<E>(
    filter: &impl IntoCondition,
    conn: &impl ConnectionTrait,
) -> Result<Vec<E::Model>, DbErr>
where
    E: EntityTrait,
{
    let condition = filter.to_condition();
    let rows = E::find().filter(condition.clone()).all(conn).await?;
    E::delete_many().filter(condition).exec(conn).await?;
    Ok(rows)
}

/// Single entry point for every catalog request. Implementors take any
/// connection-like handle and produce a typed response.
///
/// Atomicity is the caller's responsibility: multistep implementations
/// issue several inserts in sequence and will leave partial state on
/// failure if handed a raw connection. Callers that need atomicity must
/// pass a transaction handle. Several requests can share one:
///
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

/// Ownership model for a source or sink. Shared connectors are
/// user-managed and outlive any single query. Anonymous connectors are
/// owned by exactly one query and cleaned up with it. Internal behaves
/// like anonymous but is reserved for system-generated rows that are
/// hidden from user-facing listings.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Display, EnumIter, DeriveActiveEnum, Serialize, Deserialize,
)]
#[sea_orm(rs_type = "String", db_type = "Text", rename_all = "PascalCase")]
pub enum ConnectorKind {
    Shared,
    Anonymous,
    Internal,
}
