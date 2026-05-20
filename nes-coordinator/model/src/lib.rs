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

pub mod database;
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

/// Single entry point for every catalog request. Implementors take any
/// connection-like handle and produce a typed response.
///
/// Atomicity is the caller's responsibility: multi-step implementations
/// issue several inserts in sequence and will leave partial state on
/// failure if handed a raw connection. Callers that need atomicity must
/// pass a transaction handle.
pub trait Execute {
    type Response;
    fn execute(&self, conn: &impl ConnectionTrait) -> impl Future<Output = Result<Self::Response>>;
}

/// Ownership model for a source or sink. Shared connectors are
/// user-managed and outlive any single query. Inline connectors are
/// owned by exactly one query and cleaned up with it. Internal behaves
/// like inline but is reserved for system-generated rows that are
/// hidden from user-facing listings.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Display, EnumIter, DeriveActiveEnum, Serialize, Deserialize,
)]
#[sea_orm(rs_type = "String", db_type = "Text", rename_all = "PascalCase")]
pub enum ConnectorKind {
    Shared,
    Inline,
    Internal,
}
