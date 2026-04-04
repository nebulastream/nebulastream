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

mod format;
pub mod query;
pub mod request;
pub mod sink;
pub mod source;
pub mod worker;

pub use sea_orm::Set;
use sea_orm::Condition;
use proptest_derive::Arbitrary;
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};
use strum::Display;

pub trait IntoCondition {
    fn into_condition(self) -> Condition;
}

#[derive(
    Arbitrary, Clone, Copy, Debug, PartialEq, Eq, Display, EnumIter, DeriveActiveEnum, Serialize,
    Deserialize,
)]
#[sea_orm(rs_type = "String", db_type = "Text", rename_all = "PascalCase")]
pub enum ConnectorKind {
    Shared,
    Inline,
    Internal,
}

impl Default for ConnectorKind {
    fn default() -> Self {
        Self::Shared
    }
}

