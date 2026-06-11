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

use sea_orm::{DeriveActiveEnum, EnumIter};
use serde::{Deserialize, Serialize};
use strum::Display;

/// Lifecycle state of a query. Never written by the application
/// directly: a trigger derives it from the states of its fragments
/// after every fragment update, and a second trigger rejects illegal
/// transitions on the way out.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    Display,
    PartialEq,
    Eq,
    EnumIter,
    DeriveActiveEnum,
    Serialize,
    Deserialize,
)]
#[sea_orm(rs_type = "String", db_type = "Text", rename_all = "PascalCase")]
#[strum(serialize_all = "PascalCase")]
pub enum QueryState {
    #[default]
    Pending,
    Registered,
    Running,
    Completed,
    Stopped,
    Failed,
}

impl QueryState {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            QueryState::Completed | QueryState::Stopped | QueryState::Failed
        )
    }
}
