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

/// Lifecycle state of a query.
/// A trigger derives it from the states of the query's fragments after every fragment state change,
/// so the trigger that rejects illegal fragment transitions also bounds the query's transitions.
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
    Started,
    Running,
    Completed,
    Stopped,
    Failed,
}

impl QueryState {
    pub const TERMINAL: [Self; 3] = [Self::Completed, Self::Stopped, Self::Failed];

    pub fn is_terminal(&self) -> bool {
        Self::TERMINAL.contains(self)
    }

    /// Terminated without completing.
    pub fn ended_early(&self) -> bool {
        matches!(self, Self::Stopped | Self::Failed)
    }
}
