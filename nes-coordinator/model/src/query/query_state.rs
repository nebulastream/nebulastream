#[cfg(feature = "testing")]
use proptest_derive::Arbitrary;
use sea_orm::{DeriveActiveEnum, EnumIter};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use strum::{Display, EnumString, IntoEnumIterator};

#[cfg_attr(feature = "testing", derive(Arbitrary))]
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    Display,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    EnumIter,
    EnumString,
    DeriveActiveEnum,
    Serialize,
    Deserialize,
)]
#[sea_orm(
    rs_type = "String",
    db_type = "Text",
    rename_all = "PascalCase"
)]
#[strum(serialize_all = "PascalCase")]
pub enum QueryState {
    #[default]
    Pending,
    Planned,
    Registered,
    Running,
    Completed,
    Stopped,
    Failed,
}

#[cfg_attr(feature = "testing", derive(Arbitrary))]
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
#[sea_orm(
    rs_type = "String",
    db_type = "Text",
    rename_all = "PascalCase"
)]
#[strum(serialize_all = "PascalCase")]
pub enum DesiredQueryState {
    #[default]
    Completed,
    Stopped,
}

impl From<String> for QueryState {
    fn from(s: String) -> Self {
        QueryState::from_str(&s).expect("failed to parse QueryState")
    }
}

impl From<super::fragment::FragmentState> for QueryState {
    fn from(state: super::fragment::FragmentState) -> Self {
        use super::fragment::FragmentState;
        match state {
            FragmentState::Pending => Self::Pending,
            FragmentState::Registered => Self::Registered,
            FragmentState::Started | FragmentState::Running => Self::Running,
            FragmentState::Completed => Self::Completed,
            FragmentState::Stopped => Self::Stopped,
            FragmentState::Failed => Self::Failed,
        }
    }
}

impl QueryState {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            QueryState::Completed | QueryState::Stopped | QueryState::Failed
        )
    }

    pub fn transitions(&self) -> Vec<QueryState> {
        match self {
            QueryState::Pending => {
                vec![QueryState::Planned, QueryState::Stopped, QueryState::Failed]
            }
            QueryState::Planned => vec![
                QueryState::Registered,
                QueryState::Stopped,
                QueryState::Failed,
            ],
            QueryState::Registered => vec![
                QueryState::Planned,
                QueryState::Running,
                QueryState::Stopped,
                QueryState::Failed,
            ],
            QueryState::Running => vec![
                QueryState::Planned,
                QueryState::Completed,
                QueryState::Stopped,
                QueryState::Failed,
            ],
            QueryState::Completed | QueryState::Stopped | QueryState::Failed => vec![],
        }
    }

    pub fn invalid_transitions(&self) -> Vec<QueryState> {
        let valid = self.transitions();
        QueryState::iter()
            .filter(|s| *s != *self && !valid.contains(s))
            .collect()
    }
}

#[cfg(feature = "testing")]
impl crate::Generate for Vec<QueryState> {
    fn generate() -> proptest::strategy::BoxedStrategy<Self> {
        use proptest::prelude::*;
        use QueryState::*;
        prop_oneof![
            Just(vec![Pending, Planned, Registered, Running, Completed]),
            Just(vec![Pending, Planned, Registered, Running, Stopped]),
            Just(vec![Pending, Planned, Registered, Running, Failed]),
            Just(vec![Pending, Planned, Registered, Stopped]),
            Just(vec![Pending, Planned, Registered, Failed]),
            Just(vec![Pending, Planned, Stopped]),
            Just(vec![Pending, Planned, Failed]),
            Just(vec![Pending, Stopped]),
            Just(vec![Pending, Failed]),
        ]
        .boxed()
    }
}
