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

//! The query-query_fragment entity and its lifecycle types.

use crate::error::CodedError;
use crate::identifier::QueryFragmentId;
use crate::identifier::QueryId;
use crate::worker::endpoint::NetworkAddr;
use sea_orm::Set;
use sea_orm::entity::prelude::*;
use sea_orm::{ActiveValue, FromJsonQueryResult, NotSet};
use serde::{Deserialize, Serialize};
use strum::Display;
use thiserror::Error;

/// Why a query_fragment failed: an error inside the worker, or a failure to reach it.
#[derive(Debug, Clone, Error, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
pub enum QueryFragmentError {
    #[error("Internal worker error; code: {}, msg: {}, stacktrace: {}", .0.code as u16, .0.msg, .0.trace)]
    Internal(CodedError),
    #[error("Worker communication error: {msg}")]
    Transport { msg: String },
}

/// The failure that ended a query: the first query_fragment that failed, and where it ran.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
pub struct QueryError {
    pub host_addr: NetworkAddr,
    pub error: QueryFragmentError,
}

impl std::fmt::Display for QueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.host_addr, self.error)
    }
}

/// One execution unit of a query, placed on a single worker.
/// The worker's `max_operators` is decremented on insert
/// and restored automatically once the query_fragment reaches a terminal state.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, DeriveEntityModel)]
#[sea_orm(table_name = "query_fragment")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = true)]
    pub id: QueryFragmentId,
    pub query_id: QueryId,
    pub host_addr: NetworkAddr,
    #[sea_orm(column_type = "VarBinary(StringLen::None)")]
    #[serde(skip)]
    pub plan: Vec<u8>,
    pub num_operators: i32,
    pub has_source: bool,
    pub current_state: QueryFragmentState,
    pub desired_state: DesiredQueryFragmentState,
    pub start_timestamp: Option<chrono::DateTime<chrono::Utc>>,
    pub stop_timestamp: Option<chrono::DateTime<chrono::Utc>>,
    #[sea_orm(column_type = "JsonBinary")]
    pub error: Option<QueryFragmentError>,
    pub last_observed_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "crate::query::Entity",
        from = "Column::QueryId",
        to = "crate::query::Column::Id",
        on_update = "Restrict",
        on_delete = "Cascade"
    )]
    Query,
    #[sea_orm(
        belongs_to = "crate::worker::Entity",
        from = "Column::HostAddr",
        to = "crate::worker::Column::HostAddr",
        on_update = "Restrict",
        on_delete = "Restrict"
    )]
    Worker,
}

impl Related<crate::query::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Query.def()
    }
}

impl Related<crate::worker::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Worker.def()
    }
}

#[async_trait::async_trait]
impl ActiveModelBehavior for ActiveModel {
    /// Reject saves whose declared state would leave required columns unset
    /// (running needs a start timestamp; terminal needs a stop timestamp; failed also needs an error).
    /// Avoids inserting rows with surprising NULLs.
    async fn before_save<C>(self, _db: &C, _insert: bool) -> Result<Self, DbErr>
    where
        C: ConnectionTrait,
    {
        let state = match &self.current_state {
            ActiveValue::Set(s) => *s,
            _ => return Ok(self),
        };
        let ensure = |cond: bool, msg: &str| -> Result<(), DbErr> {
            if cond {
                Ok(())
            } else {
                Err(DbErr::Custom(msg.into()))
            }
        };
        // A nullable column counts as provided only when this save writes a value into it.
        // Checking only whether the column is set would accept `Set(None)`
        // and persist a NULL that the state forbids.
        match state {
            QueryFragmentState::Pending | QueryFragmentState::Started => {}
            QueryFragmentState::Running => ensure(
                matches!(self.start_timestamp, ActiveValue::Set(Some(_))),
                "start_timestamp must be set upon transitioning to running",
            )?,
            QueryFragmentState::Completed | QueryFragmentState::Stopped => ensure(
                matches!(self.stop_timestamp, ActiveValue::Set(Some(_))),
                "stop_timestamp must be set upon transitioning to a terminal state",
            )?,
            QueryFragmentState::Failed => ensure(
                matches!(self.error, ActiveValue::Set(Some(_)))
                    && matches!(self.stop_timestamp, ActiveValue::Set(Some(_))),
                "stop_timestamp and error must be set upon transitioning to a failed state",
            )?,
        }
        Ok(self)
    }
}

/// The state to move a query_fragment to, with the columns that state requires
/// (a start or stop timestamp, an error).
/// A terminal transition may also bring the start timestamp,
/// because a short query_fragment can finish before it was ever observed running.
/// Whether the step from the current state is legal is checked by a database trigger, not here.
#[derive(Debug, Clone)]
pub enum QueryFragmentTransition {
    Pending,
    Started,
    Running {
        start_timestamp: chrono::DateTime<chrono::Utc>,
    },
    Completed {
        start_timestamp: Option<chrono::DateTime<chrono::Utc>>,
        stop_timestamp: chrono::DateTime<chrono::Utc>,
    },
    Stopped {
        start_timestamp: Option<chrono::DateTime<chrono::Utc>>,
        stop_timestamp: chrono::DateTime<chrono::Utc>,
    },
    Failed {
        start_timestamp: Option<chrono::DateTime<chrono::Utc>>,
        stop_timestamp: chrono::DateTime<chrono::Utc>,
        error: QueryFragmentError,
    },
}

impl QueryFragmentTransition {
    pub fn running_now() -> Self {
        Self::Running {
            start_timestamp: chrono::Utc::now(),
        }
    }

    pub fn completed_now() -> Self {
        Self::Completed {
            start_timestamp: None,
            stop_timestamp: chrono::Utc::now(),
        }
    }

    pub fn stopped_now() -> Self {
        Self::Stopped {
            start_timestamp: None,
            stop_timestamp: chrono::Utc::now(),
        }
    }

    pub fn failed_now(error: QueryFragmentError) -> Self {
        Self::Failed {
            start_timestamp: None,
            stop_timestamp: chrono::Utc::now(),
            error,
        }
    }
}

impl ActiveModel {
    pub fn apply_transition(&mut self, transition: QueryFragmentTransition) {
        match transition {
            QueryFragmentTransition::Pending => {
                self.current_state = Set(QueryFragmentState::Pending);
            }
            QueryFragmentTransition::Started => {
                self.current_state = Set(QueryFragmentState::Started);
            }
            QueryFragmentTransition::Running { start_timestamp } => {
                self.current_state = Set(QueryFragmentState::Running);
                self.start_timestamp = Set(Some(start_timestamp));
            }
            QueryFragmentTransition::Completed {
                start_timestamp,
                stop_timestamp,
            } => {
                self.current_state = Set(QueryFragmentState::Completed);
                self.set_start_if_known(start_timestamp);
                self.stop_timestamp = Set(Some(stop_timestamp));
            }
            QueryFragmentTransition::Stopped {
                start_timestamp,
                stop_timestamp,
            } => {
                self.current_state = Set(QueryFragmentState::Stopped);
                self.set_start_if_known(start_timestamp);
                self.stop_timestamp = Set(Some(stop_timestamp));
            }
            QueryFragmentTransition::Failed {
                start_timestamp,
                stop_timestamp,
                error,
            } => {
                self.current_state = Set(QueryFragmentState::Failed);
                self.set_start_if_known(start_timestamp);
                self.stop_timestamp = Set(Some(stop_timestamp));
                self.error = Set(Some(error));
            }
        }
    }

    // A terminal report without a start timestamp leaves whatever the row already has,
    // so an earlier observed start is never overwritten with NULL.
    fn set_start_if_known(&mut self, start_timestamp: Option<chrono::DateTime<chrono::Utc>>) {
        if let Some(start_timestamp) = start_timestamp {
            self.start_timestamp = Set(Some(start_timestamp));
        }
    }

    pub fn mark_observed(&mut self, at: chrono::DateTime<chrono::Utc>) {
        self.last_observed_at = Set(Some(at));
    }
}

impl Model {
    #[cfg(test)]
    pub(crate) fn with_state(&self, state: QueryFragmentState) -> ActiveModel {
        let transition = match state {
            QueryFragmentState::Pending => QueryFragmentTransition::Pending,
            QueryFragmentState::Started => QueryFragmentTransition::Started,
            QueryFragmentState::Running => QueryFragmentTransition::running_now(),
            QueryFragmentState::Completed => QueryFragmentTransition::completed_now(),
            QueryFragmentState::Stopped => QueryFragmentTransition::stopped_now(),
            QueryFragmentState::Failed => {
                QueryFragmentTransition::failed_now(QueryFragmentError::Transport {
                    msg: "test".into(),
                })
            }
        };
        let mut fragment: ActiveModel = self.clone().into();
        fragment.apply_transition(transition);
        fragment
    }
}

impl Entity {
    /// Non-terminal fragments on `host` whose observed state differs from their target,
    /// meaning the fragments that the per-worker controller still has to reconcile.
    pub async fn actionable(
        conn: &impl ConnectionTrait,
        host: &NetworkAddr,
    ) -> Result<Vec<Model>, DbErr> {
        // The desired state is always terminal, so a non-terminal current state already differs from it;
        // the non-terminal filter alone is enough.
        Entity::find()
            .filter(Column::HostAddr.eq(host.clone()))
            .filter(Column::CurrentState.is_not_in([
                QueryFragmentState::Completed,
                QueryFragmentState::Stopped,
                QueryFragmentState::Failed,
            ]))
            .all(conn)
            .await
    }
}

#[derive(Clone, Debug, Deserialize, serde::Serialize)]
pub struct CreateQueryFragment {
    pub host_addr: NetworkAddr,
    pub plan: Vec<u8>,
    pub num_operators: i32,
    pub has_source: bool,
}

impl CreateQueryFragment {
    pub(crate) fn into_active_model(self, query_id: QueryId) -> ActiveModel {
        ActiveModel {
            id: NotSet,
            query_id: Set(query_id),
            host_addr: Set(self.host_addr),
            plan: Set(self.plan),
            num_operators: Set(self.num_operators),
            has_source: Set(self.has_source),
            current_state: NotSet,
            desired_state: NotSet,
            start_timestamp: NotSet,
            stop_timestamp: NotSet,
            error: NotSet,
            last_observed_at: NotSet,
        }
    }
}

/// Observed lifecycle state of a query_fragment.
/// The initial variant is the column default at insert time;
/// later values are written when the worker reports a state change.
/// The declaration order matches lifecycle progression (initial < registered < running < terminal),
/// which the test walker relies on to skip already-reached states.
///
/// Which transitions are legal is enforced by a database trigger, which is the authority;
/// keep it in sync when adding or reordering a state.
#[derive(
    Clone,
    Copy,
    Debug,
    Display,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    EnumIter,
    DeriveActiveEnum,
)]
#[sea_orm(rs_type = "String", db_type = "Text", rename_all = "PascalCase")]
#[strum(serialize_all = "PascalCase")]
pub enum QueryFragmentState {
    Pending,
    Started,
    Running,
    Completed,
    Stopped,
    Failed,
}

/// Target lifecycle state.
#[derive(
    Clone, Copy, Debug, Display, PartialEq, Eq, Serialize, Deserialize, EnumIter, DeriveActiveEnum,
)]
#[sea_orm(rs_type = "String", db_type = "Text", rename_all = "PascalCase")]
#[strum(serialize_all = "PascalCase")]
pub enum DesiredQueryFragmentState {
    /// Run until the input ends. The column default for a new query_fragment.
    Completed,
    /// Stop at the current position. Set by a query drop and by the query-failed cascade trigger.
    Stopped,
}

impl QueryFragmentState {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Stopped | Self::Failed)
    }
}

impl TryFrom<i32> for QueryFragmentState {
    type Error = i32;

    /// Maps the worker's proto `QueryState` enum to the coordinator's query_fragment state.
    /// Proto: Registered=0, Started=1, Running=2, Stopped=3, Failed=4.
    /// Registered maps to Started.
    /// Proto Started is an internal worker detail, so both Started and Running map to Running.
    ///
    /// Proto has no `Completed`:
    /// the worker reports the same value (3)
    /// for a query_fragment that finished on its own and one that was told to stop.
    /// It is decoded as `Completed` here;
    /// the controller later records it as `Stopped` instead when the query_fragment was asked to stop.
    ///
    /// An unrecognized value means the worker speaks a newer or incompatible protocol;
    /// it is returned as the error so the caller can handle it instead of crashing the coordinator.
    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(QueryFragmentState::Started),
            1 | 2 => Ok(QueryFragmentState::Running),
            3 => Ok(QueryFragmentState::Completed),
            4 => Ok(QueryFragmentState::Failed),
            other => Err(other),
        }
    }
}

#[cfg(any(test, feature = "testing"))]
pub(crate) use arbitrary::QueryFragmentsWithRefs;

#[cfg(any(test, feature = "testing"))]
mod arbitrary;

#[cfg(test)]
mod tests {
    use crate::Execute;
    use crate::database::Database;
    use crate::query::query_fragment::{self, QueryFragmentError, QueryFragmentState};
    use crate::query::{CreateQueryWithRefs, INVALID_TRANSITIONS, setup, setup_refs, walk_all};
    use crate::worker::GetWorker;
    use sea_orm::{ActiveModelTrait, ActiveValue::Set};
    use std::collections::HashMap;
    use test_strategy::proptest;

    #[proptest(async = "tokio")]
    async fn negative_capacity_rejected(mut req: CreateQueryWithRefs) {
        req.query.fragments[0].num_operators = -1;
        let db = Database::for_test().await;
        setup_refs(&db, &mut req).await;
        assert!(req.query.execute(&db).await.is_err());
    }

    #[proptest(async = "tokio")]
    async fn exceeding_capacity_rejected(mut req: CreateQueryWithRefs) {
        let db = Database::for_test().await;
        for w in &mut req.workers {
            w.max_operators = Some(w.max_operators.unwrap_or(0));
        }
        let target_addr = &req.query.fragments[0].host_addr;
        let cap = req
            .workers
            .iter()
            .find(|w| w.host_addr == *target_addr)
            .unwrap()
            .max_operators
            .unwrap();
        req.query.fragments[0].num_operators = cap + 1;

        setup_refs(&db, &mut req).await;
        assert!(req.query.execute(&db).await.is_err());
    }

    #[proptest(async = "tokio")]
    async fn creation_reserves_capacity(mut req: CreateQueryWithRefs) {
        let db = Database::for_test().await;
        let initial: HashMap<_, _> = req
            .workers
            .iter()
            .map(|w| (w.host_addr.clone(), w.max_operators))
            .collect();
        let used: HashMap<_, i32> =
            req.query
                .fragments
                .iter()
                .fold(HashMap::new(), |mut acc, f| {
                    *acc.entry(f.host_addr.clone()).or_default() += f.num_operators;
                    acc
                });

        setup(&db, &mut req).await;

        let workers = GetWorker::all().execute(&db).await.unwrap();
        assert!(workers.iter().all(|w| {
            let init = initial[&w.host_addr];
            let u = used.get(&w.host_addr).copied().unwrap_or(0);
            w.max_operators == init.map(|c| c - u)
        }));
    }

    #[proptest(async = "tokio")]
    async fn zero_capacity(mut req: CreateQueryWithRefs) {
        let db = Database::for_test().await;
        for w in &mut req.workers {
            w.max_operators = Some(0);
        }
        for f in &mut req.query.fragments {
            f.num_operators = 0;
        }
        setup(&db, &mut req).await;
    }

    #[proptest(async = "tokio")]
    async fn reject_missing_worker(req: CreateQueryWithRefs) {
        let db = Database::for_test().await;
        assert!(req.query.execute(&db).await.is_err());
    }

    #[proptest(async = "tokio")]
    async fn invalid_transition_rejected(
        mut req: CreateQueryWithRefs,
        #[strategy(proptest::sample::select(INVALID_TRANSITIONS.to_vec()))] transition: (
            QueryFragmentState,
            QueryFragmentState,
        ),
    ) {
        let db = Database::for_test().await;
        let (_, fragments) = setup(&db, &mut req).await;
        let (from, to) = transition;
        let (_, fragments) = walk_all(&fragments, from, &db).await;

        let mut bad: query_fragment::ActiveModel = fragments[0].clone().into();
        bad.current_state = Set(to);
        assert!(bad.update(&db).await.is_err());
    }

    #[proptest(async = "tokio")]
    async fn stop_hooks(mut req: CreateQueryWithRefs) {
        let db = Database::for_test().await;
        let (_, fragments) = setup(&db, &mut req).await;

        let mut update: query_fragment::ActiveModel = fragments[0].clone().into();
        update.current_state = Set(QueryFragmentState::Started);
        update = update.save(&db).await.unwrap();

        update.current_state = Set(QueryFragmentState::Running);
        assert!(update.clone().save(&db).await.is_err());
        update.start_timestamp = Set(None);
        assert!(update.clone().save(&db).await.is_err());

        update.start_timestamp = Set(Some(chrono::Utc::now()));
        update = update.save(&db).await.unwrap();

        update.current_state = Set(QueryFragmentState::Stopped);
        assert!(update.clone().save(&db).await.is_err());
        update.stop_timestamp = Set(None);
        assert!(update.clone().save(&db).await.is_err());

        update.stop_timestamp = Set(Some(chrono::Utc::now()));
        update.save(&db).await.unwrap();
    }

    #[proptest(async = "tokio")]
    async fn failed_hooks(mut req: CreateQueryWithRefs) {
        let db = Database::for_test().await;
        let (_, fragments) = setup(&db, &mut req).await;

        let mut update: query_fragment::ActiveModel = fragments[0].clone().into();
        update.current_state = Set(QueryFragmentState::Started);
        update = update.save(&db).await.unwrap();

        update.current_state = Set(QueryFragmentState::Running);
        update.start_timestamp = Set(Some(chrono::Utc::now()));
        update = update.save(&db).await.unwrap();

        update.current_state = Set(QueryFragmentState::Failed);
        assert!(update.clone().save(&db).await.is_err());
        update.stop_timestamp = Set(Some(chrono::Utc::now()));
        assert!(update.clone().save(&db).await.is_err());
        update.error = Set(None);
        assert!(update.clone().save(&db).await.is_err());
        update.error = Set(Some(QueryFragmentError::Transport {
            msg: "error".into(),
        }));
        update.save(&db).await.unwrap();
    }
}
