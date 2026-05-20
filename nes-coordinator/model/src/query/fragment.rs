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

use crate::identifier::FragmentId;
use crate::identifier::QueryId;
use crate::worker::CreateWorker;
use crate::worker::endpoint::NetworkAddr;
use proptest::arbitrary::Arbitrary;
use proptest::collection::vec;
use proptest::prelude::*;
use proptest::strategy::BoxedStrategy;
use proptest_derive::Arbitrary as DeriveArbitrary;
use sea_orm::Set;
use sea_orm::entity::prelude::*;
use sea_orm::{ActiveValue, FromJsonQueryResult, NotSet};
use serde::{Deserialize, Serialize};
use strum::Display;
use thiserror::Error;

#[derive(Debug, Clone, Error, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
pub enum FragmentError {
    #[error("Internal worker error; code: {code}, msg: {msg}, stacktrace: {trace}")]
    Internal {
        code: u16,
        msg: String,
        trace: String,
    },
    #[error("Worker communication error: {msg}")]
    Transport { msg: String },
}

/// One execution unit of a query, placed on a single worker. State
/// transitions are validated by a database-level state machine; the
/// worker's capacity is acquired on insert and released automatically
/// once the fragment reaches a terminal state.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, DeriveEntityModel)]
#[sea_orm(table_name = "fragment")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = true)]
    pub id: FragmentId,
    pub query_id: QueryId,
    pub host_addr: NetworkAddr,
    #[sea_orm(column_type = "VarBinary(StringLen::None)")]
    #[serde(skip)]
    pub plan: Vec<u8>,
    pub num_operators: i32,
    pub has_source: bool,
    pub current_state: FragmentState,
    pub desired_state: DesiredFragmentState,
    pub stop_mode: Option<StopMode>,
    pub start_timestamp: Option<chrono::DateTime<chrono::Local>>,
    pub stop_timestamp: Option<chrono::DateTime<chrono::Local>>,
    #[sea_orm(column_type = "JsonBinary")]
    pub error: Option<FragmentError>,
    pub last_observed_at: Option<chrono::DateTime<chrono::Local>>,
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
    /// Reject saves whose declared state would leave required columns
    /// unset (running needs a start timestamp; terminal needs a stop
    /// timestamp; failed also needs an error). Avoids inserting rows
    /// with surprising NULLs.
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
        match state {
            FragmentState::Pending | FragmentState::Registered => {}
            FragmentState::Running => ensure(
                self.start_timestamp.is_set(),
                "start_timestamp must be set upon transitioning to running",
            )?,
            FragmentState::Completed | FragmentState::Stopped => ensure(
                self.stop_timestamp.is_set(),
                "stop_timestamp must be set upon transitioning to a terminal state",
            )?,
            FragmentState::Failed => ensure(
                self.error.is_set() && self.stop_timestamp.is_set(),
                "stop_timestamp and error must be set upon transitioning to a failed state",
            )?,
        }
        Ok(self)
    }
}

#[derive(Debug, Clone)]
pub enum FragmentTransition {
    Pending,
    Registered,
    Running {
        start_timestamp: chrono::DateTime<chrono::Local>,
    },
    Completed {
        stop_timestamp: chrono::DateTime<chrono::Local>,
    },
    Stopped {
        stop_timestamp: chrono::DateTime<chrono::Local>,
    },
    Failed {
        stop_timestamp: chrono::DateTime<chrono::Local>,
        error: FragmentError,
    },
}

impl FragmentTransition {
    pub fn running_now() -> Self {
        Self::Running {
            start_timestamp: chrono::Local::now(),
        }
    }

    pub fn completed_now() -> Self {
        Self::Completed {
            stop_timestamp: chrono::Local::now(),
        }
    }

    pub fn stopped_now() -> Self {
        Self::Stopped {
            stop_timestamp: chrono::Local::now(),
        }
    }

    pub fn failed_now(error: FragmentError) -> Self {
        Self::Failed {
            stop_timestamp: chrono::Local::now(),
            error,
        }
    }
}

impl ActiveModel {
    pub fn apply_transition(&mut self, transition: FragmentTransition) {
        match transition {
            FragmentTransition::Pending => {
                self.current_state = Set(FragmentState::Pending);
            }
            FragmentTransition::Registered => {
                self.current_state = Set(FragmentState::Registered);
            }
            FragmentTransition::Running { start_timestamp } => {
                self.current_state = Set(FragmentState::Running);
                self.start_timestamp = Set(Some(start_timestamp));
            }
            FragmentTransition::Completed { stop_timestamp } => {
                self.current_state = Set(FragmentState::Completed);
                self.stop_timestamp = Set(Some(stop_timestamp));
            }
            FragmentTransition::Stopped { stop_timestamp } => {
                self.current_state = Set(FragmentState::Stopped);
                self.stop_timestamp = Set(Some(stop_timestamp));
            }
            FragmentTransition::Failed {
                stop_timestamp,
                error,
            } => {
                self.current_state = Set(FragmentState::Failed);
                self.stop_timestamp = Set(Some(stop_timestamp));
                self.error = Set(Some(error));
            }
        }
    }

    pub fn mark_observed(&mut self, at: chrono::DateTime<chrono::Local>) {
        self.last_observed_at = Set(Some(at));
    }
}

impl Model {
    #[cfg(test)]
    pub(crate) fn with_state(&self, state: FragmentState) -> ActiveModel {
        let transition = match state {
            FragmentState::Pending => FragmentTransition::Pending,
            FragmentState::Registered => FragmentTransition::Registered,
            FragmentState::Running => FragmentTransition::running_now(),
            FragmentState::Completed => FragmentTransition::completed_now(),
            FragmentState::Stopped => FragmentTransition::stopped_now(),
            FragmentState::Failed => {
                FragmentTransition::failed_now(FragmentError::Transport { msg: "test".into() })
            }
        };
        let mut fragment: ActiveModel = self.clone().into();
        fragment.apply_transition(transition);
        fragment
    }
}

impl Entity {
    /// Non-terminal fragments on `host` whose observed state differs
    /// from their target — the fragments the per-worker controller
    /// still has to reconcile.
    pub async fn actionable(
        conn: &impl ConnectionTrait,
        host: &NetworkAddr,
    ) -> Result<Vec<Model>, DbErr> {
        Entity::find()
            .filter(Column::HostAddr.eq(host.clone()))
            .filter(Expr::col(Column::CurrentState).ne(Expr::col(Column::DesiredState)))
            .filter(Column::CurrentState.is_not_in([
                FragmentState::Completed,
                FragmentState::Stopped,
                FragmentState::Failed,
            ]))
            .all(conn)
            .await
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct CreateFragment {
    pub host_addr: NetworkAddr,
    pub plan: Vec<u8>,
    pub num_operators: i32,
    pub has_source: bool,
}

impl CreateFragment {
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
            stop_mode: NotSet,
            start_timestamp: NotSet,
            stop_timestamp: NotSet,
            error: NotSet,
            last_observed_at: NotSet,
        }
    }
}

/// Observed lifecycle state of a fragment. The initial variant is the
/// column default at insert time; subsequent values are written when
/// the worker reports a state change. The declaration order matches
/// lifecycle progression — initial < registered < running < terminal —
/// which the test walker relies on to skip already-reached states.
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
pub enum FragmentState {
    Pending,
    Registered,
    Running,
    Completed,
    Stopped,
    Failed,
}

/// Target lifecycle state. Newly inserted fragments default (at the
/// column level) to graceful completion; a query-drop request and the
/// query-failed cascade trigger both override this to stop the fragment.
#[derive(
    Clone, Copy, Debug, Display, PartialEq, Eq, Serialize, Deserialize, EnumIter, DeriveActiveEnum,
)]
#[sea_orm(rs_type = "String", db_type = "Text", rename_all = "PascalCase")]
#[strum(serialize_all = "PascalCase")]
pub enum DesiredFragmentState {
    Completed,
    Stopped,
}

/// How a query-drop cascades. `Graceful` stops only source fragments
/// and lets downstream drain; `Forceful` stops every non-terminal
/// fragment immediately.
#[derive(
    DeriveArbitrary,
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    EnumIter,
    DeriveActiveEnum,
)]
#[sea_orm(rs_type = "String", db_type = "Text", rename_all = "PascalCase")]
#[strum(serialize_all = "PascalCase")]
pub enum StopMode {
    #[default]
    Graceful,
    Forceful,
}

impl From<StopMode> for i32 {
    fn from(value: StopMode) -> Self {
        match value {
            StopMode::Graceful => 0,
            StopMode::Forceful => 1,
        }
    }
}

impl FragmentState {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Stopped | Self::Failed)
    }

    pub fn next(self) -> Option<Self> {
        match self {
            Self::Pending => Some(Self::Registered),
            Self::Registered => Some(Self::Running),
            Self::Running => Some(Self::Completed),
            Self::Completed | Self::Stopped | Self::Failed => None,
        }
    }
}

impl From<i32> for FragmentState {
    /// Maps the worker's proto `QueryState` enum to coordinator `FragmentState`.
    /// Proto: Registered=0, Started=1, Running=2, Stopped=3, Failed=4.
    /// Started is an internal worker detail, so both Started(1) and Running(2) map to Running.
    fn from(value: i32) -> Self {
        match value {
            0 => FragmentState::Registered,
            1 | 2 => FragmentState::Running,
            3 => FragmentState::Completed,
            4 => FragmentState::Failed,
            // An unknown value means the worker is speaking a newer
            // protocol than this coordinator understands.
            other => panic!("unknown QueryState: {other}"),
        }
    }
}

impl From<FragmentState> for i32 {
    fn from(state: FragmentState) -> Self {
        match state {
            FragmentState::Registered => 0,
            FragmentState::Running => 2,
            FragmentState::Stopped | FragmentState::Completed => 3,
            FragmentState::Failed => 4,
            // Pending exists only on the coordinator side (a fragment
            // that hasn't been pushed to a worker yet); converting one
            // back to a proto value is a bug.
            FragmentState::Pending => panic!("Pending has no proto representation"),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct FragmentsWithRefs {
    pub(crate) workers: Vec<CreateWorker>,
    pub(crate) fragments: Vec<CreateFragment>,
}

impl Arbitrary for FragmentsWithRefs {
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        const MAX_WORKERS: u8 = 32;
        const MAX_NUM_FRAGMENTS: usize = 16;
        (
            CreateWorker::topology_dag(1, MAX_WORKERS),
            1..=MAX_NUM_FRAGMENTS,
        )
            .prop_flat_map(|(workers, num)| {
                let n = workers.len();
                (Just(workers), vec((0..n, any::<bool>()), num))
            })
            .prop_map(|(workers, placements)| {
                // Track per-worker remaining capacity so generated
                // fixtures stay within `max_operators`; otherwise the
                // insert is rejected. Cap per-fragment at 12 to keep
                // test cases small.
                let mut remaining: Vec<i32> = workers
                    .iter()
                    .map(|w| w.max_operators.unwrap_or(i32::MAX))
                    .collect();

                let mut fragments: Vec<CreateFragment> = placements
                    .into_iter()
                    .map(|(idx, has_source)| {
                        let cap = remaining[idx].min(12);
                        let ops = (cap / 2).clamp(0, cap);
                        remaining[idx] -= ops;
                        CreateFragment {
                            host_addr: workers[idx].host_addr.clone(),
                            plan: vec![],
                            num_operators: ops,
                            has_source,
                        }
                    })
                    .collect();
                fragments[0].has_source = true;

                FragmentsWithRefs { workers, fragments }
            })
            .boxed()
    }

    type Strategy = BoxedStrategy<Self>;
}

#[cfg(test)]
mod tests {
    use crate::Execute;
    use crate::database::Database;
    use crate::query::fragment::{self, FragmentError, FragmentState};
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
            FragmentState,
            FragmentState,
        ),
    ) {
        let db = Database::for_test().await;
        let (_, fragments) = setup(&db, &mut req).await;
        let (from, to) = transition;
        let (_, fragments) = walk_all(&fragments, from, &db).await;

        let mut bad: fragment::ActiveModel = fragments[0].clone().into();
        bad.current_state = Set(to);
        assert!(bad.update(&db).await.is_err());
    }

    #[proptest(async = "tokio")]
    async fn stop_hooks(mut req: CreateQueryWithRefs) {
        let db = Database::for_test().await;
        let (_, fragments) = setup(&db, &mut req).await;

        let mut update: fragment::ActiveModel = fragments[0].clone().into();
        update.current_state = Set(FragmentState::Registered);
        update = update.save(&db).await.unwrap();

        update.current_state = Set(FragmentState::Running);
        assert!(update.clone().save(&db).await.is_err());

        update.start_timestamp = Set(Some(chrono::Local::now()));
        update = update.save(&db).await.unwrap();

        update.current_state = Set(FragmentState::Stopped);
        assert!(update.clone().save(&db).await.is_err());

        update.stop_timestamp = Set(Some(chrono::Local::now()));
        update.save(&db).await.unwrap();
    }

    #[proptest(async = "tokio")]
    async fn failed_hooks(mut req: CreateQueryWithRefs) {
        let db = Database::for_test().await;
        let (_, fragments) = setup(&db, &mut req).await;

        let mut update: fragment::ActiveModel = fragments[0].clone().into();
        update.current_state = Set(FragmentState::Registered);
        update = update.save(&db).await.unwrap();

        update.current_state = Set(FragmentState::Running);
        update.start_timestamp = Set(Some(chrono::Local::now()));
        update = update.save(&db).await.unwrap();

        update.current_state = Set(FragmentState::Failed);
        assert!(update.clone().save(&db).await.is_err());
        update.stop_timestamp = Set(Some(chrono::Local::now()));
        assert!(update.clone().save(&db).await.is_err());
        update.error = Set(Some(FragmentError::Transport {
            msg: "error".into(),
        }));
        update.save(&db).await.unwrap();
    }
}
