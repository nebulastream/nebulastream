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

//! The query fragment entity and its lifecycle types.

use crate::error::CodedError;
use crate::identifier::{QueryFragmentId, QueryId};
use crate::worker::endpoint::NetworkAddr;
use sea_orm::entity::prelude::*;
use sea_orm::{DerivePartialModel, FromJsonQueryResult, NotSet, Set};
use serde::{Deserialize, Serialize};
use strum::Display;
use thiserror::Error;

/// Why a fragment failed: an error inside the worker, or a failure to reach it.
#[derive(Debug, Clone, Error, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
pub enum QueryFragmentError {
    #[error("Internal worker error; code: {}, msg: {}", .0.code as u16, .0.msg)]
    Internal(CodedError),
    #[error("Worker communication error: {msg}")]
    Transport { msg: String },
}

/// A fragment that the controller still has to move to its desired state, with only the columns that the controller reads.
#[derive(Clone, Debug, PartialEq, Eq, DerivePartialModel)]
#[sea_orm(entity = "Entity", from_query_result)]
pub struct ManagedFragment {
    pub id: QueryFragmentId,
    pub current_state: QueryFragmentState,
    pub desired_state: DesiredQueryFragmentState,
}

/// The failure that ended a query: the first fragment that failed, and where it ran.
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
/// Its operators count against the worker's capacity until it terminates.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, DeriveEntityModel)]
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

impl ActiveModelBehavior for ActiveModel {}

/// The state to move a fragment to, with the columns that the state requires.
/// A terminal transition may also set the start timestamp,
/// because a short fragment can finish before it was ever observed running.
/// Whether the step from the current state is legal is checked by a database trigger,
/// and whether the row has the required columns by CHECK constraints.
#[derive(Debug, Clone, PartialEq, Eq)]
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
    /// A terminal transition without a start timestamp keeps the start timestamp that the row already has.
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
    /// Non-terminal fragments on the given worker with an observed state that differs from their target,
    /// meaning the fragments that the per-worker controller still has to reconcile.
    pub async fn get_managed(
        conn: &impl ConnectionTrait,
        host: &NetworkAddr,
    ) -> Result<Vec<ManagedFragment>, DbErr> {
        // The desired state is always terminal, so a non-terminal current state already differs from it;
        // the non-terminal filter alone is enough.
        Entity::find()
            .filter(Column::HostAddr.eq(host.clone()))
            .filter(Column::CurrentState.is_not_in(QueryFragmentState::TERMINAL))
            .into_partial_model::<ManagedFragment>()
            .all(conn)
            .await
    }

    /// Sets the time at which the given fragments were last observed running to now.
    pub async fn mark_observed(
        conn: &impl ConnectionTrait,
        ids: Vec<QueryFragmentId>,
    ) -> Result<(), DbErr> {
        Entity::update_many()
            .col_expr(Column::LastObservedAt, chrono::Utc::now().into())
            .filter(Column::Id.is_in(ids))
            .exec(conn)
            .await
            .map(|_| ())
    }
}

/// One fragment of a query to create.
/// Its operators count against the capacity of the worker that hosts it.
#[derive(Clone, Debug, Deserialize)]
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

/// Observed lifecycle state of a fragment.
/// The initial variant is the column default at insert time;
/// later values are written when the worker reports a state change.
/// NOTE: The declaration order matches lifecycle progression (Pending < Started < Running < terminal),
/// and the state-walk tests rely on it to skip states that were already reached.
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
    /// Run until the input ends.
    /// The column default for a new fragment.
    Completed,
    /// Stop at the current position.
    /// Set by a query stop or drop, and by the trigger that stops the fragments of a failed query.
    Stopped,
}

impl QueryFragmentState {
    pub const TERMINAL: [Self; 3] = [Self::Completed, Self::Stopped, Self::Failed];

    pub fn is_terminal(self) -> bool {
        Self::TERMINAL.contains(&self)
    }
}

impl TryFrom<i32> for QueryFragmentState {
    type Error = i32;

    /// Maps the worker's proto `QueryState` enum to the state of a fragment:
    /// ```text
    /// Registered (0)  Started
    /// Started (1)     Running
    /// Running (2)     Running
    /// Stopped (3)     Completed
    /// Failed (4)      Failed
    /// ```
    /// Proto Started is an internal worker detail, so it reads as Running.
    ///
    /// NOTE: Proto has no `Completed`:
    /// the worker reports Stopped for a fragment that finished on its own and for one that was stopped.
    /// The controller records it as `Stopped` when the fragment's desired state is Stopped.
    ///
    /// An unknown value means that the worker uses a newer or incompatible protocol.
    /// It is returned as the error so the caller can handle it instead of crashing the coordinator.
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

#[cfg(test)]
mod tests {
    use crate::Execute;
    use crate::database::Database;
    use crate::error::{ErrorCode, WriteFailure, classify_write, code_of};
    use crate::query::query_fragment::{
        self, DesiredQueryFragmentState, QueryFragmentError, QueryFragmentState,
    };
    use crate::query::{CreateQueryWithRefs, free_capacities, setup, setup_refs, walk_one};
    use crate::statement::ParsedStatement;
    use crate::worker::GetWorker;
    use crate::worker::endpoint::NetworkAddr;
    use sea_orm::{ActiveModelTrait, ActiveValue::Set, DbErr, EntityTrait, PaginatorTrait};
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
        let statement = ParsedStatement::CreateQuery(req.query);
        let rejected = code_of(statement.execute_with(&db).await);
        assert_eq!(rejected, Some(ErrorCode::CatalogWriteRejected));
        assert_eq!(query_fragment::Entity::find().count(&db).await.unwrap(), 0);
    }

    #[proptest(async = "tokio")]
    async fn creation_reserves_capacity(mut req: CreateQueryWithRefs) {
        let db = Database::for_test().await;
        let used: HashMap<_, i64> =
            req.query
                .fragments
                .iter()
                .fold(HashMap::new(), |mut acc, f| {
                    *acc.entry(f.host_addr.clone()).or_default() += i64::from(f.num_operators);
                    acc
                });

        setup(&db, &mut req).await;

        let free = free_capacities(&db).await;
        let stored = GetWorker::all().execute(&db).await.unwrap();
        for w in &req.workers {
            let u = used.get(&w.host_addr).copied().unwrap_or(0);
            assert_eq!(
                free[&w.host_addr],
                w.max_operators.map(|c| i64::from(c) - u)
            );
            let row = stored.iter().find(|row| row.host_addr == w.host_addr);
            assert_eq!(row.unwrap().max_operators, w.max_operators);
        }
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
    async fn reject_missing_worker(mut req: CreateQueryWithRefs) {
        let db = Database::for_test().await;
        setup_refs(&db, &mut req).await;
        req.query.fragments[0].host_addr = NetworkAddr::new("no-such-worker", 8080).unwrap();
        let unplaced = code_of(req.query.execute(&db).await);
        assert_eq!(unplaced, Some(ErrorCode::UnknownWorker));
    }

    #[proptest(async = "tokio")]
    async fn managed_fragments(mut req: CreateQueryWithRefs) {
        let db = Database::for_test().await;
        let (_, fragments) = setup(&db, &mut req).await;
        let host = fragments[0].host_addr.clone();
        let on_host = |fragments: &[query_fragment::Model]| -> Vec<_> {
            fragments
                .iter()
                .filter(|f| f.host_addr == host)
                .map(|f| f.id)
                .collect()
        };
        let ids = |rows: Vec<query_fragment::ManagedFragment>| -> Vec<_> {
            rows.into_iter().map(|row| row.id).collect()
        };

        let rows = query_fragment::Entity::get_managed(&db, &host)
            .await
            .unwrap();
        assert_eq!(ids(rows), on_host(&fragments));

        walk_one(&fragments[0], QueryFragmentState::Stopped, &db).await;
        let rows = query_fragment::Entity::get_managed(&db, &host)
            .await
            .unwrap();
        assert!(!ids(rows).contains(&fragments[0].id));
    }

    #[proptest(async = "tokio")]
    async fn a_failed_fragment_stops_the_rest_of_its_query(mut req: CreateQueryWithRefs) {
        let db = Database::for_test().await;
        let (_, fragments) = setup(&db, &mut req).await;
        walk_one(&fragments[0], QueryFragmentState::Failed, &db).await;

        for fragment in &fragments[1..] {
            let row = query_fragment::Entity::find_by_id(fragment.id)
                .one(&db)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(row.desired_state, DesiredQueryFragmentState::Stopped);
        }
    }

    #[proptest(async = "tokio")]
    async fn deleting_a_live_fragment_releases_its_capacity(mut req: CreateQueryWithRefs) {
        let db = Database::for_test().await;
        setup_refs(&db, &mut req).await;
        let before = free_capacities(&db).await;
        let created = req.query.execute(&db).await.unwrap();

        crate::query::Entity::delete_by_id(created.query.id)
            .exec(&db)
            .await
            .unwrap();

        assert_eq!(free_capacities(&db).await, before);
    }

    fn rejected<T>(result: Result<T, DbErr>) -> bool {
        matches!(result, Err(err) if classify_write(&err) == WriteFailure::Rejected)
    }

    #[proptest(async = "tokio")]
    async fn running_and_stopped_rows_need_their_timestamps(mut req: CreateQueryWithRefs) {
        let db = Database::for_test().await;
        let (_, fragments) = setup(&db, &mut req).await;

        let mut update: query_fragment::ActiveModel = fragments[0].clone().into();
        update.current_state = Set(QueryFragmentState::Started);
        update = update.save(&db).await.unwrap();

        update.current_state = Set(QueryFragmentState::Running);
        assert!(rejected(update.clone().save(&db).await));
        update.start_timestamp = Set(None);
        assert!(rejected(update.clone().save(&db).await));

        update.start_timestamp = Set(Some(chrono::Utc::now()));
        update = update.save(&db).await.unwrap();

        update.current_state = Set(QueryFragmentState::Stopped);
        assert!(rejected(update.clone().save(&db).await));
        update.stop_timestamp = Set(None);
        assert!(rejected(update.clone().save(&db).await));

        update.stop_timestamp = Set(Some(chrono::Utc::now()));
        update.save(&db).await.unwrap();
    }

    #[proptest(async = "tokio")]
    async fn a_failed_row_needs_a_stop_timestamp_and_an_error(mut req: CreateQueryWithRefs) {
        let db = Database::for_test().await;
        let (_, fragments) = setup(&db, &mut req).await;

        let mut update: query_fragment::ActiveModel = fragments[0].clone().into();
        update.current_state = Set(QueryFragmentState::Started);
        update = update.save(&db).await.unwrap();

        update.current_state = Set(QueryFragmentState::Running);
        update.start_timestamp = Set(Some(chrono::Utc::now()));
        update = update.save(&db).await.unwrap();

        update.current_state = Set(QueryFragmentState::Failed);
        assert!(rejected(update.clone().save(&db).await));
        update.stop_timestamp = Set(Some(chrono::Utc::now()));
        assert!(rejected(update.clone().save(&db).await));
        update.error = Set(None);
        assert!(rejected(update.clone().save(&db).await));
        update.error = Set(Some(QueryFragmentError::Transport {
            msg: "error".into(),
        }));
        update.save(&db).await.unwrap();
    }
}
