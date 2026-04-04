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

mod create;
mod drop;
pub mod fragment;
mod get;
pub(crate) mod query_sink;
pub(crate) mod query_source;
pub mod query_state;

pub use create::CreateQuery;
pub use drop::DropQuery;
pub use get::GetQuery;

use crate::identifier::QueryId;
use crate::sink::{CreateSink, SinkType};
use crate::source::logical::CreateLogicalSource;
use crate::source::physical::{CreatePhysicalSource, SourceType};
use crate::worker::CreateWorker;
use proptest::arbitrary::Arbitrary;
use proptest::collection::vec;
use proptest::strategy::BoxedStrategy;
use query_state::QueryState;
use sea_orm::entity::prelude::*;
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, DeriveEntityModel)]
#[sea_orm(table_name = "query")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: QueryId,
    pub name: Option<String>,
    pub sql: String,
    pub state: QueryState,
    pub start_timestamp: Option<chrono::DateTime<chrono::Local>>,
    pub stop_timestamp: Option<chrono::DateTime<chrono::Local>>,
    #[sea_orm(column_type = "JsonBinary")]
    pub error: Option<serde_json::Value>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_many = "fragment::Entity")]
    Fragment,
}

impl Related<fragment::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Fragment.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}

#[derive(Debug, Clone)]
pub struct CreateQueryWithRefs {
    pub workers: Vec<CreateWorker>,
    pub logical_source: CreateLogicalSource,
    pub physical_sources: Vec<CreatePhysicalSource>,
    pub sink: CreateSink,
    pub query: CreateQuery,
}

impl Arbitrary for CreateQueryWithRefs {
    type Parameters = ();
    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        use fragment::FragmentsWithRefs;
        use proptest::prelude::*;
        use proptest::char::range;

        let name = (
            range('a', 'z'),
            vec(range('a', 'z'), 2..=10),
        )
            .prop_map(|(first, rest)| std::iter::once(first).chain(rest).collect::<String>());
        let statement = (
            vec(range('a', 'z'), 1..=10),
            vec(range('a', 'z'), 1..=10),
            vec(range('a', 'z'), 1..=10),
        )
            .prop_map(|(field, source, sink)| {
                format!(
                    "SELECT {} FROM {} INTO {};",
                    field.into_iter().collect::<String>(),
                    source.into_iter().collect::<String>(),
                    sink.into_iter().collect::<String>(),
                )
            });
        let sink_name = (
            range('a', 'z'),
            vec(range('a', 'z'), 2..=10),
        )
            .prop_map(|(first, rest)| std::iter::once(first).chain(rest).collect::<String>());
        any::<FragmentsWithRefs>()
            .prop_flat_map(move |fragments| {
                let n = fragments.fragments.len();
                (
                    Just(fragments),
                    name.clone(),
                    statement.clone(),
                    any::<CreateLogicalSource>(),
                    any::<SourceType>(),
                    sink_name.clone(),
                    any::<SinkType>(),
                    0..n,
                )
            })
            .prop_map(|(fragments, name, statement, logical_source, source_type, sink_name, sink_type, sink_frag_idx)| {
                let mut physical_sources: Vec<CreatePhysicalSource> = fragments.fragments.iter()
                    .filter(|f| f.has_source)
                    .map(|f| CreatePhysicalSource {
                        logical_source: logical_source.name.clone(),
                        host_addr: f.host_addr.clone(),
                        source_type,
                        source_config: serde_json::json!({}),
                        parser_config: serde_json::json!({}),
                        if_not_exists: false,
                    })
                    .collect();
                if physical_sources.is_empty() {
                    physical_sources.push(CreatePhysicalSource {
                        logical_source: logical_source.name.clone(),
                        host_addr: fragments.fragments[0].host_addr.clone(),
                        source_type,
                        source_config: serde_json::json!({}),
                        parser_config: serde_json::json!({}),
                        if_not_exists: false,
                    });
                }
                let sink = CreateSink {
                    name: sink_name,
                    host_addr: fragments.fragments[sink_frag_idx].host_addr.clone(),
                    sink_type,
                    schema: serde_json::json!({}),
                    config: serde_json::json!({}),
                    if_not_exists: false,
                };
                let query = CreateQuery::new(statement)
                    .name(name)
                    .with_fragments(fragments.fragments);
                CreateQueryWithRefs {
                    workers: fragments.workers,
                    logical_source,
                    physical_sources,
                    sink,
                    query,
                }
            })
            .boxed()
    }

    type Strategy = BoxedStrategy<Self>;
}

#[cfg(test)]
mod tests {
    use crate::Execute;
    use crate::database::Database;
    use crate::query::fragment::{self, DesiredFragmentState, FragmentError, FragmentState, StopMode};
    use crate::query::query_state::QueryState;
    use crate::query::{self, CreateQueryWithRefs, DropQuery, GetQuery};
    use crate::worker::GetWorker;
    use sea_orm::{ActiveModelTrait, ActiveValue::Set, ColumnTrait, EntityTrait, QueryFilter};
    use std::collections::HashMap;
    use test_strategy::proptest;

    /// Set up all prerequisites for a query and create it.
    /// Returns the created query and its fragments.
    async fn setup(db: &Database, req: &mut CreateQueryWithRefs) -> (query::Model, Vec<fragment::Model>) {
        for w in &req.workers {
            w.clone().execute(&db.conn).await.unwrap();
        }
        req.logical_source.clone().execute(&db.conn).await.unwrap();
        let mut source_ids = Vec::new();
        for ps in &req.physical_sources {
            let m = ps.clone().execute(&db.conn).await.unwrap();
            source_ids.push(m.id);
        }
        let sink = req.sink.clone().execute(&db.conn).await.unwrap();
        req.query.source_ids = source_ids;
        req.query.sink_ids = vec![sink.id];
        req.query.clone().execute(&db.conn).await.unwrap()
    }

    /// Walk all fragments through intermediate states to reach `target`.
    /// Returns the refreshed query and fragments.
    async fn transition_fragments(
        db: &Database,
        query_id: crate::identifier::QueryId,
        fragments: &[fragment::Model],
        target: FragmentState,
    ) -> (query::Model, Vec<fragment::Model>) {
        let path = match target {
            FragmentState::Pending => vec![],
            FragmentState::Registered => vec![FragmentState::Registered],
            FragmentState::Running => vec![FragmentState::Registered, FragmentState::Running],
            FragmentState::Completed => vec![FragmentState::Registered, FragmentState::Running, FragmentState::Completed],
            FragmentState::Stopped => vec![FragmentState::Stopped],
            FragmentState::Failed => vec![FragmentState::Failed],
        };
        let mut current_fragments = fragments.to_vec();
        for state in path {
            if current_fragments[0].current_state >= state {
                continue;
            }
            for f in &current_fragments {
                f.with_state(state).update(&db.conn).await.unwrap();
            }
            current_fragments = fragment::Entity::find()
                .filter(fragment::Column::QueryId.eq(query_id))
                .all(&db.conn)
                .await
                .unwrap();
        }
        let query = query::Entity::find_by_id(query_id)
            .one(&db.conn)
            .await
            .unwrap()
            .expect("query not found");
        (query, current_fragments)
    }

    #[proptest(async = "tokio")]
    async fn create_and_get(mut req: CreateQueryWithRefs) {
        let db = Database::for_test().await;
        let (created, _) = setup(&db, &mut req).await;
        assert_eq!(created.name, req.query.name);
        assert_eq!(created.sql, req.query.sql);
        assert_eq!(created.state, QueryState::Pending);

        let results = GetQuery::all()
            .with_id(created.id)
            .execute(&db.conn)
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, created);
    }

    #[proptest(async = "tokio")]
    async fn drop_sets_fragment_stop_mode(mut req: CreateQueryWithRefs, stop_mode: StopMode) {
        let db = Database::for_test().await;
        let (created, _) = setup(&db, &mut req).await;

        DropQuery::all()
            .with_stop_mode(stop_mode)
            .with_filters(GetQuery::all().with_id(created.id))
            .execute(&db.conn)
            .await
            .unwrap();

        let fragments = fragment::Entity::find()
            .filter(fragment::Column::QueryId.eq(created.id))
            .all(&db.conn)
            .await
            .unwrap();
        for f in &fragments {
            assert_eq!(f.stop_mode, Some(stop_mode));
            // Forceful sets all fragments to Stopped, Graceful only source fragments
            if stop_mode == StopMode::Forceful || f.has_source {
                assert_eq!(f.desired_state, DesiredFragmentState::Stopped);
            }
        }
    }

    #[proptest(async = "tokio")]
    async fn fragment_negative_capacity_rejected(mut req: CreateQueryWithRefs) {
        let db = Database::for_test().await;
        for w in &req.workers {
            w.clone().execute(&db.conn).await.unwrap();
        }
        req.logical_source.clone().execute(&db.conn).await.unwrap();
        let mut source_ids = Vec::new();
        for ps in &req.physical_sources {
            source_ids.push(ps.clone().execute(&db.conn).await.unwrap().id);
        }
        let sink = req.sink.clone().execute(&db.conn).await.unwrap();
        req.query.source_ids = source_ids;
        req.query.sink_ids = vec![sink.id];
        req.query.fragments[0].num_operators = -1;
        assert!(req.query.execute(&db.conn).await.is_err());
    }

    #[proptest(async = "tokio")]
    async fn fragment_exceeding_capacity_rejected(mut req: CreateQueryWithRefs) {
        let db = Database::for_test().await;
        for w in &mut req.workers {
            w.max_operators = Some(w.max_operators.unwrap_or(0));
        }
        let target_addr = &req.query.fragments[0].host_addr;
        let cap = req.workers.iter()
            .find(|w| w.host_addr == *target_addr)
            .unwrap()
            .max_operators
            .unwrap();
        req.query.fragments[0].num_operators = cap + 1;

        for w in &req.workers {
            w.clone().execute(&db.conn).await.unwrap();
        }
        req.logical_source.clone().execute(&db.conn).await.unwrap();
        let mut source_ids = Vec::new();
        for ps in &req.physical_sources {
            source_ids.push(ps.clone().execute(&db.conn).await.unwrap().id);
        }
        let sink = req.sink.clone().execute(&db.conn).await.unwrap();
        req.query.source_ids = source_ids;
        req.query.sink_ids = vec![sink.id];
        assert!(req.query.execute(&db.conn).await.is_err());
    }

    #[proptest(async = "tokio")]
    async fn fragment_creation_reserves_capacity(mut req: CreateQueryWithRefs) {
        let db = Database::for_test().await;
        let initial: HashMap<_, _> = req.workers.iter()
            .map(|w| (w.host_addr.clone(), w.max_operators))
            .collect();
        let used: HashMap<_, i32> = req.query.fragments.iter().fold(HashMap::new(), |mut acc, f| {
            *acc.entry(f.host_addr.clone()).or_default() += f.num_operators;
            acc
        });

        setup(&db, &mut req).await;

        let workers = GetWorker::all().execute(&db.conn).await.unwrap();
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
    async fn capacity_released_on_fragment_terminal(
        mut req: CreateQueryWithRefs,
        #[strategy(proptest::prop_oneof![
            proptest::strategy::Just(FragmentState::Completed),
            proptest::strategy::Just(FragmentState::Stopped),
            proptest::strategy::Just(FragmentState::Failed),
        ])]
        terminal_state: FragmentState,
    ) {
        let db = Database::for_test().await;
        let initial_total: i32 = req.workers.iter().filter_map(|w| w.max_operators).sum();
        let (query, fragments) = setup(&db, &mut req).await;

        transition_fragments(&db, query.id, &fragments, terminal_state).await;

        let final_workers = GetWorker::all().execute(&db.conn).await.unwrap();
        let final_total: i32 = final_workers.iter().filter_map(|w| w.max_operators).sum();
        assert_eq!(initial_total, final_total, "capacity must be fully restored after terminal state");
    }

    #[proptest(async = "tokio")]
    async fn query_state_derived_from_fragments(mut req: CreateQueryWithRefs) {
        let db = Database::for_test().await;
        let (_, fragments) = setup(&db, &mut req).await;
        let query_id = fragments[0].query_id;

        let steps: &[(FragmentState, QueryState)] = &[
            (FragmentState::Pending, QueryState::Pending),
            (FragmentState::Registered, QueryState::Registered),
            (FragmentState::Running, QueryState::Running),
            (FragmentState::Completed, QueryState::Completed),
        ];

        let mut current_fragments = fragments;
        for &(fragment_state, expected_query_state) in steps {
            let (query, frags) =
                transition_fragments(&db, query_id, &current_fragments, fragment_state).await;
            current_fragments = frags;
            assert_eq!(query.state, expected_query_state);
        }
    }

    #[proptest(async = "tokio")]
    async fn fragments_stored_with_defaults(mut req: CreateQueryWithRefs) {
        let db = Database::for_test().await;
        let (query, fragments) = setup(&db, &mut req).await;
        assert!(fragments.iter().all(|f| {
            f.query_id == query.id
                && f.current_state == FragmentState::Pending
                && f.start_timestamp.is_none()
                && f.stop_timestamp.is_none()
                && f.error.is_none()
        }));
    }

    #[proptest(async = "tokio")]
    async fn fragments_reject_missing_worker(req: CreateQueryWithRefs) {
        let db = Database::for_test().await;
        assert!(req.query.execute(&db.conn).await.is_err());
    }

    #[proptest(async = "tokio")]
    async fn one_failed_fragment_fails_query(mut req: CreateQueryWithRefs) {
        let db = Database::for_test().await;
        let (query, fragments) = setup(&db, &mut req).await;
        let (_, fragments) =
            transition_fragments(&db, query.id, &fragments, FragmentState::Running).await;

        let mut update: fragment::ActiveModel = fragments[0].clone().into();
        update.current_state = Set(FragmentState::Failed);
        update.error = Set(Some(FragmentError::Transport {
            msg: "connection lost".to_string(),
        }));
        update.update(&db.conn).await.unwrap();

        let query = query::Entity::find_by_id(query.id)
            .one(&db.conn)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(query.state, QueryState::Failed);
    }

    #[proptest(async = "tokio")]
    async fn start_timestamp_is_min(mut req: CreateQueryWithRefs) {
        use sea_orm::sqlx::types::chrono;

        let db = Database::for_test().await;
        let (query, fragments) = setup(&db, &mut req).await;
        let (_, fragments) =
            transition_fragments(&db, query.id, &fragments, FragmentState::Registered).await;

        // Transition to Running while setting start_timestamp (trigger fires on current_state change)
        for (i, f) in fragments.iter().enumerate() {
            let ts = chrono::DateTime::from_timestamp_millis((i as i64 + 1) * 1000)
                .unwrap()
                .with_timezone(&chrono::Local);
            let mut update: fragment::ActiveModel = f.clone().into();
            update.current_state = Set(FragmentState::Running);
            update.start_timestamp = Set(Some(ts));
            update.update(&db.conn).await.unwrap();
        }

        let query = query::Entity::find_by_id(query.id)
            .one(&db.conn)
            .await
            .unwrap()
            .unwrap();
        let expected = chrono::DateTime::from_timestamp_millis(1000)
            .unwrap()
            .with_timezone(&chrono::Local);
        assert_eq!(query.start_timestamp, Some(expected));
        assert!(query.stop_timestamp.is_none());
    }

    #[proptest(async = "tokio")]
    async fn stop_timestamp_is_max(mut req: CreateQueryWithRefs) {
        use sea_orm::sqlx::types::chrono;

        let db = Database::for_test().await;
        let (query, fragments) = setup(&db, &mut req).await;
        let (_, fragments) =
            transition_fragments(&db, query.id, &fragments, FragmentState::Running).await;

        for (i, f) in fragments.iter().enumerate() {
            let ts = chrono::DateTime::from_timestamp_millis((i as i64 + 1) * 1000)
                .unwrap()
                .with_timezone(&chrono::Local);
            let mut update: fragment::ActiveModel = f.clone().into();
            update.current_state = Set(FragmentState::Completed);
            update.stop_timestamp = Set(Some(ts));
            update.update(&db.conn).await.unwrap();
        }

        let query = query::Entity::find_by_id(query.id)
            .one(&db.conn)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(query.state, QueryState::Completed);
        let expected = chrono::DateTime::from_timestamp_millis(fragments.len() as i64 * 1000)
            .unwrap()
            .with_timezone(&chrono::Local);
        assert_eq!(query.stop_timestamp, Some(expected));
    }

    #[proptest(async = "tokio")]
    async fn worker_drop_blocked_by_active_fragments(mut req: CreateQueryWithRefs) {
        use crate::worker::DropWorker;

        let db = Database::for_test().await;
        let (_, fragments) = setup(&db, &mut req).await;
        let fragment_host = &fragments[0].host_addr;
        assert!(
            DropWorker::new(fragment_host.clone())
                .execute(&db.conn)
                .await
                .is_err()
        );
    }
}
