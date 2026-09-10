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

//! The query entity and the requests that manage it.

mod create;
mod drop;
mod get;
pub mod query_fragment;
pub(crate) mod query_sink;
pub(crate) mod query_source;
pub mod query_state;

pub use create::CreateQuery;
pub use drop::DropQuery;
pub use get::GetQuery;

use crate::identifier::QueryId;
#[cfg(any(test, feature = "testing"))]
use crate::sink::{CreateAnonymousSink, CreateSink};
#[cfg(any(test, feature = "testing"))]
use crate::source::physical::{CreateAnonymousSource, CreatePhysicalSource};
#[cfg(any(test, feature = "testing"))]
use crate::{ConnectorKind, Execute};
use anyhow::Result;
use query_state::QueryState;
use sea_orm::entity::prelude::*;
use serde::Serialize;

/// A submitted query. It owns a set of fragments, sources, and sinks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, DeriveEntityModel)]
#[sea_orm(table_name = "query")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: QueryId,
    pub name: Option<String>,
    pub sql: String,
    pub state: QueryState,
    pub start_timestamp: Option<chrono::DateTime<chrono::Utc>>,
    pub stop_timestamp: Option<chrono::DateTime<chrono::Utc>>,
    #[sea_orm(column_type = "JsonBinary")]
    pub error: Option<query_fragment::QueryError>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_many = "query_fragment::Entity")]
    QueryFragment,
}

impl Related<query_fragment::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::QueryFragment.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}

/// A query row and its fragment rows.
/// Every query read and create returns this shape; the fragments are empty when a read did not ask for them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QueryWithFragments {
    pub query: Model,
    pub fragments: Vec<query_fragment::Model>,
}

#[cfg(any(test, feature = "testing"))]
mod arbitrary;
#[cfg(any(test, feature = "testing"))]
pub use arbitrary::CreateQueryWithRefs;

#[cfg(any(test, feature = "testing"))]
pub async fn setup_refs(db: &crate::database::Database, req: &mut CreateQueryWithRefs) {
    for w in &req.workers {
        w.execute(db).await.unwrap();
    }
    req.logical_source.execute(db).await.unwrap();
    let shared = req.kind == ConnectorKind::Shared;
    let internal = req.kind == ConnectorKind::Internal;
    let mut source_ids = std::collections::BTreeSet::new();
    for fragment in req.query.fragments.iter().filter(|f| f.has_source) {
        let source = if shared {
            let source = CreatePhysicalSource {
                logical_source: req.logical_source.name.clone(),
                host_addr: fragment.host_addr.clone(),
                source_type: req.connector_type.clone(),
                source_config: Default::default(),
                parser_config: Default::default(),
                if_not_exists: false,
            };
            source.execute(db).await
        } else {
            let source = CreateAnonymousSource {
                source_type: req.connector_type.clone(),
                source_config: Default::default(),
                parser_config: Default::default(),
                host_addr: fragment.host_addr.clone(),
                internal,
            };
            source.execute(db).await
        };
        source_ids.insert(source.unwrap().id);
    }
    let sink = if shared {
        let sink = CreateSink {
            name: req.sink_name.clone(),
            host_addr: req.sink_host.clone(),
            sink_type: req.connector_type.clone(),
            schema: Default::default(),
            config: Default::default(),
            if_not_exists: false,
        };
        sink.execute(db).await
    } else {
        let sink = CreateAnonymousSink {
            sink_type: req.connector_type.clone(),
            schema: Default::default(),
            config: Default::default(),
            host_addr: req.sink_host.clone(),
            internal,
        };
        sink.execute(db).await
    };
    req.query.source_ids = source_ids;
    req.query.sink_ids = std::collections::BTreeSet::from([sink.unwrap().id]);
}

#[cfg(test)]
pub(crate) async fn setup(
    db: &crate::database::Database,
    req: &mut CreateQueryWithRefs,
) -> (Model, Vec<query_fragment::Model>) {
    setup_refs(db, req).await;
    let created = req.query.execute(db).await.unwrap();
    (created.query, created.fragments)
}

#[cfg(test)]
pub(crate) async fn walk_one(
    fragment: &query_fragment::Model,
    target: query_fragment::QueryFragmentState,
    db: &crate::database::Database,
) -> query_fragment::Model {
    use query_fragment::QueryFragmentState;
    let path = match target {
        QueryFragmentState::Pending => vec![],
        QueryFragmentState::Started => vec![QueryFragmentState::Started],
        QueryFragmentState::Running => {
            vec![QueryFragmentState::Started, QueryFragmentState::Running]
        }
        QueryFragmentState::Completed => vec![
            QueryFragmentState::Started,
            QueryFragmentState::Running,
            QueryFragmentState::Completed,
        ],
        QueryFragmentState::Stopped => vec![QueryFragmentState::Stopped],
        QueryFragmentState::Failed => vec![QueryFragmentState::Failed],
    };
    let mut current = fragment.clone();
    for state in path {
        if current.current_state >= state {
            continue;
        }
        current.with_state(state).update(db).await.unwrap();
        current = query_fragment::Entity::find_by_id(current.id)
            .one(db)
            .await
            .unwrap()
            .unwrap();
    }
    current
}

#[cfg(test)]
pub(crate) async fn walk_all(
    fragments: &[query_fragment::Model],
    target: query_fragment::QueryFragmentState,
    db: &crate::database::Database,
) -> (Model, Vec<query_fragment::Model>) {
    let mut updated = Vec::with_capacity(fragments.len());
    for f in fragments {
        updated.push(walk_one(f, target, db).await);
    }
    let query = Entity::find_by_id(fragments[0].query_id)
        .one(db)
        .await
        .unwrap()
        .expect("query not found");
    (query, updated)
}

#[cfg(test)]
mod tests {
    use crate::database::Database;
    use crate::error::{CodedError, ErrorCode, WriteFailure, classify_write, code_of};
    use crate::query::query_fragment::{
        self, DesiredQueryFragmentState, QueryError, QueryFragmentError, QueryFragmentState,
        QueryFragmentTransition,
    };
    use crate::query::query_state::QueryState;
    use crate::query::{
        self, CreateQuery, CreateQueryWithRefs, DropQuery, GetQuery, setup, setup_refs, walk_all,
        walk_one,
    };
    use crate::sink::{self, DropSink};
    use crate::source::physical::{self, DropPhysicalSource};
    use crate::statement::ParsedStatement;
    use crate::{ConnectorKind, Execute};
    use proptest::collection::vec;
    use proptest::option;
    use proptest::prelude::any;
    use proptest::sample::{Index, select, subsequence};
    use sea_orm::{
        ActiveEnum, ActiveModelTrait, ActiveValue::Set, ColumnTrait, EntityTrait, Iterable,
        PaginatorTrait, QueryFilter,
    };
    use test_strategy::proptest;

    #[proptest(async = "tokio")]
    async fn create_and_get(mut req: CreateQueryWithRefs) {
        let db = Database::for_test().await;
        let (created, fragments) = setup(&db, &mut req).await;
        assert_eq!(created.name, req.query.name);
        assert_eq!(created.sql, req.query.sql);
        assert_eq!(created.state, QueryState::Pending);
        assert!(created.error.is_none());
        assert!(created.start_timestamp.is_none());
        assert!(created.stop_timestamp.is_none());
        assert!(req.query.fragments.iter().all(|requested| {
            fragments.iter().any(|f| {
                (&f.host_addr, &f.plan, f.num_operators, f.has_source)
                    == (
                        &requested.host_addr,
                        &requested.plan,
                        requested.num_operators,
                        requested.has_source,
                    )
            })
        }));
        assert!(fragments.iter().all(|f| {
            f.query_id == created.id
                && f.current_state == QueryFragmentState::Pending
                && f.start_timestamp.is_none()
                && f.stop_timestamp.is_none()
                && f.error.is_none()
        }));

        let results = GetQuery::all()
            .with_id(created.id)
            .execute(&db)
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].query, created);
    }

    #[proptest(async = "tokio")]
    async fn drop_stops_every_fragment(mut req: CreateQueryWithRefs) {
        let db = Database::for_test().await;
        let (created, _) = setup(&db, &mut req).await;

        DropQuery::all()
            .with_filters(GetQuery::all().with_id(created.id))
            .execute(&db)
            .await
            .unwrap();

        let fragments = query_fragment::Entity::find()
            .filter(query_fragment::Column::QueryId.eq(created.id))
            .all(&db)
            .await
            .unwrap();
        assert!(!fragments.is_empty());
        for fragment in &fragments {
            assert_eq!(fragment.desired_state, DesiredQueryFragmentState::Stopped);
        }
    }

    #[proptest(async = "tokio")]
    async fn timestamp_derivation(mut req: CreateQueryWithRefs) {
        let db = Database::for_test().await;
        let (_, fragments) = setup(&db, &mut req).await;
        let (query, fragments) = walk_all(&fragments, QueryFragmentState::Running, &db).await;
        assert_eq!(query.state, QueryState::Running);
        assert!(query.start_timestamp.is_some());
        assert!(query.stop_timestamp.is_none());
        let (query, _) = walk_all(&fragments, QueryFragmentState::Completed, &db).await;
        assert_eq!(query.state, QueryState::Completed);
        assert!(query.start_timestamp.is_some());
        assert!(query.stop_timestamp.is_some());
    }

    #[proptest(async = "tokio")]
    async fn hard_delete_cascades_full_chain(mut req: CreateQueryWithRefs) {
        use crate::query::{query_sink, query_source};

        let db = Database::for_test().await;
        let (created, _) = setup(&db, &mut req).await;
        let query_id = created.id;
        let source_ids = req.query.source_ids.clone();
        let sink_ids = req.query.sink_ids.clone();
        let shared = req.kind == ConnectorKind::Shared;

        query::Entity::delete_by_id(query_id)
            .exec(&db)
            .await
            .unwrap();

        assert_eq!(query_fragment::Entity::find().count(&db).await.unwrap(), 0);
        assert_eq!(query_source::Entity::find().count(&db).await.unwrap(), 0);
        assert_eq!(query_sink::Entity::find().count(&db).await.unwrap(), 0);

        for id in source_ids {
            let exists = physical::Entity::find_by_id(id)
                .one(&db)
                .await
                .unwrap()
                .is_some();
            assert_eq!(exists, shared);
        }
        for id in sink_ids {
            let exists = sink::Entity::find_by_id(id)
                .one(&db)
                .await
                .unwrap()
                .is_some();
            assert_eq!(exists, shared);
        }
    }

    /// A second failure on the same query is a consequence of the first and must not replace it.
    #[proptest(async = "tokio")]
    async fn query_error_keeps_the_first_failure(mut req: CreateQueryWithRefs, internal: bool) {
        let db = Database::for_test().await;
        let (_, fragments) = setup(&db, &mut req).await;
        let error = |idx: usize| {
            let msg = format!("boom{idx}");
            if internal {
                let code = ErrorCode::PlacementFailure;
                QueryFragmentError::Internal(CodedError::relayed(code, msg, "trace".into()))
            } else {
                QueryFragmentError::Transport { msg }
            }
        };

        // Fail every query_fragment in turn, each with its own message.
        for (idx, frag) in fragments.iter().enumerate() {
            let mut update: query_fragment::ActiveModel = frag.clone().into();
            update.apply_transition(QueryFragmentTransition::failed_now(error(idx)));
            update.update(&db).await.unwrap();
        }

        let query = query::Entity::find_by_id(fragments[0].query_id)
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(query.state, QueryState::Failed);

        assert_eq!(
            query.error,
            Some(QueryError {
                host_addr: fragments[0].host_addr.clone(),
                error: error(0),
            })
        );
    }

    #[proptest(async = "tokio")]
    async fn sources_and_sinks_of_an_active_query_cannot_be_deleted(mut req: CreateQueryWithRefs) {
        let db = Database::for_test().await;
        let (_, fragments) = setup(&db, &mut req).await;
        let source = physical::Entity::delete_by_id(*req.query.source_ids.first().unwrap());
        let sink = sink::Entity::delete_by_id(*req.query.sink_ids.first().unwrap());

        assert!(source.clone().exec(&db).await.is_err());
        assert!(sink.clone().exec(&db).await.is_err());
        let shared = req.kind == ConnectorKind::Shared;
        let rejected = shared.then_some(ErrorCode::CatalogWriteRejected);
        assert_eq!(
            code_of(DropPhysicalSource::all().execute(&db).await),
            rejected
        );
        assert_eq!(code_of(DropSink::all().execute(&db).await), rejected);

        walk_all(&fragments, QueryFragmentState::Completed, &db).await;

        source.exec(&db).await.unwrap();
        sink.exec(&db).await.unwrap();
    }

    #[proptest(async = "tokio")]
    async fn queries_do_not_affect_each_other(mut req: CreateQueryWithRefs) {
        let db = Database::for_test().await;
        req.workers.iter_mut().for_each(|w| w.max_operators = None);
        let (first, first_fragments) = setup(&db, &mut req).await;
        let second = CreateQuery {
            name: Some("Second".into()),
            ..req.query.clone()
        };
        let second = second.execute(&db).await.unwrap();
        let get_second = GetQuery::all().with_id(second.query.id);
        assert_eq!(
            get_second.execute(&db).await.unwrap()[0].query,
            second.query
        );

        let drop_first = DropQuery::all().with_filters(GetQuery::all().with_id(first.id));
        assert_eq!(drop_first.execute(&db).await.unwrap(), [first.clone()]);
        walk_one(&first_fragments[0], QueryFragmentState::Failed, &db).await;

        let found = get_second.with_fragments().execute(&db).await.unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].query, second.query);
        assert!(
            found[0]
                .fragments
                .iter()
                .all(|f| second.fragments.contains(f))
        );
        let by_name = GetQuery {
            name: second.query.name.clone(),
            ..GetQuery::all()
        };
        assert_eq!(by_name.execute(&db).await.unwrap()[0].query, second.query);
        let failed = GetQuery {
            state: Some(QueryState::Failed),
            ..GetQuery::all()
        };
        let failed = failed.execute(&db).await.unwrap();
        assert_eq!((failed.len(), failed[0].query.id), (1, first.id));

        query::Entity::delete_by_id(first.id)
            .exec(&db)
            .await
            .unwrap();
        let sources = physical::Entity::find().count(&db).await.unwrap();
        assert_eq!(sources, req.query.source_ids.len() as u64);
        assert_eq!(sink::Entity::find().count(&db).await.unwrap(), 1);
    }

    #[proptest(async = "tokio")]
    async fn create_rejects_incomplete_or_dangling_queries(mut req: CreateQueryWithRefs) {
        let db = &Database::for_test().await;
        setup_refs(db, &mut req).await;
        let code = |query: CreateQuery| async move {
            code_of(ParsedStatement::CreateQuery(query).execute_with(db).await)
        };
        let valid = req.query;
        let incomplete = [
            CreateQuery {
                fragments: vec![],
                ..valid.clone()
            },
            CreateQuery {
                source_ids: [].into(),
                ..valid.clone()
            },
            CreateQuery {
                sink_ids: [].into(),
                ..valid.clone()
            },
        ];
        for query in incomplete {
            assert_eq!(code(query).await, Some(ErrorCode::QueryInvalid));
        }
        let unknown_source = CreateQuery {
            source_ids: [i64::MAX.into()].into(),
            ..valid.clone()
        };
        let unknown_sink = CreateQuery {
            sink_ids: [i64::MAX.into()].into(),
            ..valid
        };
        assert_eq!(
            code(unknown_source).await,
            Some(ErrorCode::UnknownSourceName)
        );
        assert_eq!(code(unknown_sink).await, Some(ErrorCode::UnknownSinkName));
        assert_eq!(query::Entity::find().count(db).await.unwrap(), 0);
        assert_eq!(query_fragment::Entity::find().count(db).await.unwrap(), 0);
    }

    fn states() -> Vec<QueryFragmentState> {
        QueryFragmentState::iter().collect()
    }

    fn legal(from: QueryFragmentState, to: QueryFragmentState) -> bool {
        use QueryFragmentState::*;
        match from {
            Pending => to != Pending,
            Started => to != Started,
            Running => matches!(to, Pending | Completed | Stopped | Failed),
            Completed | Stopped | Failed => false,
        }
    }

    fn derived(states: &[QueryFragmentState], previous: QueryState) -> QueryState {
        use QueryFragmentState::*;
        let only = |allowed: &[QueryFragmentState]| states.iter().all(|s| allowed.contains(s));
        if states.contains(&Failed) {
            QueryState::Failed
        } else if states.contains(&Pending) {
            QueryState::Pending
        } else if only(&[Completed]) {
            QueryState::Completed
        } else if only(&[Completed, Stopped]) {
            QueryState::Stopped
        } else if only(&[Running, Completed]) {
            QueryState::Running
        } else if only(&[Started, Running, Completed]) {
            QueryState::Started
        } else {
            previous
        }
    }

    #[proptest(async = "tokio")]
    async fn direct_query_transitions_follow_the_state_machine(
        mut req: CreateQueryWithRefs,
        #[strategy(select(states()))] from: QueryFragmentState,
        #[strategy(select(states()))] to: QueryFragmentState,
    ) {
        let db = Database::for_test().await;
        let (_, fragments) = setup(&db, &mut req).await;
        let (query, _) = walk_all(&fragments, from, &db).await;

        let mut update: query::ActiveModel = query.into();
        update.state = Set(QueryState::try_from_value(&to.to_value()).unwrap());
        assert_eq!(
            update.update(&db).await.is_ok(),
            from == to || legal(from, to)
        );
    }

    #[proptest(async = "tokio")]
    async fn fragment_transitions_follow_the_state_machine(
        mut req: CreateQueryWithRefs,
        #[strategy(subsequence(states(), 1..=6))] targets: Vec<QueryFragmentState>,
        #[strategy(vec((option::of(any::<Index>()), any::<Index>()), 0..32))] steps: Vec<(
            Option<Index>,
            Index,
        )>,
    ) {
        let db = Database::for_test().await;
        let (query, mut fragments) = setup(&db, &mut req).await;
        let mut expected = query.state;
        for (only, target) in steps {
            let to = *target.get(&targets);
            let selected = only.map_or(0..fragments.len(), |i| {
                let i = i.index(fragments.len());
                i..i + 1
            });
            for i in selected {
                let from = fragments[i].current_state;
                match fragments[i].with_state(to).update(&db).await {
                    Ok(row) => {
                        assert!(from == to || legal(from, to));
                        fragments[i] = row;
                    }
                    Err(err) => {
                        assert!(from != to && !legal(from, to));
                        assert!(
                            err.to_string()
                                .contains("Invalid query_fragment state transition"),
                            "{err}"
                        );
                        assert_eq!(classify_write(&err), WriteFailure::Rejected);
                    }
                }
                let states: Vec<_> = fragments.iter().map(|f| f.current_state).collect();
                expected = derived(&states, expected);
                let query = query::Entity::find_by_id(query.id)
                    .one(&db)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(query.state, expected);
            }
        }
    }
}
