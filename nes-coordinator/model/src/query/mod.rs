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
use crate::sink::{self, CreateInlineSink, CreateSink, SinkType};
use crate::source::logical::CreateLogicalSource;
use crate::source::physical::{self, CreateInlineSource, CreatePhysicalSource, SourceType};
use crate::worker::CreateWorker;
use crate::{ConnectorKind, Execute};
use anyhow::Result;
use proptest::arbitrary::Arbitrary;
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
    // Derived automatically from the states of this query's fragments;
    // never written by the application directly.
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
pub enum SourceFactory {
    Shared(CreatePhysicalSource),
    Inline(CreateInlineSource),
}

impl Execute for SourceFactory {
    type Response = physical::Model;
    async fn execute(&self, conn: &impl ConnectionTrait) -> Result<physical::Model> {
        match self {
            Self::Shared(s) => s.execute(conn).await,
            Self::Inline(s) => s.execute(conn).await,
        }
    }
}

#[derive(Debug, Clone)]
pub enum SinkFactory {
    Shared(CreateSink),
    Inline(CreateInlineSink),
}

impl Execute for SinkFactory {
    type Response = sink::Model;
    async fn execute(&self, conn: &impl ConnectionTrait) -> Result<sink::Model> {
        match self {
            Self::Shared(s) => s.execute(conn).await,
            Self::Inline(s) => s.execute(conn).await,
        }
    }
}

/// Test fixture bundling a `CreateQuery` with every catalog row it
/// depends on (workers, logical source, physical sources, sink). The
/// `setup` helpers insert them in dependency order.
#[derive(Debug, Clone)]
pub struct CreateQueryWithRefs {
    pub workers: Vec<CreateWorker>,
    pub logical_source: CreateLogicalSource,
    pub physical_sources: Vec<SourceFactory>,
    pub sink: SinkFactory,
    pub query: CreateQuery,
}

impl Arbitrary for CreateQueryWithRefs {
    type Parameters = ();
    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        use fragment::FragmentsWithRefs;
        use proptest::prelude::*;

        let source_name = "[a-z][a-z0-9_]{2,29}";
        let sink_name = "[a-z][a-z0-9_]{2,29}";
        let query_name = proptest::option::of("[a-z][a-z0-9_]{2,29}");
        let statement = (source_name, sink_name)
            .prop_map(|(source, sink)| format!("SELECT * FROM {} INTO {};", source, sink));
        let kind = prop_oneof![
            Just(ConnectorKind::Shared),
            Just(ConnectorKind::Inline),
            Just(ConnectorKind::Internal),
        ];

        any::<FragmentsWithRefs>()
            .prop_flat_map(move |fragments| {
                let n = fragments.fragments.len();
                (
                    Just(fragments),
                    source_name,
                    any::<SourceType>(),
                    sink_name,
                    any::<SinkType>(),
                    statement.clone(),
                    0..n,
                    kind.clone(),
                    query_name.clone(),
                )
            })
            .prop_map(
                |(
                    fragments,
                    source_name,
                    source_type,
                    sink_name,
                    sink_type,
                    statement,
                    sink_frag_idx,
                    kind,
                    query_name,
                )| {
                    let logical_source = CreateLogicalSource {
                        name: source_name.clone(),
                        schema: Default::default(),
                        if_not_exists: false,
                    };

                    let sink_host = fragments.fragments[sink_frag_idx].host_addr.clone();
                    let physical_sources: Vec<SourceFactory> = fragments
                        .fragments
                        .iter()
                        .filter(|f| f.has_source)
                        .map(|f| match kind {
                            ConnectorKind::Shared => SourceFactory::Shared(CreatePhysicalSource {
                                logical_source: source_name.clone(),
                                host_addr: f.host_addr.clone(),
                                source_type,
                                source_config: Default::default(),
                                parser_config: Default::default(),
                                if_not_exists: false,
                            }),
                            ConnectorKind::Inline | ConnectorKind::Internal => {
                                SourceFactory::Inline(CreateInlineSource {
                                    source_type,
                                    source_config: Default::default(),
                                    parser_config: Default::default(),
                                    host_addr: f.host_addr.clone(),
                                    internal: kind == ConnectorKind::Internal,
                                })
                            }
                        })
                        .collect();

                    let sink = match kind {
                        ConnectorKind::Shared => SinkFactory::Shared(CreateSink {
                            name: sink_name,
                            host_addr: sink_host,
                            sink_type,
                            schema: Default::default(),
                            config: Default::default(),
                            if_not_exists: false,
                        }),
                        ConnectorKind::Inline | ConnectorKind::Internal => {
                            SinkFactory::Inline(CreateInlineSink {
                                sink_type,
                                schema: Default::default(),
                                config: Default::default(),
                                host_addr: sink_host,
                                internal: kind == ConnectorKind::Internal,
                            })
                        }
                    };

                    let mut query = CreateQuery::new(statement).with_fragments(fragments.fragments);
                    if let Some(name) = query_name {
                        query = query.with_name(name);
                    }
                    CreateQueryWithRefs {
                        workers: fragments.workers,
                        logical_source,
                        physical_sources,
                        sink,
                        query,
                    }
                },
            )
            .boxed()
    }

    type Strategy = BoxedStrategy<Self>;
}

#[cfg(test)]
pub(crate) async fn setup_refs(db: &crate::database::Database, req: &mut CreateQueryWithRefs) {
    for w in &req.workers {
        w.execute(db).await.unwrap();
    }
    req.logical_source.execute(db).await.unwrap();
    let mut source_ids = Vec::new();
    for create_physical in &req.physical_sources {
        let m = create_physical.execute(db).await.unwrap();
        source_ids.push(m.id);
    }
    let sink = req.sink.execute(db).await.unwrap();
    req.query.source_ids = source_ids;
    req.query.sink_ids = vec![sink.id];
}

#[cfg(test)]
pub(crate) async fn setup(
    db: &crate::database::Database,
    req: &mut CreateQueryWithRefs,
) -> (Model, Vec<fragment::Model>) {
    setup_refs(db, req).await;
    req.query.execute(db).await.unwrap()
}

#[cfg(test)]
pub(crate) async fn walk_one(
    fragment: &fragment::Model,
    target: fragment::FragmentState,
    db: &crate::database::Database,
) -> fragment::Model {
    use fragment::FragmentState;
    let path = match target {
        FragmentState::Pending => vec![],
        FragmentState::Registered => vec![FragmentState::Registered],
        FragmentState::Running => vec![FragmentState::Registered, FragmentState::Running],
        FragmentState::Completed => vec![
            FragmentState::Registered,
            FragmentState::Running,
            FragmentState::Completed,
        ],
        FragmentState::Stopped => vec![FragmentState::Stopped],
        FragmentState::Failed => vec![FragmentState::Failed],
    };
    let mut current = fragment.clone();
    for state in path {
        if current.current_state >= state {
            continue;
        }
        current.with_state(state).update(db).await.unwrap();
        current = fragment::Entity::find_by_id(current.id)
            .one(db)
            .await
            .unwrap()
            .unwrap();
    }
    current
}

#[cfg(test)]
pub(crate) async fn walk_all(
    fragments: &[fragment::Model],
    target: fragment::FragmentState,
    db: &crate::database::Database,
) -> (Model, Vec<fragment::Model>) {
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
pub(crate) fn rule_vector(
    n: usize,
) -> impl proptest::strategy::Strategy<Value = (Vec<fragment::FragmentState>, QueryState)> {
    use fragment::FragmentState::*;
    use proptest::collection::vec;
    use proptest::prelude::*;
    let any_state = prop_oneof![
        Just(Pending),
        Just(Registered),
        Just(Running),
        Just(Completed),
        Just(Stopped),
        Just(Failed),
    ];
    let no_failed = prop_oneof![
        Just(Pending),
        Just(Registered),
        Just(Running),
        Just(Completed),
        Just(Stopped),
    ];
    prop_oneof![
        vec(any_state, n).prop_map(|mut v| {
            v[0] = Failed;
            (v, QueryState::Failed)
        }),
        vec(no_failed, n).prop_map(|mut v| {
            v[0] = Pending;
            (v, QueryState::Pending)
        }),
        Just((vec![Completed; n], QueryState::Completed)),
        vec(prop_oneof![Just(Stopped), Just(Completed)], n).prop_map(|mut v| {
            v[0] = Stopped;
            (v, QueryState::Stopped)
        }),
        vec(prop_oneof![Just(Running), Just(Completed)], n).prop_map(|mut v| {
            v[0] = Running;
            (v, QueryState::Running)
        }),
        vec(
            prop_oneof![Just(Registered), Just(Running), Just(Completed)],
            n,
        )
        .prop_map(|mut v| {
            v[0] = Registered;
            (v, QueryState::Registered)
        }),
    ]
}

#[cfg(test)]
pub(crate) fn req_with_targets() -> impl proptest::strategy::Strategy<
    Value = (
        CreateQueryWithRefs,
        Vec<fragment::FragmentState>,
        QueryState,
    ),
> {
    use proptest::prelude::*;
    any::<CreateQueryWithRefs>().prop_flat_map(|req| {
        let n = req.query.fragments.len();
        rule_vector(n).prop_map(move |(states, expected)| (req.clone(), states, expected))
    })
}

#[cfg(test)]
pub(crate) const INVALID_TRANSITIONS: &[(fragment::FragmentState, fragment::FragmentState)] = &[
    (
        fragment::FragmentState::Pending,
        fragment::FragmentState::Running,
    ),
    (
        fragment::FragmentState::Pending,
        fragment::FragmentState::Completed,
    ),
    (
        fragment::FragmentState::Registered,
        fragment::FragmentState::Completed,
    ),
    (
        fragment::FragmentState::Running,
        fragment::FragmentState::Registered,
    ),
    (
        fragment::FragmentState::Completed,
        fragment::FragmentState::Running,
    ),
    (
        fragment::FragmentState::Stopped,
        fragment::FragmentState::Running,
    ),
    (
        fragment::FragmentState::Failed,
        fragment::FragmentState::Running,
    ),
];

#[cfg(test)]
mod tests {
    use crate::Execute;
    use crate::database::Database;
    use crate::query::fragment::{
        self, DesiredFragmentState, FragmentError, FragmentState, FragmentTransition, StopMode,
    };
    use crate::query::query_state::QueryState;
    use crate::query::{
        self, CreateQueryWithRefs, DropQuery, GetQuery, INVALID_TRANSITIONS, SourceFactory,
        req_with_targets, setup, walk_all, walk_one,
    };
    use crate::sink;
    use crate::source::physical;
    use sea_orm::{ActiveModelTrait, ActiveValue::Set, ColumnTrait, EntityTrait, QueryFilter};
    use test_strategy::proptest;

    #[proptest(async = "tokio")]
    async fn create_and_get(mut req: CreateQueryWithRefs) {
        let db = Database::for_test().await;
        let (created, _) = setup(&db, &mut req).await;
        assert_eq!(created.name, req.query.name);
        assert_eq!(created.sql, req.query.sql);
        assert_eq!(created.state, QueryState::Pending);

        let results = GetQuery::all()
            .with_id(created.id)
            .execute(&db)
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
            .execute(&db)
            .await
            .unwrap();

        let fragments = fragment::Entity::find()
            .filter(fragment::Column::QueryId.eq(created.id))
            .all(&db)
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
    async fn query_state_derived_from_fragments(mut req: CreateQueryWithRefs) {
        let db = Database::for_test().await;
        let (_, fragments) = setup(&db, &mut req).await;

        let steps: &[(FragmentState, QueryState)] = &[
            (FragmentState::Pending, QueryState::Pending),
            (FragmentState::Registered, QueryState::Registered),
            (FragmentState::Running, QueryState::Running),
            (FragmentState::Completed, QueryState::Completed),
        ];

        let mut current_fragments = fragments;
        for &(fragment_state, expected_query_state) in steps {
            let (query, fragments) = walk_all(&current_fragments, fragment_state, &db).await;
            current_fragments = fragments;
            assert_eq!(query.state, expected_query_state);
        }
    }

    #[proptest(async = "tokio")]
    async fn correct_defaults(mut req: CreateQueryWithRefs) {
        let db = Database::for_test().await;
        let (query, fragments) = setup(&db, &mut req).await;
        assert!(query.error.is_none());
        assert!(query.start_timestamp.is_none());
        assert!(query.stop_timestamp.is_none());
        assert_eq!(query.state, QueryState::Pending);
        assert!(fragments.iter().all(|f| {
            f.query_id == query.id
                && f.current_state == FragmentState::Pending
                && f.start_timestamp.is_none()
                && f.stop_timestamp.is_none()
                && f.error.is_none()
        }));
    }

    #[proptest(async = "tokio")]
    async fn one_failed_fragment_fails_query(mut req: CreateQueryWithRefs) {
        let db = Database::for_test().await;
        let (query, fragments) = setup(&db, &mut req).await;

        let update: fragment::ActiveModel = fragments[0].with_state(FragmentState::Failed);
        update.update(&db).await.unwrap();

        let query = query::Entity::find_by_id(query.id)
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(query.state, QueryState::Failed);
    }

    #[proptest(async = "tokio")]
    async fn timestamp_derivation(mut req: CreateQueryWithRefs) {
        let db = Database::for_test().await;
        let (_, fragments) = setup(&db, &mut req).await;
        let (query, fragments) = walk_all(&fragments, FragmentState::Running, &db).await;
        assert!(query.start_timestamp.is_some());
        assert!(query.stop_timestamp.is_none());
        let (query, _) = walk_all(&fragments, FragmentState::Completed, &db).await;
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
        let shared = matches!(req.physical_sources[0], SourceFactory::Shared(_));

        query::Entity::delete_by_id(query_id)
            .exec(&db)
            .await
            .unwrap();

        assert!(
            fragment::Entity::find()
                .filter(fragment::Column::QueryId.eq(query_id))
                .all(&db)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            query_source::Entity::find()
                .filter(query_source::Column::QueryId.eq(query_id))
                .all(&db)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            query_sink::Entity::find()
                .filter(query_sink::Column::QueryId.eq(query_id))
                .all(&db)
                .await
                .unwrap()
                .is_empty()
        );

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

    #[proptest(async = "tokio")]
    async fn query_error_aggregates_per_host(mut req: CreateQueryWithRefs) {
        let db = Database::for_test().await;
        let (_, fragments) = setup(&db, &mut req).await;

        let mut update: fragment::ActiveModel = fragments[0].clone().into();
        update.apply_transition(FragmentTransition::failed_now(FragmentError::Transport {
            msg: "boom".into(),
        }));
        update.update(&db).await.unwrap();

        let query = query::Entity::find_by_id(fragments[0].query_id)
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(query.state, QueryState::Failed);

        let host = fragments[0].host_addr.to_string();
        assert_eq!(query.error, Some(serde_json::json!({ host: "boom" })));
    }

    #[proptest(async = "tokio")]
    async fn query_invalid_transition_rejected(
        mut req: CreateQueryWithRefs,
        #[strategy(proptest::sample::select(INVALID_TRANSITIONS.to_vec()))] transition: (
            FragmentState,
            FragmentState,
        ),
    ) {
        let db = Database::for_test().await;
        let (_, fragments) = setup(&db, &mut req).await;
        let (from, to) = transition;
        let (query, _) = walk_all(&fragments, from, &db).await;

        let to_query = match to {
            FragmentState::Pending => QueryState::Pending,
            FragmentState::Registered => QueryState::Registered,
            FragmentState::Running => QueryState::Running,
            FragmentState::Completed => QueryState::Completed,
            FragmentState::Stopped => QueryState::Stopped,
            FragmentState::Failed => QueryState::Failed,
        };
        let mut bad: query::ActiveModel = query.into();
        bad.state = Set(to_query);
        assert!(bad.update(&db).await.is_err());
    }

    #[proptest(async = "tokio")]
    async fn mixed_fragment_states_derive_query(
        #[strategy(req_with_targets())] case: (CreateQueryWithRefs, Vec<FragmentState>, QueryState),
    ) {
        let (mut req, targets, expected) = case;
        let db = Database::for_test().await;
        let (_, fragments) = setup(&db, &mut req).await;
        let query_id = fragments[0].query_id;

        for (f, target) in fragments.iter().zip(&targets) {
            walk_one(f, *target, &db).await;
        }

        let query = query::Entity::find_by_id(query_id)
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(query.state, expected);
    }

    #[proptest(async = "tokio")]
    async fn query_state_preserved_for_unmatched_vector(mut req: CreateQueryWithRefs) {
        proptest::prop_assume!(req.query.fragments.len() >= 2);
        let db = Database::for_test().await;
        let (_, fragments) = setup(&db, &mut req).await;

        let (query, fragments) = walk_all(&fragments, FragmentState::Running, &db).await;
        assert_eq!(query.state, QueryState::Running);

        walk_one(&fragments[0], FragmentState::Stopped, &db).await;

        let query = query::Entity::find_by_id(fragments[0].query_id)
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(query.state, QueryState::Running);
    }
}
