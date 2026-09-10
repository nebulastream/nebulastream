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

//! The query entity and its create/drop/read requests. A query owns a set of
//! fragments, sources, and sinks, and its lifecycle state is derived from its
//! fragments.

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

use crate::Execute;
use crate::identifier::QueryId;
use crate::sink::{self, CreateAnonymousSink, CreateSink};
use crate::source::physical::{self, CreateAnonymousSource, CreatePhysicalSource};
use anyhow::Result;
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
    pub start_timestamp: Option<chrono::DateTime<chrono::Utc>>,
    pub stop_timestamp: Option<chrono::DateTime<chrono::Utc>>,
    #[sea_orm(column_type = "JsonBinary")]
    pub error: Option<serde_json::Value>,
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

#[derive(Debug, Clone)]
pub enum SourceFactory {
    Shared(CreatePhysicalSource),
    Anonymous(CreateAnonymousSource),
}

impl Execute for SourceFactory {
    type Response = physical::Model;
    async fn execute(&self, conn: &impl ConnectionTrait) -> Result<physical::Model> {
        match self {
            Self::Shared(source) => source.execute(conn).await,
            Self::Anonymous(source) => source.execute(conn).await,
        }
    }
}

#[derive(Debug, Clone)]
pub enum SinkFactory {
    Shared(CreateSink),
    Anonymous(CreateAnonymousSink),
}

impl Execute for SinkFactory {
    type Response = sink::Model;
    async fn execute(&self, conn: &impl ConnectionTrait) -> Result<sink::Model> {
        match self {
            Self::Shared(sink) => sink.execute(conn).await,
            Self::Anonymous(sink) => sink.execute(conn).await,
        }
    }
}

#[cfg(any(test, feature = "testing"))]
mod arbitrary;
#[cfg(any(test, feature = "testing"))]
pub use arbitrary::CreateQueryWithRefs;

#[cfg(test)]
pub(crate) async fn setup_refs(db: &crate::database::Database, req: &mut CreateQueryWithRefs) {
    for w in &req.workers {
        w.execute(db).await.unwrap();
    }
    req.logical_source.execute(db).await.unwrap();
    let mut source_ids = std::collections::BTreeSet::new();
    for create_physical in &req.physical_sources {
        let m = create_physical.execute(db).await.unwrap();
        source_ids.insert(m.id);
    }
    let sink = req.sink.execute(db).await.unwrap();
    req.query.source_ids = source_ids;
    req.query.sink_ids = std::collections::BTreeSet::from([sink.id]);
}

#[cfg(test)]
pub(crate) async fn setup(
    db: &crate::database::Database,
    req: &mut CreateQueryWithRefs,
) -> (Model, Vec<query_fragment::Model>) {
    setup_refs(db, req).await;
    req.query.execute(db).await.unwrap()
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
pub(crate) fn rule_vector(
    n: usize,
) -> impl proptest::strategy::Strategy<Value = (Vec<query_fragment::QueryFragmentState>, QueryState)>
{
    use proptest::collection::vec;
    use proptest::prelude::*;
    use query_fragment::QueryFragmentState::*;
    let any_state = prop_oneof![
        Just(Pending),
        Just(Started),
        Just(Running),
        Just(Completed),
        Just(Stopped),
        Just(Failed),
    ];
    let no_failed = prop_oneof![
        Just(Pending),
        Just(Started),
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
            prop_oneof![Just(Started), Just(Running), Just(Completed)],
            n,
        )
        .prop_map(|mut v| {
            v[0] = Started;
            (v, QueryState::Started)
        }),
    ]
}

#[cfg(test)]
pub(crate) fn req_with_targets() -> impl proptest::strategy::Strategy<
    Value = (
        CreateQueryWithRefs,
        Vec<query_fragment::QueryFragmentState>,
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
pub(crate) const INVALID_TRANSITIONS: &[(
    query_fragment::QueryFragmentState,
    query_fragment::QueryFragmentState,
)] = &[
    (
        query_fragment::QueryFragmentState::Pending,
        query_fragment::QueryFragmentState::Running,
    ),
    (
        query_fragment::QueryFragmentState::Pending,
        query_fragment::QueryFragmentState::Completed,
    ),
    (
        query_fragment::QueryFragmentState::Running,
        query_fragment::QueryFragmentState::Started,
    ),
    (
        query_fragment::QueryFragmentState::Completed,
        query_fragment::QueryFragmentState::Running,
    ),
    (
        query_fragment::QueryFragmentState::Stopped,
        query_fragment::QueryFragmentState::Running,
    ),
    (
        query_fragment::QueryFragmentState::Failed,
        query_fragment::QueryFragmentState::Running,
    ),
];

#[cfg(test)]
mod tests {
    use crate::Execute;
    use crate::database::Database;
    use crate::query::query_fragment::{
        self, DesiredQueryFragmentState, QueryFragmentError, QueryFragmentState,
        QueryFragmentTransition,
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
    async fn query_state_derived_from_fragments(mut req: CreateQueryWithRefs) {
        let db = Database::for_test().await;
        let (_, fragments) = setup(&db, &mut req).await;

        let steps: &[(QueryFragmentState, QueryState)] = &[
            (QueryFragmentState::Pending, QueryState::Pending),
            (QueryFragmentState::Started, QueryState::Started),
            (QueryFragmentState::Running, QueryState::Running),
            (QueryFragmentState::Completed, QueryState::Completed),
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
                && f.current_state == QueryFragmentState::Pending
                && f.start_timestamp.is_none()
                && f.stop_timestamp.is_none()
                && f.error.is_none()
        }));
    }

    #[proptest(async = "tokio")]
    async fn one_failed_fragment_fails_query(mut req: CreateQueryWithRefs) {
        let db = Database::for_test().await;
        let (query, fragments) = setup(&db, &mut req).await;

        let update: query_fragment::ActiveModel =
            fragments[0].with_state(QueryFragmentState::Failed);
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
        let (query, fragments) = walk_all(&fragments, QueryFragmentState::Running, &db).await;
        assert!(query.start_timestamp.is_some());
        assert!(query.stop_timestamp.is_none());
        let (query, _) = walk_all(&fragments, QueryFragmentState::Completed, &db).await;
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
            query_fragment::Entity::find()
                .filter(query_fragment::Column::QueryId.eq(query_id))
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

        let mut update: query_fragment::ActiveModel = fragments[0].clone().into();
        update.apply_transition(QueryFragmentTransition::failed_now(
            QueryFragmentError::Transport { msg: "boom".into() },
        ));
        update.update(&db).await.unwrap();

        let query = query::Entity::find_by_id(fragments[0].query_id)
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(query.state, QueryState::Failed);

        let host = fragments[0].host_addr.to_string();
        assert_eq!(query.error, Some(serde_json::json!({ host: ["boom"] })));
    }

    #[proptest(async = "tokio")]
    async fn query_error_aggregates_multiple_fragments_on_same_host(mut req: CreateQueryWithRefs) {
        proptest::prop_assume!(req.query.fragments.len() >= 2);
        let db = Database::for_test().await;

        // Force the first two fragments onto one worker (unbounded so the
        // combined placement is never rejected), so a single host carries
        // two failing fragments.
        let host = req.workers[0].host_addr.clone();
        req.workers[0].max_operators = None;
        req.query.fragments[0].host_addr = host.clone();
        req.query.fragments[1].host_addr = host.clone();

        let (_, fragments) = setup(&db, &mut req).await;

        // Fail every fragment on that host with a distinct message.
        let on_host: Vec<_> = fragments.iter().filter(|f| f.host_addr == host).collect();
        let mut expected: Vec<String> = Vec::new();
        for (idx, frag) in on_host.iter().enumerate() {
            let msg = format!("boom{idx}");
            expected.push(msg.clone());
            let mut update: query_fragment::ActiveModel = (*frag).clone().into();
            update.apply_transition(QueryFragmentTransition::failed_now(
                QueryFragmentError::Transport { msg },
            ));
            update.update(&db).await.unwrap();
        }

        let query = query::Entity::find_by_id(fragments[0].query_id)
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(query.state, QueryState::Failed);

        // The host maps to the full list of messages, none dropped.
        let errors = query.error.expect("query.error must be set when failed");
        let messages = errors
            .get(host.to_string())
            .and_then(|m| m.as_array())
            .expect("per-host errors must be a JSON list");
        let got: std::collections::HashSet<&str> =
            messages.iter().filter_map(|m| m.as_str()).collect();
        let want: std::collections::HashSet<&str> = expected.iter().map(String::as_str).collect();
        assert_eq!(got, want);
    }

    #[proptest(async = "tokio")]
    async fn query_invalid_transition_rejected(
        mut req: CreateQueryWithRefs,
        #[strategy(proptest::sample::select(INVALID_TRANSITIONS.to_vec()))] transition: (
            QueryFragmentState,
            QueryFragmentState,
        ),
    ) {
        let db = Database::for_test().await;
        let (_, fragments) = setup(&db, &mut req).await;
        let (from, to) = transition;
        let (query, _) = walk_all(&fragments, from, &db).await;

        let to_query = match to {
            QueryFragmentState::Pending => QueryState::Pending,
            QueryFragmentState::Started => QueryState::Started,
            QueryFragmentState::Running => QueryState::Running,
            QueryFragmentState::Completed => QueryState::Completed,
            QueryFragmentState::Stopped => QueryState::Stopped,
            QueryFragmentState::Failed => QueryState::Failed,
        };
        let mut bad: query::ActiveModel = query.into();
        bad.state = Set(to_query);
        assert!(bad.update(&db).await.is_err());
    }

    #[proptest(async = "tokio")]
    async fn mixed_fragment_states_derive_query(
        #[strategy(req_with_targets())] case: (
            CreateQueryWithRefs,
            Vec<QueryFragmentState>,
            QueryState,
        ),
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

        let (query, fragments) = walk_all(&fragments, QueryFragmentState::Running, &db).await;
        assert_eq!(query.state, QueryState::Running);

        walk_one(&fragments[0], QueryFragmentState::Stopped, &db).await;

        let query = query::Entity::find_by_id(fragments[0].query_id)
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(query.state, QueryState::Running);
    }
}
