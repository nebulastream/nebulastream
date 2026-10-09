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

use super::lifecycle::{
    LifecycleAction, LifecycleStep, QueryFragmentLifecycle, WorkerCall, WorkerReply,
};
use super::{CallError, Transport};
use crate::remote::worker_rpc_service::nes::{SerializableQueryId, SerializableQueryPlan};
use crate::util::buggify::{buggify, buggify_return};
use chrono::{DateTime, Utc};
use model::error::{CodedError, ErrorCode};
use model::identifier::QueryFragmentId;
use model::query::query_fragment;
use model::query::query_fragment::{DesiredQueryFragmentState, QueryFragmentError};
use sea_orm::{ActiveModelTrait, DatabaseConnection};
use std::sync::Arc;
use tokio::sync::watch;
use tracing::debug;

/// Moves a single query_fragment through its lifecycle from pending through to a terminal state.
/// Uses a transport to call the worker this fragment is hosted on,
/// instructing it to start, stop, and report the status of the fragment.
/// Owns the catalog row of the query fragment, reads its current/desired state,
/// and updates the catalog row based on the interactions with the worker.
pub struct QueryFragmentTask<T> {
    fragment: query_fragment::Model,
    update: query_fragment::ActiveModel,
    db: DatabaseConnection,
    transport: T,
    /// Communicate state changes to the `request_handler`.
    state_tx: Arc<watch::Sender<()>>,
    desired: watch::Receiver<DesiredQueryFragmentState>,
    /// Encodes the state transitions based on the observed interactions with the worker.
    lifecycle: QueryFragmentLifecycle,
}

impl<T: Transport> QueryFragmentTask<T> {
    pub fn new(
        fragment: query_fragment::Model,
        db: DatabaseConnection,
        transport: T,
        state_tx: Arc<watch::Sender<()>>,
        desired: watch::Receiver<DesiredQueryFragmentState>,
    ) -> Self {
        let lifecycle = QueryFragmentLifecycle::new(&fragment, transport.poll_interval());
        let update = fragment.clone().into();
        Self {
            fragment,
            update,
            db,
            transport,
            state_tx,
            desired,
            lifecycle,
        }
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        debug!(state = %self.fragment.current_state, "starting");
        loop {
            self.lifecycle.desired(*self.desired.borrow_and_update());
            match self.lifecycle.next() {
                LifecycleAction::Done => return Ok(()),
                LifecycleAction::Wait(wait) => {
                    tokio::select! {
                        () = tokio::time::sleep(wait) => {}
                        _ = self.desired.changed() => {}
                    }
                }
                LifecycleAction::Call(call) => {
                    let reply = self.call(call).await;
                    let now = Utc::now();
                    let step = self.lifecycle.apply(reply, now);
                    if let Some(wait) = step.retry_wait {
                        tokio::time::sleep(wait).await;
                    }
                    self.persist(step, now).await?;
                }
            }
        }
    }

    async fn call(&self, call: WorkerCall) -> WorkerReply {
        match call {
            WorkerCall::Start => {
                WorkerReply::Start(match plan_for(self.fragment.id, &self.fragment.plan) {
                    Ok(plan) => self.transport.start(plan).await,
                    Err(err) => Err(CallError::Failed(err)),
                })
            }
            WorkerCall::Status => {
                WorkerReply::Status(self.transport.status(self.fragment.id).await)
            }
            WorkerCall::Stop => WorkerReply::Stop(self.transport.stop(self.fragment.id).await),
        }
    }

    async fn persist(&mut self, step: LifecycleStep, now: DateTime<Utc>) -> anyhow::Result<()> {
        if step.transition.is_none() && !step.observed {
            return Ok(());
        }
        let prev = self.fragment.current_state;
        if let Some(transition) = step.transition {
            self.update.apply_transition(transition);
        }
        if step.observed {
            self.update.mark_observed(now);
        }
        self.write().await?;
        if self.fragment.current_state != prev {
            debug!(from = %prev, to = %self.fragment.current_state, "transition");
        }
        buggify_return!(Ok(()));
        let _ = self.state_tx.send(());
        Ok(())
    }

    async fn write(&mut self) -> anyhow::Result<()> {
        if buggify!() {
            buggify_return!(Ok(()));
            return Err(anyhow::anyhow!("buggify: write failed"));
        }
        self.fragment = self.update.clone().update(&self.db).await?;
        self.update = self.fragment.clone().into();
        Ok(())
    }
}

// The stored plan has no id; the worker knows the plan by the query_fragment's id.
fn plan_for(id: QueryFragmentId, plan: &[u8]) -> Result<SerializableQueryPlan, QueryFragmentError> {
    let mut query_plan: SerializableQueryPlan = prost::Message::decode(plan).map_err(|e| {
        QueryFragmentError::Internal(CodedError::new(
            ErrorCode::UnknownException,
            format!("failed to decode stored query plan: {e}"),
        ))
    })?;
    query_plan.query_id = Some(SerializableQueryId::from_fragment_id(*id));
    Ok(query_plan)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RETRY_INTERVAL;
    use crate::query_fragment::{QueryFragmentTasks, RawStatus};
    use model::Execute;
    use model::database::Database;
    use model::identifier::{QueryId, SinkId, SourceId};
    use model::query::query_fragment::{CreateQueryFragment, QueryFragmentState};
    use model::query::{CreateQuery, DropQuery, GetQuery};
    use model::sink::CreateSink;
    use model::source::logical::CreateLogicalSource;
    use model::source::physical::CreatePhysicalSource;
    use model::worker::CreateWorker;
    use model::worker::endpoint::NetworkAddr;
    use sea_orm::EntityTrait;
    use std::collections::{BTreeSet, VecDeque};
    use std::sync::Mutex;
    use std::time::{Duration, Instant};

    const HOST: &str = "127.0.0.1:1";

    struct Scripted {
        answers: Mutex<VecDeque<WorkerReply>>,
        calls: Arc<Mutex<Vec<WorkerCall>>>,
        poll: Duration,
        db: DatabaseConnection,
        query_id: QueryId,
        desired_tx: Arc<watch::Sender<DesiredQueryFragmentState>>,
        stop_before: Option<usize>,
        stop_after: Option<(usize, Duration)>,
    }

    async fn request_stop(
        db: DatabaseConnection,
        query_id: QueryId,
        desired_tx: Arc<watch::Sender<DesiredQueryFragmentState>>,
    ) {
        DropQuery::all()
            .with_filters(GetQuery::all().with_id(query_id))
            .execute(&db)
            .await
            .unwrap();
        let _ = desired_tx.send(DesiredQueryFragmentState::Stopped);
    }

    impl Scripted {
        async fn answer(&self, call: WorkerCall) -> WorkerReply {
            let count = {
                let mut calls = self.calls.lock().unwrap();
                calls.push(call);
                calls.len()
            };
            if self.stop_before == Some(count) {
                request_stop(self.db.clone(), self.query_id, self.desired_tx.clone()).await;
            }
            if let Some((after, delay)) = self.stop_after
                && after == count
            {
                let (db, query_id, desired_tx) =
                    (self.db.clone(), self.query_id, self.desired_tx.clone());
                tokio::spawn(async move {
                    tokio::time::sleep(delay).await;
                    request_stop(db, query_id, desired_tx).await;
                });
            }
            self.answers
                .lock()
                .unwrap()
                .pop_front()
                .expect("a scripted answer for every call")
        }
    }

    impl Transport for Scripted {
        fn poll_interval(&self) -> Duration {
            self.poll
        }

        async fn start(&self, _: SerializableQueryPlan) -> Result<(), CallError> {
            match self.answer(WorkerCall::Start).await {
                WorkerReply::Start(result) => result,
                _ => panic!("the script did not expect a start"),
            }
        }

        async fn stop(&self, _: QueryFragmentId) -> Result<(), CallError> {
            match self.answer(WorkerCall::Stop).await {
                WorkerReply::Stop(result) => result,
                _ => panic!("the script did not expect a stop"),
            }
        }

        async fn status(&self, _: QueryFragmentId) -> Result<RawStatus, CallError> {
            match self.answer(WorkerCall::Status).await {
                WorkerReply::Status(result) => result,
                _ => panic!("the script did not expect a status read"),
            }
        }
    }

    struct Silent;

    impl Transport for Silent {
        fn poll_interval(&self) -> Duration {
            Duration::ZERO
        }

        async fn start(&self, _: SerializableQueryPlan) -> Result<(), CallError> {
            std::future::pending().await
        }

        async fn stop(&self, _: QueryFragmentId) -> Result<(), CallError> {
            std::future::pending().await
        }

        async fn status(&self, _: QueryFragmentId) -> Result<RawStatus, CallError> {
            std::future::pending().await
        }
    }

    async fn fragment_row(db: &Database) -> query_fragment::Model {
        let conn = db.connection();
        let host: NetworkAddr = HOST.parse().unwrap();
        CreateWorker {
            host_addr: host.clone(),
            data_addr: "127.0.0.1:2".parse().unwrap(),
            max_operators: None,
            peers: vec![],
            config: Default::default(),
            if_not_exists: false,
        }
        .execute(conn)
        .await
        .unwrap();
        CreateLogicalSource {
            name: "src".to_string(),
            schema: Default::default(),
            if_not_exists: false,
        }
        .execute(conn)
        .await
        .unwrap();
        let source = CreatePhysicalSource {
            logical_source: "src".to_string(),
            host_addr: host.clone(),
            source_type: "File".to_string(),
            source_config: Default::default(),
            parser_config: Default::default(),
            if_not_exists: false,
        }
        .execute(conn)
        .await
        .unwrap();
        let sink = CreateSink {
            name: "snk".to_string(),
            host_addr: host.clone(),
            sink_type: "File".to_string(),
            schema: Default::default(),
            config: Default::default(),
            if_not_exists: false,
        }
        .execute(conn)
        .await
        .unwrap();
        CreateQuery {
            name: None,
            sql: "SELECT * FROM src INTO snk".to_string(),
            fragments: vec![CreateQueryFragment {
                host_addr: host.clone(),
                plan: vec![],
                num_operators: 1,
                has_source: true,
            }],
            source_ids: BTreeSet::from([SourceId::new(*source.id)]),
            sink_ids: BTreeSet::from([SinkId::new(*sink.id)]),
        }
        .execute(conn)
        .await
        .unwrap();
        query_fragment::Entity::actionable(conn, &host)
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap()
    }

    fn wire(state: QueryFragmentState) -> i32 {
        (0..16)
            .find(|value| QueryFragmentState::try_from(*value) == Ok(state))
            .unwrap()
    }

    fn status(state: QueryFragmentState, start: Option<u64>, stop: Option<u64>) -> WorkerReply {
        WorkerReply::Status(Ok(RawStatus {
            state: wire(state),
            start_ms: start,
            stop_ms: stop,
            error: None,
        }))
    }

    struct Script {
        answers: Vec<WorkerReply>,
        poll: Duration,
        stop_before: Option<usize>,
        stop_after: Option<(usize, Duration)>,
    }

    impl Script {
        fn new(answers: Vec<WorkerReply>) -> Self {
            Self {
                answers,
                poll: Duration::ZERO,
                stop_before: None,
                stop_after: None,
            }
        }
    }

    async fn drive(
        db: &Database,
        fragment: query_fragment::Model,
        script: Script,
    ) -> (Vec<WorkerCall>, query_fragment::Model) {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let (desired_tx, desired_rx) = watch::channel(fragment.desired_state);
        let transport = Scripted {
            answers: Mutex::new(script.answers.into()),
            calls: calls.clone(),
            poll: script.poll,
            db: db.connection().clone(),
            query_id: fragment.query_id,
            desired_tx: Arc::new(desired_tx),
            stop_before: script.stop_before,
            stop_after: script.stop_after,
        };
        let (state_tx, _state_rx) = watch::channel(());
        let id = fragment.id;
        QueryFragmentTask::new(
            fragment,
            db.connection().clone(),
            transport,
            Arc::new(state_tx),
            desired_rx,
        )
        .run()
        .await
        .unwrap();
        let row = query_fragment::Entity::find_by_id(id)
            .one(db.connection())
            .await
            .unwrap()
            .unwrap();
        let calls = calls.lock().unwrap().clone();
        (calls, row)
    }

    #[tokio::test]
    async fn a_fragment_is_started_and_observed_until_it_completes() {
        let db = Database::for_test().await;
        let fragment = fragment_row(&db).await;
        let script = Script::new(vec![
            WorkerReply::Start(Ok(())),
            status(QueryFragmentState::Running, Some(1_000), None),
            status(QueryFragmentState::Completed, Some(1_000), Some(2_000)),
        ]);
        let (calls, row) = drive(&db, fragment, script).await;
        assert_eq!(
            calls,
            vec![WorkerCall::Start, WorkerCall::Status, WorkerCall::Status]
        );
        assert_eq!(row.current_state, QueryFragmentState::Completed);
        assert_eq!(row.start_timestamp.unwrap().timestamp_millis(), 1_000);
        assert_eq!(row.stop_timestamp.unwrap().timestamp_millis(), 2_000);
        assert!(row.last_observed_at.is_some());
    }

    #[tokio::test]
    async fn a_stop_request_turns_the_advance_into_a_stop() {
        let db = Database::for_test().await;
        let fragment = fragment_row(&db).await;
        let mut script = Script::new(vec![
            WorkerReply::Start(Ok(())),
            status(QueryFragmentState::Running, Some(1_000), None),
            status(QueryFragmentState::Running, Some(1_000), None),
            WorkerReply::Stop(Ok(())),
            status(QueryFragmentState::Completed, Some(1_000), Some(3_000)),
        ]);
        script.stop_before = Some(3);
        let (calls, row) = drive(&db, fragment, script).await;
        assert_eq!(
            calls,
            vec![
                WorkerCall::Start,
                WorkerCall::Status,
                WorkerCall::Status,
                WorkerCall::Stop,
                WorkerCall::Status
            ]
        );
        assert_eq!(row.current_state, QueryFragmentState::Stopped);
        assert_eq!(row.desired_state, DesiredQueryFragmentState::Stopped);
        assert_eq!(row.stop_timestamp.unwrap().timestamp_millis(), 3_000);
    }

    #[tokio::test]
    async fn a_stop_request_interrupts_the_poll_wait() {
        let db = Database::for_test().await;
        let fragment = fragment_row(&db).await;
        let mut script = Script::new(vec![
            WorkerReply::Start(Ok(())),
            status(QueryFragmentState::Running, Some(1_000), None),
            WorkerReply::Stop(Ok(())),
            status(QueryFragmentState::Completed, Some(1_000), Some(3_000)),
        ]);
        script.poll = Duration::from_secs(30);
        script.stop_after = Some((2, Duration::from_millis(100)));
        let started = Instant::now();
        let (calls, row) = drive(&db, fragment, script).await;
        assert_eq!(
            calls,
            vec![
                WorkerCall::Start,
                WorkerCall::Status,
                WorkerCall::Stop,
                WorkerCall::Status
            ]
        );
        assert_eq!(row.current_state, QueryFragmentState::Stopped);
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "{:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn a_retryable_failure_is_retried_after_the_interval() {
        let db = Database::for_test().await;
        let fragment = fragment_row(&db).await;
        let script = Script::new(vec![
            WorkerReply::Start(Err(CallError::Transient(QueryFragmentError::Transport {
                msg: "worker busy".to_string(),
            }))),
            WorkerReply::Start(Ok(())),
            status(QueryFragmentState::Completed, Some(1_000), Some(2_000)),
        ]);
        let started = Instant::now();
        let (calls, row) = drive(&db, fragment, script).await;
        assert_eq!(
            calls,
            vec![WorkerCall::Start, WorkerCall::Start, WorkerCall::Status]
        );
        assert_eq!(row.current_state, QueryFragmentState::Completed);
        assert!(row.error.is_none());
        assert!(started.elapsed() >= RETRY_INTERVAL);
    }

    #[tokio::test]
    async fn the_task_set_spawns_once_pushes_the_desired_state_and_forgets_finished_rows() {
        let db = Database::for_test().await;
        let mut fragment = fragment_row(&db).await;
        let (state_tx, _state_rx) = watch::channel(());
        let state_tx = Arc::new(state_tx);
        let mut tasks = QueryFragmentTasks::new();

        tasks.reconcile(vec![fragment.clone()], db.connection(), &state_tx, || {
            Silent
        });
        assert!(tasks.tasks().is_tracked(&fragment.id));
        assert_eq!(
            *tasks.stop_signals[&fragment.id].borrow(),
            DesiredQueryFragmentState::Completed
        );

        fragment.desired_state = DesiredQueryFragmentState::Stopped;
        tasks.reconcile(vec![fragment.clone()], db.connection(), &state_tx, || {
            Silent
        });
        assert_eq!(
            *tasks.stop_signals[&fragment.id].borrow(),
            DesiredQueryFragmentState::Stopped
        );

        tasks.reconcile(vec![], db.connection(), &state_tx, || Silent);
        assert!(tasks.stop_signals.is_empty());
    }
}
