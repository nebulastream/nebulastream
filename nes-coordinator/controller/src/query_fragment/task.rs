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
use crate::config::RETRY_INTERVAL;
use crate::remote::worker_rpc_service::nes::{SerializableQueryId, SerializableQueryPlan};
use crate::util::buggify::buggify;
use chrono::{DateTime, Utc};
use model::database::Database;
use model::error::{CodedError, ErrorCode, WriteFailure, classify_write};
use model::identifier::QueryFragmentId;
use model::query::query_fragment;
use model::query::query_fragment::{
    DesiredQueryFragmentState, QueryFragmentError, QueryFragmentTransition,
};
use sea_orm::{ActiveModelTrait, DbErr, EntityTrait, RuntimeErr};
use tokio::sync::watch;
use tracing::{debug, error, warn};

enum Persisted {
    Written,
    Finished,
}

/// Moves a single query_fragment through its lifecycle from pending through to a terminal state.
/// Uses a transport to call the worker this fragment is hosted on,
/// instructing it to start, stop, and report the status of the fragment.
/// Owns the catalog row of the query fragment, receives its desired state over a channel,
/// and updates the catalog row based on the interactions with the worker.
pub(crate) struct QueryFragmentTask<T> {
    fragment: query_fragment::Model,
    update: query_fragment::ActiveModel,
    db: Database,
    transport: T,
    /// Communicate state changes to the `request_handler`.
    state_tx: watch::Sender<()>,
    desired: watch::Receiver<DesiredQueryFragmentState>,
    /// Encodes the state transitions based on the observed interactions with the worker.
    lifecycle: QueryFragmentLifecycle,
}

impl<T: Transport> QueryFragmentTask<T> {
    pub fn new(
        fragment: query_fragment::Model,
        db: Database,
        transport: T,
        state_tx: watch::Sender<()>,
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
                    if matches!(self.persist(step, now).await?, Persisted::Finished) {
                        return Ok(());
                    }
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

    async fn persist(
        &mut self,
        step: LifecycleStep,
        now: DateTime<Utc>,
    ) -> anyhow::Result<Persisted> {
        if step.transition.is_none() && !step.observed {
            return Ok(Persisted::Written);
        }
        let prev = self.fragment.current_state;
        if let Some(transition) = step.transition {
            self.update.apply_transition(transition);
        }
        if step.observed {
            self.update.mark_observed(now);
        }
        while let Err(err) = self.write().await {
            match classify_write(&err) {
                WriteFailure::RowGone => return Ok(Persisted::Finished),
                WriteFailure::Rejected => return self.fail_rejected(err).await,
                WriteFailure::Transient => {
                    warn!(error = %err, "catalog write failed, retrying");
                    tokio::time::sleep(RETRY_INTERVAL).await;
                }
            }
        }
        if self.fragment.current_state != prev {
            debug!(from = %prev, to = %self.fragment.current_state, "transition");
        }
        if buggify() {
            return Ok(Persisted::Written);
        }
        let _ = self.state_tx.send(());
        Ok(Persisted::Written)
    }

    async fn fail_rejected(&self, err: DbErr) -> anyhow::Result<Persisted> {
        error!(error = %err, "catalog rejected the write");
        let Some(row) = query_fragment::Entity::find_by_id(self.fragment.id)
            .one(&self.db)
            .await?
        else {
            return Ok(Persisted::Finished);
        };
        if row.current_state.is_terminal() {
            return Ok(Persisted::Finished);
        }
        if let Err(CallError::Failed(stop_err) | CallError::Transient(stop_err)) =
            self.transport.stop(self.fragment.id).await
        {
            warn!(error = %stop_err, "could not stop the fragment after the rejected write");
        }
        let mut update: query_fragment::ActiveModel = row.into();
        update.apply_transition(QueryFragmentTransition::failed_now(
            QueryFragmentError::Internal(CodedError::new(ErrorCode::CatalogWriteRejected, err)),
        ));
        update.update(&self.db).await?;
        let _ = self.state_tx.send(());
        Ok(Persisted::Finished)
    }

    async fn write(&mut self) -> Result<(), DbErr> {
        if buggify() {
            return Err(DbErr::Conn(RuntimeErr::Internal(
                "buggify: write failed".to_string(),
            )));
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
    use crate::worker_task::{ActiveFragments, Backend, stop_orphans, stop_removed_fragments};
    use model::Execute;
    use model::database::Database;
    use model::identifier::{QueryId, SinkId, SourceId};
    use model::query::query_fragment::{
        ActionableFragment, CreateQueryFragment, QueryFragmentState,
    };
    use model::query::{CreateQuery, DropQuery, GetQuery};
    use model::sink::CreateSink;
    use model::source::logical::CreateLogicalSource;
    use model::source::physical::CreatePhysicalSource;
    use model::worker::endpoint::NetworkAddr;
    use model::worker::{CreateWorker, DropWorker};
    use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
    use std::collections::{BTreeSet, VecDeque};
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::time::{Duration, Instant};

    const HOST: &str = "127.0.0.1:1";

    struct Scripted {
        answers: Mutex<VecDeque<WorkerReply>>,
        calls: Arc<Mutex<Vec<WorkerCall>>>,
        poll: Duration,
        db: Database,
        query_id: QueryId,
        desired_tx: Arc<watch::Sender<DesiredQueryFragmentState>>,
        stop_before: Option<usize>,
        stop_after: Option<(usize, Duration)>,
    }

    async fn request_stop(
        db: Database,
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
        let conn = db;
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
        query_fragment::Entity::find()
            .filter(query_fragment::Column::HostAddr.eq(host))
            .one(conn)
            .await
            .unwrap()
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
            db: db.clone(),
            query_id: fragment.query_id,
            desired_tx: Arc::new(desired_tx),
            stop_before: script.stop_before,
            stop_after: script.stop_after,
        };
        let (state_tx, _state_rx) = watch::channel(());
        let id = fragment.id;
        QueryFragmentTask::new(fragment, db.clone(), transport, state_tx, desired_rx)
            .run()
            .await
            .unwrap();
        let row = query_fragment::Entity::find_by_id(id)
            .one(db)
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
    async fn a_rejected_transition_stops_the_worker_and_fails_the_fragment() {
        let db = Database::for_test().await;
        let fragment = fragment_row(&db).await;
        let script = Script::new(vec![
            WorkerReply::Start(Ok(())),
            status(QueryFragmentState::Running, Some(1_000), None),
            status(QueryFragmentState::Started, None, None),
            WorkerReply::Stop(Ok(())),
        ]);
        let (calls, row) = drive(&db, fragment, script).await;
        assert_eq!(
            calls,
            vec![
                WorkerCall::Start,
                WorkerCall::Status,
                WorkerCall::Status,
                WorkerCall::Stop
            ]
        );
        assert_eq!(row.current_state, QueryFragmentState::Failed);
        let Some(QueryFragmentError::Internal(coded)) = row.error else {
            panic!("expected a coded error");
        };
        assert_eq!(coded.code, ErrorCode::CatalogWriteRejected);
    }

    struct RecordingStops(Arc<Mutex<Vec<QueryFragmentId>>>);

    impl Transport for RecordingStops {
        fn poll_interval(&self) -> Duration {
            Duration::ZERO
        }

        async fn start(&self, _: SerializableQueryPlan) -> Result<(), CallError> {
            unreachable!("removing a worker only stops fragments")
        }

        async fn stop(&self, id: QueryFragmentId) -> Result<(), CallError> {
            self.0.lock().unwrap().push(id);
            Ok(())
        }

        async fn status(&self, _: QueryFragmentId) -> Result<RawStatus, CallError> {
            unreachable!("removing a worker only stops fragments")
        }
    }

    struct RecordingBackend {
        stopped: Arc<Mutex<Vec<QueryFragmentId>>>,
        active: Vec<(QueryFragmentId, Option<u64>)>,
    }

    impl Backend for RecordingBackend {
        type Link = ();
        type Transport = RecordingStops;

        async fn connect(&self, _: &NetworkAddr) -> anyhow::Result<()> {
            Ok(())
        }

        fn transport(&self, (): &(), _: &NetworkAddr) -> RecordingStops {
            RecordingStops(self.stopped.clone())
        }

        fn heartbeat(&self) -> Option<Duration> {
            None
        }

        async fn healthy(&self, (): &()) -> bool {
            true
        }

        async fn active_fragments(&self, (): &(), _: u64) -> anyhow::Result<ActiveFragments> {
            Ok(ActiveFragments {
                fragments: self.active.clone(),
                until: 100,
            })
        }
    }

    #[tokio::test]
    async fn removing_a_worker_stops_its_fragments() {
        let db = Database::for_test().await;
        let fragment = fragment_row(&db).await;
        let host: NetworkAddr = HOST.parse().unwrap();
        DropWorker::new(host.clone()).execute(&db).await.unwrap();

        let stopped = Arc::new(Mutex::new(Vec::new()));
        let backend = RecordingBackend {
            stopped: stopped.clone(),
            active: Vec::new(),
        };
        stop_removed_fragments(&backend, &db, &host).await;

        assert_eq!(*stopped.lock().unwrap(), vec![fragment.id]);
    }

    #[tokio::test]
    async fn a_fragment_the_worker_runs_but_the_catalog_does_not_want_is_stopped() {
        let db = Database::for_test().await;
        let wanted = fragment_row(&db).await;
        let unknown = QueryFragmentId::new(*wanted.id + 1);
        let host: NetworkAddr = HOST.parse().unwrap();
        let stopped = Arc::new(Mutex::new(Vec::new()));
        let backend = RecordingBackend {
            stopped: stopped.clone(),
            active: vec![(wanted.id, Some(10)), (unknown, Some(20))],
        };

        let after = stop_orphans(&backend, &(), &db, &host, 0).await;
        assert_eq!(*stopped.lock().unwrap(), vec![unknown]);
        assert_eq!(
            after, 10,
            "the next listing must still include the oldest active fragment"
        );

        // Removing the worker fails the fragment, so the catalog no longer wants it either.
        DropWorker::new(host.clone()).execute(&db).await.unwrap();
        stopped.lock().unwrap().clear();
        stop_orphans(&backend, &(), &db, &host, 10).await;
        assert_eq!(*stopped.lock().unwrap(), vec![wanted.id, unknown]);
    }

    #[tokio::test]
    async fn a_deleted_row_ends_the_task() {
        let db = Database::for_test().await;
        let fragment = fragment_row(&db).await;
        query_fragment::Entity::delete_by_id(fragment.id)
            .exec(&db)
            .await
            .unwrap();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let (desired_tx, desired_rx) = watch::channel(fragment.desired_state);
        let transport = Scripted {
            answers: Mutex::new(vec![WorkerReply::Start(Ok(()))].into()),
            calls: calls.clone(),
            poll: Duration::ZERO,
            db: db.clone(),
            query_id: fragment.query_id,
            desired_tx: Arc::new(desired_tx),
            stop_before: None,
            stop_after: None,
        };
        let (state_tx, _state_rx) = watch::channel(());
        QueryFragmentTask::new(fragment, db.clone(), transport, state_tx, desired_rx)
            .run()
            .await
            .unwrap();
        assert_eq!(*calls.lock().unwrap(), vec![WorkerCall::Start]);
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

    /// A start whose compilation outlasts the RPC deadline, and a drop of the query while the worker still compiles.
    /// The coordinator sees the start time out (retryable), and after the retry interval it sends a stop instead.
    /// The worker's engine does not know the query yet, so it drops the stop with a warning
    /// (`QueryCatalog::stopQuery`: "Query was not submitted to the engine") and the worker replies OK.
    fn start_timed_out_while_compiling() -> WorkerReply {
        WorkerReply::Start(Err(CallError::Transient(QueryFragmentError::Transport {
            msg: "deadline exceeded".to_string(),
        })))
    }

    /// The worker on main: the query log has no entry until compilation ends, so a status read says `QueryNotFound`.
    /// The coordinator records the fragment as stopped, and the orphan check stops it once the worker runs it.
    #[tokio::test]
    async fn a_stop_during_compilation_is_repaired_when_the_worker_reports_not_found() {
        let db = Database::for_test().await;
        let fragment = fragment_row(&db).await;
        let mut script = Script::new(vec![
            start_timed_out_while_compiling(),
            WorkerReply::Stop(Ok(())),
            WorkerReply::Status(Err(CallError::NotFound)),
        ]);
        script.stop_before = Some(1);
        let (calls, row) = drive(&db, fragment.clone(), script).await;
        assert_eq!(
            calls,
            vec![WorkerCall::Start, WorkerCall::Stop, WorkerCall::Status]
        );
        assert_eq!(row.current_state, QueryFragmentState::Stopped);

        // Compilation ends and the worker runs the fragment; it now lists it as active.
        let stopped = Arc::new(Mutex::new(Vec::new()));
        let backend = RecordingBackend {
            stopped: stopped.clone(),
            active: vec![(fragment.id, Some(1_000))],
        };
        stop_orphans(&backend, &(), &db, &HOST.parse().unwrap(), 0).await;
        assert_eq!(
            *stopped.lock().unwrap(),
            vec![fragment.id],
            "the orphan check stops the fragment that started after its stop"
        );
    }

    /// The worker with #2270: the query log gets `Registered` before compilation, so a status read during
    /// compilation says `Registered` (mapped to `Started`) instead of `QueryNotFound`.
    /// This test asserts the resulting behavior: the fragment runs on, and nothing stops it again.
    #[tokio::test]
    async fn a_stop_during_compilation_is_lost_when_the_worker_reports_registered() {
        let db = Database::for_test().await;
        let fragment = fragment_row(&db).await;
        let mut answers = vec![
            start_timed_out_while_compiling(),
            WorkerReply::Stop(Ok(())),
            // Still compiling: proto QueryState Registered = 0.
            WorkerReply::Status(Ok(RawStatus {
                state: 0,
                start_ms: None,
                stop_ms: None,
                error: None,
            })),
        ];
        // Compilation ends and the engine runs the fragment: the acknowledged stop never reached it.
        answers.extend((0..200).map(|_| status(QueryFragmentState::Running, Some(1_000), None)));
        let calls = Arc::new(Mutex::new(Vec::new()));
        let (desired_tx, desired_rx) = watch::channel(fragment.desired_state);
        let transport = Scripted {
            answers: Mutex::new(answers.into()),
            calls: calls.clone(),
            poll: Duration::from_millis(10),
            db: db.clone(),
            query_id: fragment.query_id,
            desired_tx: Arc::new(desired_tx),
            stop_before: Some(1),
            stop_after: None,
        };
        let (state_tx, _state_rx) = watch::channel(());
        let task = QueryFragmentTask::new(
            fragment.clone(),
            db.clone(),
            transport,
            state_tx,
            desired_rx,
        )
        .run();
        let finished =
            tokio::time::timeout(RETRY_INTERVAL + Duration::from_millis(500), task).await;

        assert!(
            finished.is_err(),
            "the task still waits for a fragment that it asked to stop"
        );
        let calls = calls.lock().unwrap().clone();
        assert_eq!(
            calls[..3],
            [WorkerCall::Start, WorkerCall::Stop, WorkerCall::Status]
        );
        assert!(
            calls.len() > 10,
            "the fragment keeps being polled: {} calls",
            calls.len()
        );
        assert!(
            calls[3..].iter().all(|call| *call == WorkerCall::Status),
            "no second stop is ever sent: {calls:?}"
        );
        let row = query_fragment::Entity::find_by_id(fragment.id)
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.desired_state, DesiredQueryFragmentState::Stopped);
        assert_eq!(
            row.current_state,
            QueryFragmentState::Running,
            "the dropped fragment is recorded as running"
        );

        // The orphan check skips the fragment, because a non-terminal row counts as wanted.
        let stopped = Arc::new(Mutex::new(Vec::new()));
        let backend = RecordingBackend {
            stopped: stopped.clone(),
            active: vec![(fragment.id, Some(1_000))],
        };
        stop_orphans(&backend, &(), &db, &HOST.parse().unwrap(), 0).await;
        assert!(
            stopped.lock().unwrap().is_empty(),
            "the orphan check does not stop the fragment either"
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
        let mut tasks = QueryFragmentTasks::new();
        let spawned = std::cell::Cell::new(0);
        let silent = || {
            spawned.set(spawned.get() + 1);
            Silent
        };

        tasks.reconcile(
            vec![ActionableFragment::from(&fragment)],
            &db,
            &state_tx,
            silent,
        );
        assert!(tasks.tasks().is_tracked(&fragment.id));
        let mut desired = tasks.stop_signals[&fragment.id].subscribe();
        assert_eq!(*desired.borrow(), DesiredQueryFragmentState::Completed);

        fragment.desired_state = DesiredQueryFragmentState::Stopped;
        tasks.reconcile(
            vec![ActionableFragment::from(&fragment)],
            &db,
            &state_tx,
            silent,
        );
        assert_eq!(spawned.get(), 1);
        assert!(desired.has_changed().unwrap());
        assert_eq!(
            *desired.borrow_and_update(),
            DesiredQueryFragmentState::Stopped
        );

        tasks.reconcile(vec![], &db, &state_tx, || Silent);
        assert!(tasks.stop_signals.is_empty());
    }
}
