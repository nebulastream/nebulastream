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

use client::{Client, ClientError, WaitKind};
use model::identifier::{SinkId, SourceId};
use model::query::query_fragment::CreateQueryFragment;
use model::query::{CreateQuery, DropQuery, GetQuery};
use model::statement::{Statement, StatementResult};
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

const WORKER: &str = "127.0.0.1:1";
const CAP: Duration = Duration::from_millis(100);
const DEADLINE: Duration = Duration::from_millis(350);

struct Coordinator {
    url: String,
    stop: Option<oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl Coordinator {
    fn spawn() -> Self {
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (stop_tx, stop_rx) = oneshot::channel::<()>();
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                let db = model::database::Database::for_test().await;
                let config = server::Config {
                    wait_cap: CAP,
                    ..server::Config::default()
                };
                let (router, coordinator) =
                    server::start(db, None, None, config, vec![]).await.unwrap();
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                ready_tx
                    .send(format!("http://{}", listener.local_addr().unwrap()))
                    .unwrap();
                server::serve(listener, router, async {
                    let _ = stop_rx.await;
                })
                .await
                .unwrap();
                coordinator.abort();
            });
        });
        Self {
            url: ready_rx.recv().unwrap(),
            stop: Some(stop_tx),
            thread: Some(thread),
        }
    }

    fn client(&self) -> Client {
        Client::new(self.url.parse().unwrap()).unwrap()
    }
}

impl Drop for Coordinator {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn query_statement(client: &Client) -> Statement {
    let _: Value = client
        .create(
            "v1/workers",
            &json!({
                "host_addr": WORKER,
                "data_addr": "127.0.0.1:2",
                "max_operators": 100,
                "peers": [],
                "config": {},
            }),
        )
        .unwrap();
    let _: Value = client
        .create(
            "v1/sources/logical",
            &json!({ "name": "src", "schema": [] }),
        )
        .unwrap();
    let source: Value = client
        .create(
            "v1/sources/physical",
            &json!({ "logical_source": "src", "host_addr": WORKER, "source_type": "Generator" }),
        )
        .unwrap();
    let sink: Value = client
        .create(
            "v1/sinks",
            &json!({ "name": "snk", "host_addr": WORKER, "sink_type": "Print", "schema": [] }),
        )
        .unwrap();
    Statement::CreateQuery(CreateQuery {
        name: None,
        sql: "SELECT * FROM src INTO snk".to_owned(),
        fragments: vec![CreateQueryFragment {
            host_addr: WORKER.parse().unwrap(),
            plan: Vec::new(),
            num_operators: 1,
            has_source: true,
        }],
        source_ids: BTreeSet::from([SourceId::new(source["id"].as_i64().unwrap())]),
        sink_ids: BTreeSet::from([SinkId::new(sink["id"].as_i64().unwrap())]),
    })
}

fn deadline_message(error: &anyhow::Error) -> String {
    match error.downcast_ref::<ClientError>() {
        Some(deadline @ ClientError::Deadline { .. }) => deadline.to_string(),
        other => panic!("expected the client's deadline, got {other:?}"),
    }
}

#[test]
fn a_wait_that_needs_nothing_is_answered_at_once() {
    let coordinator = Coordinator::spawn();
    let client = coordinator.client();
    let started = Instant::now();
    let result = client
        .statement_wait(
            &Statement::GetQuery(GetQuery::all()),
            WaitKind::Terminated,
            Some(started + Duration::from_secs(5)),
        )
        .unwrap();
    assert!(matches!(result, StatementResult::Queries(rows) if rows.is_empty()));
    assert!(started.elapsed() < Duration::from_secs(1));
}

#[test]
fn a_create_whose_wait_outlasts_the_cap_goes_on_until_the_deadline() {
    let coordinator = Coordinator::spawn();
    let client = coordinator.client();
    let statement = query_statement(&client);
    let started = Instant::now();
    let error = client
        .statement_wait(&statement, WaitKind::Completed, Some(started + DEADLINE))
        .unwrap_err();
    assert_eq!(
        deadline_message(&error),
        "timed out waiting for query 1 to be completed"
    );
    assert!(started.elapsed() >= DEADLINE);
    assert_eq!(client.queries(&[], false).unwrap().len(), 1);
}

#[test]
fn a_read_and_a_drop_go_on_by_the_queries_they_listed() {
    let coordinator = Coordinator::spawn();
    let client = coordinator.client();
    client.statement(&query_statement(&client)).unwrap();

    let read = Statement::GetQuery(GetQuery::all().with_fragments());
    let error = client
        .statement_wait(&read, WaitKind::Terminated, Some(Instant::now() + DEADLINE))
        .unwrap_err();
    assert_eq!(
        deadline_message(&error),
        "timed out waiting for queries 1 to be terminated"
    );

    let drop = Statement::DropQuery(DropQuery::all());
    let error = client
        .statement_wait(&drop, WaitKind::Terminated, Some(Instant::now() + DEADLINE))
        .unwrap_err();
    assert_eq!(
        deadline_message(&error),
        "timed out waiting for queries 1 to be terminated"
    );
}

#[test]
fn a_cancel_ends_a_wait_within_the_slice() {
    let coordinator = Coordinator::spawn();
    let client = coordinator
        .client()
        .with_poll_slice(Duration::from_millis(50));
    client.statement(&query_statement(&client)).unwrap();
    let read = Statement::GetQuery(GetQuery::all());

    let started = Instant::now();
    let error = std::thread::scope(|scope| {
        scope.spawn(|| {
            std::thread::sleep(Duration::from_millis(150));
            client.cancel();
        });
        client
            .statement_wait(&read, WaitKind::Terminated, None)
            .unwrap_err()
    });
    assert!(matches!(
        error.downcast_ref::<ClientError>(),
        Some(ClientError::Cancelled)
    ));
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "{:?}",
        started.elapsed()
    );

    let error = client
        .statement_wait(&read, WaitKind::Terminated, None)
        .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<ClientError>(),
        Some(ClientError::Cancelled)
    ));
    assert!(client.statement(&read).is_ok());
}
