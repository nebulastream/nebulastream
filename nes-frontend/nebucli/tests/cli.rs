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

use model::identifier::{SinkId, SourceId};
use model::query::CreateQuery;
use model::query::query_fragment::CreateQueryFragment;
use model::statement::{Statement, StatementResult};
use nebucli::Client;
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::JoinHandle;
use tokio::sync::oneshot;

const WORKER: &str = "127.0.0.1:1";

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
                let (router, coordinator) =
                    server::start(db, None, None, server::Config::default(), vec![])
                        .await
                        .unwrap();
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

    fn cli(&self, args: &[&str]) -> (anyhow::Result<()>, String) {
        let mut out = Vec::new();
        let argv = ["nebucli", "--coordinator", &self.url]
            .into_iter()
            .chain(args.iter().copied())
            .map(String::from);
        let result = nebucli::run_with(argv, &mut out, false);
        (result, String::from_utf8(out).unwrap())
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

const SETUP: &str = r#"
workers:
  - host: 127.0.0.1:1
    max_operators: 100
logical_sources:
  - name: src
    schema:
      - name: value
        type: UINT64
physical_sources:
  - logical_source: src
    host: 127.0.0.1:1
    source_type: Generator
sinks:
  - name: snk
    host: 127.0.0.1:1
    sink_type: Print
    schema:
      - name: value
        type: UINT64
query: SELECT value FROM src INTO snk
"#;

fn setup_file() -> PathBuf {
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    let dir = std::env::temp_dir().join(format!(
        "nebucli-test-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("setup.yaml");
    std::fs::write(&path, SETUP).unwrap();
    path
}

fn root_cause(error: &anyhow::Error) -> String {
    error.root_cause().to_string()
}

fn register_query(coordinator: &Coordinator) -> i64 {
    let (result, _) = coordinator.cli(&["-s", setup_file().to_str().unwrap(), "dump"]);
    assert!(
        root_cause(&result.unwrap_err()).contains("no SQL planner configured"),
        "the registration should reach the planner"
    );
    let client = coordinator.client();
    let sources: Vec<model::source::physical::Model> =
        client.list("v1/sources/physical", &[]).unwrap();
    let sinks: Vec<model::sink::Model> = client.list("v1/sinks", &[]).unwrap();
    let statement = Statement::CreateQuery(CreateQuery {
        name: None,
        sql: "SELECT VALUE FROM SRC INTO SNK".to_owned(),
        fragments: vec![CreateQueryFragment {
            host_addr: WORKER.parse().unwrap(),
            plan: Vec::new(),
            num_operators: 1,
            has_source: true,
        }],
        source_ids: BTreeSet::from([SourceId::new(*sources[0].id)]),
        sink_ids: BTreeSet::from([SinkId::new(*sinks[0].id)]),
    });
    match client.statement(&statement).unwrap() {
        StatementResult::CreatedQuery(created) => *created.query.id,
        other => panic!("expected a created query, got {other:?}"),
    }
}

#[test]
fn start_registers_the_setup_and_reports_the_missing_planner() {
    let coordinator = Coordinator::spawn();
    let (result, _) = coordinator.cli(&["-s", setup_file().to_str().unwrap(), "start"]);
    let message = root_cause(&result.unwrap_err());
    assert!(message.contains("1 query/queries failed"), "{message}");
    assert!(message.contains("no SQL planner configured"), "{message}");

    let client = coordinator.client();
    assert_eq!(client.workers().unwrap().len(), 1);
    let sources: Vec<model::source::logical::Model> =
        client.list("v1/sources/logical", &[]).unwrap();
    assert_eq!(sources.len(), 1);
    assert_eq!(sources[0].name, "SRC");
    let sinks: Vec<model::sink::Model> = client.list("v1/sinks", &[]).unwrap();
    assert_eq!(sinks.len(), 1);

    let (result, _) = coordinator.cli(&["-s", setup_file().to_str().unwrap(), "start"]);
    assert!(root_cause(&result.unwrap_err()).contains("no SQL planner configured"));
    assert_eq!(client.workers().unwrap().len(), 1);
}

#[test]
fn status_prints_the_flat_shape_or_tables() {
    let coordinator = Coordinator::spawn();
    let id = register_query(&coordinator);

    let (result, stdout) = coordinator.cli(&["status"]);
    result.unwrap();
    let rows: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(rows[0]["id"], id);
    assert_eq!(rows[0]["state"], "Pending");
    assert_eq!(rows[0]["fragments"][0]["host_addr"], WORKER);
    assert!(rows[0]["fragments"][0].get("worker_state").is_some());

    let (result, stdout) = coordinator.cli(&["status", &(id + 1).to_string()]);
    result.unwrap();
    assert_eq!(stdout.trim(), "[]");

    let (result, stdout) = coordinator.cli(&["-o", "table", "status"]);
    result.unwrap();
    assert!(stdout.starts_with("+-"), "{stdout}");
    assert!(stdout.contains(WORKER), "{stdout}");
}

#[test]
fn stop_waits_for_termination_until_its_deadline() {
    let coordinator = Coordinator::spawn();
    let id = register_query(&coordinator);

    let (result, _) = coordinator.cli(&["stop", &id.to_string(), "--wait", "1"]);
    let message = root_cause(&result.unwrap_err());
    assert!(message.contains("timed out"), "{message}");

    let (result, _) = coordinator.cli(&["stop", "999"]);
    let message = root_cause(&result.unwrap_err());
    assert!(message.contains("QueryNotFound"), "{message}");
}

#[test]
fn sql_reaches_the_statements_endpoint() {
    let coordinator = Coordinator::spawn();
    let (result, _) = coordinator.cli(&["sql", "SELECT 1"]);
    let message = root_cause(&result.unwrap_err());
    assert!(message.contains("no SQL planner configured"), "{message}");
}

#[test]
fn an_unreachable_coordinator_is_reported_as_such() {
    let mut out = Vec::new();
    let argv = ["nebucli", "--coordinator", "http://127.0.0.1:1", "status"]
        .into_iter()
        .map(String::from);
    let error = nebucli::run_with(argv, &mut out, false).unwrap_err();
    assert!(
        root_cause(&error).contains("cannot reach the coordinator"),
        "{error:#}"
    );
}
