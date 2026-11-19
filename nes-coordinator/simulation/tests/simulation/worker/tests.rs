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

//! Tests of the fake worker's contract: the behaviour of a real worker that the controller relies on.
//! The binary runs without the default test harness, so the tests are registered as trials here.

use crate::worker::WorkerService;
use controller::remote::worker_rpc_service::QueryState as WorkerQueryState;
use controller::remote::worker_rpc_service::nes::{SerializableQueryId, SerializableQueryPlan};
use controller::remote::worker_rpc_service::worker_rpc_service_server::WorkerRpcService;
use controller::remote::worker_rpc_service::{
    QueryMetrics, QueryStatusRequest, StartQueryRequest, StopQueryRequest, WorkerStatusRequest,
};
use libtest_mimic::Trial;
use madsim::Config;
use madsim::runtime::Runtime;
use std::time::Duration;
use tonic::{Code, Request};

const SEEDS: u64 = 32;

pub fn build_trials() -> Vec<Trial> {
    vec![
        build_trial("worker::lists_by_state", worker_lists_by_state),
        build_trial("worker::stops_before_running", worker_stops_before_running),
        build_trial(
            "worker::rejects_invalid_requests",
            worker_rejects_invalid_requests,
        ),
    ]
}

fn build_trial(name: &str, test: fn()) -> Trial {
    Trial::test(format!("unit::{name}"), move || {
        test();
        Ok(())
    })
}

fn simulate<T>(seed: u64, body: impl Future<Output = T>) -> T {
    Runtime::with_seed_and_config(seed, Config::default()).block_on(body)
}

const FRAGMENT: i64 = 7;
/// Longer than the fake worker's start and running delays together, and longer than its stop delay,
/// so every pending state change has happened.
const SETTLE_TIME: Duration = Duration::from_secs(13);

fn build_query_id(id: i64) -> SerializableQueryId {
    SerializableQueryId::from_fragment_id(id)
}

async fn start_fragment(worker: &WorkerService, id: i64) -> Result<(), Code> {
    let query_plan = SerializableQueryPlan {
        query_id: Some(build_query_id(id)),
        ..Default::default()
    };
    worker
        .start_query(Request::new(StartQueryRequest {
            query_plan: Some(query_plan),
        }))
        .await
        .map(|_| ())
        .map_err(|status| status.code())
}

async fn stop_fragment(worker: &WorkerService, id: i64) -> Result<(), Code> {
    worker
        .stop_query(Request::new(StopQueryRequest {
            query_id: Some(build_query_id(id)),
        }))
        .await
        .map(|_| ())
        .map_err(|status| status.code())
}

async fn get_fragment_status(
    worker: &WorkerService,
    id: i64,
) -> Result<(WorkerQueryState, QueryMetrics), Code> {
    let reply = worker
        .request_query_status(Request::new(QueryStatusRequest {
            query_id: Some(build_query_id(id)),
        }))
        .await
        .map_err(|status| status.code())?
        .into_inner();
    Ok((
        reply.state(),
        reply.metrics.expect("a status reply contains metrics"),
    ))
}

async fn list_active_fragments(worker: &WorkerService, after: u64) -> Vec<(i64, Option<u64>)> {
    worker
        .request_status(Request::new(WorkerStatusRequest {
            after_unix_timestamp_in_milli_seconds: after,
        }))
        .await
        .unwrap()
        .into_inner()
        .active_queries
        .into_iter()
        .map(|q| {
            let id = q
                .query_id
                .as_ref()
                .and_then(SerializableQueryId::fragment_id);
            (id.unwrap(), q.started_unix_timestamp_in_milli_seconds)
        })
        .collect()
}

/// Polls the fragment every 250 ms for 16 s and stops it at 13 s, the settle time,
/// so every seed observes Running and then Stopped.
fn worker_lists_by_state() {
    for seed in 0..SEEDS {
        simulate(seed, async {
            let worker = WorkerService::new();
            start_fragment(&worker, FRAGMENT).await.unwrap();
            let mut seen = Vec::new();
            for tick in 0..64 {
                if tick == 52 {
                    stop_fragment(&worker, FRAGMENT).await.unwrap();
                }
                let (state, metrics) = get_fragment_status(&worker, FRAGMENT).await.unwrap();
                let listed = list_active_fragments(&worker, 0).await;
                match state {
                    WorkerQueryState::Started => {
                        assert_eq!(listed, [(FRAGMENT, None)], "seed {seed}");
                        let started = metrics.start_unix_time_in_ms.unwrap();
                        assert_eq!(
                            list_active_fragments(&worker, started).await,
                            [(FRAGMENT, None)],
                            "seed {seed}"
                        );
                        assert!(
                            list_active_fragments(&worker, started + 1).await.is_empty(),
                            "seed {seed}"
                        );
                    }
                    WorkerQueryState::Running => {
                        let running = metrics.running_unix_time_in_ms.unwrap();
                        assert_eq!(listed, [(FRAGMENT, Some(running))], "seed {seed}");
                        assert_eq!(
                            list_active_fragments(&worker, running).await,
                            [(FRAGMENT, Some(running))],
                            "seed {seed}"
                        );
                        assert!(
                            list_active_fragments(&worker, running + 1).await.is_empty(),
                            "seed {seed}"
                        );
                    }
                    other => assert!(listed.is_empty(), "seed {seed}: {other:?} is listed"),
                }
                if seen.last() != Some(&state) {
                    seen.push(state);
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            assert_eq!(
                seen.first(),
                Some(&WorkerQueryState::Registered),
                "seed {seed}"
            );
            assert!(
                seen.ends_with(&[WorkerQueryState::Running, WorkerQueryState::Stopped]),
                "seed {seed}: {seen:?}"
            );
            assert!(
                seen.is_sorted_by_key(|&state| state as i32),
                "seed {seed}: {seen:?}"
            );
        });
    }
}

fn worker_stops_before_running() {
    for seed in 0..SEEDS {
        simulate(seed, async {
            let worker = WorkerService::new();
            start_fragment(&worker, FRAGMENT).await.unwrap();
            stop_fragment(&worker, FRAGMENT).await.unwrap();

            tokio::time::sleep(SETTLE_TIME).await;

            let (state, metrics) = get_fragment_status(&worker, FRAGMENT).await.unwrap();
            assert_eq!(state, WorkerQueryState::Stopped, "seed {seed}");
            assert!(metrics.stop_unix_time_in_ms.is_some(), "seed {seed}");
            assert!(
                list_active_fragments(&worker, 0).await.is_empty(),
                "seed {seed}"
            );
        });
    }
}

fn worker_rejects_invalid_requests() {
    simulate(0, async {
        let worker = WorkerService::new();
        assert_eq!(stop_fragment(&worker, FRAGMENT).await, Err(Code::NotFound));
        assert_eq!(
            get_fragment_status(&worker, FRAGMENT).await.err(),
            Some(Code::NotFound)
        );

        start_fragment(&worker, FRAGMENT).await.unwrap();
        assert_eq!(
            start_fragment(&worker, FRAGMENT).await,
            Err(Code::AlreadyExists)
        );

        stop_fragment(&worker, FRAGMENT).await.unwrap();
        tokio::time::sleep(SETTLE_TIME).await;
        assert_eq!(stop_fragment(&worker, FRAGMENT).await, Ok(()));
        assert_eq!(
            start_fragment(&worker, FRAGMENT).await,
            Err(Code::AlreadyExists)
        );

        tokio::time::sleep(SETTLE_TIME).await;
        let (state, _) = get_fragment_status(&worker, FRAGMENT).await.unwrap();
        assert_eq!(state, WorkerQueryState::Stopped);
        assert!(list_active_fragments(&worker, 0).await.is_empty());
    });
}
