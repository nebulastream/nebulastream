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

use crate::config::{TrialConfig, TrialFile};
use crate::harness::Harness;
use crate::invariant::is_stopped_or_failed_by_drop;
use crate::model_state::{ModelState, Operation, Step};
use crate::worker::WorkerService;
use crate::workload::ops::pick_offsets;
use crate::workload::{Workload, pick_victims};
use controller::remote::worker_rpc_service::QueryState as WorkerQueryState;
use controller::remote::worker_rpc_service::nes::{SerializableQueryId, SerializableQueryPlan};
use controller::remote::worker_rpc_service::worker_rpc_service_server::WorkerRpcService;
use controller::remote::worker_rpc_service::{
    QueryMetrics, QueryStatusRequest, StartQueryRequest, StopQueryRequest, WorkerStatusRequest,
    WorkerStatusResponse,
};
use libtest_mimic::Trial;
use madsim::Config;
use madsim::runtime::{Handle, Runtime};
use madsim::task::NodeId;
use model::ConnectorKind;
use model::identifier::{QueryFragmentId, QueryId, SinkId, SourceId};
use model::query::query_fragment::{self, DesiredQueryFragmentState, QueryFragmentState};
use model::query::query_state::QueryState;
use model::query::{self, QueryWithFragments};
use model::sink;
use model::source::{logical, physical};
use model::statement::{ParsedStatement, StatementResult};
use model::worker::endpoint::{DEFAULT_DATA_PORT, DEFAULT_HOST_PORT, NetworkAddr};
use model::worker::{self, DesiredWorkerState, WorkerState};
use std::collections::{BTreeSet, HashSet};
use std::hash::Hash;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::time::Duration;
use toml::de::Error as TomlError;
use tonic::{Code, Request};

const SEEDS: u64 = 32;

pub fn build_trials() -> Vec<Trial> {
    vec![
        build_trial("config::rejects_unknown_keys", config_rejects_unknown_keys),
        build_trial("config::applies_defaults", config_applies_defaults),
        build_trial("config::rejects_bad_ranges", config_rejects_bad_ranges),
        build_trial(
            "config::rejects_late_activity",
            config_rejects_late_activity,
        ),
        build_trial(
            "invariant::stopped_or_failed_by_drop",
            invariant_stopped_or_failed_by_drop,
        ),
        build_trial(
            "model_state::orders_prerequisites",
            model_orders_prerequisites,
        ),
        build_trial(
            "model_state::places_source_and_sink",
            model_places_source_and_sink,
        ),
        build_trial(
            "model_state::spares_source_and_sink",
            model_spares_source_and_sink,
        ),
        build_trial("model_state::reserves_hosts", model_reserves_hosts),
        build_trial(
            "model_state::drops_only_active_queries",
            model_drops_only_active_queries,
        ),
        build_trial("model_state::drops_by_id", model_drops_by_id),
        build_trial("model_state::reserves_queries", model_reserves_queries),
        build_trial(
            "model_state::fails_queries_of_dropped_worker",
            model_fails_queries_of_dropped_worker,
        ),
        build_trial(
            "model_state::skips_ended_queries",
            model_skips_ended_queries,
        ),
        build_trial(
            "model_state::fails_late_placement",
            model_fails_late_placement,
        ),
        build_trial("model_state::drops_failed_query", model_drops_failed_query),
        build_trial(
            "model_state::rejects_double_drop",
            model_rejects_double_drop,
        ),
        build_trial(
            "model_state::allocates_distinct_ips",
            model_allocates_distinct_ips,
        ),
        build_trial(
            "model_state::rejects_inconsistent_responses",
            model_rejects_inconsistent_responses,
        ),
        build_trial("worker::lists_by_state", worker_lists_by_state),
        build_trial("worker::stops_before_running", worker_stops_before_running),
        build_trial(
            "worker::rejects_invalid_requests",
            worker_rejects_invalid_requests,
        ),
        build_trial("harness::tracks_workers", harness_tracks_workers),
        build_trial(
            "workload::keeps_offsets_in_window",
            workload_keeps_offsets_in_window,
        ),
        build_trial("workload::picks_victims", workload_picks_victims),
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

fn panics(f: impl FnOnce()) -> bool {
    catch_unwind(AssertUnwindSafe(f)).is_err()
}

fn parse(text: &str) -> Result<TrialFile, TomlError> {
    toml::from_str(text)
}

fn build_toml(extra: &str, workload: &str) -> String {
    format!("[[test]]\ntitle = \"t\"\ntimeout = 60\n{extra}\n[[test.workload]]\n{workload}\n")
}

fn build_config(workload: &str) -> TrialConfig {
    let mut file = parse(&build_toml("", workload)).unwrap();
    file.trials.pop().unwrap()
}

fn rejects(workload: &str) -> bool {
    build_config(workload).validate().is_err()
}

const WORKER_OPS: &str =
    "name = \"Worker\"\nnum_ops = 1\nend = 10\ncreate_weight = 1\ndrop_weight = 0";

fn config_rejects_unknown_keys() {
    let misplaced_workload = format!(
        "{}[[workload]]\nname = \"Stall\"\nbegin = 1\nend = 2\npause_rate = 0.5\n",
        build_toml("", WORKER_OPS)
    );
    assert!(
        parse(&misplaced_workload).is_err(),
        "a workload table outside of a test is dropped without an error"
    );
    assert!(parse(&build_toml("buggify_untill = 10", WORKER_OPS)).is_err());
    assert!(parse(&build_toml("", &format!("{WORKER_OPS}\nbgein = 2"))).is_err());
    let unknown_workload = build_toml("", "name = \"Stal\"\nbegin = 1\nend = 2\npause_rate = 0.5");
    assert!(parse(&unknown_workload).is_err());
}

fn config_applies_defaults() {
    let cfg = build_config(WORKER_OPS);
    assert_eq!(cfg.buggify_until, None);
    assert_eq!(cfg.min_failed_queries, 0);
    let [Workload::Worker(ops)] = cfg.workloads.as_slice() else {
        panic!("expect one Worker workload, got {:?}", cfg.workloads);
    };
    assert_eq!(ops.begin, 0);
}

fn config_rejects_bad_ranges() {
    for accepted in [
        "name = \"Query\"\nnum_ops = 1\nbegin = 5\nend = 5\ncreate_weight = 1\ndrop_weight = 0",
        "name = \"Attrition\"\nbegin = 1\nkill_rate = 1.0\nrestart_delay_lo = 1\nrestart_delay_hi = 3",
        "name = \"Attrition\"\nbegin = 1\nkill_rate = 0.0\nrestart_delay_lo = 1\nrestart_delay_hi = 3",
        "name = \"Degradation\"\nbegin = 4\nend = 5\nlatency_lo_ms = 1\nlatency_hi_ms = 2\nloss_rate = 0.0",
    ] {
        assert!(
            !rejects(accepted),
            "a valid workload is rejected: {accepted}"
        );
    }
    for rejected in [
        "name = \"Query\"\nnum_ops = 1\nbegin = 6\nend = 5\ncreate_weight = 1\ndrop_weight = 0",
        "name = \"Query\"\nnum_ops = 1\nend = 5\ncreate_weight = 0\ndrop_weight = 0",
        "name = \"Query\"\nnum_ops = 1\nend = 5\ncreate_weight = 4294967295\ndrop_weight = 1",
        "name = \"Attrition\"\nbegin = 1\nkill_rate = 0.5\nrestart_delay_lo = 3\nrestart_delay_hi = 3",
        "name = \"Attrition\"\nbegin = 1\nkill_rate = 1.5\nrestart_delay_lo = 1\nrestart_delay_hi = 3",
        "name = \"Attrition\"\nbegin = 1\nkill_rate = -0.1\nrestart_delay_lo = 1\nrestart_delay_hi = 3",
        "name = \"Clog\"\nbegin = 4\nend = 4\nclog_rate = 0.5",
        "name = \"Clog\"\nbegin = 4\nend = 5\nclog_rate = -0.1",
        "name = \"Stall\"\nbegin = 4\nend = 4\npause_rate = 0.5",
        "name = \"Stall\"\nbegin = 4\nend = 5\npause_rate = 1.5",
        "name = \"Degradation\"\nbegin = 4\nend = 4\nlatency_lo_ms = 1\nlatency_hi_ms = 2\nloss_rate = 0.1",
        "name = \"Degradation\"\nbegin = 4\nend = 5\nlatency_lo_ms = 2\nlatency_hi_ms = 2\nloss_rate = 0.1",
        "name = \"Degradation\"\nbegin = 4\nend = 5\nlatency_lo_ms = 3\nlatency_hi_ms = 2\nloss_rate = 0.1",
        "name = \"Degradation\"\nbegin = 4\nend = 5\nlatency_lo_ms = 1\nlatency_hi_ms = 2\nloss_rate = nan",
        "name = \"Degradation\"\nbegin = 4\nend = 5\nlatency_lo_ms = 1\nlatency_hi_ms = 2\nloss_rate = 1.5",
    ] {
        assert!(
            rejects(rejected),
            "an invalid workload is accepted: {rejected}"
        );
    }
}

fn config_rejects_late_activity() {
    let ops = |end: u64| {
        format!("name = \"Worker\"\nnum_ops = 1\nend = {end}\ncreate_weight = 1\ndrop_weight = 0")
    };
    assert!(!rejects(&ops(59)));
    assert!(rejects(&ops(60)));

    let attrition = |begin: u64, hi: u64| {
        format!(
            "name = \"Attrition\"\nbegin = {begin}\nkill_rate = 0.5\nrestart_delay_lo = 1\nrestart_delay_hi = {hi}"
        )
    };
    assert!(!rejects(&attrition(50, 9)));
    assert!(rejects(&attrition(50, 10)));

    for window in [
        "name = \"Clog\"\nbegin = 10\nend = 60\nclog_rate = 0.5",
        "name = \"Stall\"\nbegin = 10\nend = 60\npause_rate = 0.5",
        "name = \"Degradation\"\nbegin = 10\nend = 60\n\
         latency_lo_ms = 1\nlatency_hi_ms = 2\nloss_rate = 0.1",
    ] {
        assert!(rejects(window), "{window}");
    }

    let mut cfg = build_config(WORKER_OPS);
    cfg.buggify_until = Some(59);
    assert!(cfg.validate().is_ok());
    cfg.buggify_until = Some(60);
    assert!(cfg.validate().is_err());
}

fn build_addr(octet: u8) -> NetworkAddr {
    NetworkAddr::new(format!("10.0.0.{octet}"), DEFAULT_HOST_PORT).unwrap()
}

fn build_worker_row(host_addr: NetworkAddr) -> worker::Model {
    let data_addr = NetworkAddr::new(host_addr.host(), DEFAULT_DATA_PORT).unwrap();
    worker::Model {
        host_addr,
        data_addr,
        max_operators: None,
        config: Default::default(),
        current_state: WorkerState::Active,
        desired_state: DesiredWorkerState::Active,
    }
}

fn build_query_row(id: i64) -> query::Model {
    query::Model {
        id: QueryId::new(id),
        name: None,
        sql: String::new(),
        global_plan: Vec::new(),
        state: QueryState::Running,
        start_timestamp: None,
        stop_timestamp: None,
        error: None,
        dropped: false,
    }
}

fn build_fragment_row(
    id: i64,
    query_id: i64,
    host_addr: NetworkAddr,
    current_state: QueryFragmentState,
) -> query_fragment::Model {
    query_fragment::Model {
        id: QueryFragmentId::new(id),
        query_id: QueryId::new(query_id),
        host_addr,
        plan: vec![],
        num_operators: 1,
        has_source: false,
        current_state,
        desired_state: DesiredQueryFragmentState::Completed,
        start_timestamp: None,
        stop_timestamp: None,
        error: None,
        last_observed_at: None,
    }
}

fn build_source_row(host_addr: NetworkAddr) -> physical::Model {
    physical::Model {
        id: SourceId::new(1),
        logical_source: Some("source".to_string()),
        host_addr,
        source_type: "File".to_string(),
        source_config: Default::default(),
        parser_config: Default::default(),
        kind: ConnectorKind::Shared,
        query_id: None,
    }
}

fn build_sink_row(host_addr: NetworkAddr) -> sink::Model {
    sink::Model {
        id: SinkId::new(1),
        name: Some("sink".to_string()),
        host_addr,
        sink_type: "File".to_string(),
        schema: Default::default(),
        config: Default::default(),
        kind: ConnectorKind::Shared,
        query_id: None,
    }
}

fn build_created_query(id: i64, hosts: &[(i64, &NetworkAddr)]) -> StatementResult {
    StatementResult::CreatedQuery(QueryWithFragments {
        query: build_query_row(id),
        fragments: hosts
            .iter()
            .map(|&(fragment, host)| {
                build_fragment_row(fragment, id, host.clone(), QueryFragmentState::Running)
            })
            .collect(),
    })
}

fn build_model(workers: &[&NetworkAddr]) -> ModelState {
    let mut model = ModelState::default();
    for &addr in workers {
        model.observe(StatementResult::CreatedWorker(build_worker_row(
            addr.clone(),
        )));
    }
    model
}

fn observe_dropped_worker(model: &mut ModelState, addr: &NetworkAddr) {
    model.observe(StatementResult::DroppedWorker(Some(build_worker_row(
        addr.clone(),
    ))));
}

fn build_fragment_ids(ids: &[i64]) -> HashSet<QueryFragmentId> {
    ids.iter().copied().map(QueryFragmentId::new).collect()
}

fn build_query_ids(ids: &[i64]) -> HashSet<QueryId> {
    ids.iter().copied().map(QueryId::new).collect()
}

fn collect<T: Hash + Eq>(items: Vec<T>) -> HashSet<T> {
    items.into_iter().collect()
}

fn invariant_stopped_or_failed_by_drop() {
    let (dropped, live) = (build_addr(1), build_addr(2));
    let mut model = build_model(&[&dropped, &live]);
    observe_dropped_worker(&mut model, &dropped);

    let accepts = |host: &NetworkAddr, state| {
        is_stopped_or_failed_by_drop(&build_fragment_row(1, 1, host.clone(), state), &model)
    };
    assert!(accepts(&live, QueryFragmentState::Stopped));
    assert!(accepts(&dropped, QueryFragmentState::Stopped));
    assert!(accepts(&dropped, QueryFragmentState::Failed));
    assert!(!accepts(&live, QueryFragmentState::Failed));
    assert!(!accepts(&dropped, QueryFragmentState::Running));
    assert!(!accepts(&live, QueryFragmentState::Pending));
}

fn model_orders_prerequisites() {
    simulate(0, async {
        let mut model = ModelState::default();
        let mut sent = Vec::new();
        loop {
            match model.resolve(Operation::CreateQuery).unwrap() {
                Step::Ready(ParsedStatement::CreateQuery(_)) => break,
                Step::Ready(other) => panic!("expect a query as the ready step, got {other:?}"),
                Step::Prerequisite(statement) => {
                    let (kind, reply) = match statement {
                        ParsedStatement::CreateWorker(create) => (
                            "worker",
                            StatementResult::CreatedWorker(build_worker_row(create.host_addr)),
                        ),
                        ParsedStatement::CreateLogicalSource(create) => (
                            "logical",
                            StatementResult::CreatedLogicalSource(logical::Model {
                                name: create.name.into(),
                                schema: Default::default(),
                            }),
                        ),
                        ParsedStatement::CreatePhysicalSource(create) => (
                            "physical",
                            StatementResult::CreatedPhysicalSource(build_source_row(
                                create.host_addr,
                            )),
                        ),
                        ParsedStatement::CreateSink(create) => (
                            "sink",
                            StatementResult::CreatedSink(build_sink_row(create.host_addr)),
                        ),
                        other => panic!("unexpected prerequisite {other:?}"),
                    };
                    sent.push(kind);
                    model.observe(reply);
                }
            }
            assert!(sent.len() <= 4, "prerequisites do not converge: {sent:?}");
        }
        assert_eq!(sent, ["logical", "worker", "physical", "sink"]);
    });
}

fn model_places_source_and_sink() {
    let workers = [1, 2, 3, 4].map(build_addr);
    let [source_host, sink_host, ..] = &workers;
    for seed in 0..SEEDS {
        simulate(seed, async {
            let mut model = build_model(&workers.iter().collect::<Vec<_>>());
            model.observe(StatementResult::CreatedPhysicalSource(build_source_row(
                source_host.clone(),
            )));
            model.observe(StatementResult::CreatedSink(build_sink_row(
                sink_host.clone(),
            )));

            let Ok(Step::Ready(ParsedStatement::CreateQuery(query))) =
                model.resolve(Operation::CreateQuery)
            else {
                panic!("seed {seed}: a model with a source and a sink must resolve a ready query");
            };
            let hosts: HashSet<_> = query.fragments.iter().map(|f| &f.host_addr).collect();
            assert_eq!(
                hosts.len(),
                query.fragments.len(),
                "seed {seed}: two fragments on one host"
            );
            assert!(
                hosts.contains(source_host),
                "seed {seed}: no fragment on the source host"
            );
            assert!(
                hosts.contains(sink_host),
                "seed {seed}: no fragment on the sink host"
            );
            assert!(
                hosts.iter().all(|h| workers.contains(h)),
                "seed {seed}: fragment on an unknown host"
            );
            for f in &query.fragments {
                assert_eq!(
                    f.has_source,
                    &f.host_addr == source_host,
                    "seed {seed}: has_source on {}",
                    f.host_addr
                );
            }
            assert_eq!(query.source_ids, BTreeSet::from([SourceId::new(1)]));
            assert_eq!(query.sink_ids, BTreeSet::from([SinkId::new(1)]));
        });
    }
}

fn model_spares_source_and_sink() {
    let (source_host, sink_host, spare) = (build_addr(1), build_addr(2), build_addr(3));
    for seed in 0..SEEDS {
        simulate(seed, async {
            let mut model = build_model(&[&source_host, &sink_host, &spare]);
            model.observe(StatementResult::CreatedPhysicalSource(build_source_row(
                source_host.clone(),
            )));
            model.observe(StatementResult::CreatedSink(build_sink_row(
                sink_host.clone(),
            )));
            let Ok(Step::Ready(ParsedStatement::DropWorker(drop))) =
                model.resolve(Operation::DropWorker)
            else {
                panic!("seed {seed}: expect a ready worker drop");
            };
            assert_eq!(drop.host_addr, spare, "seed {seed}");

            observe_dropped_worker(&mut model, &spare);
            assert!(
                model.resolve(Operation::DropWorker).is_err(),
                "seed {seed}: only the source and sink hosts are left"
            );
        });
    }
}

fn model_reserves_hosts() {
    simulate(0, async {
        let a = build_addr(1);
        let mut model = build_model(&[&a]);
        model.observe(StatementResult::CreatedLogicalSource(logical::Model {
            name: "source".to_string(),
            schema: Default::default(),
        }));
        let Ok(Step::Prerequisite(ParsedStatement::CreatePhysicalSource(_))) =
            model.resolve(Operation::CreateQuery)
        else {
            panic!("expect the physical source as the next prerequisite");
        };
        assert!(
            model.resolve(Operation::DropWorker).is_err(),
            "the source host is dropped before the source's reply is observed"
        );

        let b = build_addr(2);
        let mut model = build_model(&[&b]);
        model.observe(StatementResult::CreatedLogicalSource(logical::Model {
            name: "source".to_string(),
            schema: Default::default(),
        }));
        assert!(model.resolve(Operation::DropWorker).is_ok());
        assert!(
            model.resolve(Operation::DropWorker).is_err(),
            "a worker with a drop in flight is dropped again"
        );
        let Ok(Step::Prerequisite(ParsedStatement::CreateWorker(_))) =
            model.resolve(Operation::CreateQuery)
        else {
            panic!("a worker with a drop in flight hosts the source");
        };
    });
}

fn model_drops_only_active_queries() {
    simulate(0, async {
        let (a, b) = (build_addr(1), build_addr(2));
        let mut model = build_model(&[&a, &b]);
        model.observe(build_created_query(1, &[(10, &a)]));
        model.observe(build_created_query(2, &[(20, &b)]));
        model.observe(StatementResult::DroppedQueries(vec![build_query_row(1)]));
        observe_dropped_worker(&mut model, &b);

        assert!(
            model.resolve(Operation::DropQuery).is_err(),
            "a dropped or failed query must not be picked again"
        );
    });
}

fn model_drops_by_id() {
    let (a, b) = (build_addr(1), build_addr(2));
    for seed in 0..SEEDS {
        simulate(seed, async {
            let mut model = build_model(&[&a, &b]);
            model.observe(build_created_query(1, &[(10, &a)]));
            model.observe(build_created_query(2, &[(20, &b)]));
            model.observe(build_created_query(3, &[(30, &a)]));
            model.observe(StatementResult::DroppedQueries(vec![build_query_row(1)]));
            observe_dropped_worker(&mut model, &b);

            let Ok(Step::Ready(ParsedStatement::DropQuery(drop))) =
                model.resolve(Operation::DropQuery)
            else {
                panic!("seed {seed}: expect a ready query drop");
            };
            assert_eq!(drop.filters.ids, Some(vec![QueryId::new(3)]), "seed {seed}");
            assert_eq!(drop.filters.name, None, "seed {seed}");
            assert_eq!(drop.filters.state, None, "seed {seed}");
        });
    }
}

fn model_reserves_queries() {
    simulate(0, async {
        let a = build_addr(1);
        let mut model = build_model(&[&a]);
        model.observe(build_created_query(1, &[(10, &a)]));
        model.observe(build_created_query(2, &[(20, &a)]));

        let picked: HashSet<_> = (0..2)
            .map(|_| {
                let Ok(Step::Ready(ParsedStatement::DropQuery(drop))) =
                    model.resolve(Operation::DropQuery)
                else {
                    panic!("expect a ready query drop");
                };
                drop.filters.ids
            })
            .collect();
        assert_eq!(
            picked,
            HashSet::from([Some(vec![QueryId::new(1)]), Some(vec![QueryId::new(2)])])
        );
        assert!(
            model.resolve(Operation::DropQuery).is_err(),
            "a query with a drop in flight is dropped again"
        );
    });
}

fn model_fails_queries_of_dropped_worker() {
    let (a, b, c) = (build_addr(1), build_addr(2), build_addr(3));
    let mut model = build_model(&[&a, &b, &c]);
    model.observe(build_created_query(1, &[(10, &a), (11, &b)]));
    model.observe(build_created_query(2, &[(20, &b), (21, &c)]));
    model.observe(build_created_query(3, &[(30, &c)]));

    observe_dropped_worker(&mut model, &a);

    assert_eq!(collect(model.failed_queries()), build_query_ids(&[1]));
    assert_eq!(collect(model.active_queries()), build_query_ids(&[2, 3]));
    assert_eq!(model.failed_fragments(), build_fragment_ids(&[10, 11]));
    assert_eq!(model.active_fragments(), build_fragment_ids(&[20, 21, 30]));
    assert!(model.dropped_queries().is_empty());
    assert_eq!(model.dropped_workers(), &HashSet::from([a]));
    assert_eq!(model.active_workers(), &HashSet::from([b, c]));
}

fn model_skips_ended_queries() {
    let (a, b, c) = (build_addr(1), build_addr(2), build_addr(3));
    let mut model = build_model(&[&a, &b, &c]);
    model.observe(build_created_query(1, &[(10, &a)]));
    model.observe(build_created_query(2, &[(20, &b), (21, &c)]));
    model.observe(StatementResult::DroppedQueries(vec![build_query_row(1)]));
    observe_dropped_worker(&mut model, &b);

    observe_dropped_worker(&mut model, &a);
    observe_dropped_worker(&mut model, &c);

    assert_eq!(collect(model.dropped_queries()), build_query_ids(&[1]));
    assert_eq!(collect(model.failed_queries()), build_query_ids(&[2]));
    assert_eq!(model.failed_fragments(), build_fragment_ids(&[20, 21]));
}

fn model_fails_late_placement() {
    let (a, b) = (build_addr(1), build_addr(2));
    let mut model = build_model(&[&a, &b]);
    observe_dropped_worker(&mut model, &a);

    model.observe(build_created_query(1, &[(10, &a), (11, &b)]));

    assert!(model.active_queries().is_empty());
    assert_eq!(collect(model.failed_queries()), build_query_ids(&[1]));
    assert_eq!(model.failed_fragments(), build_fragment_ids(&[10, 11]));
}

fn model_drops_failed_query() {
    let (a, b) = (build_addr(1), build_addr(2));
    let mut model = build_model(&[&a, &b]);
    model.observe(build_created_query(1, &[(10, &a), (11, &b)]));
    observe_dropped_worker(&mut model, &a);

    model.observe(StatementResult::DroppedQueries(vec![build_query_row(1)]));

    assert_eq!(collect(model.dropped_queries()), build_query_ids(&[1]));
    assert!(model.failed_queries().is_empty());
    assert_eq!(model.failed_query_count(), 1);
}

fn model_rejects_double_drop() {
    let a = build_addr(1);
    let mut model = build_model(&[&a]);
    model.observe(build_created_query(1, &[(10, &a)]));

    model.observe(StatementResult::DroppedQueries(vec![build_query_row(1)]));

    assert!(model.active_queries().is_empty());
    assert_eq!(collect(model.dropped_queries()), build_query_ids(&[1]));
    assert!(panics(|| model.observe(StatementResult::DroppedQueries(
        vec![build_query_row(1)]
    ))));
}

/// Octets 2 to 255 give 254 worker IPs.
/// Every second worker is created and dropped, and the others never get a reply, so the test checks that neither case frees an IP.
fn model_allocates_distinct_ips() {
    let mut model = ModelState::default();
    let mut hosts = HashSet::new();
    for n in 0..254 {
        let Ok(Step::Ready(ParsedStatement::CreateWorker(create))) =
            model.resolve(Operation::CreateWorker)
        else {
            panic!("worker {n}: expect a ready worker create");
        };
        assert_eq!(
            create.host_addr.host(),
            create.data_addr.host(),
            "worker {n}"
        );
        assert_eq!(create.host_addr.port(), DEFAULT_HOST_PORT, "worker {n}");
        assert_eq!(create.data_addr.port(), DEFAULT_DATA_PORT, "worker {n}");
        assert_ne!(
            create.host_addr.host(),
            "192.168.1.1",
            "worker {n} got the coordinator's IP"
        );
        assert!(
            hosts.insert(create.host_addr.host().to_string()),
            "worker {n} reuses {}",
            create.host_addr
        );
        if n % 2 == 0 {
            model.observe(StatementResult::CreatedWorker(build_worker_row(
                create.host_addr.clone(),
            )));
            observe_dropped_worker(&mut model, &create.host_addr);
        }
    }
    assert!(
        panics(|| drop(model.resolve(Operation::CreateWorker))),
        "the worker IPs wrap around instead of running out"
    );
}

fn model_rejects_inconsistent_responses() {
    let (a, b) = (build_addr(1), build_addr(2));
    let build = || {
        let mut model = build_model(&[&a]);
        model.observe(build_created_query(1, &[(10, &a)]));
        model
    };
    assert!(
        panics(|| build().observe(build_created_query(1, &[(11, &a)]))),
        "a query created twice"
    );
    assert!(
        panics(|| build().observe(StatementResult::DroppedQueries(vec![]))),
        "a drop that removed nothing"
    );
    assert!(
        panics(|| build().observe(StatementResult::DroppedQueries(vec![
            build_query_row(1),
            build_query_row(2),
        ]))),
        "a drop that removed two queries"
    );
    assert!(
        panics(|| build().observe(StatementResult::DroppedQueries(vec![build_query_row(2)]))),
        "a drop of a query that is not in the model"
    );
    assert!(
        panics(|| build().observe(StatementResult::DroppedWorker(None))),
        "a drop of a worker that is not in the catalog"
    );
    assert!(
        panics(|| observe_dropped_worker(&mut build(), &b)),
        "a drop of a worker that is not in the model"
    );
    assert!(
        panics(|| {
            let mut model = build();
            observe_dropped_worker(&mut model, &a);
            observe_dropped_worker(&mut model, &a);
        }),
        "a worker dropped twice"
    );
    assert!(!panics(|| observe_dropped_worker(&mut build(), &a)));
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

async fn request_status(worker: &WorkerService, after: u64) -> WorkerStatusResponse {
    worker
        .request_status(Request::new(WorkerStatusRequest {
            after_unix_timestamp_in_milli_seconds: after,
        }))
        .await
        .unwrap()
        .into_inner()
}

fn fragment_id(id: Option<&SerializableQueryId>) -> i64 {
    id.and_then(SerializableQueryId::fragment_id).unwrap()
}

async fn list_active_fragments(worker: &WorkerService, after: u64) -> Vec<(i64, Option<u64>)> {
    request_status(worker, after)
        .await
        .active_queries
        .into_iter()
        .map(|q| {
            (
                fragment_id(q.query_id.as_ref()),
                q.started_unix_timestamp_in_milli_seconds,
            )
        })
        .collect()
}

async fn list_terminated_fragments(worker: &WorkerService, after: u64) -> Vec<i64> {
    request_status(worker, after)
        .await
        .terminated_queries
        .into_iter()
        .map(|q| fragment_id(q.query_id.as_ref()))
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
                // The requested time filters only stopped fragments.
                match state {
                    WorkerQueryState::Registered | WorkerQueryState::Started => {
                        assert_eq!(listed, [(FRAGMENT, None)], "seed {seed}");
                        assert_eq!(
                            list_active_fragments(&worker, u64::MAX).await,
                            [(FRAGMENT, None)],
                            "seed {seed}"
                        );
                    }
                    WorkerQueryState::Running => {
                        let running = metrics.running_unix_time_in_ms.unwrap();
                        assert_eq!(listed, [(FRAGMENT, Some(running))], "seed {seed}");
                        assert_eq!(
                            list_active_fragments(&worker, running + 1).await,
                            [(FRAGMENT, Some(running))],
                            "seed {seed}"
                        );
                    }
                    WorkerQueryState::Stopped => {
                        assert!(listed.is_empty(), "seed {seed}");
                        let stopped = metrics.stop_unix_time_in_ms.unwrap();
                        assert_eq!(
                            list_terminated_fragments(&worker, stopped).await,
                            [FRAGMENT],
                            "seed {seed}"
                        );
                        assert!(
                            list_terminated_fragments(&worker, stopped + 1)
                                .await
                                .is_empty(),
                            "seed {seed}"
                        );
                    }
                    other => panic!("seed {seed}: unexpected state {other:?}"),
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

fn harness_tracks_workers() {
    simulate(0, async {
        let harness = Harness::start();
        let mut model = ModelState::default();
        for _ in 0..2 {
            let Ok(Step::Ready(create)) = model.resolve(Operation::CreateWorker) else {
                panic!("expect a ready worker create");
            };
            model.observe(harness.send(create).await.unwrap());
        }
        assert_eq!(harness.get_worker_nodes().len(), 2);

        let Ok(Step::Ready(ParsedStatement::DropWorker(drop))) =
            model.resolve(Operation::DropWorker)
        else {
            panic!("expect a ready worker drop");
        };
        let addr = drop.host_addr.clone();
        let reply = harness
            .send(ParsedStatement::DropWorker(drop))
            .await
            .unwrap();
        let StatementResult::DroppedWorker(Some(row)) = &reply else {
            panic!("expect the dropped worker's row, got {reply:?}");
        };
        assert_eq!(row.host_addr, addr);
        model.observe(reply);

        assert_eq!(harness.get_worker_nodes().len(), 1);
        assert!(
            harness
                .get_worker_fragments(model.active_workers())
                .await
                .is_empty()
        );
    });
}

fn workload_keeps_offsets_in_window() {
    for seed in 0..SEEDS {
        simulate(seed, async {
            let offsets = pick_offsets(10, 7);
            assert_eq!(offsets.len(), 10, "seed {seed}");
            assert!(offsets.is_sorted(), "seed {seed}: offsets out of order");
            assert!(
                offsets.iter().all(|&o| o <= Duration::from_secs(7)),
                "seed {seed}: an operation runs past the end of its window: {offsets:?}"
            );
        });
    }
    simulate(0, async {
        assert!(pick_offsets(0, 7).is_empty());
        assert_eq!(pick_offsets(3, 0), vec![Duration::ZERO; 3]);
    });
}

fn workload_picks_victims() {
    assert!(panics(|| drop(pick_victims(&[], 0.5))));
    for seed in 0..SEEDS {
        simulate(seed, async {
            let nodes: Vec<NodeId> = (0..5)
                .map(|_| Handle::current().create_node().build().id())
                .collect();
            for (rate, expected) in [(0.0, 1), (0.4, 2), (1.0, 5)] {
                let victims = pick_victims(&nodes, rate);
                assert_eq!(victims.len(), expected, "seed {seed}, rate {rate}");
                let distinct: HashSet<NodeId> = victims.into_iter().collect();
                assert_eq!(distinct.len(), expected, "seed {seed}: a node picked twice");
                assert!(distinct.iter().all(|id| nodes.contains(id)));
            }
        });
    }
}
