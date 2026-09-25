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

use crate::placement;
use crate::query_gen::{
    CollectRequest, GenerateError, TimeCharacteristic, TimeMeasure, TimeUnit, VALUE, WindowType,
    probe_query, window_clause,
};
use crate::registry::{CollectionDomain, Metric, Report};
use crate::service::{
    Deployment, PROBE_SOURCE_PORT_BASE, PROBE_SOURCE_PORT_RANGE, ProbeImpulse, QuerySubmitter,
    SchemaResolver, StatisticError, StatisticService, SubmitError, WorkerCatalog,
};
use model::worker::endpoint::NetworkAddr;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const SOURCE: &str = "teststream";
const ADDRESS: &str = "localhost:1234";
const SOURCE_WORKER: &str = "source-node:8080";

fn worker(address: &str) -> NetworkAddr {
    address.parse().unwrap()
}

fn sink_on(address: &str) -> String {
    format!("'{address}' AS \"SINK\".\"HOST\"")
}

/// The worker a generated query puts its sink on.
fn sink_worker_in(sql: &str) -> String {
    let end = sql
        .find("' AS \"SINK\".\"HOST\"")
        .expect("a statistic query places its sink");
    let start = sql[..end].rfind('\'').unwrap() + 1;
    sql[start..end].to_string()
}

/// The statistic a generated query builds, if it builds one.
fn statistic_built_in(sql: &str) -> Option<u64> {
    let rest = &sql[sql.find("STATISTIC_BUILD(")? + "STATISTIC_BUILD(".len()..];
    rest[..rest.find(',')?].parse().ok()
}

#[derive(Default)]
struct StubSubmitter {
    submitted: Mutex<Vec<String>>,
    stopped: Mutex<Vec<u64>>,
    fail: bool,
    /// Workers the planner refuses to place a sink on.
    unplaceable_sinks: Vec<String>,
    /// The worker the planner puts a statistic store writer on; the sink's worker when unset.
    writer_on: Option<String>,
    /// Deploys a build without its writer, as a planner that lost it would.
    omit_writer: bool,
}

#[async_trait::async_trait]
impl QuerySubmitter for StubSubmitter {
    async fn submit(&self, sql: String) -> Result<Deployment, SubmitError> {
        let mut submitted = self.submitted.lock().unwrap();
        submitted.push(sql.clone());
        if self.fail {
            return Err(SubmitError::Other("submission refused".into()));
        }
        if let Some(refused) = self
            .unplaceable_sinks
            .iter()
            .find(|refused| sql.contains(&sink_on(refused)))
        {
            return Err(SubmitError::Placement(format!(
                "no path from the source to the sink on {refused}"
            )));
        }
        let fragments = match statistic_built_in(&sql) {
            Some(statistic_id) if !self.omit_writer => {
                let worker = self
                    .writer_on
                    .clone()
                    .unwrap_or_else(|| sink_worker_in(&sql));
                let writer = format!(
                    r#"{{"type": "StatisticStoreWriter", "config": {{"statisticId": {statistic_id}}}}}"#
                );
                vec![placement::fragment(&worker, &[&writer])]
            }
            _ => vec![],
        };
        Ok(Deployment {
            query_id: submitted.len() as u64,
            fragments,
        })
    }

    async fn stop(&self, query_id: u64) -> Result<(), String> {
        self.stopped.lock().unwrap().push(query_id);
        Ok(())
    }
}

impl StubSubmitter {
    fn queries(&self) -> Vec<String> {
        self.submitted.lock().unwrap().clone()
    }

    fn stopped(&self) -> Vec<u64> {
        self.stopped.lock().unwrap().clone()
    }
}

struct StubCatalog {
    source_workers: Vec<String>,
    workers: Vec<String>,
}

impl Default for StubCatalog {
    fn default() -> Self {
        Self {
            source_workers: vec![SOURCE_WORKER.into()],
            workers: vec![SOURCE_WORKER.into()],
        }
    }
}

#[async_trait::async_trait]
impl WorkerCatalog for StubCatalog {
    async fn source_workers(&self, logical_source: &str) -> Result<Vec<NetworkAddr>, String> {
        assert_eq!(logical_source, SOURCE);
        Ok(self
            .source_workers
            .iter()
            .map(|address| worker(address))
            .collect())
    }

    async fn workers(&self) -> Result<Vec<NetworkAddr>, String> {
        Ok(self.workers.iter().map(|address| worker(address)).collect())
    }
}

struct StubResolver;

#[async_trait::async_trait]
impl SchemaResolver for StubResolver {
    async fn field_type(&self, _logical_source: &str, _field: &str) -> Option<String> {
        Some("uint64".into())
    }
}

/// Accepts every request and remembers the endpoints it was sent to.
#[derive(Default)]
struct RecordingImpulse {
    endpoints: Mutex<Vec<NetworkAddr>>,
}

#[async_trait::async_trait]
impl ProbeImpulse for RecordingImpulse {
    async fn request(
        &self,
        endpoint: &NetworkAddr,
        _statistic_id: u64,
        _start: u64,
        _end: u64,
    ) -> Result<(), String> {
        self.endpoints.lock().unwrap().push(endpoint.clone());
        Ok(())
    }
}

fn probe_id_in(sql: &str) -> u64 {
    let marker = " AS \"SINK\".PROBE_ID";
    let end = sql
        .find(marker)
        .expect("a probe query must stamp its probe id");
    let head = sql[..end].trim_end_matches('\'');
    let start = head.rfind('\'').expect("the probe id is quoted");
    head[start + 1..]
        .parse()
        .expect("the probe id must be a number")
}

fn service_with(
    submitter: Arc<StubSubmitter>,
    catalog: StubCatalog,
    impulse: Arc<RecordingImpulse>,
) -> Arc<StatisticService> {
    Arc::new(
        StatisticService::new(
            submitter,
            Arc::new(catalog),
            Arc::new(StubResolver),
            impulse,
            ADDRESS.into(),
        )
        .with_probe_timings(Duration::from_millis(80), Duration::from_millis(10)),
    )
}

fn service(submitter: Arc<StubSubmitter>) -> Arc<StatisticService> {
    service_with(
        submitter,
        StubCatalog::default(),
        Arc::new(RecordingImpulse::default()),
    )
}

fn average_over_value(condition: &str) -> CollectRequest {
    CollectRequest {
        domain: CollectionDomain::Data {
            logical_source_name: SOURCE.into(),
            field_name: "value".into(),
        },
        metric: Metric::Average,
        window: WindowType::Tumbling {
            size: TimeMeasure {
                value: 1000,
                unit: TimeUnit::Milliseconds,
            },
        },
        time_characteristic: TimeCharacteristic::Event {
            field_name: "ts".into(),
            unit: TimeUnit::Milliseconds,
        },
        condition: condition.into(),
        options: HashMap::new(),
    }
}

#[tokio::test]
async fn never_send_terminates_in_a_void_sink_and_never_probes() {
    let submitter = Arc::new(StubSubmitter::default());
    let service = service(submitter.clone());

    let result = service
        .collect_new_statistic(&average_over_value("false"), None)
        .await
        .unwrap();
    assert!(!result.already_existed);

    let sql = submitter.queries().pop().unwrap();
    assert!(sql.contains("STATISTIC_BUILD(1, AVG(value))"), "{sql}");
    assert!(
        sql.ends_with(&format!("INTO Void({})", sink_on(SOURCE_WORKER))),
        "{sql}"
    );
    assert!(!sql.contains("_PROBE"), "{sql}");
    assert!(!sql.contains("Grpc"), "{sql}");
}

#[tokio::test]
async fn always_send_probes_but_does_not_filter() {
    let submitter = Arc::new(StubSubmitter::default());
    let service = service(submitter.clone());

    service
        .collect_new_statistic(&average_over_value("true"), None)
        .await
        .unwrap();

    let sql = submitter.queries().pop().unwrap();
    assert!(
        sql.contains("AVG_PROBE(1, STATISTICVALUE, float64)"),
        "{sql}"
    );
    assert!(
        sql.contains("INTO Grpc('localhost' AS \"SINK\".GRPC_HOST, '1234' AS \"SINK\".GRPC_PORT"),
        "{sql}"
    );
    assert!(!sql.contains("WHERE"), "{sql}");
}

#[tokio::test]
async fn a_condition_becomes_a_where_over_the_probed_value() {
    let submitter = Arc::new(StubSubmitter::default());
    let service = service(submitter.clone());

    service
        .collect_new_statistic(&average_over_value("STATISTICVALUE > 100.0"), None)
        .await
        .unwrap();

    let sql = submitter.queries().pop().unwrap();
    assert!(sql.contains("AVG_PROBE"), "{sql}");
    assert!(sql.contains("WHERE STATISTICVALUE > 100.0"), "{sql}");
    assert!(sql.contains("INTO Grpc"), "{sql}");
}

#[tokio::test]
async fn identical_requests_are_deduplicated() {
    let submitter = Arc::new(StubSubmitter::default());
    let service = service(submitter.clone());

    let first = service
        .collect_new_statistic(&average_over_value("false"), None)
        .await
        .unwrap();
    let second = service
        .collect_new_statistic(&average_over_value("false"), None)
        .await
        .unwrap();

    assert!(!first.already_existed);
    assert!(second.already_existed);
    assert_eq!(first.statistic_id, second.statistic_id);
    assert_eq!(first.query_id, second.query_id);
    assert_eq!(
        submitter.queries().len(),
        1,
        "a duplicate request must not deploy a second query"
    );
}

#[tokio::test]
async fn unsupported_requests_fail_with_not_implemented() {
    let submitter = Arc::new(StubSubmitter::default());
    let service = service(submitter.clone());

    let mut infrastructure = average_over_value("false");
    infrastructure.domain = CollectionDomain::Infrastructure {
        host_id: "worker1".into(),
    };
    assert!(matches!(
        service.collect_new_statistic(&infrastructure, None).await,
        Err(StatisticError::Generate(GenerateError::NotImplemented(_)))
    ));

    let mut workload = average_over_value("false");
    workload.domain = CollectionDomain::Workload {
        query_id: 1,
        operator_id: 2,
        field_name: "value".into(),
    };
    assert!(matches!(
        service.collect_new_statistic(&workload, None).await,
        Err(StatisticError::Generate(GenerateError::NotImplemented(_)))
    ));

    for synopsis in [Metric::Cardinality, Metric::Selectivity] {
        let mut request = average_over_value("false");
        request.metric = synopsis;
        assert!(matches!(
            service.collect_new_statistic(&request, None).await,
            Err(StatisticError::Generate(GenerateError::NotImplemented(_)))
        ));
    }

    assert!(
        submitter.queries().is_empty(),
        "a rejected request must deploy nothing"
    );
}

#[tokio::test]
async fn get_statistics_yields_nothing_when_no_report_arrives() {
    let submitter = Arc::new(StubSubmitter::default());
    let service = service(submitter.clone());

    let request = average_over_value("false");
    service.collect_new_statistic(&request, None).await.unwrap();
    let key = StatisticService::key_of(&request);

    let probed = service
        .get_statistics(std::slice::from_ref(&key), 0, 10_000)
        .await
        .unwrap();
    assert_eq!(
        probed, None,
        "a probe nothing reports to must not answer with a value"
    );
}

#[tokio::test]
async fn get_statistics_sums_what_the_probe_reports() {
    let submitter = Arc::new(StubSubmitter::default());
    let service = service(submitter.clone());

    let request = average_over_value("false");
    let collected = service.collect_new_statistic(&request, None).await.unwrap();
    let key = StatisticService::key_of(&request);

    let reporter = service.clone();
    let reporting_submitter = submitter.clone();
    let statistic_id = collected.statistic_id;
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(10)).await;
        let probe_id = probe_id_in(&reporting_submitter.queries().pop().unwrap());
        reporter.on_report(Report {
            statistic_id,
            start_ts: 0,
            end_ts: 1000,
            value: 20.0,
            probe_id,
        });
        reporter.on_report(Report {
            statistic_id,
            start_ts: 1000,
            end_ts: 2000,
            value: 200.0,
            probe_id,
        });
    });

    let probed = service.get_statistics(&[key], 0, 10_000).await.unwrap();
    assert_eq!(
        probed,
        Some(220.0),
        "expected the sum of both windows' averages"
    );
    assert_eq!(
        submitter.stopped().len(),
        1,
        "the probe query must not outlive the probe that deployed it"
    );
}

#[tokio::test]
async fn get_statistics_rejects_an_unregistered_key() {
    let submitter = Arc::new(StubSubmitter::default());
    let service = service(submitter.clone());
    let key = StatisticService::key_of(&average_over_value("false"));

    assert!(matches!(
        service.get_statistics(&[key], 0, 1).await,
        Err(StatisticError::UnknownKey)
    ));
}

#[tokio::test]
async fn triggers_can_be_added_and_entries_removed() {
    let submitter = Arc::new(StubSubmitter::default());
    let service = service(submitter.clone());

    let request = average_over_value("false");
    let unknown_key = StatisticService::key_of(&request);
    let (sink, reports) = async_channel::unbounded();
    assert!(
        !service.add_condition_trigger(&unknown_key, sink.clone()),
        "a trigger cannot attach to a statistic that was never requested"
    );

    let collected = service.collect_new_statistic(&request, None).await.unwrap();
    let key = StatisticService::key_of(&request);
    assert!(service.add_condition_trigger(&key, sink));

    service.on_report(Report {
        statistic_id: collected.statistic_id,
        start_ts: 0,
        end_ts: 1000,
        value: 42.0,
        probe_id: 0,
    });
    assert_eq!(reports.try_recv().unwrap().value, 42.0);

    assert_eq!(service.registered_statistics(), 1);
    assert!(service.deregister_statistic(&key));
    assert!(
        !service.deregister_statistic(&key),
        "deregistering twice removes nothing the second time"
    );
    assert_eq!(service.registered_statistics(), 0);
}

#[tokio::test]
async fn a_failing_submission_surfaces_as_an_error() {
    let submitter = Arc::new(StubSubmitter {
        fail: true,
        ..Default::default()
    });
    let service = service(submitter.clone());

    assert!(matches!(
        service
            .collect_new_statistic(&average_over_value("false"), None)
            .await,
        Err(StatisticError::Submit(_))
    ));
    assert_eq!(
        submitter.queries().len(),
        1,
        "only a placement failure moves on to another worker"
    );
}

#[test]
fn a_probe_reads_one_statistic_on_the_worker_it_names() {
    let payload = vec![(VALUE.to_string(), "float64".to_string())];
    let sql = probe_query(
        3,
        "AVG",
        &payload,
        10000,
        "localhost:1234",
        9,
        &worker("worker-2:8080"),
    )
    .unwrap();

    assert!(
        sql.contains(
            "SELECT AVG_PROBE_RANGE(3, STATISTICVALUE, float64) FROM (SELECT * FROM Grpc('10000'"
        ),
        "{sql}"
    );
    assert!(
        sql.contains("'worker-2:8080' AS \"SOURCE\".\"HOST\""),
        "{sql}"
    );
    assert!(sql.contains(&sink_on("worker-2:8080")), "{sql}");
    assert!(sql.contains("'9' AS \"SINK\".PROBE_ID"), "{sql}");
}

#[tokio::test]
async fn the_sink_is_placed_on_the_source_worker() {
    for condition in ["false", "true"] {
        let submitter = Arc::new(StubSubmitter::default());
        let service = service(submitter.clone());

        service
            .collect_new_statistic(&average_over_value(condition), None)
            .await
            .unwrap();

        let sql = submitter.queries().pop().unwrap();
        assert!(sql.contains(&sink_on(SOURCE_WORKER)), "{sql}");
    }
}

#[tokio::test]
async fn a_sink_the_planner_cannot_place_moves_on_to_the_next_worker() {
    let submitter = Arc::new(StubSubmitter {
        unplaceable_sinks: vec!["source-a:8080".into(), "source-b:8080".into()],
        ..Default::default()
    });
    let catalog = StubCatalog {
        source_workers: vec![
            "source-b:8080".into(),
            "source-a:8080".into(),
            "source-b:8080".into(),
        ],
        workers: vec![
            "source-a:8080".into(),
            "source-b:8080".into(),
            "sink-node:8080".into(),
        ],
    };
    let service = service_with(
        submitter.clone(),
        catalog,
        Arc::new(RecordingImpulse::default()),
    );

    let collected = service
        .collect_new_statistic(&average_over_value("false"), None)
        .await
        .unwrap();

    let tried: Vec<bool> = submitter
        .queries()
        .iter()
        .map(|sql| sql.contains(&sink_on("sink-node:8080")))
        .collect();
    assert_eq!(
        tried,
        vec![false, false, true],
        "each source worker is tried once, in order, before the other workers"
    );
    assert!(submitter.queries()[0].contains(&sink_on("source-b:8080")));
    assert!(submitter.queries()[1].contains(&sink_on("source-a:8080")));
    assert_eq!(collected.query_id, 3);
}

#[tokio::test]
async fn a_statistic_no_worker_can_place_is_rejected() {
    let submitter = Arc::new(StubSubmitter {
        unplaceable_sinks: vec![SOURCE_WORKER.into()],
        ..Default::default()
    });
    let service = service(submitter.clone());
    let request = average_over_value("false");

    assert!(matches!(
        service.collect_new_statistic(&request, None).await,
        Err(StatisticError::NoPlacement { .. })
    ));
    assert_eq!(
        service.registered_statistics(),
        0,
        "a statistic nothing deployed must not be registered"
    );
}

#[tokio::test]
async fn a_source_without_physical_sources_is_rejected() {
    let submitter = Arc::new(StubSubmitter::default());
    let catalog = StubCatalog {
        source_workers: vec![],
        ..Default::default()
    };
    let service = service_with(
        submitter.clone(),
        catalog,
        Arc::new(RecordingImpulse::default()),
    );

    assert!(matches!(
        service
            .collect_new_statistic(&average_over_value("false"), None)
            .await,
        Err(StatisticError::NoPhysicalSource(_))
    ));
    assert!(submitter.queries().is_empty());
}

#[tokio::test]
async fn each_statistic_is_probed_on_its_own_worker() {
    let submitter = Arc::new(StubSubmitter::default());
    let catalog = StubCatalog {
        source_workers: vec!["source-a:8080".into()],
        workers: vec!["source-a:8080".into()],
    };
    let impulse = Arc::new(RecordingImpulse::default());
    let service = service_with(submitter.clone(), catalog, impulse.clone());

    let first = average_over_value("false");
    let mut second = average_over_value("false");
    second.window = WindowType::Tumbling {
        size: TimeMeasure {
            value: 2000,
            unit: TimeUnit::Milliseconds,
        },
    };
    service.collect_new_statistic(&first, None).await.unwrap();
    service.collect_new_statistic(&second, None).await.unwrap();

    let keys = [
        StatisticService::key_of(&first),
        StatisticService::key_of(&second),
    ];
    assert_eq!(
        service.get_statistics(&keys, 0, 10_000).await.unwrap(),
        None
    );

    let probes: Vec<String> = submitter.queries().split_off(2);
    assert_eq!(probes.len(), 2, "one probe query per statistic: {probes:?}");
    assert!(probes[0].contains("AVG_PROBE_RANGE(1,"), "{}", probes[0]);
    assert!(probes[1].contains("AVG_PROBE_RANGE(2,"), "{}", probes[1]);
    for probe in &probes {
        assert!(
            probe.contains("'source-a:8080' AS \"SOURCE\".\"HOST\""),
            "{probe}"
        );
        assert!(probe.contains(&sink_on("source-a:8080")), "{probe}");
    }

    let endpoints = impulse.endpoints.lock().unwrap().clone();
    assert_eq!(endpoints.len(), 2);
    assert!(endpoints.iter().all(|endpoint| endpoint.host == "source-a"));
    assert!(endpoints.iter().all(|endpoint| {
        (PROBE_SOURCE_PORT_BASE..PROBE_SOURCE_PORT_BASE + PROBE_SOURCE_PORT_RANGE)
            .contains(&endpoint.port)
    }));
    assert_ne!(
        endpoints[0].port, endpoints[1].port,
        "probes on one worker must not share a port"
    );
    assert_eq!(submitter.stopped(), vec![3, 4]);
}

#[tokio::test]
async fn a_statistic_is_probed_where_its_writer_ran_not_where_its_sink_is() {
    let submitter = Arc::new(StubSubmitter {
        writer_on: Some("middle-node:8080".into()),
        ..Default::default()
    });
    let impulse = Arc::new(RecordingImpulse::default());
    let service = service_with(submitter.clone(), StubCatalog::default(), impulse.clone());

    let request = average_over_value("false");
    service.collect_new_statistic(&request, None).await.unwrap();
    assert!(submitter.queries()[0].contains(&sink_on(SOURCE_WORKER)));

    let key = StatisticService::key_of(&request);
    service.get_statistics(&[key], 0, 10_000).await.unwrap();

    let probe = submitter.queries().pop().unwrap();
    assert!(
        probe.contains("'middle-node:8080' AS \"SOURCE\".\"HOST\""),
        "{probe}"
    );
    assert!(probe.contains(&sink_on("middle-node:8080")), "{probe}");
    let endpoints = impulse.endpoints.lock().unwrap().clone();
    assert_eq!(endpoints.len(), 1);
    assert_eq!(endpoints[0].host, "middle-node");
}

#[tokio::test]
async fn a_build_whose_writer_cannot_be_found_is_stopped_and_rejected() {
    let submitter = Arc::new(StubSubmitter {
        omit_writer: true,
        ..Default::default()
    });
    let service = service(submitter.clone());

    assert!(matches!(
        service
            .collect_new_statistic(&average_over_value("false"), None)
            .await,
        Err(StatisticError::WriterNotPlaced {
            query_id: 1,
            statistic_id: 1
        })
    ));
    assert_eq!(
        submitter.stopped(),
        vec![1],
        "a statistic that cannot be read back must not keep running"
    );
    assert_eq!(service.registered_statistics(), 0);
}

#[test]
fn the_service_listens_on_loopback_only_when_advertised_there() {
    use crate::hosting::bind_ip;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    assert_eq!(bind_ip("localhost"), IpAddr::V4(Ipv4Addr::LOCALHOST));
    assert_eq!(bind_ip("127.0.0.1"), IpAddr::V4(Ipv4Addr::LOCALHOST));
    assert_eq!(bind_ip("[::1]"), IpAddr::V6(Ipv6Addr::LOCALHOST));
    assert_eq!(bind_ip("coordinator"), IpAddr::V4(Ipv4Addr::UNSPECIFIED));
    assert_eq!(bind_ip("10.0.0.5"), IpAddr::V4(Ipv4Addr::UNSPECIFIED));
    assert_eq!(bind_ip("fd00::5"), IpAddr::V6(Ipv6Addr::UNSPECIFIED));
}

#[tokio::test]
async fn a_rate_is_built_as_a_count() {
    let submitter = Arc::new(StubSubmitter::default());
    let service = service(submitter.clone());

    let mut request = average_over_value("true");
    request.metric = Metric::Rate;
    service.collect_new_statistic(&request, None).await.unwrap();

    let sql = submitter.queries().pop().unwrap();
    assert!(sql.contains("STATISTIC_BUILD(1, COUNT(value))"), "{sql}");
    assert!(
        sql.contains("COUNT_PROBE(1, STATISTICVALUE, uint64)"),
        "{sql}"
    );
}

#[test]
fn window_clauses_cover_ingestion_and_event_time() {
    let size = TimeMeasure {
        value: 5,
        unit: TimeUnit::Seconds,
    };
    let slide = TimeMeasure {
        value: 500,
        unit: TimeUnit::Milliseconds,
    };

    assert_eq!(
        window_clause(
            WindowType::Tumbling { size },
            &TimeCharacteristic::Ingestion
        ),
        "WINDOW TUMBLING(size 5 sec)"
    );
    assert_eq!(
        window_clause(
            WindowType::Tumbling { size },
            &TimeCharacteristic::Event {
                field_name: "ts".into(),
                unit: TimeUnit::Milliseconds
            }
        ),
        "WINDOW TUMBLING(ts, size 5 sec)"
    );
    assert_eq!(
        window_clause(
            WindowType::Sliding { size, slide },
            &TimeCharacteristic::Event {
                field_name: "ts".into(),
                unit: TimeUnit::Milliseconds
            }
        ),
        "WINDOW SLIDING(ts, size 5 sec, advance by 500 ms)"
    );
}

#[test]
fn a_window_size_is_normalised_to_milliseconds_for_the_registry_key() {
    let mut request = average_over_value("false");
    request.window = WindowType::Tumbling {
        size: TimeMeasure {
            value: 5,
            unit: TimeUnit::Seconds,
        },
    };
    assert_eq!(StatisticService::key_of(&request).window_size_ms, 5_000);

    request.window = WindowType::Tumbling {
        size: TimeMeasure {
            value: 5000,
            unit: TimeUnit::Milliseconds,
        },
    };
    assert_eq!(
        StatisticService::key_of(&request).window_size_ms,
        5_000,
        "the same window written in two units is the same request"
    );
}

mod transport {
    use super::*;
    use crate::grpc::{ControlService, ReportService};
    use crate::proto;

    fn measure(value: u64) -> proto::TimeMeasure {
        proto::TimeMeasure {
            value,
            unit: proto::TimeUnit::Milliseconds as i32,
        }
    }

    fn data_domain() -> proto::CollectionDomain {
        proto::CollectionDomain {
            domain: Some(proto::collection_domain::Domain::Data(proto::DataDomain {
                logical_source_name: SOURCE.into(),
                field_name: "value".into(),
            })),
        }
    }

    fn collect_request(condition: &str) -> proto::CollectStatisticRequest {
        proto::CollectStatisticRequest {
            domain: Some(data_domain()),
            metric: proto::Metric::Average as i32,
            window_type: Some(proto::TimeBasedWindowType {
                window: Some(proto::time_based_window_type::Window::Tumbling(
                    proto::TumblingWindow {
                        size: Some(measure(1000)),
                    },
                )),
            }),
            time_characteristic: Some(proto::TimeCharacteristic {
                characteristic: Some(proto::time_characteristic::Characteristic::Event(
                    proto::EventTime {
                        field_name: "ts".into(),
                        unit: proto::TimeUnit::Milliseconds as i32,
                    },
                )),
            }),
            condition: condition.into(),
            options: Default::default(),
        }
    }

    fn statistic_key() -> proto::StatisticKey {
        proto::StatisticKey {
            metric: proto::Metric::Average as i32,
            domain: Some(data_domain()),
            window_size: Some(measure(1000)),
        }
    }

    async fn serve(service: Arc<StatisticService>) -> String {
        let address: std::net::SocketAddr = format!(
            "127.0.0.1:{}",
            crate::hosting::pick_port(std::net::Ipv4Addr::LOCALHOST.into(), 0).unwrap()
        )
        .parse()
        .unwrap();

        let control = proto::statistic_control_service_server::StatisticControlServiceServer::new(
            ControlService::new(service.clone()),
        );
        let report = proto::statistic_report_service_server::StatisticReportServiceServer::new(
            ReportService::new(service),
        );
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(control)
                .add_service(report)
                .serve(address)
                .await
        });

        let endpoint = format!("http://{address}");
        for _ in 0..100 {
            if tonic::transport::Endpoint::from_shared(endpoint.clone())
                .unwrap()
                .connect()
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        endpoint
    }

    #[tokio::test]
    async fn a_collect_call_crosses_the_wire_and_deploys_a_query() {
        let submitter = Arc::new(StubSubmitter::default());
        let service = service(submitter.clone());
        let endpoint = serve(service).await;

        let mut client =
            proto::statistic_control_service_client::StatisticControlServiceClient::connect(
                endpoint,
            )
            .await
            .unwrap();
        let response = client
            .collect_new_statistic(collect_request("false"))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.statistic_id, 1);
        assert!(!response.already_existed);

        let sql = submitter.queries().pop().unwrap();
        assert!(sql.contains("STATISTIC_BUILD(1, AVG(value))"), "{sql}");
        assert!(sql.contains("WINDOW TUMBLING(ts, size 1000 ms)"), "{sql}");

        let second = client
            .collect_new_statistic(collect_request("false"))
            .await
            .unwrap()
            .into_inner();
        assert!(
            second.already_existed,
            "the wire path must reuse the registry's dedup"
        );
    }

    #[tokio::test]
    async fn an_unspecified_metric_is_rejected_rather_than_guessed() {
        let submitter = Arc::new(StubSubmitter::default());
        let service = service(submitter.clone());
        let endpoint = serve(service).await;

        let mut client =
            proto::statistic_control_service_client::StatisticControlServiceClient::connect(
                endpoint,
            )
            .await
            .unwrap();
        let mut request = collect_request("false");
        request.metric = proto::Metric::Unspecified as i32;

        let status = client.collect_new_statistic(request).await.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(submitter.queries().is_empty());
    }

    #[tokio::test]
    async fn a_reported_statistic_reaches_a_watching_client() {
        let submitter = Arc::new(StubSubmitter::default());
        let service = service(submitter.clone());
        let endpoint = serve(service.clone()).await;

        let mut control =
            proto::statistic_control_service_client::StatisticControlServiceClient::connect(
                endpoint.clone(),
            )
            .await
            .unwrap();
        let collected = control
            .collect_new_statistic(collect_request("true"))
            .await
            .unwrap()
            .into_inner();

        let mut stream = control
            .watch_statistic(proto::WatchStatisticRequest {
                key: Some(statistic_key()),
                condition: "true".into(),
            })
            .await
            .unwrap()
            .into_inner();

        let mut reporter =
            proto::statistic_report_service_client::StatisticReportServiceClient::connect(endpoint)
                .await
                .unwrap();
        reporter
            .report_statistics(proto::StatisticReportBatch {
                reports: vec![proto::StatisticReport {
                    statistic_id: collected.statistic_id,
                    start_ts: 0,
                    end_ts: 1000,
                    value: 200.0,
                    probe_id: 0,
                }],
            })
            .await
            .unwrap();

        let report = tokio::time::timeout(Duration::from_secs(5), stream.message())
            .await
            .expect("the watch stream never delivered")
            .unwrap()
            .expect("the watch stream closed without a report");
        assert_eq!(report.statistic_id, collected.statistic_id);
        assert_eq!(report.value, 200.0);
    }

    #[tokio::test]
    async fn watching_an_unknown_statistic_is_not_found() {
        let submitter = Arc::new(StubSubmitter::default());
        let service = service(submitter);
        let endpoint = serve(service).await;

        let mut client =
            proto::statistic_control_service_client::StatisticControlServiceClient::connect(
                endpoint,
            )
            .await
            .unwrap();
        let status = client
            .watch_statistic(proto::WatchStatisticRequest {
                key: Some(statistic_key()),
                condition: "true".into(),
            })
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn deregistering_over_the_wire_removes_the_entry() {
        let submitter = Arc::new(StubSubmitter::default());
        let service = service(submitter);
        let endpoint = serve(service.clone()).await;

        let mut client =
            proto::statistic_control_service_client::StatisticControlServiceClient::connect(
                endpoint,
            )
            .await
            .unwrap();
        client
            .collect_new_statistic(collect_request("false"))
            .await
            .unwrap();
        assert_eq!(service.registered_statistics(), 1);

        let removed = client
            .deregister_statistic(proto::DeregisterStatisticRequest {
                key: Some(statistic_key()),
            })
            .await
            .unwrap()
            .into_inner();
        assert!(removed.removed);
        assert_eq!(service.registered_statistics(), 0);
    }
}
