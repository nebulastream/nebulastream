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

use crate::query_gen::{
    CollectRequest, GenerateError, TimeCharacteristic, TimeMeasure, TimeUnit, WindowType,
    window_clause,
};
use crate::registry::{CollectionDomain, Metric, Report};
use crate::service::{
    ProbeImpulse, QuerySubmitter, SchemaResolver, StatisticError, StatisticService,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const SOURCE: &str = "teststream";
const ADDRESS: &str = "localhost:1234";

#[derive(Default)]
struct StubSubmitter {
    submitted: Mutex<Vec<String>>,
    stopped: Mutex<Vec<u64>>,
    fail: bool,
}

#[async_trait::async_trait]
impl QuerySubmitter for StubSubmitter {
    async fn submit(&self, sql: String) -> Result<u64, String> {
        if self.fail {
            return Err("submission refused".into());
        }
        let mut submitted = self.submitted.lock().unwrap();
        submitted.push(sql);
        Ok(submitted.len() as u64)
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

struct StubResolver;

#[async_trait::async_trait]
impl SchemaResolver for StubResolver {
    async fn field_type(&self, _logical_source: &str, _field: &str) -> Option<String> {
        Some("uint64".into())
    }
}

#[derive(Default)]
struct SilentImpulse;

#[async_trait::async_trait]
impl ProbeImpulse for SilentImpulse {
    async fn request(
        &self,
        _port: u16,
        _statistic_id: u64,
        _start: u64,
        _end: u64,
    ) -> Result<(), String> {
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

fn service(submitter: Arc<StubSubmitter>) -> Arc<StatisticService> {
    Arc::new(
        StatisticService::new(
            submitter,
            Arc::new(StubResolver),
            Arc::new(SilentImpulse),
            ADDRESS.into(),
        )
        .with_probe_timings(Duration::from_millis(80), Duration::from_millis(10)),
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
    assert!(sql.contains("STATISTIC_BUILD(1, 'Avg', value)"), "{sql}");
    assert!(sql.ends_with("INTO Void()"), "{sql}");
    assert!(!sql.contains("STATISTIC_PROBE"), "{sql}");
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
        sql.contains("STATISTIC_PROBE(1, 'Avg', STATISTICVALUE, float64)"),
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
    assert!(sql.contains("STATISTIC_PROBE"), "{sql}");
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
async fn selectivity_stores_a_sample_without_reporting() {
    let submitter = Arc::new(StubSubmitter::default());
    let service = service(submitter.clone());

    let mut request = average_over_value("true");
    request.metric = Metric::Selectivity;
    request.options.insert("sampleSize".into(), "64".into());
    request.options.insert("seed".into(), "7".into());
    service.collect_new_statistic(&request, None).await.unwrap();

    let sql = submitter.queries().pop().unwrap();
    assert!(sql.contains("RESERVOIR(1, 64, 7)"), "{sql}");
    assert!(
        sql.ends_with("INTO Void()"),
        "a sample has no build-time value to report on: {sql}"
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

    let mut cardinality = average_over_value("false");
    cardinality.metric = Metric::Cardinality;
    assert!(matches!(
        service.collect_new_statistic(&cardinality, None).await,
        Err(StatisticError::Generate(GenerateError::NotImplemented(_)))
    ));

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
    let service = service(submitter);

    assert!(matches!(
        service
            .collect_new_statistic(&average_over_value("false"), None)
            .await,
        Err(StatisticError::Submit(_))
    ));
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
        let address: std::net::SocketAddr =
            format!("127.0.0.1:{}", crate::hosting::pick_port(0).unwrap())
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
        assert!(sql.contains("STATISTIC_BUILD(1, 'Avg', value)"), "{sql}");
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
