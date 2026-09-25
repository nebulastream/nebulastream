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

use crate::proto;
use crate::query_gen::{CollectRequest, TimeCharacteristic, TimeMeasure, TimeUnit, WindowType};
use crate::registry::{CollectionDomain, Key, Metric, Report};
use crate::service::StatisticService;
use std::collections::HashMap;
use std::sync::Arc;
use tonic::{Request, Response, Status};

const WATCH_CHANNEL_CAPACITY: usize = 128;

fn metric_from_proto(metric: i32) -> Result<Metric, Status> {
    match proto::Metric::try_from(metric) {
        Ok(proto::Metric::Cardinality) => Ok(Metric::Cardinality),
        Ok(proto::Metric::MinVal) => Ok(Metric::MinVal),
        Ok(proto::Metric::MaxVal) => Ok(Metric::MaxVal),
        Ok(proto::Metric::Rate) => Ok(Metric::Rate),
        Ok(proto::Metric::Average) => Ok(Metric::Average),
        Ok(proto::Metric::Selectivity) => Ok(Metric::Selectivity),
        Ok(proto::Metric::Unspecified) | Err(_) => {
            Err(Status::invalid_argument("metric is unspecified"))
        }
    }
}

fn unit_from_proto(unit: i32) -> Result<TimeUnit, Status> {
    match proto::TimeUnit::try_from(unit) {
        Ok(proto::TimeUnit::Milliseconds) => Ok(TimeUnit::Milliseconds),
        Ok(proto::TimeUnit::Seconds) => Ok(TimeUnit::Seconds),
        Ok(proto::TimeUnit::Minutes) => Ok(TimeUnit::Minutes),
        Ok(proto::TimeUnit::Hours) => Ok(TimeUnit::Hours),
        Ok(proto::TimeUnit::Days) => Ok(TimeUnit::Days),
        Ok(proto::TimeUnit::Unspecified) | Err(_) => {
            Err(Status::invalid_argument("time unit is unspecified"))
        }
    }
}

fn measure_from_proto(measure: &Option<proto::TimeMeasure>) -> Result<TimeMeasure, Status> {
    let measure = measure
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("a time measure is missing"))?;
    Ok(TimeMeasure {
        value: measure.value,
        unit: unit_from_proto(measure.unit)?,
    })
}

fn domain_from_proto(domain: &Option<proto::CollectionDomain>) -> Result<CollectionDomain, Status> {
    let domain = domain
        .as_ref()
        .and_then(|domain| domain.domain.as_ref())
        .ok_or_else(|| Status::invalid_argument("collection domain is missing"))?;
    Ok(match domain {
        proto::collection_domain::Domain::Data(data) => CollectionDomain::Data {
            logical_source_name: data.logical_source_name.clone(),
            field_name: data.field_name.clone(),
        },
        proto::collection_domain::Domain::Workload(workload) => CollectionDomain::Workload {
            query_id: workload.query_id,
            operator_id: workload.operator_id,
            field_name: workload.field_name.clone(),
        },
        proto::collection_domain::Domain::Infrastructure(infrastructure) => {
            CollectionDomain::Infrastructure {
                host_id: infrastructure.host_id.clone(),
            }
        }
    })
}

fn window_from_proto(window: &Option<proto::TimeBasedWindowType>) -> Result<WindowType, Status> {
    let window = window
        .as_ref()
        .and_then(|window| window.window.as_ref())
        .ok_or_else(|| Status::invalid_argument("window type is missing"))?;
    Ok(match window {
        proto::time_based_window_type::Window::Tumbling(tumbling) => WindowType::Tumbling {
            size: measure_from_proto(&tumbling.size)?,
        },
        proto::time_based_window_type::Window::Sliding(sliding) => WindowType::Sliding {
            size: measure_from_proto(&sliding.size)?,
            slide: measure_from_proto(&sliding.slide)?,
        },
    })
}

fn characteristic_from_proto(
    characteristic: &Option<proto::TimeCharacteristic>,
) -> Result<TimeCharacteristic, Status> {
    match characteristic
        .as_ref()
        .and_then(|value| value.characteristic.as_ref())
    {
        None | Some(proto::time_characteristic::Characteristic::Ingestion(_)) => {
            Ok(TimeCharacteristic::Ingestion)
        }
        Some(proto::time_characteristic::Characteristic::Event(event)) => {
            Ok(TimeCharacteristic::Event {
                field_name: event.field_name.clone(),
                unit: unit_from_proto(event.unit)?,
            })
        }
    }
}

fn key_from_proto(key: &Option<proto::StatisticKey>) -> Result<Key, Status> {
    let key = key
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("statistic key is missing"))?;
    Ok(Key {
        metric: metric_from_proto(key.metric)?,
        domain: domain_from_proto(&key.domain)?,
        window_size_ms: measure_from_proto(&key.window_size)?.as_millis(),
    })
}

fn request_from_proto(request: &proto::CollectStatisticRequest) -> Result<CollectRequest, Status> {
    Ok(CollectRequest {
        domain: domain_from_proto(&request.domain)?,
        metric: metric_from_proto(request.metric)?,
        window: window_from_proto(&request.window_type)?,
        time_characteristic: characteristic_from_proto(&request.time_characteristic)?,
        condition: request.condition.clone(),
        options: request
            .options
            .clone()
            .into_iter()
            .collect::<HashMap<_, _>>(),
    })
}

pub struct ReportService {
    service: Arc<StatisticService>,
}

impl ReportService {
    pub fn new(service: Arc<StatisticService>) -> Self {
        Self { service }
    }
}

#[tonic::async_trait]
impl proto::statistic_report_service_server::StatisticReportService for ReportService {
    async fn report_statistics(
        &self,
        request: Request<proto::StatisticReportBatch>,
    ) -> Result<Response<()>, Status> {
        for report in request.into_inner().reports {
            self.service.on_report(Report {
                statistic_id: report.statistic_id,
                start_ts: report.start_ts,
                end_ts: report.end_ts,
                value: report.value,
                probe_id: report.probe_id,
            });
        }
        Ok(Response::new(()))
    }
}

pub struct ControlService {
    service: Arc<StatisticService>,
}

impl ControlService {
    pub fn new(service: Arc<StatisticService>) -> Self {
        Self { service }
    }
}

#[tonic::async_trait]
impl proto::statistic_control_service_server::StatisticControlService for ControlService {
    type WatchStatisticStream = async_channel::Receiver<Result<proto::StatisticReport, Status>>;

    async fn collect_new_statistic(
        &self,
        request: Request<proto::CollectStatisticRequest>,
    ) -> Result<Response<proto::CollectStatisticResponse>, Status> {
        let request = request_from_proto(&request.into_inner())?;
        let result = self
            .service
            .collect_new_statistic(&request, None)
            .await
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        Ok(Response::new(proto::CollectStatisticResponse {
            query_id: result.query_id,
            statistic_id: result.statistic_id,
            already_existed: result.already_existed,
        }))
    }

    async fn get_statistics(
        &self,
        request: Request<proto::GetStatisticsRequest>,
    ) -> Result<Response<proto::GetStatisticsResponse>, Status> {
        let request = request.into_inner();
        let keys = request
            .keys
            .iter()
            .map(|key| key_from_proto(&Some(key.clone())))
            .collect::<Result<Vec<_>, _>>()?;
        let value = self
            .service
            .get_statistics(&keys, request.start_ts, request.end_ts)
            .await
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        Ok(Response::new(proto::GetStatisticsResponse {
            has_value: value.is_some(),
            value: value.unwrap_or_default(),
        }))
    }

    async fn deregister_statistic(
        &self,
        request: Request<proto::DeregisterStatisticRequest>,
    ) -> Result<Response<proto::DeregisterStatisticResponse>, Status> {
        let key = key_from_proto(&request.into_inner().key)?;
        Ok(Response::new(proto::DeregisterStatisticResponse {
            removed: self.service.deregister_statistic(&key),
        }))
    }

    async fn watch_statistic(
        &self,
        request: Request<proto::WatchStatisticRequest>,
    ) -> Result<Response<Self::WatchStatisticStream>, Status> {
        let request = request.into_inner();
        let key = key_from_proto(&request.key)?;
        let (reports_tx, reports_rx) = async_channel::bounded::<Report>(WATCH_CHANNEL_CAPACITY);
        if !self.service.add_condition_trigger(&key, reports_tx) {
            return Err(Status::not_found("statistic key is not registered"));
        }

        let (stream_tx, stream_rx) = async_channel::bounded(WATCH_CHANNEL_CAPACITY);
        tokio::spawn(async move {
            while let Ok(report) = reports_rx.recv().await {
                let message = proto::StatisticReport {
                    statistic_id: report.statistic_id,
                    start_ts: report.start_ts,
                    end_ts: report.end_ts,
                    value: report.value,
                    probe_id: report.probe_id,
                };
                if stream_tx.send(Ok(message)).await.is_err() {
                    break;
                }
            }
        });
        Ok(Response::new(stream_rx))
    }
}
