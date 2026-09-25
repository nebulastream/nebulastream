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

use crate::registry::{CollectionDomain, Metric};
use model::worker::endpoint::NetworkAddr;
use std::collections::HashMap;

pub const STATISTIC_ID: &str = "STATISTICID";
pub const START_TS: &str = "STATISTICSTART";
pub const END_TS: &str = "STATISTICEND";
pub const VALUE: &str = "STATISTICVALUE";

#[derive(Debug, thiserror::Error)]
pub enum GenerateError {
    #[error("{0}")]
    NotImplemented(String),
    #[error("{0}")]
    InvalidRequest(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimeUnit {
    Milliseconds,
    Seconds,
    Minutes,
    Hours,
    Days,
}

impl TimeUnit {
    pub fn sql(self) -> &'static str {
        match self {
            TimeUnit::Milliseconds => "ms",
            TimeUnit::Seconds => "sec",
            TimeUnit::Minutes => "minute",
            TimeUnit::Hours => "hour",
            TimeUnit::Days => "day",
        }
    }

    pub fn millis_per_unit(self) -> u64 {
        match self {
            TimeUnit::Milliseconds => 1,
            TimeUnit::Seconds => 1_000,
            TimeUnit::Minutes => 60_000,
            TimeUnit::Hours => 3_600_000,
            TimeUnit::Days => 86_400_000,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimeMeasure {
    pub value: u64,
    pub unit: TimeUnit,
}

impl TimeMeasure {
    pub fn as_millis(self) -> u64 {
        self.value.saturating_mul(self.unit.millis_per_unit())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WindowType {
    Tumbling {
        size: TimeMeasure,
    },
    Sliding {
        size: TimeMeasure,
        slide: TimeMeasure,
    },
}

impl WindowType {
    pub fn size(self) -> TimeMeasure {
        match self {
            WindowType::Tumbling { size } | WindowType::Sliding { size, .. } => size,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TimeCharacteristic {
    Ingestion,
    Event { field_name: String, unit: TimeUnit },
}

#[derive(Clone, Debug)]
pub struct CollectRequest {
    pub domain: CollectionDomain,
    pub metric: Metric,
    pub window: WindowType,
    pub time_characteristic: TimeCharacteristic,
    pub condition: String,
    pub options: HashMap<String, String>,
}

/// The SQL aggregation a metric is built with. Its probes are named after it: `AVG_PROBE`, `AVG_PROBE_RANGE`.
pub fn aggregation(metric: Metric) -> Result<&'static str, GenerateError> {
    match metric {
        Metric::Average => Ok("AVG"),
        Metric::MinVal => Ok("MIN"),
        Metric::MaxVal => Ok("MAX"),
        Metric::Rate => Ok("COUNT"),
        Metric::Cardinality | Metric::Selectivity => Err(GenerateError::NotImplemented(format!(
            "Metric {metric:?} needs a synopsis statistic, which this port does not provide"
        ))),
    }
}

pub fn window_clause(window: WindowType, characteristic: &TimeCharacteristic) -> String {
    let timestamp = match characteristic {
        TimeCharacteristic::Ingestion => String::new(),
        TimeCharacteristic::Event { field_name, .. } => format!("{field_name}, "),
    };
    match window {
        WindowType::Tumbling { size } => {
            format!(
                "WINDOW TUMBLING({}size {} {})",
                timestamp,
                size.value,
                size.unit.sql()
            )
        }
        WindowType::Sliding { size, slide } => format!(
            "WINDOW SLIDING({}size {} {}, advance by {} {})",
            timestamp,
            size.value,
            size.unit.sql(),
            slide.value,
            slide.unit.sql()
        ),
    }
}

fn split_address(address: &str) -> Result<(&str, &str), GenerateError> {
    address.rsplit_once(':').ok_or_else(|| {
        GenerateError::InvalidRequest(format!(
            "statistic service address '{address}' is not in host:port form"
        ))
    })
}

fn sends_never(condition: &str) -> bool {
    let normalised = condition.trim().to_lowercase();
    normalised.is_empty() || normalised == "false"
}

fn sends_always(condition: &str) -> bool {
    condition.trim().to_lowercase() == "true"
}

/// Places a source or sink on a worker. The coordinator only fills in a missing host in embedded deployments, and the
/// worker a statistic query runs on is what decides which worker's statistic store it reaches.
fn on_worker(group: &str, worker: &NetworkAddr) -> String {
    format!("'{worker}' AS \"{group}\".\"HOST\"")
}

fn grpc_sink(address: &str, probe_id: u64, worker: &NetworkAddr) -> Result<String, GenerateError> {
    let (host, port) = split_address(address)?;
    Ok(format!(
        "INTO Grpc('{host}' AS \"SINK\".GRPC_HOST, '{port}' AS \"SINK\".GRPC_PORT, '{probe_id}' AS \"SINK\".PROBE_ID, 'CSV' AS \"SINK\".OUTPUT_FORMAT, {})",
        on_worker("SINK", worker)
    ))
}

pub fn build_expression(
    request: &CollectRequest,
    statistic_id: u64,
    field_name: &str,
) -> Result<String, GenerateError> {
    let aggregation = aggregation(request.metric)?;
    Ok(format!(
        "STATISTIC_BUILD({statistic_id}, {aggregation}({field_name}))"
    ))
}

pub fn intrinsic_payload_type(metric: Metric) -> Option<&'static str> {
    match metric {
        Metric::Average => Some("float64"),
        Metric::Rate => Some("uint64"),
        _ => None,
    }
}

/// The logical source and field a request collects over. Only the data domain can be collected so far.
pub fn collected_field(request: &CollectRequest) -> Result<(&str, &str), GenerateError> {
    match &request.domain {
        CollectionDomain::Data {
            logical_source_name,
            field_name,
        } => Ok((logical_source_name, field_name)),
        CollectionDomain::Workload {
            query_id,
            operator_id,
            ..
        } => Err(GenerateError::NotImplemented(format!(
            "Collecting a statistic over the output of query {query_id} operator {operator_id} is not implemented"
        ))),
        CollectionDomain::Infrastructure { host_id } => Err(GenerateError::NotImplemented(
            format!("Collecting infrastructure statistics for worker {host_id} is not implemented"),
        )),
    }
}

/// The query that builds a statistic, and probes it right back when the request reports on a condition.
/// Its sink is placed on `sink_worker`; the build and the probe land between that worker and the source's workers.
pub fn collection_query(
    request: &CollectRequest,
    statistic_id: u64,
    service_address: &str,
    payload_type: Option<&str>,
    sink_worker: &NetworkAddr,
) -> Result<String, GenerateError> {
    let (source, field) = collected_field(request)?;
    let build = build_expression(request, statistic_id, field)?;
    let window = window_clause(request.window, &request.time_characteristic);
    let inner = format!("SELECT {build} FROM {source} {window}");

    if sends_never(&request.condition) {
        return Ok(format!(
            "{inner} INTO Void({})",
            on_worker("SINK", sink_worker)
        ));
    }

    let aggregation = aggregation(request.metric)?;
    let payload_type = payload_type.ok_or_else(|| {
        GenerateError::NotImplemented(format!(
            "reporting a {:?} statistic needs the declared type of {source}.{field}, which this deployment cannot resolve",
            request.metric
        ))
    })?;
    let probe = format!(
        "SELECT {aggregation}_PROBE({statistic_id}, {VALUE}, {payload_type}) FROM ({inner})"
    );
    let filter = if sends_always(&request.condition) {
        String::new()
    } else {
        format!(" WHERE {}", request.condition)
    };

    Ok(format!(
        "SELECT {STATISTIC_ID}, {START_TS}, {END_TS}, {VALUE} FROM ({probe}){filter} {}",
        grpc_sink(service_address, 0, sink_worker)?
    ))
}

pub fn probe_source(port: u16, worker: &NetworkAddr) -> String {
    format!(
        "Grpc('{port}' AS \"SOURCE\".GRPC_PORT, 'CSV' AS INPUT_FORMATTER.\"TYPE\", \
         SCHEMA({STATISTIC_ID} UINT64 NOT NULL, {START_TS} UINT64 NOT NULL, {END_TS} UINT64 NOT NULL) AS \"SOURCE\".\"SCHEMA\", {})",
        on_worker("SOURCE", worker)
    )
}

/// The query that reads one statistic back out of the store of `worker`, the worker its build wrote it to.
/// Its source and sink are both placed on that worker, which leaves the placement no other worker for the read.
pub fn probe_query(
    statistic_id: u64,
    aggregation: &str,
    payload: &[(String, String)],
    source_port: u16,
    service_address: &str,
    probe_id: u64,
    worker: &NetworkAddr,
) -> Result<String, GenerateError> {
    let declared = payload
        .iter()
        .map(|(name, type_name)| format!("{name}, {type_name}"))
        .collect::<Vec<_>>()
        .join(", ");

    Ok(format!(
        "SELECT {STATISTIC_ID}, {START_TS}, {END_TS}, {VALUE} FROM (\
         SELECT {aggregation}_PROBE_RANGE({statistic_id}, {declared}) FROM (SELECT * FROM {})) {}",
        probe_source(source_port, worker),
        grpc_sink(service_address, probe_id, worker)?
    ))
}
