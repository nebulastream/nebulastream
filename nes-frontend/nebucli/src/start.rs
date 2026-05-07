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
use crate::send_timed;
use anyhow::{Context, Result, bail};
use coordinator_bridge::CoordinatorHandle;
use model::request::Payload;
use model::sink::CreateSink;
use model::source::logical::CreateLogicalSource;
use model::source::physical::CreatePhysicalSource;
use model::statement::Statement;
use model::statement::StatementResult;
use model::worker::CreateWorker;
use model::worker::endpoint::NetworkAddr;
use serde::Deserialize;
use std::io::Read;
use tracing::error;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Setup {
    #[serde(default)]
    query: Query,
    #[serde(default)]
    sinks: Vec<Sink>,
    #[serde(default)]
    logical_sources: Vec<LogicalSource>,
    #[serde(default)]
    physical_sources: Vec<CreatePhysicalSource>,
    workers: Vec<Worker>,
    #[serde(default)]
    optimizer: serde_yaml::Value,
}

/// YAML-friendly schema field. Converted to the C++ JSON format
/// `{"fields": [{"name": "...", "type": ["TYPE", nullable]}]}` on the way in.
#[derive(Deserialize)]
struct SchemaField {
    name: String,
    #[serde(rename = "type")]
    data_type: String,
    #[serde(default = "ret_true")]
    nullable: bool,
}

fn ret_true() -> bool {
    true
}

/// Normalize an identifier to uppercase, matching the C++ SQL parser behavior
/// for unquoted identifiers. Backtick-quoted identifiers preserve their casing.
fn bind_identifier(name: &str) -> String {
    if name.starts_with('`') && name.ends_with('`') && name.len() > 2 {
        name[1..name.len() - 1].to_string()
    } else {
        name.to_uppercase()
    }
}

fn convert_schema(fields: Vec<SchemaField>) -> serde_json::Value {
    let fields: Vec<_> = fields
        .into_iter()
        .map(|f| serde_json::json!({"name": bind_identifier(&f.name), "type": [f.data_type, f.nullable]}))
        .collect();
    serde_json::json!({ "fields": fields })
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LogicalSource {
    name: String,
    schema: Vec<SchemaField>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Sink {
    name: String,
    host_addr: NetworkAddr,
    sink_type: model::sink::SinkType,
    schema: Vec<SchemaField>,
    #[serde(default)]
    config: serde_json::Value,
    #[serde(default)]
    parser_config: serde_json::Value,
}

#[derive(Deserialize, Default)]
#[serde(untagged)]
enum Query {
    #[default]
    None,
    Single(String),
    Multiple(Vec<String>),
}

impl Query {
    fn into_vec(self) -> Vec<String> {
        match self {
            Self::None => vec![],
            Self::Single(s) => vec![s],
            Self::Multiple(v) => v,
        }
    }
}

/// Worker YAML uses `config` as nested YAML that gets flattened to dot-separated JSON keys.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Worker {
    host_addr: NetworkAddr,
    #[serde(default)]
    data_addr: Option<NetworkAddr>,
    #[serde(default)]
    max_operators: Option<i32>,
    #[serde(default)]
    peers: Vec<NetworkAddr>,
    #[serde(default)]
    config: serde_yaml::Value,
}

/// Coerce all values in a JSON object to strings (the C++ config maps expect `map<string, string>`).
fn stringify_values(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let map = map
                .into_iter()
                .map(|(k, v)| {
                    let s = match v {
                        serde_json::Value::String(s) => s,
                        other => other.to_string(),
                    };
                    (k, serde_json::Value::String(s))
                })
                .collect();
            serde_json::Value::Object(map)
        }
        other => other,
    }
}

fn flatten_cfg(value: &serde_yaml::Value) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    flatten_recursive(value, "", &mut map);
    serde_json::Value::Object(map)
}

fn flatten_recursive(
    value: &serde_yaml::Value,
    prefix: &str,
    out: &mut serde_json::Map<String, serde_json::Value>,
) {
    match value {
        serde_yaml::Value::Mapping(m) => {
            for (k, v) in m {
                let key = k.as_str().unwrap_or_default();
                let full = if prefix.is_empty() {
                    key.to_string()
                } else {
                    format!("{prefix}.{key}")
                };
                flatten_recursive(v, &full, out);
            }
        }
        serde_yaml::Value::String(s) => {
            out.insert(prefix.to_string(), serde_json::Value::String(s.clone()));
        }
        serde_yaml::Value::Number(n) => {
            out.insert(prefix.to_string(), serde_json::Value::String(n.to_string()));
        }
        serde_yaml::Value::Bool(b) => {
            out.insert(prefix.to_string(), serde_json::Value::String(b.to_string()));
        }
        _ => {}
    }
}

impl Setup {
    /// Returns the optimizer config as a flattened dot-separated JSON string
    /// suitable for C++ `BaseConfiguration::overwriteConfigWithCommandLineInput()`.
    pub fn optimizer_config_json(&self) -> String {
        if self.optimizer.is_null() {
            return String::new();
        }
        let flat = flatten_cfg(&self.optimizer);
        serde_json::to_string(&flat).unwrap_or_default()
    }

    fn into_statements(self) -> Result<(Vec<Statement>, Vec<String>)> {
        let mut stmts = Vec::new();

        for worker in self.workers {
            let data_addr = worker.data_addr.unwrap_or_else(|| {
                NetworkAddr::new(worker.host_addr.host.clone(), worker.host_addr.port + 1000)
            });
            stmts.push(Statement::CreateWorker(CreateWorker {
                host_addr: worker.host_addr,
                data_addr,
                max_operators: worker.max_operators,
                peers: worker.peers,
                config: flatten_cfg(&worker.config),
                if_not_exists: true,
            }));
        }

        for logical_source in self.logical_sources {
            stmts.push(Statement::CreateLogicalSource(CreateLogicalSource {
                name: bind_identifier(&logical_source.name),
                schema: convert_schema(logical_source.schema),
                if_not_exists: true,
            }));
        }

        for mut physical_source in self.physical_sources {
            physical_source.logical_source = bind_identifier(&physical_source.logical_source);
            physical_source.source_config = stringify_values(physical_source.source_config);
            physical_source.parser_config = stringify_values(physical_source.parser_config);
            physical_source.if_not_exists = true;
            stmts.push(Statement::CreatePhysicalSource(physical_source));
        }

        for sink in self.sinks {
            stmts.push(Statement::CreateSink(CreateSink {
                name: bind_identifier(&sink.name),
                host_addr: sink.host_addr,
                sink_type: sink.sink_type,
                schema: convert_schema(sink.schema),
                config: stringify_values(sink.config),
                if_not_exists: true,
            }));
        }

        Ok((stmts, self.query.into_vec()))
    }
}

pub fn load_setup_file(path: Option<&str>) -> Result<Setup> {
    let yaml = if let Some(path) = path {
        if path == "-" {
            let mut buf = String::new();
            std::io::stdin().read_to_string(&mut buf)?;
            buf
        } else {
            std::fs::read_to_string(path)
                .with_context(|| format!("cannot read setup file: {path}"))?
        }
    } else if let Ok(path) = std::env::var("NES_SETUP_FILE") {
        std::fs::read_to_string(&path)
            .with_context(|| format!("cannot read setup file from NES_SETUP_FILE: {path}"))?
    } else if std::path::Path::new("setup.yaml").exists() {
        std::fs::read_to_string("setup.yaml")?
    } else if std::path::Path::new("setup.yml").exists() {
        std::fs::read_to_string("setup.yml")?
    } else {
        bail!("no setup file found (tried -s flag, $NES_SETUP_FILE, setup.yaml, setup.yml)");
    };

    serde_yaml::from_str(&yaml).context("failed to parse setup file")
}

pub fn send_setup(handle: &CoordinatorHandle, setup: Setup) -> Result<()> {
    let (statements, _) = setup.into_statements()?;
    for stmt in statements {
        send_timed(handle, Payload::structured(stmt))?;
    }
    Ok(())
}

pub fn run(
    handle: &CoordinatorHandle,
    setup: Setup,
    cli_queries: &[String],
    until_completed: bool,
) -> Result<()> {
    use model::query::query_state::QueryState;

    let (statements, file_queries) = setup.into_statements()?;

    for stmt in statements {
        handle.send(Payload::structured(stmt))?;
    }

    let queries: &[String] = if cli_queries.is_empty() {
        &file_queries
    } else {
        cli_queries
    };

    if queries.is_empty() {
        bail!("no queries provided (pass as arguments or include in setup file)");
    }

    let target = if until_completed {
        QueryState::Completed
    } else {
        QueryState::Running
    };

    let send: fn(&CoordinatorHandle, Payload) -> Result<StatementResult> = if until_completed {
        CoordinatorHandle::send
    } else {
        send_timed
    };

    let errors: Vec<_> = std::thread::scope(|scope| {
        queries
            .iter()
            .map(|query| {
                scope.spawn(|| send(handle, Payload::sql(query.clone()).block_until(target)))
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|h| h.join().expect("query thread panicked"))
            .filter_map(|rsp| rsp.err())
            .collect()
    });

    if errors.is_empty() {
        Ok(())
    } else {
        for err in &errors {
            error!("{err:#}");
        }
        bail!("{} query/queries failed", errors.len())
    }
}
