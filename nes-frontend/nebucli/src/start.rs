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
use crate::DEFAULT_REQUEST_TIMEOUT;
use anyhow::{Context, Result, bail};
use coordinator_bridge::Coordinator;
use model::error::{CodedError, ErrorCode};
use model::identifier::QueryId;
use model::request::Statement;
use model::sink::CreateSink;
use model::source::logical::CreateLogicalSource;
use model::source::physical::CreatePhysicalSource;
use model::statement::{ParsedStatement, StatementResult};
use model::worker::CreateWorker;
use model::worker::endpoint::NetworkAddr;
use serde::Deserialize;
use std::collections::HashSet;
use std::io::Read;
use tracing::error;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Setup {
    #[serde(default)]
    query: Query,
    #[serde(default)]
    sinks: Vec<Sink>,
    #[serde(default, alias = "logical")]
    logical_sources: Vec<LogicalSource>,
    #[serde(default, alias = "physical")]
    physical_sources: Vec<CreatePhysicalSource>,
    #[serde(default)]
    models: Vec<Model>,
    workers: Vec<Worker>,
    #[serde(default)]
    optimizer: serde_yaml::Value,
}

/// Field data types, mirroring the C++ `DataType::Type` enum.
/// The catalog stores a type as its numeric discriminant,
/// so these variants must stay in the same order.
#[derive(Deserialize, Clone, Copy)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[repr(u8)]
enum DataTypeKind {
    Uint8,
    Uint16,
    Uint32,
    Uint64,
    Int8,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Boolean,
    Char,
    Undefined,
    Varsized,
}

impl DataTypeKind {
    /// The type as it is written in SQL.
    const fn sql_name(self) -> &'static str {
        match self {
            Self::Uint8 => "UINT8",
            Self::Uint16 => "UINT16",
            Self::Uint32 => "UINT32",
            Self::Uint64 => "UINT64",
            Self::Int8 => "INT8",
            Self::Int16 => "INT16",
            Self::Int32 => "INT32",
            Self::Int64 => "INT64",
            Self::Float32 => "FLOAT32",
            Self::Float64 => "FLOAT64",
            Self::Boolean => "BOOLEAN",
            Self::Char => "CHAR",
            Self::Undefined => "UNDEFINED",
            Self::Varsized => "VARSIZED",
        }
    }
}

/// YAML-friendly schema field. Converted to the C++ JSON format
/// `[{"name": "...", "dataType": [<type-discriminant>, nullable]}]` on the way in.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SchemaField {
    name: String,
    #[serde(rename = "type")]
    data_type: DataTypeKind,
    #[serde(default)]
    nullable: bool,
}

/// Normalize an identifier to uppercase, matching the C++ SQL parser behavior
/// for unquoted identifiers. Quoted identifiers preserve their casing.
fn bind_identifier(name: &str) -> String {
    let quoted = |quote: char| name.starts_with(quote) && name.ends_with(quote) && name.len() > 2;
    if quoted('`') || quoted('"') {
        name[1..name.len() - 1].to_string()
    } else {
        name.to_uppercase()
    }
}

fn convert_schema(fields: Vec<SchemaField>) -> serde_json::Value {
    let fields: Vec<_> = fields
        .into_iter()
        .map(|f| serde_json::json!({"name": bind_identifier(&f.name), "dataType": [f.data_type as u8, f.nullable]}))
        .collect();
    serde_json::Value::Array(fields)
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
    #[serde(alias = "host")]
    host_addr: NetworkAddr,
    #[serde(alias = "type")]
    sink_type: String,
    schema: Vec<SchemaField>,
    #[serde(default)]
    config: serde_json::Value,
    #[serde(default)]
    parser_config: serde_json::Value,
}

/// An ML model definition.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Model {
    name: String,
    path: String,
    input: Vec<SchemaField>,
    output: Vec<SchemaField>,
}

/// A name as it is written in SQL.
/// The setup file also accepts backticks as quotes, and SQL only double quotes.
fn sql_identifier(name: &str) -> String {
    if name.len() > 2 && name.starts_with('`') && name.ends_with('`') {
        format!("\"{}\"", &name[1..name.len() - 1])
    } else {
        name.to_string()
    }
}

impl Model {
    /// The model as a CREATE MODEL statement.
    /// It is sent as SQL so that the planner loads and validates the model file.
    fn into_sql(self) -> String {
        let fields = |fields: &[SchemaField]| {
            fields
                .iter()
                .map(|f| format!("{} {}", sql_identifier(&f.name), f.data_type.sql_name()))
                .collect::<Vec<_>>()
                .join(", ")
        };
        format!(
            "CREATE MODEL {} ('{}') INPUT ({}) OUTPUT ({})",
            sql_identifier(&self.name),
            self.path.replace('\'', "''"),
            fields(&self.input),
            fields(&self.output),
        )
    }
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
    #[serde(alias = "host")]
    host_addr: NetworkAddr,
    #[serde(default, alias = "data_address")]
    data_addr: Option<NetworkAddr>,
    #[serde(default)]
    max_operators: Option<i32>,
    #[serde(default, alias = "downstream")]
    peers: Vec<NetworkAddr>,
    #[serde(default)]
    config: serde_yaml::Value,
}

/// Normalize a config object for the catalog wire format: canonicalize each key to its
/// case-folded spelling (unquoted keys are upper-cased, matching the read path) and coerce
/// every value to a string (the C++ config maps hold string-to-string entries). An absent or
/// non-object config becomes an empty object, since the read path always expects an object.
fn normalize_config(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let map = map
                .into_iter()
                .map(|(k, v)| {
                    let s = match v {
                        serde_json::Value::String(s) => s,
                        other => other.to_string(),
                    };
                    (bind_identifier(&k), serde_json::Value::String(s))
                })
                .collect();
            serde_json::Value::Object(map)
        }
        _ => serde_json::Value::Object(serde_json::Map::new()),
    }
}

/// Key under which a sink's output-formatter options are nested inside its config.
/// The catalog read path splits this key back out into the sink's separate
/// formatter config, so the value must be an object of formatter options.
const OUTPUT_FORMATTER_KEY: &str = "OUTPUT_FORMATTER";

/// Fold a sink's output-formatter options into its config under
/// [`OUTPUT_FORMATTER_KEY`], the way the SQL surface stores them. An empty
/// formatter is left out entirely, so a sink without one is unchanged.
fn merge_format_config(
    config: serde_json::Value,
    parser_config: serde_json::Value,
) -> serde_json::Value {
    let mut config = normalize_config(config);
    let formatter = normalize_config(parser_config);
    if let (serde_json::Value::Object(cfg), serde_json::Value::Object(fmt)) =
        (&mut config, &formatter)
        && !fmt.is_empty()
    {
        cfg.insert(OUTPUT_FORMATTER_KEY.to_string(), formatter.clone());
    }
    config
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
    pub fn optimizer_config_json(&self) -> String {
        if self.optimizer.is_null() {
            return String::new();
        }
        let mut flat = serde_json::Map::new();
        flatten_recursive(&self.optimizer, "optimizer", &mut flat);
        serde_json::to_string(&flat).unwrap_or_default()
    }

    /// The statements that create everything in the setup file, and the queries of the file.
    fn into_statements(self, embedded: bool) -> Result<(Vec<Statement>, Vec<String>)> {
        let mut stmts = Vec::new();

        // Every statement below is created with `if_not_exists`, so a host that is listed twice would not be an error in the catalog.
        let mut hosts = HashSet::new();
        for worker in &self.workers {
            if !hosts.insert(&worker.host_addr) {
                bail!("duplicate worker host '{}'", worker.host_addr);
            }
        }

        for worker in self.workers {
            let data_addr = match worker.data_addr {
                Some(addr) => addr,
                None => NetworkAddr::new(worker.host_addr.host(), worker.host_addr.port() + 1000)?,
            };
            stmts.push(ParsedStatement::CreateWorker(CreateWorker {
                host_addr: worker.host_addr,
                data_addr,
                max_operators: worker.max_operators,
                peers: worker.peers,
                config: if embedded {
                    flatten_cfg(&worker.config)
                } else {
                    serde_json::Value::Null
                },
                if_not_exists: true,
            }));
        }

        for logical_source in self.logical_sources {
            stmts.push(ParsedStatement::CreateLogicalSource(CreateLogicalSource {
                name: bind_identifier(&logical_source.name),
                schema: convert_schema(logical_source.schema),
                if_not_exists: true,
            }));
        }

        for mut physical_source in self.physical_sources {
            physical_source.logical_source = bind_identifier(&physical_source.logical_source);
            physical_source.source_config = normalize_config(physical_source.source_config);
            physical_source.parser_config = normalize_config(physical_source.parser_config);
            physical_source.if_not_exists = true;
            stmts.push(ParsedStatement::CreatePhysicalSource(physical_source));
        }

        for sink in self.sinks {
            let name = bind_identifier(&sink.name);
            // A sink without a name is identified by its numeric id, so a name of only digits is reserved.
            if name.chars().all(|c| c.is_ascii_digit()) {
                bail!("sink name '{name}' is invalid: only-digit names are reserved");
            }
            stmts.push(ParsedStatement::CreateSink(CreateSink {
                name,
                host_addr: sink.host_addr,
                sink_type: sink.sink_type,
                schema: convert_schema(sink.schema),
                config: merge_format_config(sink.config, sink.parser_config),
                if_not_exists: true,
            }));
        }

        let mut stmts: Vec<Statement> = stmts.into_iter().map(Statement::Parsed).collect();
        for model in self.models {
            stmts.push(Statement::Sql(model.into_sql()));
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

/// Sends the setup statements and returns the queries of the setup file.
/// The setup is sent on every invocation, so a model that already exists is not an error.
/// The other statements are created with `if_not_exists`, which SQL cannot express.
pub fn send_setup(handle: &Coordinator, setup: Setup, embedded: bool) -> Result<Vec<String>> {
    let (statements, queries) = setup.into_statements(embedded)?;
    for stmt in statements {
        if let Err(error) = handle.send(stmt) {
            let already_exists = error
                .downcast_ref::<CodedError>()
                .is_some_and(|coded| coded.code == ErrorCode::ModelAlreadyExists);
            if !already_exists {
                return Err(error);
            }
        }
    }
    Ok(queries)
}

pub fn run(
    handle: &Coordinator,
    setup: Setup,
    embedded: bool,
    cli_queries: &[String],
    until_completed: bool,
) -> Result<()> {
    let file_queries = send_setup(handle, setup, embedded)?;

    let queries: &[String] = if cli_queries.is_empty() {
        &file_queries
    } else {
        cli_queries
    };

    if queries.is_empty() {
        bail!("no queries provided (pass as arguments or include in setup file)");
    }

    // A query should reach Running within the request timeout. Running to
    // Completed can take arbitrarily long, so that waits without a deadline.
    let timeout = if until_completed {
        None
    } else {
        Some(DEFAULT_REQUEST_TIMEOUT)
    };

    let results: Vec<_> = std::thread::scope(|scope| {
        queries
            .iter()
            .map(|query| {
                scope.spawn(move || -> Result<QueryId> {
                    let client = handle.client();
                    handle.block_on(async {
                        let id = client.create(query.clone()).await?.query.id;
                        if until_completed {
                            client.await_completed(vec![id], timeout).await?;
                        } else {
                            client.await_running(vec![id], timeout).await?;
                        }
                        Ok(id)
                    })
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|h| h.join().expect("query thread panicked"))
            .collect()
    });

    // The id of each started query is printed on its own line, in the order of the queries.
    let mut errors = Vec::new();
    for result in results {
        match result {
            Ok(id) => println!("{}", *id),
            Err(error) => errors.push(error),
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        for err in &errors {
            error!("{err:#}");
        }
        bail!("{} query/queries failed", errors.len())
    }
}
