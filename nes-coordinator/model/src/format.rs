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

//! Renders catalog results as fixed-width ASCII tables for the CLI's
//! human-readable output.

use crate::ml_model;
use crate::query;
use crate::query::query_fragment;
use crate::sink;
use crate::source::{logical, physical};
use crate::statement::StatementResult;
use crate::worker;
use sea_orm::Iden;
use std::fmt;

/// Minimal fixed-width ASCII table renderer for the CLI's human-readable
/// output. Kept small on purpose rather than pulling in a table-formatting
/// dependency.
struct Table {
    headers: Vec<String>,
    rows: Vec<Vec<String>>,
}

impl Table {
    fn new<I: IntoIterator<Item = impl Iden>>(columns: I) -> Self {
        Self::with_headers(columns.into_iter().map(|c| c.to_string()))
    }

    // For a result that is not a row of an entity and so has no columns to name itself with.
    fn with_headers<I: IntoIterator<Item = impl Into<String>>>(headers: I) -> Self {
        Self {
            headers: headers.into_iter().map(Into::into).collect(),
            rows: Vec::new(),
        }
    }

    fn row(&mut self, mut cells: Vec<String>) {
        // A mismatch means a table function fell out of sync with its column
        // list. Flag it in debug builds, but resize rather than let the
        // renderer index out of bounds and crash the CLI at runtime.
        debug_assert_eq!(cells.len(), self.headers.len());
        cells.resize(self.headers.len(), String::new());
        self.rows.push(cells);
    }
}

impl fmt::Display for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Width is measured in characters, not bytes, so multibyte UTF-8
        // content still lines up with the borders.
        let mut widths: Vec<usize> = self.headers.iter().map(|h| h.chars().count()).collect();
        for row in &self.rows {
            for (i, cell) in row.iter().enumerate() {
                widths[i] = widths[i].max(cell.chars().count());
            }
        }

        // Border line: +--------+--------+
        let border: String = widths
            .iter()
            .map(|w| format!("+{}", "-".repeat(w + 2)))
            .collect::<String>()
            + "+";

        writeln!(f, "{border}")?;

        write!(f, "|")?;
        for (i, header) in self.headers.iter().enumerate() {
            write!(f, " {:<width$} |", header, width = widths[i])?;
        }
        writeln!(f)?;

        writeln!(f, "{border}")?;

        for row in &self.rows {
            write!(f, "|")?;
            for (i, cell) in row.iter().enumerate() {
                write!(f, " {:<width$} |", cell, width = widths[i])?;
            }
            writeln!(f)?;
        }

        write!(f, "{border}")?;
        Ok(())
    }
}

fn json_str(v: &impl serde::Serialize) -> String {
    serde_json::to_string(v).unwrap_or_default()
}

fn opt<T: fmt::Display>(v: &Option<T>) -> String {
    match v {
        Some(v) => v.to_string(),
        None => String::new(),
    }
}

fn opt_ts(v: &Option<chrono::DateTime<chrono::Utc>>) -> String {
    match v {
        // Stored as UTC; render in the local time of whoever runs the
        // coordinator so the CLI output reads as wall-clock time.
        Some(ts) => ts
            .with_timezone(&chrono::Local)
            .format("%Y-%m-%d %H:%M:%S%.3f")
            .to_string(),
        None => String::new(),
    }
}

fn logical_table(sources: &[logical::Model]) -> Table {
    use logical::Column::*;
    let mut t = Table::new([Name, Schema]);
    for source in sources {
        t.row(vec![source.name.clone(), json_str(&source.schema)]);
    }
    t
}

fn physical_table(sources: &[physical::Model]) -> Table {
    use physical::Column::*;
    let mut t = Table::new([
        Id,
        LogicalSource,
        HostAddr,
        SourceType,
        SourceConfig,
        ParserConfig,
        Kind,
    ]);
    for source in sources {
        t.row(vec![
            source.id.to_string(),
            source.logical_source.clone().unwrap_or_default(),
            source.host_addr.to_string(),
            source.source_type.to_string(),
            json_str(&source.source_config),
            json_str(&source.parser_config),
            source.kind.to_string(),
        ]);
    }
    t
}

fn sink_table(sinks: &[sink::Model]) -> Table {
    use sink::Column::*;
    let mut t = Table::new([Id, Name, HostAddr, SinkType, Schema, Config, Kind]);
    for sink in sinks {
        t.row(vec![
            sink.id.to_string(),
            sink.name.clone().unwrap_or_default(),
            sink.host_addr.to_string(),
            sink.sink_type.to_string(),
            json_str(&sink.schema),
            json_str(&sink.config),
            sink.kind.to_string(),
        ]);
    }
    t
}

fn query_table(queries: &[query::Model]) -> Table {
    use query::Column::*;
    let mut t = Table::new([Id, Name, State, StartTimestamp, StopTimestamp, Error]);
    for query in queries {
        t.row(vec![
            query.id.to_string(),
            query.name.clone().unwrap_or_default(),
            query.state.to_string(),
            opt_ts(&query.start_timestamp),
            opt_ts(&query.stop_timestamp),
            opt(&query.error),
        ]);
    }
    t
}

fn worker_version_table(versions: &[worker::WorkerVersion]) -> Table {
    let mut t = Table::with_headers(["worker", "version", "error"]);
    for reported in versions {
        t.row(vec![
            reported.worker.to_string(),
            opt(&reported.version),
            opt(&reported.error),
        ]);
    }
    t
}

fn worker_table(workers: &[worker::Model]) -> Table {
    use worker::Column::*;
    let mut t = Table::new([
        HostAddr,
        DataAddr,
        MaxOperators,
        Config,
        CurrentState,
        DesiredState,
    ]);
    for worker in workers {
        t.row(vec![
            worker.host_addr.to_string(),
            worker.data_addr.to_string(),
            opt(&worker.max_operators),
            json_str(&worker.config),
            worker.current_state.to_string(),
            worker.desired_state.to_string(),
        ]);
    }
    t
}

fn ml_model_table(models: &[ml_model::Model]) -> Table {
    use ml_model::Column::*;
    let mut t = Table::new([Name, Path, InputSchema, OutputSchema]);
    for model in models {
        t.row(vec![
            model.name.clone(),
            model.path.clone(),
            json_str(&model.input_schema),
            json_str(&model.output_schema),
        ]);
    }
    t
}

fn fragment_table(fragments: &[query_fragment::Model]) -> Table {
    use query_fragment::Column::*;
    let mut t = Table::new([
        Id,
        QueryId,
        HostAddr,
        NumOperators,
        HasSource,
        CurrentState,
        DesiredState,
        StartTimestamp,
        StopTimestamp,
        Error,
    ]);
    for fragment in fragments {
        t.row(vec![
            fragment.id.to_string(),
            fragment.query_id.to_string(),
            fragment.host_addr.to_string(),
            fragment.num_operators.to_string(),
            fragment.has_source.to_string(),
            fragment.current_state.to_string(),
            fragment.desired_state.to_string(),
            opt_ts(&fragment.start_timestamp),
            opt_ts(&fragment.stop_timestamp),
            opt(&fragment.error),
        ]);
    }
    t
}

impl fmt::Display for StatementResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CreatedLogicalSource(model) => {
                write!(f, "{}", logical_table(std::slice::from_ref(model)))
            }
            Self::CreatedPhysicalSource(model) => {
                write!(f, "{}", physical_table(std::slice::from_ref(model)))
            }
            Self::CreatedSink(model) => write!(f, "{}", sink_table(std::slice::from_ref(model))),
            Self::CreatedQuery(model, _fragments) => {
                write!(f, "{}", query_table(std::slice::from_ref(model)))?;
                Ok(())
            }
            Self::CreatedWorker(model) => {
                write!(f, "{}", worker_table(std::slice::from_ref(model)))
            }
            Self::DroppedLogicalSources(v) => write!(f, "{}", logical_table(v)),
            Self::DroppedPhysicalSources(v) => write!(f, "{}", physical_table(v)),
            Self::DroppedSinks(v) => write!(f, "{}", sink_table(v)),
            Self::DroppedQueries(v) => write!(f, "{}", query_table(v)),
            Self::DroppedWorker(opt) => match opt {
                Some(worker) => write!(f, "{}", worker_table(std::slice::from_ref(worker))),
                None => write!(f, "(no matching worker)"),
            },
            Self::LogicalSource(v) => write!(f, "{}", logical_table(v)),
            Self::PhysicalSources(v) => write!(f, "{}", physical_table(v)),
            Self::Sinks(v) => write!(f, "{}", sink_table(v)),
            Self::ExplainedQuery(s) => write!(f, "{s}"),
            Self::Queries(v) => {
                let mut queries: Vec<_> = v.iter().map(|(query, _)| query.clone()).collect();
                queries.sort_by_key(|query| query.id);
                write!(f, "{}", query_table(&queries))?;
                Ok(())
            }
            Self::Workers(v) => write!(f, "{}", worker_table(v)),
            Self::WorkerVersions(v) => write!(f, "{}", worker_version_table(v)),
            Self::WorkerStatus(worker, fragments) => {
                writeln!(f, "{}", worker_table(std::slice::from_ref(worker)))?;
                write!(f, "{}", fragment_table(fragments))
            }
            Self::CreatedMlModel(model) => {
                write!(f, "{}", ml_model_table(std::slice::from_ref(model)))
            }
            Self::DroppedMlModels(v) => write!(f, "{}", ml_model_table(v)),
            Self::MlModels(v) => write!(f, "{}", ml_model_table(v)),
        }
    }
}
