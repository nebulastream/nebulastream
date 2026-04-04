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

use crate::query;
use crate::query::fragment;
use crate::request::StatementResponse;
use crate::sink;
use crate::source::{logical_source, physical_source};
use crate::worker;
use sea_orm::Iden;
use std::fmt;

struct Table {
    headers: Vec<String>,
    rows: Vec<Vec<String>>,
}

impl Table {
    fn new<I: IntoIterator<Item = impl Iden>>(columns: I) -> Self {
        Self {
            headers: columns.into_iter().map(|c| c.to_string()).collect(),
            rows: Vec::new(),
        }
    }

    fn row(&mut self, cells: Vec<String>) {
        debug_assert_eq!(cells.len(), self.headers.len());
        self.rows.push(cells);
    }
}

impl fmt::Display for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut widths: Vec<usize> = self.headers.iter().map(|h| h.len()).collect();
        for row in &self.rows {
            for (i, cell) in row.iter().enumerate() {
                widths[i] = widths[i].max(cell.len());
            }
        }

        // Border line: +--------+--------+
        let border: String = widths
            .iter()
            .map(|w| format!("+{}", "-".repeat(w + 2)))
            .collect::<String>()
            + "+";

        // Top border
        writeln!(f, "{border}")?;

        // Header
        write!(f, "|")?;
        for (i, header) in self.headers.iter().enumerate() {
            write!(f, " {:<width$} |", header, width = widths[i])?;
        }
        writeln!(f)?;

        // Separator
        writeln!(f, "{border}")?;

        // Rows
        for row in &self.rows {
            write!(f, "|")?;
            for (i, cell) in row.iter().enumerate() {
                write!(f, " {:<width$} |", cell, width = widths[i])?;
            }
            writeln!(f)?;
        }

        // Bottom border
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

fn opt_ts(v: &Option<chrono::DateTime<chrono::Local>>) -> String {
    match v {
        Some(ts) => ts.format("%Y-%m-%d %H:%M:%S%.3f").to_string(),
        None => String::new(),
    }
}

fn logical_source_table(sources: &[logical_source::Model]) -> Table {
    use logical_source::Column::*;
    let mut t = Table::new([Name, Schema]);
    for source in sources {
        t.row(vec![source.name.clone(), json_str(&source.schema)]);
    }
    t
}

fn physical_source_table(sources: &[physical_source::Model]) -> Table {
    use physical_source::Column::*;
    let mut t = Table::new([Id, LogicalSource, HostAddr, SourceType, SourceConfig, ParserConfig, Kind]);
    for source in sources {
        t.row(vec![
            source.id.to_string(),
            source.logical_source.clone().unwrap_or_default(),
            source.host_addr.as_ref().map(|a| a.to_string()).unwrap_or_default(),
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
            sink.host_addr.as_ref().map(|a| a.to_string()).unwrap_or_default(),
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
    let mut t = Table::new([Id, Name, CurrentState, DesiredState, StartTimestamp, StopTimestamp, StopMode, Error]);
    for query in queries {
        t.row(vec![
            query.id.to_string(),
            query.name.clone().unwrap_or_default(),
            query.current_state.to_string(),
            query.desired_state.to_string(),
            opt_ts(&query.start_timestamp),
            opt_ts(&query.stop_timestamp),
            opt(&query.stop_mode),
            opt(&query.error),
        ]);
    }
    t
}

fn worker_table(workers: &[worker::Model]) -> Table {
    use worker::Column::*;
    let mut t = Table::new([HostAddr, DataAddr, MaxOperators, Config, CurrentState, DesiredState]);
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

fn fragment_table(fragments: &[fragment::Model]) -> Table {
    use fragment::Column::*;
    let mut t = Table::new([Id, QueryId, HostAddr, NumOperators, HasSource, CurrentState, StartTimestamp, StopTimestamp, Error]);
    for fragment in fragments {
        t.row(vec![
            fragment.id.to_string(),
            fragment.query_id.to_string(),
            fragment.host_addr.to_string(),
            fragment.num_operators.to_string(),
            fragment.has_source.to_string(),
            fragment.current_state.to_string(),
            opt_ts(&fragment.start_timestamp),
            opt_ts(&fragment.stop_timestamp),
            opt(&fragment.error),
        ]);
    }
    t
}

impl fmt::Display for StatementResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CreatedLogicalSource(m) => write!(f, "{}", logical_source_table(&[m.clone()])),
            Self::CreatedPhysicalSource(m) => {
                write!(f, "{}", physical_source_table(&[m.clone()]))
            }
            Self::CreatedSink(m) => write!(f, "{}", sink_table(&[m.clone()])),
            Self::CreatedQuery(m) => write!(f, "{}", query_table(&[m.clone()])),
            Self::CreatedWorker(m) => write!(f, "{}", worker_table(&[m.clone()])),
            Self::DroppedLogicalSources(v) => write!(f, "{}", logical_source_table(v)),
            Self::DroppedPhysicalSources(v) => write!(f, "{}", physical_source_table(v)),
            Self::DroppedSinks(v) => write!(f, "{}", sink_table(v)),
            Self::DroppedQueries(v) => write!(f, "{}", query_table(v)),
            Self::DroppedWorker(opt) => match opt {
                Some(m) => write!(f, "{}", worker_table(&[m.clone()])),
                None => write!(f, "(no matching worker)"),
            },
            Self::LogicalSource(v) => write!(f, "{}", logical_source_table(v)),
            Self::PhysicalSources(v) => write!(f, "{}", physical_source_table(v)),
            Self::Sinks(v) => write!(f, "{}", sink_table(v)),
            Self::ExplainedQuery(s) => write!(f, "{s}"),
            Self::Queries(v) => {
                let queries: Vec<_> = v.iter().map(|(q, _)| q.clone()).collect();
                write!(f, "{}", query_table(&queries))?;
                for (q, fragments) in v {
                    if !fragments.is_empty() {
                        write!(
                            f,
                            "\nFragments for query {}:\n{}",
                            q.id,
                            fragment_table(fragments)
                        )?;
                    }
                }
                Ok(())
            }
            Self::Workers(v) => write!(f, "{}", worker_table(v)),
        }
    }
}
