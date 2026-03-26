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
        let ncols = self.headers.len();
        let mut widths: Vec<usize> = self.headers.iter().map(|h| h.len()).collect();
        for row in &self.rows {
            for (i, cell) in row.iter().enumerate() {
                widths[i] = widths[i].max(cell.len());
            }
        }

        // Header
        for (i, header) in self.headers.iter().enumerate() {
            if i > 0 {
                write!(f, " | ")?;
            }
            write!(f, "{:<width$}", header, width = widths[i])?;
        }
        writeln!(f)?;

        // Separator
        let total: usize = widths.iter().sum::<usize>() + (ncols.saturating_sub(1)) * 3;
        writeln!(f, "{}", "-".repeat(total))?;

        // Rows
        for row in &self.rows {
            for (i, cell) in row.iter().enumerate() {
                if i > 0 {
                    write!(f, " | ")?;
                }
                write!(f, "{:<width$}", cell, width = widths[i])?;
            }
            writeln!(f)?;
        }
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

fn logical_source_table(models: &[logical_source::Model]) -> Table {
    use logical_source::Column::*;
    let mut t = Table::new([Name, Schema]);
    for m in models {
        t.row(vec![m.name.clone(), json_str(&m.schema)]);
    }
    t
}

fn physical_source_table(models: &[physical_source::Model]) -> Table {
    use physical_source::Column::*;
    let mut t = Table::new([Id, LogicalSource, HostAddr, SourceType, SourceConfig, ParserConfig]);
    for m in models {
        t.row(vec![
            m.id.to_string(),
            m.logical_source.clone(),
            m.host_addr.to_string(),
            m.source_type.to_string(),
            json_str(&m.source_config),
            json_str(&m.parser_config),
        ]);
    }
    t
}

fn sink_table(models: &[sink::Model]) -> Table {
    use sink::Column::*;
    let mut t = Table::new([Name, HostAddr, SinkType, Schema, Config]);
    for m in models {
        t.row(vec![
            m.name.clone(),
            m.host_addr.to_string(),
            m.sink_type.to_string(),
            json_str(&m.schema),
            json_str(&m.config),
        ]);
    }
    t
}

fn query_table(models: &[query::Model]) -> Table {
    use query::Column::*;
    let mut t = Table::new([Id, Name, CurrentState, DesiredState]);
    for m in models {
        t.row(vec![
            m.id.to_string(),
            m.name.clone().unwrap_or_default(),
            m.current_state.to_string(),
            m.desired_state.to_string(),
        ]);
    }
    t
}

fn worker_table(models: &[worker::Model]) -> Table {
    use worker::Column::*;
    let mut t = Table::new([HostAddr, DataAddr, Capacity, CurrentState, DesiredState]);
    for m in models {
        t.row(vec![
            m.host_addr.to_string(),
            m.data_addr.to_string(),
            opt(&m.capacity),
            m.current_state.to_string(),
            m.desired_state.to_string(),
        ]);
    }
    t
}

fn fragment_table(models: &[fragment::Model]) -> Table {
    use fragment::Column::*;
    let mut t = Table::new([Id, QueryId, HostAddr, CurrentState]);
    for m in models {
        t.row(vec![
            m.id.to_string(),
            m.query_id.to_string(),
            m.host_addr.to_string(),
            m.current_state.to_string(),
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
                let models: Vec<_> = v.iter().map(|(q, _)| q.clone()).collect();
                write!(f, "{}", query_table(&models))?;
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
