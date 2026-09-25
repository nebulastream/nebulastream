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

use model::worker::endpoint::NetworkAddr;
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Metric {
    Cardinality,
    MinVal,
    MaxVal,
    Rate,
    Average,
    Selectivity,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum CollectionDomain {
    Data {
        logical_source_name: String,
        field_name: String,
    },
    Workload {
        query_id: u64,
        operator_id: u64,
        field_name: String,
    },
    Infrastructure {
        host_id: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Key {
    pub metric: Metric,
    pub domain: CollectionDomain,
    pub window_size_ms: u64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Report {
    pub statistic_id: u64,
    pub start_ts: u64,
    pub end_ts: u64,
    pub value: f64,
    pub probe_id: u64,
}

pub type Trigger = async_channel::Sender<Report>;

/// A deployed statistic: the query building it, its id, and the worker whose statistic store holds it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Registered {
    pub query_id: u64,
    pub statistic_id: u64,
    pub worker: NetworkAddr,
}

pub struct Entry {
    pub registered: Registered,
    pub triggers: Vec<Trigger>,
}

#[derive(Default)]
pub struct Registry {
    entries: Mutex<HashMap<Key, Entry>>,
}

impl Registry {
    pub fn find(&self, key: &Key) -> Option<Registered> {
        let entries = self.entries.lock().expect("statistic registry poisoned");
        entries.get(key).map(|entry| entry.registered.clone())
    }

    pub fn register(&self, key: Key, registered: Registered, trigger: Option<Trigger>) {
        let mut entries = self.entries.lock().expect("statistic registry poisoned");
        entries.insert(
            key,
            Entry {
                registered,
                triggers: trigger.into_iter().collect(),
            },
        );
    }

    pub fn add_trigger(&self, key: &Key, trigger: Trigger) -> bool {
        let mut entries = self.entries.lock().expect("statistic registry poisoned");
        match entries.get_mut(key) {
            Some(entry) => {
                entry.triggers.push(trigger);
                true
            }
            None => false,
        }
    }

    pub fn deregister(&self, key: &Key) -> bool {
        let mut entries = self.entries.lock().expect("statistic registry poisoned");
        entries.remove(key).is_some()
    }

    pub fn len(&self) -> usize {
        self.entries
            .lock()
            .expect("statistic registry poisoned")
            .len()
    }

    pub fn dispatch(&self, report: Report) {
        let mut entries = self.entries.lock().expect("statistic registry poisoned");
        for entry in entries.values_mut() {
            if entry.registered.statistic_id != report.statistic_id {
                continue;
            }
            entry
                .triggers
                .retain(|trigger| trigger.try_send(report).is_ok());
        }
    }
}
