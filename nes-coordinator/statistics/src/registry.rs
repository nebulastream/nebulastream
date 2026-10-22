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

use crate::query_gen::QueryShape;
use crate::service::StatisticError;
use futures::future::{BoxFuture, Shared};
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

/// A report as a watcher receives it, with how many reports it missed right before this one because it read too slowly.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Watched {
    pub report: Report,
    pub dropped_before: u64,
}

pub type Trigger = async_channel::Sender<Watched>;

/// A trigger and how many reports did not fit into it since the last one that did.
struct Attached {
    trigger: Trigger,
    dropped: u64,
}

/// What every collect for a deploying statistic awaits: the registered statistic, or why its deployment failed.
pub type Outcome = Shared<BoxFuture<'static, Result<Registered, StatisticError>>>;

/// A deployed statistic: the query building it, its id, the worker whose statistic store holds it, and the shape the
/// query was built with.
#[derive(Clone, Debug)]
pub struct Registered {
    pub query_id: u64,
    pub statistic_id: u64,
    pub worker: NetworkAddr,
    pub shape: QueryShape,
}

/// A statistic whose query is still being deployed. It holds its key, so a concurrent collect waits for its outcome
/// rather than deploying a query of its own.
#[derive(Clone)]
pub struct Deploying {
    pub statistic_id: u64,
    pub shape: QueryShape,
    pub outcome: Outcome,
}

/// The statistic holding a key.
#[derive(Clone)]
pub enum Slot {
    Deploying(Deploying),
    Registered(Registered),
    /// Being deregistered: its query is being stopped, and the entry goes once it has.
    Stopping(Registered),
}

impl Slot {
    pub fn statistic_id(&self) -> u64 {
        match self {
            Slot::Deploying(deploying) => deploying.statistic_id,
            Slot::Registered(registered) | Slot::Stopping(registered) => registered.statistic_id,
        }
    }

    pub fn shape(&self) -> &QueryShape {
        match self {
            Slot::Deploying(deploying) => &deploying.shape,
            Slot::Registered(registered) | Slot::Stopping(registered) => &registered.shape,
        }
    }
}

struct Entry {
    slot: Slot,
    triggers: Vec<Attached>,
}

pub enum Reservation {
    /// The key was free and is now held by the deployment `start` began.
    Reserved(Deploying),
    /// A statistic already holds the key.
    Taken(Slot),
}

pub enum Deregistration {
    /// No statistic holds the key.
    Unknown,
    /// The statistic was still deploying and is forgotten; its deployment stops the query once it is up.
    Forgotten,
    /// The statistic is now stopping, and its query has to be stopped.
    Stop(Registered),
    /// The statistic is already being deregistered.
    AlreadyStopping { statistic_id: u64 },
}

#[derive(Default)]
pub struct Registry {
    entries: Mutex<HashMap<Key, Entry>>,
}

impl Registry {
    /// The statistic registered under `key`, unless it is still deploying or being deregistered.
    pub fn find(&self, key: &Key) -> Option<Registered> {
        let entries = self.entries.lock().expect("statistic registry poisoned");
        match entries.get(key).map(|entry| &entry.slot) {
            Some(Slot::Registered(registered)) => Some(registered.clone()),
            _ => None,
        }
    }

    /// Holds a free `key` for the deployment `start` begins, or returns the statistic already holding it. `start` runs
    /// under the registry's lock, so it must not block.
    pub fn reserve(&self, key: &Key, start: impl FnOnce() -> Deploying) -> Reservation {
        let mut entries = self.entries.lock().expect("statistic registry poisoned");
        if let Some(entry) = entries.get(key) {
            return Reservation::Taken(entry.slot.clone());
        }
        let deploying = start();
        entries.insert(
            key.clone(),
            Entry {
                slot: Slot::Deploying(deploying.clone()),
                triggers: Vec::new(),
            },
        );
        Reservation::Reserved(deploying)
    }

    /// Registers a deployed statistic in place of its reservation. False if the reservation is gone because the key
    /// was deregistered meanwhile, so that nothing holds the query any more.
    pub fn finish(&self, key: &Key, registered: Registered) -> bool {
        let mut entries = self.entries.lock().expect("statistic registry poisoned");
        match entries.get_mut(key) {
            Some(entry) if is_deploying(&entry.slot, registered.statistic_id) => {
                entry.slot = Slot::Registered(registered);
                true
            }
            _ => false,
        }
    }

    /// Frees the key a deployment holds if it ended without registering its statistic; does nothing once it has.
    pub fn abandon(&self, key: &Key, statistic_id: u64) {
        let mut entries = self.entries.lock().expect("statistic registry poisoned");
        if entries
            .get(key)
            .is_some_and(|entry| is_deploying(&entry.slot, statistic_id))
        {
            entries.remove(key);
        }
    }

    /// Sends the reports of the statistic under `key` to `trigger`. Refused if no statistic holds the key, it is being
    /// deregistered, or its query reports nothing.
    pub fn add_trigger(&self, key: &Key, trigger: Trigger) -> Result<(), StatisticError> {
        let mut entries = self.entries.lock().expect("statistic registry poisoned");
        let entry = entries.get_mut(key).ok_or(StatisticError::UnknownKey)?;
        if matches!(entry.slot, Slot::Stopping(_)) {
            return Err(StatisticError::Deregistering {
                statistic_id: entry.slot.statistic_id(),
            });
        }
        if !entry.slot.shape().reports() {
            return Err(StatisticError::NeverReports);
        }
        entry.triggers.push(Attached {
            trigger,
            dropped: 0,
        });
        Ok(())
    }

    /// Starts deregistering the statistic under `key`.
    pub fn begin_deregister(&self, key: &Key) -> Deregistration {
        let mut entries = self.entries.lock().expect("statistic registry poisoned");
        let Some(entry) = entries.get_mut(key) else {
            return Deregistration::Unknown;
        };
        match &entry.slot {
            Slot::Deploying(_) => {
                entries.remove(key);
                Deregistration::Forgotten
            }
            Slot::Registered(registered) => {
                let registered = registered.clone();
                entry.slot = Slot::Stopping(registered.clone());
                Deregistration::Stop(registered)
            }
            Slot::Stopping(registered) => Deregistration::AlreadyStopping {
                statistic_id: registered.statistic_id,
            },
        }
    }

    /// Ends deregistering the statistic `statistic_id`: forgets it once its query stopped, or registers it again.
    pub fn end_deregister(&self, key: &Key, statistic_id: u64, stopped: bool) {
        let mut entries = self.entries.lock().expect("statistic registry poisoned");
        let Some(entry) = entries.get_mut(key) else {
            return;
        };
        let Slot::Stopping(registered) = &entry.slot else {
            return;
        };
        if registered.statistic_id != statistic_id {
            return;
        }
        if stopped {
            entries.remove(key);
        } else {
            entry.slot = Slot::Registered(registered.clone());
        }
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
            if entry.slot.statistic_id() != report.statistic_id {
                continue;
            }
            entry.triggers.retain_mut(|attached| {
                let watched = Watched {
                    report,
                    dropped_before: attached.dropped,
                };
                match attached.trigger.try_send(watched) {
                    Ok(()) => {
                        attached.dropped = 0;
                        true
                    }
                    // A watcher reading too slowly misses this report rather than its watch, and learns of the gap
                    // with the next report that fits.
                    Err(async_channel::TrySendError::Full(_)) => {
                        attached.dropped = attached.dropped.saturating_add(1);
                        true
                    }
                    Err(async_channel::TrySendError::Closed(_)) => false,
                }
            });
        }
    }
}

fn is_deploying(slot: &Slot, statistic_id: u64) -> bool {
    matches!(slot, Slot::Deploying(deploying) if deploying.statistic_id == statistic_id)
}
