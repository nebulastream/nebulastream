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

//! Driver workload that waits on queries while the other workloads change them, and checks each reply against the ledger.

use crate::harness::Harness;
use crate::ledger::{Ledger, Outcome};
use crate::workload::{Seconds, pick_offsets};
use futures::future::join_all;
use madsim::rand::thread_rng;
use serde::Deserialize;
use std::cell::RefCell;
use std::time::Duration;
use tokio::time::Instant;
use tracing::info;

/// Each watch starts at its own time in the window and waits up to its own timeout, so watches overlap.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WatchWorkload {
    #[serde(default)]
    pub begin: u64,
    pub end: u64,
    pub num_ops: usize,
    pub timeout: Seconds,
    /// The trial fails if fewer watches than this were released before their timeout.
    /// A watch that matches no query does not count.
    #[serde(default)]
    pub min_released: usize,
    /// The trial fails if fewer watches than this expired.
    #[serde(default)]
    pub min_expired: usize,
}

impl WatchWorkload {
    pub async fn run(&self, ledger: &RefCell<Ledger>, harness: &Harness) {
        info!(
            "Watch: running {} watches between ({}s..{}s) with a timeout of {:?}",
            self.num_ops, self.begin, self.end, self.timeout
        );

        tokio::time::sleep(Duration::from_secs(self.begin)).await;

        let window_start = Instant::now();
        let watches = pick_offsets(self.num_ops, self.end - self.begin)
            .into_iter()
            .map(|offset| async move {
                tokio::time::sleep_until(window_start + offset).await;
                let timeout = self.timeout.draw(&mut thread_rng());
                let (read, wait, sent) = ledger.borrow().plan_watch(timeout);
                let reply = harness.watch(read, wait).await;
                ledger.borrow().record_watch(sent, reply)
            });
        let outcomes = join_all(watches).await;

        let count = |outcome| outcomes.iter().filter(|&&o| o == outcome).count();
        let (released, expired) = (count(Outcome::Released), count(Outcome::Expired));
        assert!(
            released >= self.min_released && expired >= self.min_expired,
            "Watch: {released} watches were released and {expired} expired, expect at least {} and {}",
            self.min_released,
            self.min_expired
        );
    }
}
