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

use tokio::sync::watch;

// Communication primitive to subscribe/notify services on changes.
// Uses a watch channel to indicate that something changed, the subscriber needs to poll 
// to find out about the actual change.
// The advantage of a watch channel is constant space, and it enables a hybrid approach to bridge
// between edge-triggered (consuming change events directly) and level-triggered (periodic polling).
pub(crate) struct NotificationChannel {
    // Notifies subscribers when the intent changes (something should happen)
    intent_tx: watch::Sender<()>,
    intent_rx: watch::Receiver<()>,
    // Notifies subscribers when the state changes (something has happened)
    state_tx: watch::Sender<()>,
    state_rx: watch::Receiver<()>,
}

impl NotificationChannel {
    pub(crate) fn new() -> Self {
        let (intent_tx, intent_rx) = watch::channel(());
        let (state_tx, state_rx) = watch::channel(());
        Self {
            intent_tx,
            intent_rx,
            state_tx,
            state_rx,
        }
    }

    // Send a notification to the receiver of the intention (e.g., a query should be started/dropped)
    pub(crate) fn notify_intent(&self) {
        self.intent_tx
            .send(())
            .expect("receiver is owned by this object and should therefore be alive");
    }

    // Send a notification to the receiver of state changes (e.g., a query reached some target state)
    pub(crate) fn notify_state(&self) {
        self.state_tx
            .send(())
            .expect("receiver is owned by this object and should therefore be alive");
    }

    // Subscribe to intent/state notifications
    pub(crate) fn subscribe_intent(&self) -> watch::Receiver<()> {
        self.intent_rx.clone()
    }
    pub(crate) fn subscribe_state(&self) -> watch::Receiver<()> {
        self.state_rx.clone()
    }
}

// To be implemented by catalogs to inform anyone interested when an intent/state change
// is persisted in the database.
pub trait Reconcilable {
    type Model;
    fn subscribe_intent(&self) -> watch::Receiver<()>;
    fn subscribe_state(&self) -> watch::Receiver<()>;
    fn get_mismatch(&self) -> impl Future<Output = anyhow::Result<Vec<Self::Model>>>;
}
