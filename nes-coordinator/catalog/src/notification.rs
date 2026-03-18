use tokio::sync::watch;

pub(crate) struct NotificationChannel {
    // Notifies subscribers when the intent changes (something should happen)
    intent_tx: watch::Sender<()>,
    intent_rx: watch::Receiver<()>,
    // Notifies subscribers when the state changes (something has happened)
    state_tx: watch::Sender<()>,
    state_rx: watch::Receiver<()>,
}

impl Default for NotificationChannel {
    fn default() -> Self {
        Self::new()
    }
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

    pub(crate) fn notify_intent(&self) {
        self.intent_tx
            .send(())
            .expect("receiver is owned and should therefore be alive");
    }

    pub(crate) fn notify_state(&self) {
        self.state_tx
            .send(())
            .expect("receiver is owned and should therefore be alive");
    }

    pub(crate) fn subscribe_intent(&self) -> watch::Receiver<()> {
        self.intent_rx.clone()
    }

    pub(crate) fn subscribe_state(&self) -> watch::Receiver<()> {
        self.state_rx.clone()
    }
}

#[allow(async_fn_in_trait)]
pub trait Reconcilable {
    type Model;
    fn subscribe_intent(&self) -> watch::Receiver<()>;
    fn subscribe_state(&self) -> watch::Receiver<()>;
    async fn get_mismatch(&self) -> anyhow::Result<Vec<Self::Model>>;
}
