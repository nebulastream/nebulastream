// Singleton embedded MQTT broker shared across all TokioMqtt source and sink instances.
//
// The broker starts lazily when the first source or sink registers and shuts down
// when the last one unregisters. If new registrations happen later, the
// broker is restarted. Each source gets its own internal link subscribed
// to exactly its topic — rumqttd's router handles topic filtering.
// Sinks get a LinkTx for publishing to their topic.
//
// Architecture:
//   +--------------------------------------------------+
//   |                 rumqttd Broker                    |
//   |  (TCP listener thread + router, started once)    |
//   +--+-------+-------+-------+----------------------+
//      |       |       |       |
//      | s-0   | s-1   | s-2   | k-0
//      | Rx    | Rx    | Rx    | Tx (sink)
//      v       v       v       v
//   source A source B source C sink D

use std::collections::HashMap;
use std::sync::Mutex;

use rumqttd::{
    Broker, BrokerHandle, Config, ConnectionSettings, Notification, RouterConfig, ServerSettings,
    local::{LinkRx, LinkTx},
};
use tokio::sync::watch;
use tracing::{error, info};

/// Default broker settings. Port and tuning will become worker-level
/// configuration in the future; for now these are fixed.
const DEFAULT_HOST: &str = "0.0.0.0";
const DEFAULT_PORT: u16 = 1883;
const DEFAULT_MAX_CONNECTIONS: usize = 100;
const DEFAULT_MAX_PAYLOAD_SIZE: usize = 262144;

/// Handle returned to each source on registration.
/// Dropping it unregisters the source from the shared broker.
///
/// The link_rx (and its internal router_tx sender) must be dropped before
/// unregister() joins the router thread, so we use Option + manual drop
/// in the Drop impl to control ordering.
pub struct SourceHandle {
    pub link_rx: Option<LinkRx>,
    /// Signals when the broker thread exits (start() returned).
    /// Sources should select on this to detect broker failures instead of
    /// hanging on link_rx.next() forever.
    pub broker_stopped: watch::Receiver<Option<String>>,
    registration_id: u64,
}

impl Drop for SourceHandle {
    fn drop(&mut self) {
        // Drop the link first to release its router_tx sender clone.
        // This must happen before unregister() tries to join the router thread.
        self.link_rx.take();
        unregister(self.registration_id);
    }
}

/// Handle returned to each sink on registration.
/// Dropping it unregisters the sink from the shared broker.
pub struct SinkHandle {
    pub link_tx: Option<LinkTx>,
    /// Signals when the broker thread exits (start() returned).
    pub broker_stopped: watch::Receiver<Option<String>>,
    registration_id: u64,
}

impl Drop for SinkHandle {
    fn drop(&mut self) {
        // Drop the link first to release its router_tx sender clone.
        self.link_tx.take();
        unregister(self.registration_id);
    }
}

// ---- Internal types ----

struct BrokerState {
    handle: BrokerHandle,
    /// Receiver for "broker thread exited" signal. Cloned into each handle.
    broker_stopped: watch::Receiver<Option<String>>,
    /// JoinHandle for the broker thread. Joined on shutdown to ensure the
    /// TCP listener is fully closed before a new broker can start.
    broker_thread: Option<std::thread::JoinHandle<()>>,
    /// JoinHandle for the rumqttd router thread. Must be joined after all
    /// router_tx senders are dropped (including BrokerHandle) so the router
    /// can exit cleanly.
    router_thread: Option<std::thread::JoinHandle<()>>,
    /// Number of active registrations (sources + sinks).
    ref_count: u64,
    next_id: u64,
}

static STATE: Mutex<Option<BrokerState>> = Mutex::new(None);

// ---- Public API ----

/// Register a new source with the shared broker.
///
/// On first call (or after all registrations have been dropped), starts the broker
/// on the default port (0.0.0.0:1883). Each source gets its own internal
/// link subscribed to exactly its topic. The returned handle includes a
/// `broker_stopped` watch that fires if the broker thread exits (e.g., port
/// conflict). Sources should select on this to avoid hanging forever.
pub fn register_source(topic: String) -> Result<SourceHandle, String> {
    let mut guard = STATE.lock().unwrap();

    let state = match guard.as_mut() {
        Some(state) => state,
        None => {
            let (handle, broker_stopped, broker_thread, router_thread) = start_broker()?;
            guard.insert(BrokerState {
                handle,
                broker_stopped,
                broker_thread: Some(broker_thread),
                router_thread,
                ref_count: 0,
                next_id: 0,
            })
        }
    };

    let id = state.next_id;
    state.next_id += 1;
    state.ref_count += 1;

    // Create a dedicated internal link for this source.
    let client_id = format!("nes-source-{id}");
    let (mut link_tx, link_rx) = state.handle.link(&client_id)
        .map_err(|e| format!("failed to create broker link: {e}"))?;

    link_tx.subscribe(&topic)
        .map_err(|e| format!("failed to subscribe to {topic}: {e}"))?;

    info!(registration_id = id, topic = %topic, "MQTT source registered with dedicated link");

    Ok(SourceHandle {
        link_rx: Some(link_rx),
        broker_stopped: state.broker_stopped.clone(),
        registration_id: id,
    })
}

/// Register a new sink with the shared broker.
///
/// On first call (or after all registrations have been dropped), starts the broker.
/// The sink gets a LinkTx for publishing to its topic. No subscription is created
/// since sinks only publish.
pub fn register_sink(topic: String) -> Result<SinkHandle, String> {
    let mut guard = STATE.lock().unwrap();

    let state = match guard.as_mut() {
        Some(state) => state,
        None => {
            let (handle, broker_stopped, broker_thread, router_thread) = start_broker()?;
            guard.insert(BrokerState {
                handle,
                broker_stopped,
                broker_thread: Some(broker_thread),
                router_thread,
                ref_count: 0,
                next_id: 0,
            })
        }
    };

    let id = state.next_id;
    state.next_id += 1;
    state.ref_count += 1;

    // Create a dedicated internal link for this sink.
    let client_id = format!("nes-sink-{id}");
    let (link_tx, _link_rx) = state.handle.link(&client_id)
        .map_err(|e| format!("failed to create broker link: {e}"))?;

    info!(registration_id = id, topic = %topic, "MQTT sink registered with dedicated link");

    Ok(SinkHandle {
        link_tx: Some(link_tx),
        broker_stopped: state.broker_stopped.clone(),
        registration_id: id,
    })
}

fn unregister(registration_id: u64) {
    let mut guard = STATE.lock().unwrap();
    let should_shutdown = if let Some(state) = guard.as_mut() {
        state.ref_count -= 1;
        info!(registration_id, remaining = state.ref_count, "MQTT registration dropped");
        state.ref_count == 0
    } else {
        false
    };

    if should_shutdown {
        if let Some(mut state) = guard.take() {
            info!("Last MQTT registration dropped, shutting down broker");
            state.handle.shutdown();

            // 1. Join the broker thread. This waits for Broker::start() to
            //    return (which joins all server threads). After this, Broker
            //    is dropped on that thread, releasing Broker.router_tx.
            let broker_thread = state.broker_thread.take();
            let router_thread = state.router_thread.take();
            if let Some(thread) = broker_thread {
                let _ = thread.join();
            }
            info!("Broker thread joined, port released");

            // 2. Drop BrokerHandle (and the rest of BrokerState). This
            //    releases the BrokerHandle's router_tx sender. Other senders
            //    (from internal link connections) are released when the
            //    SourceHandle/SinkHandle links are dropped by the caller.
            drop(state);

            // 3. The router thread exits when ALL router_tx senders are
            //    dropped (including those held by internal links). Since
            //    links may outlive this function (dropped after unregister
            //    returns), we can't join the router thread synchronously.
            //    Instead, spawn a detached cleanup thread that waits for
            //    the router to exit. The port is already released (step 1),
            //    so a new broker can start immediately.
            if let Some(thread) = router_thread {
                std::thread::Builder::new()
                    .name("mqtt-router-cleanup".into())
                    .spawn(move || {
                        let _ = thread.join();
                        info!("Router thread joined (cleanup complete)");
                    })
                    .ok();
            }

            info!("Broker shutdown complete, port released");
        }
    }
}

// ---- Broker startup ----

fn start_broker() -> Result<(BrokerHandle, watch::Receiver<Option<String>>, std::thread::JoinHandle<()>, Option<std::thread::JoinHandle<()>>), String> {
    let listen_addr = format!("{DEFAULT_HOST}:{DEFAULT_PORT}")
        .parse()
        .map_err(|e| format!("invalid listen address: {e}"))?;

    let mut v4 = HashMap::new();
    v4.insert("1".to_string(), ServerSettings {
        name: "nes-mqtt-v4".to_string(),
        listen: listen_addr,
        tls: None,
        next_connection_delay_ms: 1,
        connections: ConnectionSettings {
            connection_timeout_ms: 60000,
            max_payload_size: DEFAULT_MAX_PAYLOAD_SIZE,
            max_inflight_count: 100,
            auth: None,
            external_auth: None,
            dynamic_filters: true,
        },
    });

    let rumqttd_config = Config {
        id: 0,
        router: RouterConfig {
            max_connections: DEFAULT_MAX_CONNECTIONS,
            max_outgoing_packet_count: 200,
            max_segment_size: 100 * 1024,
            max_segment_count: 10,
            custom_segment: None,
            initialized_filters: None,
            shared_subscriptions_strategy: Default::default(),
        },
        v4: Some(v4),
        v5: None,
        ws: None,
        cluster: None,
        console: None,
        bridge: None,
        prometheus: None,
        metrics: None,
    };

    let mut broker = Broker::new(rumqttd_config);

    // Grab a handle before moving broker to the start thread.
    let handle = broker.handle();

    // Take the router thread JoinHandle before moving broker to the start
    // thread. This lets us join the router thread during shutdown to prevent
    // it from racing against a new broker startup.
    let router_thread = broker.take_router_thread();

    // Watch channel: broker thread signals all registrations when it exits.
    // Initial value is None (running). Set to Some(reason) when start() returns.
    let (stopped_tx, stopped_rx) = watch::channel(None);

    // Broker thread — runs the TCP listener (blocking, spawns internal runtimes).
    let join_handle = std::thread::Builder::new()
        .name("mqtt-broker".into())
        .spawn(move || {
            let reason = match broker.start() {
                Ok(()) => "broker shut down".to_string(),
                Err(e) => {
                    error!(error = %e, "MQTT broker stopped with error");
                    format!("broker error: {e}")
                }
            };
            info!(reason = %reason, "MQTT broker thread exiting");
            let _ = stopped_tx.send(Some(reason));
        })
        .map_err(|e| format!("failed to spawn broker thread: {e}"))?;

    info!(
        listen = %format!("{DEFAULT_HOST}:{DEFAULT_PORT}"),
        "Shared MQTT broker started"
    );

    Ok((handle, stopped_rx, join_handle, router_thread))
}

// ---- Notification helpers ----

/// Extract the payload from a Forward notification, if any.
pub fn forward_payload(notification: &Notification) -> Option<&[u8]> {
    match notification {
        Notification::Forward(f) if !f.publish.payload.is_empty() => {
            Some(&f.publish.payload)
        }
        _ => None,
    }
}
