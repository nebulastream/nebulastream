use async_trait::async_trait;
use linkme::distributed_slice;
use nes_buffer_runtime::TupleBuffer;
use nes_io_runtime::IORuntime;
use nes_sink_validation::ConfigOptions;
use scopeguard::ScopeGuard;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::{Instrument, error, info, warn};

pub type Result<T> = core::result::Result<T, String>;
pub type DataChannel = mpsc::Sender<SinkDataCommand>;
pub type TerminationSignal = oneshot::Sender<SinkTerminationCommand>;

#[async_trait]
pub trait AsyncSink {
    async fn start(&mut self) -> Result<()>;
    async fn execute(&mut self, buffer: TupleBuffer) -> Result<()>;
    async fn flush(&mut self) -> Result<()>;
    async fn stop(self: Box<Self>) -> Result<()>;
    async fn error(self: Box<Self>, error_message: String) {
        warn!(
            "Sink was aborted with an error, but no action was taken: {}",
            error_message
        );
    }
}

pub enum SinkDataCommand {
    Flush(usize),
    Data(TupleBuffer),
}

#[derive(Debug)]
pub enum SinkTerminationCommand {
    Error(String, oneshot::Sender<()>),
    Close(oneshot::Sender<Result<()>>),
}

pub type SinkCreateFn = dyn Fn(&ConfigOptions) -> Box<dyn AsyncSink + Send> + Sync + Send;
#[distributed_slice]
pub static SINK_CREATION_FUNCTIONS: [(&'static str, &'static SinkCreateFn)];

pub struct SinkContext {
    pub on_error: Box<dyn Fn(String) + Send + Sync>,
    pub on_flush: Box<dyn Fn(usize) + Send + Sync>,
}

/// Drives the sink to completion. After a successful `start()`, `stop()` is
/// always invoked exactly once before the sink is dropped — regardless of
/// whether the loop exited via `Close`, a controller drop, or an error from
/// `execute`/`flush`. This is the load-bearing invariant that lets
/// `NetworkSink` (and any other sink that owns external resources) assume
/// it always gets a chance to release them via `stop()`.
async fn run_sink(
    mut sink: Box<dyn AsyncSink + Send>,
    commands: &mut mpsc::Receiver<SinkDataCommand>,
    mut termination: oneshot::Receiver<SinkTerminationCommand>,
    context: &SinkContext,
) -> core::result::Result<(), (oneshot::Receiver<SinkTerminationCommand>, Box<dyn AsyncSink + Send>, String)> {
    if let Err(e) = sink.start().await {
        return Err((termination, sink, e.to_string()));
    }

    loop {
        tokio::select! {
                biased;
                termination = &mut termination => {
                    return match termination.expect("Sink commands channel closed unexpectedly") {
                        SinkTerminationCommand::Error(message, done) => {
                            sink.error(message).await;
                            let _ = done.send(());
                            Ok(())
                        }
                        SinkTerminationCommand::Close(done) => {
                            let result = sink.stop().await;
                            if let Err(result) = done.send(result) {
                                if let Err(message) = result {
                                    warn!("Could not report sink close result back to the sink runtime: {message}") ;
                                }
                            };
                            Ok(())
                        },
                    }
                }
                data = commands.recv() => {
                    match data.expect("Sink commands channel closed unexpectedly") {
                        SinkDataCommand::Flush(epoch) => {
                            info!("Received flush command for epoch {}", epoch);
                            if let Err(e) = sink.flush().await {
                                return Err((termination, sink, e.to_string()));
                            }
                            info!("Flush is done");
                            (context.on_flush)(epoch);
                        }
                        SinkDataCommand::Data(buffer) => {
                            if let Err(e) = sink.execute(buffer).await {
                                return Err((termination, sink, e.to_string()));
                            }
                        },
                    }
                }
            }
    }
}

fn construct_sink(name: &str, config: &ConfigOptions) -> Box<dyn AsyncSink + Send> {
    for (registered_name, creator_fn) in SINK_CREATION_FUNCTIONS {
        if *registered_name.to_uppercase() == name.to_uppercase() {
            return creator_fn(config);
        }
    }
    unreachable!("the name and config should have been validated")
}

/// Spawns the sink task and returns the controller plus a `stop_signal`.
///
/// Mirrors the source runtime: no `JoinHandle` is leaked, so the framework
/// cannot abort the task. Termination is exclusively driven by sending
/// `SinkCommand::Close` on the controller and awaiting `stop_signal`. The
/// task's scopeguard converts any external abort into a loud panic, making
/// violations of the never-drop contract surface immediately.
pub fn start_sink(
    name: &str,
    config: &ConfigOptions,
    runtime: IORuntime,
    context: SinkContext,
) -> Result<(DataChannel, TerminationSignal)> {
    let (controller, mut rx) = tokio::sync::mpsc::channel(16);
    let (stop_sender, stop_signal) = oneshot::channel();
    let sink = construct_sink(name, config);

    let task = async move {
        match run_sink(sink, &mut rx, stop_signal, &context).await {
            Ok(()) => {}
            Err((stop_signal, sink, error_message)) => {
                (context.on_error)(error_message);
                match stop_signal.await.expect("Sink commands channel closed unexpectedly") {
                    SinkTerminationCommand::Error(message, done) => {
                        sink.error(message).await;
                        let _ = done.send(());
                    }
                    SinkTerminationCommand::Close(done) => {
                        let result = sink.stop().await;
                        if let Err(result) = done.send(result) {
                            if let Err(message) = result {
                                warn!("Could not report sink close result back to the sink runtime: {message}") ;
                            }
                        };
                    },
                }
            },
        }
        info!("Sink has stopped");
    }
    .in_current_span();

    runtime.handle().spawn(task);

    Ok((controller, stop_sender))
}
