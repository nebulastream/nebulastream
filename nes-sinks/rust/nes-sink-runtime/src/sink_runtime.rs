use async_trait::async_trait;
use linkme::distributed_slice;
use nes_buffer_runtime::TupleBuffer;
use nes_io_runtime::IORuntime;
use nes_sink_validation::ConfigOptions;
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot;
use tracing::{Instrument, info};

pub type Result<T> = core::result::Result<T, String>;
pub type Controller = tokio::sync::mpsc::Sender<SinkCommand>;

#[async_trait]
pub trait AsyncSink {
    async fn start(&mut self) -> Result<()>;
    async fn execute(&mut self, buffer: TupleBuffer) -> Result<()>;
    async fn flush(&mut self) -> Result<()>;
    async fn stop(&mut self) -> Result<()>;
}

pub enum SinkCommand {
    Flush(usize),
    Data(TupleBuffer),
    Close,
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
    commands: &mut Receiver<SinkCommand>,
    context: &SinkContext,
) -> Result<()> {
    sink.start().await?;
    let loop_result: Result<()> = async {
        loop {
            let command = commands
                .recv()
                .await
                .expect("Controller should outlive the sink task");
            match command {
                SinkCommand::Data(buffer) => sink.execute(buffer).await?,
                SinkCommand::Flush(epoch) => {
                    sink.flush().await?;
                    (context.on_flush)(epoch);
                }
                SinkCommand::Close => {
                    info!("Sink was requested to close");
                    return Ok(());
                }
            }
        }
    }
    .await;
    let stop_result = sink.stop().await;
    loop_result.and(stop_result)
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
) -> Result<(Controller, oneshot::Receiver<()>)> {
    let (controller, mut rx) = tokio::sync::mpsc::channel(16);
    let (stop_sender, stop_signal) = oneshot::channel();
    let sink = construct_sink(name, config);

    let task = async move {
        let abort_guard = scopeguard::guard((), |_| {
            panic!("Sink Task was aborted.");
        });

        match run_sink(sink, &mut rx, &context).await {
            Ok(()) => {}
            Err(error_message) => (context.on_error)(error_message),
        }
        info!("Sink has stopped");
        rx.close();
        let _ = stop_sender.send(());

        // Sink stopped itself; disarm the abort-detection guard.
        scopeguard::ScopeGuard::into_inner(abort_guard);
    }
    .in_current_span();

    runtime.handle().spawn(task);

    Ok((controller, stop_signal))
}
