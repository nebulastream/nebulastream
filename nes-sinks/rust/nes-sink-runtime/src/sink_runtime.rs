use async_trait::async_trait;
use linkme::distributed_slice;
use nes_buffer_runtime::TupleBuffer;
use nes_io_runtime::IORuntime;
use nes_sink_validation::ConfigOptions;
use tokio::select;
use tokio::sync::mpsc::Receiver;
use tracing::{Instrument, debug, info};

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
    FlushStop(usize),
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

async fn run_sink(
    mut sink: Box<dyn AsyncSink + Send>,
    mut commands: Receiver<SinkCommand>,
    context: &SinkContext,
) -> Result<()> {
    sink.start().await?;
    'run: loop {
        select! {
            command = commands.recv() => {
                let Some(command) = command else {
                    info!("Sink sender was dropped, terminating the sink");
                    break 'run;
                };
                match command {
                    SinkCommand::Flush(epoch) => {sink.flush().await?; (context.on_flush)(epoch)},
                    SinkCommand::Data(buffer) => {sink.execute(buffer).await?},
                    SinkCommand::Close => {
                    info!("Sink was requested to close, terminating the sink.");
                        break 'run;},
                    SinkCommand::FlushStop(epoch) => {sink.flush().await?; sink.stop().await?; (context.on_flush)(epoch); return Ok(())},
                }
            }
        }
    }
    sink.stop().await
}

fn construct_sink(name: &str, config: &ConfigOptions) -> Box<dyn AsyncSink + Send> {
    for (registered_name, creator_fn) in SINK_CREATION_FUNCTIONS {
        if *registered_name.to_uppercase() == name.to_uppercase() {
            return creator_fn(config);
        }
    }
    unreachable!("the name and config should have been validated")
}

pub fn start_sink(
    name: &str,
    config: &ConfigOptions,
    runtime: IORuntime,
    context: SinkContext,
) -> Result<(Controller, tokio::task::JoinHandle<()>)> {
    let (controller, rx) = tokio::sync::mpsc::channel(16);
    let sink = construct_sink(name, config);
    let task = runtime.handle().spawn(
        async move {
            let log_on_source_drop = scopeguard::guard((), |_| {
                debug!("Sink Task was aborted.");
            });
            match run_sink(sink, rx, &context).await {
                Ok(()) => {}
                Err(error_message) => (context.on_error)(error_message),
            }

            // sink stops itself, it can no longer be aborted
            scopeguard::ScopeGuard::into_inner(log_on_source_drop);
        }
        .in_current_span(),
    );

    Ok((controller, task))
}
