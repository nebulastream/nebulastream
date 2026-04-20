use async_trait::async_trait;
use linkme::distributed_slice;
use nes_buffer_runtime::{BufferProvider, TupleBuffer};
use nes_io_runtime;
use nes_io_runtime::IORuntime;
use nes_source_validation::ConfigOptions;
use std::pin::Pin;
use std::sync::Arc;
use tokio::select;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{Instrument, info};

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;
pub type Result<T> = core::result::Result<T, String>;
pub type Controller = tokio::sync::mpsc::Sender<SourceCommand>;
pub type EmitFunction = Box<dyn Fn(SourceResult) -> BoxFuture<Result<()>> + Send + Sync>;

#[async_trait]
pub trait AsyncEmitter {
    async fn eos(&mut self);
    async fn error(&mut self, error_message: String);
    async fn data(&mut self, result: TupleBuffer, size: usize) -> Result<()>;
}

pub enum SourceResult {
    Data(TupleBuffer, usize),
    EoS,
}

pub type Emitter = Sender<SourceResult>;

#[async_trait]
pub trait AsyncSource {
    async fn start(&mut self) -> Result<()>;
    async fn receive(&mut self, provider: &mut BufferProvider) -> Result<SourceResult>;
    async fn stop(&mut self) -> Result<()>;
}

pub enum SourceCommand {
    Stop,
}

pub type SourceCreateFn = dyn Fn(&ConfigOptions) -> Box<dyn AsyncSource + Send> + Sync + Send;
#[distributed_slice]
pub static SOURCE_CREATION_FUNCTIONS: [(&'static str, &'static SourceCreateFn)];

async fn run_source(
    mut source: Box<dyn AsyncSource + Send>,
    commands: &mut Receiver<SourceCommand>,
    emit: &mut impl AsyncEmitter,
    mut buffer_provider: BufferProvider,
) -> Result<()> {
    source.start().await?;
    'run: loop {
        select! {
            result = source.receive(&mut buffer_provider) => {
                match result? {
                    SourceResult::Data(buffer, size) => {
                        emit.data(buffer, size).await.expect("Bridge thread should remain alive");
                    },
                    SourceResult::EoS => {
                        info!("EoS received, stopping source");
                        emit.eos().await;
                        break 'run;
                    }
                }
            }
            command = commands.recv() => {
                let Some(command) = command else {
                    break 'run;
                };
                match command {
                    SourceCommand::Stop => {break 'run;}
                }
            }
        }
    }
    source.stop().await
}

fn construct_source(name: &str, config: &ConfigOptions) -> Box<dyn AsyncSource + Send> {
    for (registered_name, creator_fn) in SOURCE_CREATION_FUNCTIONS {
        if *registered_name.to_uppercase() == name.to_uppercase() {
            return creator_fn(config);
        }
    }
    unreachable!("the name and config should have been validated")
}

pub fn start_source<T: AsyncEmitter + Send + 'static>(
    name: &str,
    config: &ConfigOptions,
    runtime: Arc<IORuntime>,
    mut emit: T,
    buffer_provider: BufferProvider,
) -> Result<(Controller, tokio::sync::oneshot::Receiver<T>)> {
    let (controller, mut rx) = tokio::sync::mpsc::channel(16);
    let (stop_sender, stop_signal) = tokio::sync::oneshot::channel();
    let source = construct_source(name, config);
    runtime.runtime.spawn(
        async move {
            match run_source(source, &mut rx, &mut emit, buffer_provider).await {
                Ok(()) => {}
                Err(error_message) => {
                    let _ = emit.error(error_message).await;
                }
            }
            let _ = stop_sender.send(emit);
            rx.close();
        }
        .in_current_span(),
    );

    Ok((controller, stop_signal))
}
