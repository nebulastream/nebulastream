use async_trait::async_trait;
use linkme::distributed_slice;
use nes_buffer_runtime::{BufferProvider, TupleBuffer};
use nes_io_runtime;
use nes_io_runtime::IORuntime;
use nes_source_validation::ConfigOptions;
use std::sync::{Arc, OnceLock};
use std::thread::{JoinHandle, Thread};
use tokio::select;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::info;

pub type Result<T> = core::result::Result<T, String>;
pub type Controller = tokio::sync::mpsc::Sender<SourceCommand>;
pub type BlockingEmit = Box<dyn Fn(SourceResult) + Send + Sync>;
pub enum SourceResult {
    Data(TupleBuffer),
    Error(String),
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

fn bridge_thread(mut receiver: Receiver<SourceResult>, blocking_emit: Box<dyn Fn(SourceResult)>) {
    info!("Starting bridge thread. Waiting for data from source.");
    loop {
        let Some(data) = receiver.blocking_recv() else {
            break;
        };
        blocking_emit(data)
    }
    info!("Stopping bridge thread.");
}
async fn run_source(
    mut source: Box<dyn AsyncSource + Send>,
    mut commands: Receiver<SourceCommand>,
    emitter: &Emitter,
    mut buffer_provider: BufferProvider,
) -> Result<()> {
    source.start().await?;
    'run: loop {
        select! {
            result = source.receive(&mut buffer_provider) => {
                emitter.send(result?).await.expect("Bridge thread should remain alive");
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

static BRIDGE_THREAD: std::sync::OnceLock<(JoinHandle<()>, Emitter)> = OnceLock::new();

pub fn start_source(
    name: &str,
    config: &ConfigOptions,
    runtime: Arc<IORuntime>,
    emit: BlockingEmit,
    buffer_provider: BufferProvider,
) -> Result<Controller> {
    let emitter = BRIDGE_THREAD
        .get_or_init(|| {
            let (tx, rx) = tokio::sync::mpsc::channel(32);
            let handle = std::thread::spawn(|| bridge_thread(rx, emit));
            (handle, tx)
        })
        .1
        .clone();

    let (controller, rx) = tokio::sync::mpsc::channel(16);

    let source = construct_source(name, config);
    runtime.runtime.spawn(async move {
        match run_source(source, rx, &emitter, buffer_provider).await {
            Ok(()) => {}
            Err(error_message) => emitter
                .send(SourceResult::Error(error_message))
                .await
                .expect("Bridge thread should remain alive"),
        }
    });

    Ok(controller)
}
