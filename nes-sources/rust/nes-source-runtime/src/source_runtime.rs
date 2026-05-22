use async_trait::async_trait;
use linkme::distributed_slice;
use nes_buffer_runtime::{BufferProvider, TupleBuffer};
use nes_io_runtime::IORuntime;
use nes_source_validation::ConfigOptions;
use std::pin::Pin;
use std::result;
use tokio::select;
use tokio::sync::mpsc::{Receiver, Sender};
#[cfg(tokio_unstable)]
use tokio::task::Builder;
use tracing::{Instrument, debug, info};

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;
pub type Result<T> = core::result::Result<T, String>;
pub type Controller = tokio::sync::mpsc::Sender<SourceCommand>;
pub type EmitFunction = Box<dyn Fn(SourceResult) -> BoxFuture<Result<()>> + Send + Sync>;

#[async_trait]
pub trait AsyncEmitter {
    async fn eos(&mut self);
    async fn error(&mut self, error_message: String);
    async fn data(&mut self, result: TupleBuffer);
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

    /// If true, the source populates all buffer metadata
    /// (sequence_number, origin_id, chunk_number, watermark, last_chunk, number_of_tuples)
    /// itself; the runtime does not overwrite them. Used by sources that preserve
    /// wire-level metadata end-to-end (e.g. NetworkSource). Defaults to false.
    fn adds_metadata(&self) -> bool {
        false
    }
}

pub enum SourceCommand {
    Stop,
}
pub struct QueryContext {
    pub query_id: String,
    pub distributed_query_id: String,
    pub source_id: u64,
}

pub type SourceCreateFn = dyn Fn(&ConfigOptions) -> Box<dyn AsyncSource + Send> + Sync + Send;
#[distributed_slice]
pub static SOURCE_CREATION_FUNCTIONS: [(&'static str, &'static SourceCreateFn)];

async fn race_against_stop_signal<R, Fn: Future<Output = R>>(
    commands: &mut Receiver<SourceCommand>,
    f: Fn,
) -> Option<R> {
    // Cancellation Safety: If the stop signal cancels the original future, we may lose data,
    // however, since the source is terminated anyway, we are never going to call the future again.
    select! {
        result = commands.recv() => {
            match result {
                Some(SourceCommand::Stop) => None,
                None => unreachable!("Controller should outlive the source task"),
            }
        }
        result = f => {
            Some(result)
        }
    }
}
async fn run_source(
    mut source: Box<dyn AsyncSource + Send>,
    origin_id: u64,
    commands: &mut Receiver<SourceCommand>,
    emit: &mut impl AsyncEmitter,
    mut buffer_provider: BufferProvider,
) -> Result<()> {
    source.start().await?;
    let adds_metadata = source.adds_metadata();
    let mut sequence_number: u64 = 1; // INITIAL sequence number (matches DataHandler behavior)
    loop {
        let Some(result) =
            race_against_stop_signal(commands, source.receive(&mut buffer_provider)).await
        else {
            info!("Source was requested to stop");
            break;
        };

        match result? {
            SourceResult::Data(mut buffer, size) => {
                if !adds_metadata {
                    buffer.set_origin_id(origin_id);
                    buffer.set_sequence_number(sequence_number);
                    sequence_number += 1;
                    buffer.set_chunk_number(1); // INITIAL_CHUNK_NUMBER
                    buffer.set_last_chunk(true);
                    buffer.set_number_of_tuples(size);
                }

                if race_against_stop_signal(commands, emit.data(buffer))
                    .await
                    .is_none()
                {
                    info!("Source was requested to stop, while emitting data.");
                    break;
                };
            }
            SourceResult::EoS => {
                info!("EoS received, stopping source");
                if race_against_stop_signal(commands, emit.eos())
                    .await
                    .is_none()
                {
                    info!("Source was requested to stop, while emitting eos.");
                };
                break;
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
    query_context: QueryContext,
    config: &ConfigOptions,
    runtime: IORuntime,
    mut emit: T,
    buffer_provider: BufferProvider,
) -> Result<(Controller, tokio::sync::oneshot::Receiver<T>)> {
    let (controller, mut rx) = tokio::sync::mpsc::channel(16);
    let (stop_sender, stop_signal) = tokio::sync::oneshot::channel();
    let source = construct_source(name, config);
    let task_name = format!(
        "SourceTask-{}-{}",
        query_context.query_id, query_context.source_id
    );
    let task = async move {
        match run_source(
            source,
            query_context.source_id,
            &mut rx,
            &mut emit,
            buffer_provider,
        )
        .await
        {
            Ok(()) => {}
            Err(error_message) => {
                let _ = emit.error(error_message).await;
            }
        }
        info!("Source has stopped");
        let _ = stop_sender.send(emit);
        rx.close();
    }
    .in_current_span();

    spawn_source_task(&runtime, &task_name, task);

    Ok((controller, stop_signal))
}

#[cfg(tokio_unstable)]
fn spawn_source_task<F>(runtime: &IORuntime, task_name: &str, task: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    Builder::new()
        .name(task_name)
        .spawn_on(task, &runtime.handle())
        .expect("failed to spawn source task");
}

#[cfg(not(tokio_unstable))]
fn spawn_source_task<F>(runtime: &IORuntime, _task_name: &str, task: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    runtime.handle().spawn(task);
}
