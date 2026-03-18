pub mod backpressure;
pub mod bridge;
pub mod config;
pub mod context;
pub mod error;
pub mod generator;
pub mod handle;
pub mod runtime;
pub mod plugin;
pub mod source;

// Re-export buffer types from nes_buffer_runtime
pub use nes_buffer_runtime as buffer;
pub use nes_buffer_runtime::{BufferProviderHandle, TupleBufferHandle};

pub use backpressure::BackpressureState;
pub use bridge::{BridgeMessage, EmitCallback, EmitFnPtr};
pub use error::{SourceError, SourceResult};
pub use context::SourceContext;
pub use generator::GeneratorSource;
pub use handle::SourceTaskHandle;
pub use runtime::{init_io_runtime, io_runtime};
pub use source::AsyncSource;
