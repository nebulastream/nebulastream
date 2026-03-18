pub mod buffer_iterator;
pub mod context;
pub mod error;
pub mod handle;
pub mod schema;
pub mod plugin;
pub mod sink;

pub use buffer_iterator::BufferIterator;
pub use context::{SinkContext, SinkMessage};
pub use error::SinkError;
pub use handle::{SinkTaskHandle, stop_sink, is_sink_done};
pub use schema::SchemaField;
pub use sink::AsyncSink;

// Re-export config types from source-runtime for sink validation
pub use nes_source_runtime::config::{ConfigParam, ParamType, validate_config};
