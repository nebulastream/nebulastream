mod backup;
mod gateway;
mod progress;

type OriginId = u64;
type SequenceNumber = u64;
type ChunkNumber = u64;
type Epoch = u64;

pub use backup::{LogControlMessage, LogManager};
pub use gateway::{ReceiverGateway, ReceiverGatewayHandle, GatewayControlMessage};
