#[cfg(feature = "file-source")]
mod file_source;
#[cfg(feature = "file-source")]
pub use file_source::*;

#[cfg(feature = "checksum-sink")]
mod checksum_sink;
#[cfg(feature = "checksum-sink")]
pub use checksum_sink::*;

type ErrCode = i32;
pub const OK: ErrCode = 0;
pub const IO_ERROR: ErrCode = -10;
