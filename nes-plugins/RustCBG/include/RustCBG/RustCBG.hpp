#include <cstdarg>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <ostream>
#include <new>

namespace rust {

struct RustChecksumSinkImpl;

struct RustFileSourceImpl;

using ErrCode = int32_t;

constexpr static const ErrCode OK = 0;

constexpr static const ErrCode IO_ERROR = -10;

constexpr static const ErrCode ALRIGHT = 0;

constexpr static const ErrCode ALREADY_OPEN = -10;

constexpr static const ErrCode NOT_OPEN = -11;

constexpr static const ErrCode NULL_PTR = -12;

constexpr static const ErrCode FILE_NOT_FOUND = -14;

constexpr static const ErrCode CHECKSUM_SINK_ALREADY_OPEN = -20;

constexpr static const ErrCode STOP_WITHOUT_STARTED = -21;

extern "C" {

RustFileSourceImpl *new_rust_file_source(const char *path);

ErrCode openn(RustFileSourceImpl *rfs);

void closee(RustFileSourceImpl *rfs);

int64_t fill_tuple_bufferr(RustFileSourceImpl *rfs, uint8_t *tuple_buffer, uint64_t buf_len);

void free_file_source(RustFileSourceImpl *rfs);

RustChecksumSinkImpl *new_rust_checksum_sink(const char *path);

ErrCode start_rust_checksum_sink(RustChecksumSinkImpl *rcs);

ErrCode execute_rust_checksum_sink(RustChecksumSinkImpl *rcs, const char *path);

ErrCode stop_rust_checksum_sink(RustChecksumSinkImpl *rcs);

}  // extern "C"

}  // namespace rust
