#include <cstdarg>
#include <cstdint>
#include <cstdlib>
#include <ostream>
#include <new>

namespace rust {

struct RustFileSourceImpl;

using ErrCode = int32_t;

constexpr static const ErrCode ALRIGHT = 0;

constexpr static const ErrCode ALREADY_OPEN = -10;

constexpr static const ErrCode NOT_OPEN = -11;

constexpr static const ErrCode NULL_PTR = -12;

constexpr static const ErrCode IO_ERROR = -13;

constexpr static const ErrCode FILE_NOT_FOUND = -14;

extern "C" {

ErrCode open(RustFileSourceImpl *rfs);

void close(RustFileSourceImpl *rfs);

int64_t fill_tuple_buffer(RustFileSourceImpl *rfs, uint8_t *tuple_buffer, uint64_t buf_len);

RustFileSourceImpl *new_rust_file_source(const uint8_t *path_buf, uintptr_t path_len);

void free_rust_file_source(RustFileSourceImpl *rfs);

const uint8_t *to_string(const RustFileSourceImpl *rfs, uintptr_t *len);

void free_string(uint8_t *ptr, uintptr_t len);

}  // extern "C"

}  // namespace rust
