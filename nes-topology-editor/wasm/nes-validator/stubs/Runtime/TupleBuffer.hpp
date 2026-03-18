#pragma once
// Stub for WASM build -- runtime tuple buffers not needed for validation
#include <cstddef>
#include <cstdint>

namespace NES {
class TupleBuffer {
public:
    TupleBuffer() = default;
    uint8_t* getBuffer() const { return nullptr; }
    size_t getBufferSize() const { return 0; }
    size_t getNumberOfTuples() const { return 0; }
    void setNumberOfTuples(size_t) {}
};
} // namespace NES
