#pragma once
// Stub for WASM build -- runtime buffer management not needed for validation
namespace NES {
class AbstractBufferProvider {
public:
    virtual ~AbstractBufferProvider() = default;
};
} // namespace NES
