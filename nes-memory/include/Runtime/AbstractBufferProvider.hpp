/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/
#pragma once

#include <chrono>
#include <cstddef>
#include <optional>
#include <vector>
#include <Runtime/TupleBuffer.hpp>

/// This enum reflects the different types of buffer managers in the system
/// global: overall buffer manager
/// local: buffer manager that we give to the processing
/// fixed: buffer manager that we use for sources
namespace NES
{
enum class BufferManagerType : uint8_t
{
    GLOBAL,
    LOCAL,
    FIXED
};

class AbstractBufferProvider
{
public:
    virtual ~AbstractBufferProvider() = default;

    virtual BufferManagerType getBufferManagerType() const = 0;

    virtual size_t getBufferSize() const = 0;

    virtual size_t getNumOfPooledBuffers() const = 0;
    virtual size_t getNumOfUnpooledBuffers() const = 0;

    virtual TupleBuffer getBufferBlocking() = 0;

    virtual std::optional<TupleBuffer> getBufferNoBlocking() = 0;

    virtual std::optional<TupleBuffer> getBufferWithTimeout(std::chrono::milliseconds timeout_ms) = 0;

    /// Acquires a buffer that is allowed to draw from the liveness reserve (see BufferManager). This is intended
    /// for the drain/emit path (window-trigger emit, sinks, network egress) so that forward progress is always
    /// possible even when the normal pool is exhausted by in-flight buffers, breaking the buffer-exhaustion
    /// deadlock. Providers without a dedicated reserve fall back to the normal non-reserved acquisition.
    virtual std::optional<TupleBuffer> tryGetReservedBuffer() { return getBufferNoBlocking(); }

    /// Blocking-with-timeout variant of tryGetReservedBuffer(). Waits up to timeout_ms for a (reserved or normal)
    /// buffer to become available.
    virtual std::optional<TupleBuffer> getReservedBufferWithTimeout(std::chrono::milliseconds timeout_ms)
    {
        return getBufferWithTimeout(timeout_ms);
    }

    /// Returns an unpooled buffer of size bufferSize wrapped in an optional or an invalid option if an error
    virtual std::optional<TupleBuffer> getUnpooledBuffer(size_t bufferSize) = 0;
};

}
