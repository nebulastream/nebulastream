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
#include <cstdint>
#include <optional>
#include <utility>
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

    /// Returns an unpooled buffer of size bufferSize wrapped in an optional or an invalid option if an error
    virtual std::optional<TupleBuffer> getUnpooledBuffer(size_t bufferSize) = 0;

    /// If this provider is backed by a SINGLE contiguous allocation (the pool slab carved into all pooled
    /// buffers), return {base, byteSize}; otherwise nullopt. Lets a source register the whole region once
    /// with io_uring (io_uring_register_buffers + prep_read_fixed): O_DIRECT reads then skip get_user_pages
    /// per read (the pages are pinned once) and, on recent kernels, reuse a persistent DMA/IOMMU mapping --
    /// the dominant per-read kernel cost (HANDOVER §34). Every pooled buffer's payload lies inside this
    /// region, so one registered buffer (index 0) covers them all. Default: not slab-backed (registration off).
    [[nodiscard]] virtual std::optional<std::pair<uint8_t*, size_t>> getContiguousSlab() const { return std::nullopt; }

    /// Alignment (bytes) every pooled buffer payload is guaranteed to satisfy. A source can use this to decide
    /// whether fixed-buffer O_DIRECT reads are safe (they need device-block, e.g. 512, aligned payloads).
    /// Default 0 = no guarantee.
    [[nodiscard]] virtual size_t getBufferAlignment() const { return 0; }
};

}
