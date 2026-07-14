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


#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <OutputFormatters/OutputParser.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/VariableSizedAccess.hpp>
#include <Util/Strings.hpp>
#include <nautilus/inline.hpp> /// TMP DIAGNOSTIC: NAUTILUS_INLINE marker for the output write proxies
#include <std/cstring.h>
#include <ErrorHandling.hpp>
#include <OutputParserRegistry.hpp>
#include <function.hpp>
#include <static.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES
{
/// Write `size` raw bytes from `data` completely into the tuple buffer (no null-termination / strlen).
/// Child buffers may be allocated only if it does not fit completely into the main memory of the tuple buffer.
/// Will return the amount of bytes written in the main memory of the buffer.
/// This is the fast path for varsized passthrough: it copies the source bytes straight into the output
/// buffer without an intermediate std::string allocation.
/// Slow path: the value does not fit in the remaining main-buffer space, so it spills into one or more
/// child buffers (or appends to existing children). Kept out of the inlined fast path so the JIT loop
/// stays tiny.
inline uint64_t writeBytesToBufferSpilling(
    const char* data,
    const size_t size,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider,
    int8_t* bufferStartingAddress)
{
    size_t remainingBytes = size;
    uint32_t numOfChildBuffers = tupleBuffer->getNumberOfChildBuffers();
    uint64_t writtenToMainMemory = 0;
    /// Fill up the remaing space in the main tuple buffer before allocating any child buffers
    if (numOfChildBuffers == 0)
    {
        const size_t fitsInMainBuffer = std::min(remainingBytes, remainingSpace);
        writtenToMainMemory += fitsInMainBuffer;
        std::memcpy(bufferStartingAddress, data, fitsInMainBuffer);
        remainingBytes -= fitsInMainBuffer;
        /// Create the first child buffer, if necessary
        if (remainingBytes > 0)
        {
            auto newChildBuffer = bufferProvider->getBufferBlocking();
            (void)tupleBuffer->storeChildBuffer(newChildBuffer);
            ++numOfChildBuffers;
        }
    }
    while (remainingBytes > 0)
    {
        /// Write as many bytes in the latest child buffer as possible and allocate a new one if space does not suffice
        const VariableSizedAccess::Index childIndex{numOfChildBuffers - 1};
        auto lastChildBuffer = tupleBuffer->loadChildBuffer(childIndex);
        const auto bufferOffset = lastChildBuffer.getNumberOfTuples();
        const uint32_t valueOffset = size - remainingBytes;
        const uint64_t writable = std::min(remainingBytes, lastChildBuffer.getBufferSize() - bufferOffset);
        std::memcpy(lastChildBuffer.getAvailableMemoryArea<>().data() + bufferOffset, data + valueOffset, writable);
        remainingBytes -= writable;
        lastChildBuffer.setNumberOfTuples(bufferOffset + writable);
        if (remainingBytes > 0)
        {
            auto newChildBuffer = bufferProvider->getBufferBlocking();
            (void)tupleBuffer->storeChildBuffer(newChildBuffer);
            ++numOfChildBuffers;
        }
    }
    return writtenToMainMemory;
}

NAUTILUS_INLINE inline uint64_t writeBytesToBuffer(
    const char* data,
    const size_t size,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider,
    int8_t* bufferStartingAddress)
{
    /// Fast path: the value fits entirely in the main buffer. The main buffer overflows into child buffers
    /// at most once, and that overflow drains remainingSpace to 0 (the caller subtracts the full
    /// remainingSpace). Hence `size <= remainingSpace` guarantees no child buffers exist yet and the whole
    /// value is written to main -- so we can skip the per-write getNumberOfChildBuffers() proxy call.
    if (size <= remainingSpace)
    {
        std::memcpy(bufferStartingAddress, data, size);
        return size;
    }
    return writeBytesToBufferSpilling(data, size, remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}

/// Write the null-terminated string completely into the tuple buffer (delegates to writeBytesToBuffer).
inline uint64_t writeValueToBuffer(
    const char* value,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider,
    int8_t* bufferStartingAddress)
{
    return writeBytesToBuffer(value, std::string_view{value}.size(), remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}

/// Emit a HOST-KNOWN constant byte sequence as immediate stores, folding the bytes themselves
/// into the JIT IR (not just a pointer + length). Why: with an opaque pointer the backend must
/// reload the source every row (it cannot prove no-alias with the destination); with visible
/// bytes LLVM hoists the constants into registers outside the pipeline loop and merges adjacent
/// stores (isolated microbench: 21-34% faster glue emission than the const-length memcpy shape;
/// piparse-bench scripts/const_emit_microbench.cpp).
/// Chunked 8/4/2/1 little-endian at trace time; the emit loop uses a static_val induction
/// variable because operations are tagged by code location -- a plain host loop re-emitting the
/// same store op trips the tracer's "constant loop" detection. Guarded by the same
/// `size <= remainingSpace` invariant as writeBytesToBuffer's fast path; the buffer-boundary
/// case falls back to the generic spilling writer, which needs a stable pointer -- so `constant`
/// must OUTLIVE the compiled query (a formatter member or a static).
inline nautilus::val<uint64_t> writeConstantBytes(
    const std::string& constant,
    const nautilus::val<int8_t*>& destination,
    const nautilus::val<uint64_t>& remainingSpace,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider)
{
    const uint64_t size = constant.size();

    struct Chunk
    {
        uint64_t offset;
        uint64_t width;
        uint64_t bits;
    };

    std::vector<Chunk> chunks;
    for (uint64_t offset = 0; offset < size;)
    {
        const uint64_t width = (size - offset >= 8) ? 8 : (size - offset >= 4) ? 4 : (size - offset >= 2) ? 2 : 1;
        uint64_t bits = 0;
        std::memcpy(&bits, constant.data() + offset, width);
        chunks.push_back({.offset = offset, .width = width, .bits = bits});
        offset += width;
    }
    nautilus::val<uint64_t> written{0};
    if (remainingSpace >= nautilus::val<uint64_t>{size})
    {
        for (nautilus::static_val<uint64_t> chunkIndex = 0; chunkIndex < chunks.size(); ++chunkIndex)
        {
            const auto& [offset, width, bits] = chunks[static_cast<uint64_t>(chunkIndex)];
            const auto chunkDestination = destination + nautilus::val<uint64_t>{offset};
            switch (width)
            {
                case 8:
                    *static_cast<nautilus::val<uint64_t*>>(chunkDestination) = nautilus::val<uint64_t>{bits};
                    break;
                case 4:
                    *static_cast<nautilus::val<uint32_t*>>(chunkDestination) = nautilus::val<uint32_t>{static_cast<uint32_t>(bits)};
                    break;
                case 2:
                    *static_cast<nautilus::val<uint16_t*>>(chunkDestination) = nautilus::val<uint16_t>{static_cast<uint16_t>(bits)};
                    break;
                default:
                    *static_cast<nautilus::val<int8_t*>>(chunkDestination) = nautilus::val<int8_t>{static_cast<int8_t>(bits)};
            }
        }
        written = nautilus::val<uint64_t>{size};
    }
    else
    {
        written = nautilus::invoke(
            writeBytesToBuffer,
            nautilus::val<const char*>{constant.c_str()},
            nautilus::val<uint64_t>{size},
            remainingSpace,
            recordBuffer.getReference(),
            bufferProvider,
            destination);
    }
    return written;
}

/// Parses the varval into a string.
/// The string is then written into the memory of the record buffer, starting from address.
/// Child buffers might be allocated to fit the whole string into the buffer.
/// Returns the amount of bytes written in the record buffer itself, children excluded.
inline nautilus::val<uint64_t> formatAndWriteVal(
    const VarVal& value,
    const nautilus::val<int8_t*>& address,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const std::string& parserType)
{
    /// Create the parser for the value
    constexpr OutputParserRegistryArguments arguments{};
    if (const auto parser = OutputParserRegistry::instance().create(parserType, arguments))
    {
        /// Parse the value
        return parser.value()->parseAndWrite(value, remainingSize, recordBuffer, bufferProvider, address);
    }
    throw UnknownOutputParserType("Unknown Output Parser: {}", parserType);
}
}
