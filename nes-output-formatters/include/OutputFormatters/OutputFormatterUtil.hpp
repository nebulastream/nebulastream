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

/// Emit a HOST-KNOWN constant byte sequence, folding the bytes themselves into the JIT module
/// (not just a pointer + length): nautilus::embedConstantBytes materializes the constant as a
/// private constant global, so the fused writeBytesToBuffer's memcpy has a compile-time-visible
/// source -- LLVM hoists the constants into registers outside the pipeline loop and merges
/// stores. With an opaque host pointer the backend must instead reload the source every row (it
/// cannot prove no-alias with the destination); isolated microbench: folded constants are
/// 21-34% faster glue emission (piparse-bench scripts/const_emit_microbench.cpp).
/// The buffer-boundary spill path inside writeBytesToBuffer works unchanged (the global's
/// address is valid runtime memory). In INTERPRETED mode embedConstantBytes returns the host
/// pointer, so `constant` must outlive execution there -- pass a formatter member or a static.
inline nautilus::val<uint64_t> writeConstantBytes(
    const std::string& constant,
    const nautilus::val<int8_t*>& destination,
    const nautilus::val<uint64_t>& remainingSpace,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider)
{
    const auto embedded = nautilus::embedConstantBytes(constant.data(), constant.size());
    return nautilus::invoke(
        writeBytesToBuffer,
        static_cast<nautilus::val<const char*>>(embedded),
        nautilus::val<uint64_t>{constant.size()},
        remainingSpace,
        recordBuffer.getReference(),
        bufferProvider,
        destination);
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
