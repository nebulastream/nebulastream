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
#include <array>
#include <cstddef>
#include <cstdint>
#include <string_view>
#include <vector>

#include <ErrorHandling.hpp>
#include <SIMDCSVKernel.hpp>

namespace NES::SimdCsv
{

/// Portable structural walk shared by every kernel. Drives `sink` with the exact same
/// `FieldOffsets` emission contract as the scalar CSVInputFormatIndexer:
///   per complete tuple emit `numFields + 1` offsets = [field0Start, .., field(n-1)Start, newlinePos]
/// where fieldKStart is the byte right after the (k-1)-th unquoted comma and the final sentinel is the
/// position of the row-terminating newline. Bytes before the first newline and after the last newline
/// are the spanning fragments and are deliberately not emitted (handled by the SequenceShredder).
///
/// `Sink` must provide: startSetup(size_t, size_t), emplaceFieldOffset(uint32_t),
/// markNoTupleDelimiters(), markWithTupleDelimiters(uint32_t, uint32_t). `FieldOffsets<ONE>` does; the
/// differential test supplies a recording sink.
///
/// Newlines and field separators are discovered together in the single SIMD pass (never a separate
/// newline scan). Quote parity is assumed balanced within each row, which holds for valid CSV and is
/// guaranteed here because newlines never occur inside quotes.
template <typename Sink>
void indexInto(
    Sink& sink,
    const std::string_view view,
    const char fieldDelim,
    const char tupleDelim,
    const bool quoteAware,
    const uint64_t numFields,
    const ComputeBlocksFn computeBlocks)
{
    constexpr size_t sizeOfFieldDelimiter = 1;
    sink.startSetup(numFields, sizeOfFieldDelimiter);

    const char* data = view.data();
    const size_t numBytes = view.size();

    /// Walk state, threaded across SIMD blocks and into the scalar tail.
    bool seenFirstNewline = false;
    uint32_t firstNewline = 0;
    uint32_t lastNewline = 0;
    size_t regionStart = 0; ///< start of the current (not-yet-closed) tuple
    uint64_t quoteCarry = 0; ///< all-ones if the previous block ended inside a quote, else zero

    /// Field-start offsets accumulated for the current tuple. Only flushed once its terminating
    /// newline is seen, so an unterminated trailing tuple never leaves a partial offset group.
    /// thread_local: reused across calls (the indexer runs concurrently), never reentrant per thread.
    static thread_local std::vector<uint32_t> pending;
    pending.clear();

    const auto handleStructural = [&](const size_t pos, const bool isNewline)
    {
        if (!seenFirstNewline)
        {
            if (isNewline)
            {
                seenFirstNewline = true;
                firstNewline = static_cast<uint32_t>(pos);
                /// last == first until a further newline closes a complete tuple; matches the scalar
                /// indexer, whose offsetOfLastTupleDelimiter is the last newline in the buffer (which is
                /// the first one when there is only a single newline).
                lastNewline = static_cast<uint32_t>(pos);
                regionStart = pos + 1;
                pending.clear();
            }
            /// commas before the first newline belong to the leading spanning fragment: ignore
            return;
        }
        if (isNewline)
        {
            sink.emplaceFieldOffset(static_cast<uint32_t>(regionStart));
            for (const uint32_t fieldStart : pending)
            {
                sink.emplaceFieldOffset(fieldStart);
            }
            sink.emplaceFieldOffset(static_cast<uint32_t>(pos)); ///< sentinel == newline position
            const size_t fieldCount = 1 + pending.size();
            if (fieldCount != numFields)
            {
                throw CannotFormatSourceData(
                    "Number of parsed fields does not match number of fields in schema (parsed {} vs {} schema", fieldCount, numFields);
            }
            lastNewline = static_cast<uint32_t>(pos);
            regionStart = pos + 1;
            pending.clear();
        }
        else
        {
            pending.push_back(static_cast<uint32_t>(pos + 1));
        }
    };

    /// SIMD region: only complete 64-byte blocks (block end <= numBytes), so we never read past the
    /// valid bytes of the buffer (spanning-tuple buffers have no trailing padding).
    const size_t numBlocks = numBytes / 64;
    if (numBlocks > 0)
    {
        constexpr size_t chunkBlocks = 16;
        std::array<BlockBits, chunkBlocks> chunk{};
        for (size_t base = 0; base < numBlocks; base += chunkBlocks)
        {
            const size_t count = std::min(chunkBlocks, numBlocks - base);
            computeBlocks(data + (base * 64), count, chunk.data(), fieldDelim, tupleDelim);
            for (size_t i = 0; i < count; ++i)
            {
                const BlockBits bits = chunk[i];
                const size_t blockBase = (base + i) * 64;
                uint64_t insideQuotes = 0;
                if (quoteAware)
                {
                    uint64_t quoted = prefixXor(bits.quote);
                    quoted ^= quoteCarry;
                    quoteCarry = uint64_t{0} - (quoted >> 63);
                    insideQuotes = quoted;
                }
                const uint64_t commaSep = quoteAware ? (bits.comma & ~insideQuotes) : bits.comma;
                uint64_t fieldSep = commaSep | bits.newline;
                while (fieldSep != 0)
                {
                    const auto bit = static_cast<unsigned>(__builtin_ctzll(fieldSep));
                    const bool isNewline = ((bits.newline >> bit) & uint64_t{1}) != 0;
                    handleStructural(blockBase + bit, isNewline);
                    fieldSep &= fieldSep - 1;
                }
            }
        }
    }

    /// Scalar tail: the remaining `numBytes % 64` bytes, with the same quote/field/tuple semantics.
    bool insideQuote = quoteAware && (quoteCarry != 0);
    for (size_t i = numBlocks * 64; i < numBytes; ++i)
    {
        const char byte = data[i];
        if (byte == tupleDelim)
        {
            handleStructural(i, true);
        }
        else if (byte == fieldDelim && (!quoteAware || !insideQuote))
        {
            handleStructural(i, false);
        }
        if (quoteAware && byte == '"')
        {
            insideQuote = !insideQuote;
        }
    }

    if (!seenFirstNewline)
    {
        sink.markNoTupleDelimiters();
        return;
    }
    sink.markWithTupleDelimiters(firstNewline, lastNewline);
}

}
