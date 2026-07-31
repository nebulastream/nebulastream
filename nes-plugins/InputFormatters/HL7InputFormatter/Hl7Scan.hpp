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
#include <bit>
#include <cstddef>
#include <cstdint>
#include <string_view>

#include <ErrorHandling.hpp>
#include <Hl7SimdKernel.hpp>

/// The scalar and SIMD structural walks of the HL7 indexer.
///
/// The SCALAR walk (the "HL7" indexer) emits per complete message `numFields + 1` offsets into a
/// Sink -- the FieldOffsets<ONE> contract:
///   [leaf0Start, leaf1Start, ..., leaf(n-1)Start, bodyEnd]
/// where each following-leaf start is the byte after a structural delimiter and bodyEnd is the
/// position of the terminating message delimiter. Bytes before the first and after the last
/// message delimiter are spanning fragments and are not emitted.
/// `Sink` must provide: startSetup(size_t, size_t), emplaceFieldOffset(uint32_t),
/// markNoTupleDelimiters(), markWithTupleDelimiters(uint32_t, uint32_t).
///
/// The SIMD walk (the "SIMDHL7" indexer) instead FLATTENS every structural event into ONE raw
/// position band that the read phase addresses in place with a fixed stride (Hl7FieldIndex) --
/// no per-message group framing, no second copy. A complete message of F leaves contributes
/// exactly F events (F-1 structural delimiters + its terminating message-delimiter pair), so for
/// message k with band index of its OPENING pair at I0 + k*F:
///   leaf i  =  [ band[I0 + k*F + i] + (i == 0 ? 2 : 1),  band[I0 + k*F + i + 1] )
/// (the first leaf skips the two delimiter bytes; every later leaf starts after its 1-byte
/// structural delimiter; the leaf ends AT the next event position). Arity validation reduces to
/// "consecutive pair band-indexes differ by exactly F" (validateHl7MessageArity).
namespace NES::SimdHl7
{

[[noreturn]] inline void throwHl7ArityMismatch(const size_t leafCount, const uint64_t numFields)
{
    throw CannotFormatSourceData(
        "HL7 message produced {} leaves, but the schema expects {} (a value containing a structural "
        "delimiter byte, or a message shape not matching the schema)",
        leafCount,
        numFields);
}

/// Scalar reference walk: arbitrary-length message delimiter (find-based), structural class via
/// find_first_of. Used by the "HL7" indexer.
template <typename Sink>
void indexHl7ScalarInto(
    Sink& sink, const std::string_view view, const std::string_view messageDelimiter, const std::string_view structuralBytes, const uint64_t numFields)
{
    sink.startSetup(numFields, /*sizeOfFieldDelimiter*/ 1);

    const auto offsetOfFirstTupleDelimiter = view.find(messageDelimiter);
    if (offsetOfFirstTupleDelimiter == std::string_view::npos)
    {
        sink.markNoTupleDelimiters();
        return;
    }

    auto startIdxOfNextTuple = offsetOfFirstTupleDelimiter + messageDelimiter.size();
    auto endIdxOfNextTuple = view.find(messageDelimiter, startIdxOfNextTuple);
    while (endIdxOfNextTuple != std::string_view::npos)
    {
        INVARIANT(startIdxOfNextTuple <= endIdxOfNextTuple, "The start index of a tuple cannot be larger than the end index.");
        const auto message = view.substr(startIdxOfNextTuple, endIdxOfNextTuple - startIdxOfNextTuple);

        /// The start of the message body is the offset of its first leaf
        sink.emplaceFieldOffset(static_cast<uint32_t>(startIdxOfNextTuple));
        size_t leafIdx = 1;
        for (size_t delimiterPos = message.find_first_of(structuralBytes, 0); delimiterPos != std::string_view::npos;
             delimiterPos = message.find_first_of(structuralBytes, delimiterPos + 1))
        {
            /// All structural delimiters are 1 byte; the byte after the delimiter starts the next leaf
            sink.emplaceFieldOffset(static_cast<uint32_t>(startIdxOfNextTuple + delimiterPos + 1));
            ++leafIdx;
        }
        /// The closing offset is the end of the message body, which lets the read phase size the
        /// last leaf without extra calculations
        sink.emplaceFieldOffset(static_cast<uint32_t>(startIdxOfNextTuple + message.size()));
        if (leafIdx != numFields)
        {
            throwHl7ArityMismatch(leafIdx, numFields);
        }

        startIdxOfNextTuple = endIdxOfNextTuple + messageDelimiter.size();
        endIdxOfNextTuple = view.find(messageDelimiter, startIdxOfNextTuple);
    }
    const auto offsetOfLastTupleDelimiter = static_cast<uint32_t>(startIdxOfNextTuple - messageDelimiter.size());
    sink.markWithTupleDelimiters(static_cast<uint32_t>(offsetOfFirstTupleDelimiter), offsetOfLastTupleDelimiter);
}

struct Hl7FlattenResult
{
    size_t numEvents; ///< total flattened positions (leading/trailing-fragment events included)
    size_t numPairs; ///< number of message-delimiter pairs among them
};

/// simdjson-style unconditional flatten: extracts every set bit of `events` as an absolute byte
/// position (blockBase + bit) into `dst`. Writes in fixed batches of 8 slots regardless of the
/// true count -- countr_zero(0) == 64 puts garbage in the slack slots, which the next block's
/// batch overwrites (or nothing ever reads) -- removing the branch-per-bit of a while(bits) walk.
/// The caller advances by the returned count and must provide 16 slots of slack beyond it.
inline size_t flattenBlockEvents(uint32_t* const dst, const size_t blockBase, uint64_t events)
{
    const auto count = static_cast<size_t>(std::popcount(events));
    for (size_t j = 0; j < 8; ++j)
    {
        dst[j] = static_cast<uint32_t>(blockBase + static_cast<size_t>(std::countr_zero(events)));
        events &= events - 1;
    }
    if (count > 8) [[unlikely]]
    {
        for (size_t j = 8; j < 16; ++j)
        {
            dst[j] = static_cast<uint32_t>(blockBase + static_cast<size_t>(std::countr_zero(events)));
            events &= events - 1;
        }
        if (count > 16) [[unlikely]]
        {
            size_t j = 16;
            while (events != 0)
            {
                dst[j] = static_cast<uint32_t>(blockBase + static_cast<size_t>(std::countr_zero(events)));
                events &= events - 1;
                ++j;
            }
        }
    }
    return count;
}

/// SIMD walk: requires a TWO-byte message delimiter (the MLLP "\x1C\r" trailer) whose first byte is
/// not in the structural class (both enforced by the SIMDHL7 indexer's precondition). Discovers the
/// structural class and the message-delimiter pair in one SIMD pass; the pair is matched across
/// 64-byte block boundaries via a carry, and the matched pair's second byte (a <CR>, which is also
/// the segment delimiter) is suppressed from the structural mask so it does not split a leaf.
///
/// Every event -- structural delimiter or the FIRST byte of a matched message-delimiter pair -- is
/// appended to `band` as its absolute byte position, in order, with no per-event branching (see
/// flattenBlockEvents). Pair events additionally record their BAND INDEX in `pairBandIdx` (their
/// ordinal within a block is the popcount of the events below their bit). A trailing lone <FS> at
/// the buffer end is NOT a delimiter -- its <CR> may arrive in the next raw buffer; the
/// spanning-tuple reassembly reunites the pair.
///
/// Caller-provided capacity: `band` >= view.size() + 16 entries (the unconditional 8-slot batches
/// overshoot), `pairBandIdx` >= view.size() / 2 + 2 entries.
inline Hl7FlattenResult flattenHl7SimdInto(
    uint32_t* const band,
    uint32_t* const pairBandIdx,
    const std::string_view view,
    const std::array<char, 4>& structuralChars,
    const char msgFirst,
    const char msgSecond,
    const ComputeBlocksFn computeBlocks)
{
    const char* data = view.data();
    const size_t numBytes = view.size();
    size_t numEvents = 0;
    size_t numPairs = 0;

    /// SIMD region: only complete 64-byte blocks, so we never read past the buffer's valid bytes.
    const size_t numBlocks = numBytes / 64;
    /// True iff the previous block ended with an UNRESOLVED <FS> (bit 63) whose potential <CR>
    /// partner is the next block's (or the scalar tail's) first byte.
    bool pendingFirstAtBoundary = false;
    if (numBlocks > 0)
    {
        constexpr size_t chunkBlocks = 16;
        std::array<BlockBits, chunkBlocks> chunk{};
        for (size_t base = 0; base < numBlocks; base += chunkBlocks)
        {
            const size_t count = std::min(chunkBlocks, numBlocks - base);
            computeBlocks(data + (base * 64), count, chunk.data(), structuralChars, msgFirst, msgSecond);
            for (size_t i = 0; i < count; ++i)
            {
                const BlockBits bits = chunk[i];
                const size_t blockBase = (base + i) * 64;

                /// In-block delimiter pairs: <FS> at bit b with <CR> at bit b+1 (b < 63).
                const uint64_t msgPairs = bits.msgFirst & (bits.msgSecond >> 1);
                /// Suppress each matched pair's <CR> from the structural mask (it terminates the
                /// message, it does not split a leaf).
                uint64_t structural = bits.structural & ~(msgPairs << 1);

                /// Resolve the cross-boundary pair: previous block ended with <FS>, this block
                /// starts with <CR>. The delimiter position is the <FS> (last byte of the previous
                /// block); its <CR> is this block's bit 0 -- suppress it as well. Appended BEFORE
                /// this block's events, preserving position order.
                if (pendingFirstAtBoundary && (bits.msgSecond & 1) != 0)
                {
                    structural &= ~uint64_t{1};
                    band[numEvents] = static_cast<uint32_t>(blockBase - 1);
                    pairBandIdx[numPairs] = static_cast<uint32_t>(numEvents);
                    ++numPairs;
                    ++numEvents;
                }
                pendingFirstAtBoundary = (bits.msgFirst >> 63) != 0;

                const uint64_t events = structural | msgPairs;

                /// Band indexes of this block's pair events (rare next to structurals).
                uint64_t pairs = msgPairs;
                while (pairs != 0)
                {
                    const auto bit = static_cast<unsigned>(std::countr_zero(pairs));
                    const auto ordinalInBlock = static_cast<size_t>(std::popcount(events & ((uint64_t{1} << bit) - 1)));
                    pairBandIdx[numPairs] = static_cast<uint32_t>(numEvents + ordinalInBlock);
                    ++numPairs;
                    pairs &= pairs - 1;
                }

                numEvents += flattenBlockEvents(band + numEvents, blockBase, events);
            }
        }
    }

    /// Scalar tail: the remaining `numBytes % 64` bytes with identical semantics.
    std::array<bool, 256> isStructural{};
    for (const char structuralChar : structuralChars)
    {
        isStructural[static_cast<unsigned char>(structuralChar)] = true;
    }
    size_t tailIdx = numBlocks * 64;
    if (pendingFirstAtBoundary && tailIdx < numBytes && data[tailIdx] == msgSecond)
    {
        band[numEvents] = static_cast<uint32_t>(tailIdx - 1);
        pairBandIdx[numPairs] = static_cast<uint32_t>(numEvents);
        ++numPairs;
        ++numEvents;
        ++tailIdx; /// the <CR> is consumed by the delimiter
    }
    for (; tailIdx < numBytes; ++tailIdx)
    {
        const char byte = data[tailIdx];
        if (byte == msgFirst && tailIdx + 1 < numBytes && data[tailIdx + 1] == msgSecond)
        {
            band[numEvents] = static_cast<uint32_t>(tailIdx);
            pairBandIdx[numPairs] = static_cast<uint32_t>(numEvents);
            ++numPairs;
            ++numEvents;
            ++tailIdx; /// skip the <CR>
        }
        else if (isStructural[static_cast<unsigned char>(byte)])
        {
            band[numEvents] = static_cast<uint32_t>(tailIdx);
            ++numEvents;
        }
    }
    return {.numEvents = numEvents, .numPairs = numPairs};
}

/// Validates that every complete message (between consecutive delimiter pairs) splits into exactly
/// `numFields` leaves. In the flattened band a message's leaf count is the distance between the
/// band indexes of its enclosing pairs: the structural events between them, plus the first leaf.
/// Throws on the FIRST violating message, matching the scalar walk's in-order rejection.
inline void validateHl7MessageArity(const uint32_t* const pairBandIdx, const size_t numPairs, const uint64_t numFields)
{
    for (size_t k = 0; k + 1 < numPairs; ++k)
    {
        const size_t leafCount = pairBandIdx[k + 1] - pairBandIdx[k];
        if (leafCount != numFields)
        {
            throwHl7ArityMismatch(leafCount, numFields);
        }
    }
}

}
