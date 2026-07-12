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

#include <cstddef>
#include <cstdint>
#include <new>
#include <optional>
#include <span>
#include <string>
#include <Runtime/TupleBuffer.hpp>
#include <RawTupleBuffer.hpp>
#include <SpanningTupleBufferState.hpp>

namespace NES
{

/// Fragment-copy fork of the SpanningTupleBufferEntry (see SpanningTupleBufferState.hpp).
/// Instead of pinning the raw TupleBuffer (two refcount increments) until neighbour buffers complete the
/// spanning tuples, this entry COPIES the few boundary bytes that the spanning-tuple assembly actually
/// consumes into owned strings and drops the buffer reference immediately:
/// - leadingFragment:  the HEAD bytes [0, firstDelimiterOffset) — consumed as the BACK of a leading spanning tuple
/// - trailingFragment: the TAIL bytes (lastDelimiterOffset + delimiterSize, end) — consumed as the FRONT
/// This removes the per-source frontier hold (buffer k pinned until its successor k+1 arrives) that couples
/// the required buffer-pool size to the source fan-in (the N=512 wedge).
/// Fallback (buffer stays pinned exactly as in the classic entry) in three cases:
/// - no-delimiter INTERIOR buffer: the whole content is needed, copying it would defeat the purpose
/// - the last-delimiter offset is not yet known at set time (lazy SIMDCSV offset path)
/// - a boundary fragment exceeds maxFragmentBytes (unbounded VARSIZED rows) or the offsets do not describe
///   valid byte ranges (mocked buffers in tests smuggle metadata through the offsets)
/// The AtomicState (bit layout, CAS-claim protocol, publish ordering) is reused verbatim: fragments are
/// written BEFORE the atomic state store that makes the entry visible, giving the identical happens-before
/// edge as the classic buffer-ref publish.
class alignas(std::hardware_destructive_interference_size) FragmentSpanningTupleBufferEntry /// NOLINT(readability-magic-numbers)
{
public:
    using EntryState = SpanningTupleBufferEntry::EntryState;

    FragmentSpanningTupleBufferEntry() = default;

    /// Sets the state of the first entry to a value that allows triggering the first leading spanning tuple of a stream.
    /// Unlike the classic entry, no dummy TupleBuffer is needed: an empty owned trailing fragment serves as the dummy FRONT.
    void setStateOfFirstIndex();

    /// Overwrites an invalid offset to the trailing SpanningTuple and flips the AtomicState-bit that represents a valid trailing offset.
    /// The trailing buffer reference stays pinned in this (lazy-offset) case; only the offset becomes visible.
    void setOffsetOfTrailingSpanningTuple(FieldIndex offsetOfLastTuple);

    /// A thread can claim a spanning tuple by claiming the first buffer of the spanning tuple (SpanningTuple).
    /// Multiple threads may concurrently call 'tryClaimSpanningTuple()', but only one thread can successfully claim the SpanningTuple.
    /// The winner receives either the owned TAIL fragment or (fallback) the pinned buffer as a StagedBuffer.
    std::optional<StagedBuffer> tryClaimSpanningTuple(ABAItNo abaItNumber);

    /// Checks if the current entry was used to construct both a leading and trailing spanning tuple (given it matches abaItNumber - 1)
    [[nodiscard]] bool isCurrentEntryUsedUp(ABAItNo abaItNumber) const;

    /// Sets the indexed buffer as the new entry, if the prior entry is used up and its expected ABA iteration number is (abaItNumber - 1)
    /// Copies the HEAD/TAIL fragments (or pins the buffer, see class comment) before publishing the new atomic state.
    bool trySetWithDelimiter(ABAItNo abaItNumber, const StagedBuffer& indexedBuffer, size_t delimiterSize, size_t maxFragmentBytes);
    bool trySetWithoutDelimiter(ABAItNo abaItNumber, const StagedBuffer& indexedBuffer);

    /// Claim a buffer without a delimiter (that connects two buffers with delimiters), taking the pinned buffer and
    /// atomically setting both uses at once (interior buffers are always fallback-pinned, never fragments)
    void claimNoDelimiterBuffer(std::span<StagedBuffer> spanningTupleVector, size_t spanningTupleIdx);

    /// Claim the leading use of a buffer with a delimiter, taking the HEAD fragment (or the pinned fallback buffer)
    void claimLeadingBuffer(std::span<StagedBuffer> spanningTupleVector, size_t spanningTupleIdx);

    /// Atomically loads the state of an entry, checks if its ABA iteration number matches the expected and if it has a tuple delimiter
    [[nodiscard]] EntryState getEntryState(ABAItNo expectedABAItNo) const;

    /// Iterates over all FragmentSpanningTupleBufferEntries, checking that they don't hold any buffer references if they should not
    /// and that their atomic bitmap state is correct. Logs errors and returns 'false' if at least one entry is in an invalid state
    [[nodiscard]] bool validateFinalState(
        SpanningTupleBufferIdx bufferIdx, const FragmentSpanningTupleBufferEntry& nextEntry, SpanningTupleBufferIdx lastIdxOfBuffer) const;

private:
    /// HEAD bytes [0, firstDelimiterOffset); consumed by claimLeadingBuffer; valid iff leadingFallbackRef is empty
    std::string leadingFragment;
    /// TAIL bytes after the last delimiter; consumed by tryClaimSpanningTuple; valid iff trailingFallbackRef is empty
    std::string trailingFragment;
    /// Pinned raw buffer for the respective role when the fragment could not be captured (see class comment)
    TupleBuffer leadingFallbackRef;
    TupleBuffer trailingFallbackRef;
    AtomicState atomicState;
    uint32_t firstDelimiterOffset = 0;
    uint32_t lastDelimiterOffset = 0;
};

static_assert(sizeof(FragmentSpanningTupleBufferEntry) % std::hardware_destructive_interference_size == 0);
static_assert(alignof(FragmentSpanningTupleBufferEntry) % std::hardware_destructive_interference_size == 0);
}
