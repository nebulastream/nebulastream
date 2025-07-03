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
#include <optional>
#include <queue>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Sequencing/SequenceData.hpp>
#include <folly/Synchronized.h>
#include <ErrorHandling.hpp>

namespace NES
{
/// The Sequencer enforces that SequenceData objects are processed in sequence order.
/// It manages both sequence numbers and chunk numbers within sequences.
///
/// Canonical usage:
/// ```
/// auto dataToProcess = sequencer.isNext(sequenceData, value);
/// while(dataToProcess) {
///     processData(*dataToProcess);
///     dataToProcess = sequencer.advanceAndGetNext(sequenceData);
/// }
/// ```
template <typename T>
class Sequencer
{
public:
    /// Checks if the given SequenceData is the next expected in the sequence.
    ///
    /// @param sequence The sequence metadata to check
    /// @param data The data payload associated with this sequence element
    /// @return If the given SequenceData is the next expected element, returns the value wrapped
    ///         in an optional. If not, stores the data internally and returns an empty optional.
    ///         Note that this method does not advance the sequence counter.
    std::optional<T> isNext(SequenceData sequence, T data)
    {
        auto state = stateMtx.ulock();
        if (sequence.sequenceNumber == state->nextSequence && sequence.chunkNumber == state->nextChunkNumber)
        {
            /// This is the correct buffer
            /// However we don't update the counter yet
            return data;
        }
        auto stateWrite = std::move(state).moveFromUpgradeToWrite();
        stateWrite->queue.emplace(sequence, data);
        return {};
    }

    /// Marks the current SequenceData as processed and advances the sequence counter.
    ///
    /// @param data The sequence metadata of the current element being marked as processed
    /// @return If the next expected sequence element has already been received, returns it
    ///         wrapped in an optional. If the next element is not yet available, returns an empty optional.
    /// @throws PreconditionException if the provided data doesn't match the expected next chunk
    std::optional<T> advanceAndGetNext(SequenceData data)
    {
        auto state = stateMtx.wlock();
        PRECONDITION(
            state->nextChunkNumber == data.chunkNumber && data.sequenceNumber == state->nextSequence,
            "advance was called wtih invalid sequence. Expected: {} but received: {}",
            SequenceData(SequenceNumber(state->nextSequence), ChunkNumber(state->nextChunkNumber), false),
            data);
        if (data.lastChunk)
        {
            state->nextSequence = data.sequenceNumber + 1;
            state->nextChunkNumber = ChunkNumber::INITIAL;
        }
        else
        {
            state->nextChunkNumber = data.chunkNumber + 1;
        }
        if (!state->queue.empty() && state->queue.top().first.sequenceNumber == state->nextSequence
            && state->queue.top().first.chunkNumber == state->nextChunkNumber)
        {
            auto next = state->queue.top().second;
            state->queue.pop();
            return next;
        }
        return {};
    }

private:
    struct CompareQueueElements
    {
        bool operator()(const std::pair<SequenceData, T>& lhs, const std::pair<SequenceData, T>& rhs) { return rhs.first < lhs.first; }
    };
    /// Internal state for the Sequencer
    struct State
    {
        SequenceNumber::Underlying nextSequence = SequenceNumber::INITIAL; /// The next expected sequence number
        ChunkNumber::Underlying nextChunkNumber = ChunkNumber::INITIAL; /// The next expected chunk number within the current sequence
        std::priority_queue<std::pair<SequenceData, T>, std::vector<std::pair<SequenceData, T>>, CompareQueueElements>
            queue; /// Queue of out-of-order elements
    };
    folly::Synchronized<State> stateMtx{}; /// Thread-safe state management
};
}
