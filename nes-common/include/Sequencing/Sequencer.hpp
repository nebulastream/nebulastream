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

#include <deque>
#include <map>
#include <optional>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Sequencing/SequenceData.hpp>
#include <folly/Synchronized.h>
#include <ErrorHandling.hpp>

namespace NES
{

/// Buffers values until the next sequence/chunk is available. At most one value is checked out at a time.
template <typename T>
class Sequencer
{
    using Key = std::pair<SequenceNumber::Underlying, ChunkNumber::Underlying>;

    struct State
    {
        SequenceNumber::Underlying nextSequence = SequenceNumber::INITIAL;
        ChunkNumber::Underlying nextChunk = ChunkNumber::INITIAL;
        std::optional<SequenceData> checkedOut;
        bool processing = false;
        std::map<Key, std::pair<SequenceData, T>> buffered;
    };

public:
    /// Returns the value only when it is next and no other caller is processing it. Otherwise, buffers future values.
    std::optional<T> take(SequenceData sequence, T value)
    {
        const auto state = stateLock.wlock();
        const Key key{sequence.sequenceNumber, sequence.chunkNumber};

        if (state->checkedOut)
        {
            if (sequence == *state->checkedOut && !state->processing)
            {
                state->processing = true;
                return value;
            }
            if (sequence != *state->checkedOut)
            {
                state->buffered.try_emplace(key, sequence, std::move(value));
            }
            return std::nullopt;
        }

        const Key expected{state->nextSequence, state->nextChunk};
        if (key == expected)
        {
            state->checkedOut = sequence;
            state->processing = true;
            return value;
        }
        if (key > expected)
        {
            state->buffered.try_emplace(key, sequence, std::move(value));
        }
        return std::nullopt;
    }

    /// Marks the checked-out value as processed and checks out the next buffered value, if available.
    std::optional<T> acknowledge(const SequenceData& sequence)
    {
        const auto state = stateLock.wlock();
        validateCheckedOut(*state, sequence);

        if (sequence.lastChunk)
        {
            state->nextSequence = sequence.sequenceNumber + 1;
            state->nextChunk = ChunkNumber::INITIAL;
        }
        else
        {
            state->nextChunk = sequence.chunkNumber + 1;
        }
        state->checkedOut.reset();
        state->processing = false;

        const Key expected{state->nextSequence, state->nextChunk};
        state->buffered.erase(state->buffered.begin(), state->buffered.lower_bound(expected));
        const auto next = state->buffered.find(expected);
        if (next == state->buffered.end())
        {
            return std::nullopt;
        }

        auto node = state->buffered.extract(next);
        state->checkedOut = node.mapped().first;
        state->processing = true;
        return std::move(node.mapped().second);
    }

    /// Releases processing ownership without advancing, allowing the same value to be retried.
    void defer(const SequenceData& sequence)
    {
        const auto state = stateLock.wlock();
        validateCheckedOut(*state, sequence);
        state->processing = false;
    }

private:
    static void validateCheckedOut(const State& state, const SequenceData& sequence)
    {
        PRECONDITION(
            state.checkedOut && state.processing && *state.checkedOut == sequence,
            "Expected acknowledgement for checked-out sequence {}, received {}",
            state.checkedOut.value_or(SequenceData{}),
            sequence);
    }

    folly::Synchronized<State> stateLock;
};

/// Emits values in arrival order while dropping values older than the last acknowledged value.
/// At most one value is checked out at a time; arrivals during processing wait for acknowledgement.
template <typename T>
class DropOutOfOrderSequencer
{
    struct State
    {
        std::optional<SequenceData> lastAcknowledged;
        std::optional<SequenceData> checkedOut;
        bool processing = false;
        std::deque<std::pair<SequenceData, T>> buffered;
    };

public:
    std::optional<T> take(SequenceData sequence, T value)
    {
        const auto state = stateLock.wlock();
        if (state->checkedOut)
        {
            if (sequence == *state->checkedOut && !state->processing)
            {
                state->processing = true;
                return value;
            }
            if (sequence != *state->checkedOut)
            {
                state->buffered.emplace_back(sequence, std::move(value));
            }
            return std::nullopt;
        }
        if (state->lastAcknowledged && !comesAfter(sequence, *state->lastAcknowledged))
        {
            return std::nullopt;
        }
        state->checkedOut = sequence;
        state->processing = true;
        return value;
    }

    std::optional<T> acknowledge(const SequenceData& sequence)
    {
        const auto state = stateLock.wlock();
        validateCheckedOut(*state, sequence);
        state->lastAcknowledged = sequence;
        state->checkedOut.reset();
        state->processing = false;

        while (!state->buffered.empty())
        {
            auto [candidateSequence, candidate] = std::move(state->buffered.front());
            state->buffered.pop_front();
            if (comesAfter(candidateSequence, *state->lastAcknowledged))
            {
                state->checkedOut = candidateSequence;
                state->processing = true;
                return std::move(candidate);
            }
        }
        return std::nullopt;
    }

    void defer(const SequenceData& sequence)
    {
        const auto state = stateLock.wlock();
        validateCheckedOut(*state, sequence);
        state->processing = false;
    }

private:
    static bool comesAfter(const SequenceData& candidate, const SequenceData& previous)
    {
        if (candidate.sequenceNumber != previous.sequenceNumber)
        {
            return candidate.sequenceNumber > previous.sequenceNumber;
        }
        return !previous.lastChunk && candidate.chunkNumber > previous.chunkNumber;
    }

    static void validateCheckedOut(const State& state, const SequenceData& sequence)
    {
        PRECONDITION(
            state.checkedOut && state.processing && *state.checkedOut == sequence,
            "Expected acknowledgement for checked-out sequence {}, received {}",
            state.checkedOut.value_or(SequenceData{}),
            sequence);
    }

    folly::Synchronized<State> stateLock;
};

}
