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

#include <mutex>
#include <set>
#include <Identifiers/Identifiers.hpp>

namespace NES
{

class SourceCheckpointProgress
{
public:
    void reset()
    {
        std::scoped_lock lock(mutex);
        highestContiguousCompletedSequence = INVALID_SEQ_NUMBER.getRawValue();
        completedSequences.clear();
    }

    void noteCompletedSequence(const SequenceNumber sequenceNumber)
    {
        const auto completedSequence = sequenceNumber.getRawValue();
        std::scoped_lock lock(mutex);
        if (completedSequence <= highestContiguousCompletedSequence)
        {
            return;
        }

        completedSequences.emplace(completedSequence);

        auto nextSequence = highestContiguousCompletedSequence == INVALID_SEQ_NUMBER.getRawValue()
            ? SequenceNumber::INITIAL
            : highestContiguousCompletedSequence + 1;
        auto it = completedSequences.find(nextSequence);
        while (it != completedSequences.end())
        {
            highestContiguousCompletedSequence = nextSequence;
            completedSequences.erase(it);
            nextSequence += 1;
            it = completedSequences.find(nextSequence);
        }
    }

    [[nodiscard]] SequenceNumber::Underlying getHighestContiguousCompletedSequence() const
    {
        std::scoped_lock lock(mutex);
        return highestContiguousCompletedSequence;
    }

private:
    mutable std::mutex mutex;
    SequenceNumber::Underlying highestContiguousCompletedSequence = INVALID_SEQ_NUMBER.getRawValue();
    std::set<SequenceNumber::Underlying> completedSequences;
};

}
