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

/// Tracks which buffers of a source the query has finished processing. Buffers complete out of order, but only the highest sequence
/// number up to which all buffers completed may be covered by a checkpoint.
class SourceCheckpointProgress
{
public:
    void noteCompletedSequence(const SequenceNumber sequenceNumber)
    {
        const std::scoped_lock lock(mutex);
        if (sequenceNumber.getRawValue() <= highestContiguousCompletedSequence)
        {
            return;
        }
        pendingSequences.insert(sequenceNumber.getRawValue());
        while (!pendingSequences.empty() && *pendingSequences.begin() == highestContiguousCompletedSequence + 1)
        {
            highestContiguousCompletedSequence = *pendingSequences.begin();
            pendingSequences.erase(pendingSequences.begin());
        }
    }

    /// INVALID_SEQ_NUMBER, if not even the first buffer completed yet.
    [[nodiscard]] SequenceNumber::Underlying getHighestContiguousCompletedSequence() const
    {
        const std::scoped_lock lock(mutex);
        return highestContiguousCompletedSequence;
    }

private:
    mutable std::mutex mutex;
    SequenceNumber::Underlying highestContiguousCompletedSequence = INVALID_SEQ_NUMBER.getRawValue();
    std::set<SequenceNumber::Underlying> pendingSequences;
};

}
