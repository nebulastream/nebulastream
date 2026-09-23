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
#include <functional>
#include <map>
#include <string>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Sequencing/ChunkCollector.hpp>
#include <Sequencing/SequenceData.hpp>
#include <Time/Timestamp.hpp>
#include <folly/Synchronized.h>
#include <ErrorHandling.hpp>

namespace NES::Sequencing
{

template <class T>
class NonBlockingMonotonicSeqQueue
{
private:
    struct Container
    {
        T value;
        SequenceNumber predecessor;
        bool isHead;
    };

    using Map = std::map<SequenceNumber::Underlying, Container>;

public:
    NonBlockingMonotonicSeqQueue() = default;
    ~NonBlockingMonotonicSeqQueue() = default;

    void emplace(SequenceData sequenceData, T newValue)
    {
        if (auto result = chunks.collect(sequenceData, Timestamp(newValue)))
        {
            insertAndCompact(result->first, static_cast<T>(result->second.getRawValue()), SequenceNumber(sequenceData.predecessor));
        }
    }

    /// @brief Returns the current value.
    T getCurrentValue()
    {
        auto wlocked = containers.wlock();
        if (wlocked->empty())
        {
            return T{};
        }
        auto& head = wlocked->begin()->second;
        if (!head.isHead)
        {
            return T{};
        }

        return head.value;
    }

private:
    void insertAndCompact(SequenceNumber sequenceNumber, T value, SequenceNumber predecessor)
    {
        auto wlocked = containers.wlock();
        bool isHead = predecessor.getRawValue() == SequenceNumber::INVALID;
        auto it = wlocked->insert_or_assign(sequenceNumber.getRawValue(), Container{value, predecessor, isHead}).first;
        compact(*wlocked, it);
    }

    static void compact(Map& map, Map::iterator it)
    {
        auto current = it;

        // Connect to next sequence number, if possible
        auto next = std::next(current);
        if (next != map.end() && current->first >= next->second.predecessor.getRawValue())
        {
            mergeInto(next->second, current->second);
            current = map.erase(current);
        }

        // Connect to previous sequence number, if possible
        if (current != map.begin())
        {
            auto prev = std::prev(current);
            if (prev->first >= current->second.predecessor.getRawValue())
            {
                mergeInto(current->second, prev->second);
                map.erase(prev);
            }
        }
    }

    static void mergeInto(Container& survivor, Container& other)
    {
        survivor.isHead = survivor.isHead || other.isHead;
        survivor.predecessor = std::min(survivor.predecessor, other.predecessor);
    }

    folly::Synchronized<Map> containers;
    ChunkCollector chunks;
};

}
