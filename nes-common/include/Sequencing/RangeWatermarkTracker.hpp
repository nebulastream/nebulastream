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

#include <atomic>
#include <cstdint>
#include <list>
#include <map>
#include <mutex>
#include <optional>
#include <Sequencing/FracturedNumber.hpp>
#include <Sequencing/SequenceRange.hpp>
#include <Time/Timestamp.hpp>
#include <folly/Synchronized.h>

#include "ErrorHandling.hpp"

namespace NES
{

class SequenceRangeTracker
{
    mutable std::mutex mutex;
    std::list<SequenceRange> ranges;

    void fixup()
    {
        // ranges.sort();
        auto pred = ranges.begin();
        auto succ = ++ranges.begin();

        while (succ != ranges.end())
        {
            if (pred->isAdjacentTo(*succ))
            {
                pred->end = succ->end;
                succ = ranges.erase(succ);
            }
            else
            {
                pred = succ;
                succ = ++pred;
            }
        }
    }

public:
    SequenceRangeTracker(size_t depth) : ranges({SequenceRange::initial(depth)}) { }

    void completeRange(const SequenceRange& range)
    {
        std::scoped_lock lock(mutex);

        auto it = std::ranges::lower_bound(ranges, range);
        INVARIANT(it == ranges.end() || !it->overlap(range), "Ranges should never overlap");
        INVARIANT(!std::prev(it)->overlap(range), "Ranges should never overlap");
        /// It points to successor of range, without actually inserting it
        if (std::prev(it)->isAdjacentTo(range))
        {
            std::prev(it)->end = range.end;
            if (it != ranges.end() && range.isAdjacentTo(*it))
            {
                /// closing gap to successor
                std::prev(it)->end = it->end;
                ranges.erase(it);
            }
            return;
        }

        if (it != ranges.end() && range.isAdjacentTo(*it))
        {
            /// closing gap
            it->start = range.start;
            return;
        }

        ranges.insert(it, range);
    }

    FracturedNumber completedUpTo() const

    {
        std::scoped_lock lock(mutex);
        return ranges.begin()->end;
    }

    FracturedNumber watermark() const
    {
        std::scoped_lock lock(mutex);
        return ranges.back().end;
    }
};

/// Tracks the contiguous watermark for a single origin using SequenceRanges.
/// Ranges arrive out-of-order and are merged; the watermark advances as the
/// contiguous prefix from the initial position grows.
/// Reads of the current watermark are lock-free via std::atomic.
class RangeWatermarkTracker
{
public:
    /// Insert a range with its associated watermark timestamp.
    void insert(const SequenceRange& range, Timestamp watermark);

    /// Returns the current watermark (lock-free atomic read).
    [[nodiscard]] Timestamp getCurrentWatermark() const;

private:
    struct RangeEntry
    {
        FracturedNumber end;
        Timestamp::Underlying maxWatermark;
    };

    struct State
    {
        /// Sorted map of non-overlapping ranges: start -> (end, maxWatermark)
        std::map<FracturedNumber, RangeEntry> ranges;
        /// End of the contiguous prefix from the start; nullopt until first insert
        std::optional<FracturedNumber> contiguousEnd;
        Timestamp::Underlying watermark = 0;
    };

    folly::Synchronized<State> state_;
    std::atomic<Timestamp::Underlying> currentWatermark_{0};
};
}
