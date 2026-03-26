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

#include <Sequencing/RangeWatermarkTracker.hpp>

#include <algorithm>
#include <cstdint>
#include <vector>
#include <Sequencing/FracturedNumber.hpp>
#include <Sequencing/SequenceRange.hpp>
#include <Time/Timestamp.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

void RangeWatermarkTracker::insert(const SequenceRange& range, Timestamp watermark)
{
    PRECONDITION(range.isValid(), "Cannot insert invalid range");

    auto newStart = range.start;
    auto newEnd = range.end;
    auto newWatermark = watermark.getRawValue();

    auto locked = state_.wlock();

    /// Initialize contiguousEnd on first insert based on the range's depth.
    /// Starts at {1, 0, 0, ...} — the INITIAL range [0, 1) is implicitly consumed.
    if (!locked->contiguousEnd.has_value())
    {
        auto depth = newStart.depth();
        std::vector<uint64_t> components(depth, 0);
        components[0] = 1;
        locked->contiguousEnd = FracturedNumber(std::move(components));
    }
    else
    {
        PRECONDITION(
            newStart.depth() == locked->contiguousEnd->depth(),
            "Range depth {} does not match tracker depth {}",
            newStart.depth(),
            locked->contiguousEnd->depth());
    }

    /// Try to merge with the right neighbor: if an entry starts where we end
    auto rightIt = locked->ranges.find(newEnd);
    if (rightIt != locked->ranges.end())
    {
        newEnd = rightIt->second.end;
        newWatermark = std::max(newWatermark, rightIt->second.maxWatermark);
        locked->ranges.erase(rightIt);
    }

    /// Try to merge with the left neighbor: find the entry just before our start
    auto it = locked->ranges.lower_bound(newStart);
    if (it != locked->ranges.begin())
    {
        auto prev = std::prev(it);
        if (prev->second.end == newStart)
        {
            newStart = prev->first;
            newWatermark = std::max(newWatermark, prev->second.maxWatermark);
            locked->ranges.erase(prev);
        }
    }

    /// Insert the (possibly merged) range
    locked->ranges[newStart] = RangeEntry{newEnd, newWatermark};

    /// Advance contiguous prefix
    auto advIt = locked->ranges.find(*locked->contiguousEnd);
    while (advIt != locked->ranges.end())
    {
        locked->contiguousEnd = advIt->second.end;
        locked->watermark = std::max(locked->watermark, advIt->second.maxWatermark);
        locked->ranges.erase(advIt);
        advIt = locked->ranges.find(*locked->contiguousEnd);
    }

    /// CAS-update atomic watermark (monotonic: only increase)
    auto current = currentWatermark_.load(std::memory_order_relaxed);
    while (locked->watermark > current)
    {
        if (currentWatermark_.compare_exchange_weak(current, locked->watermark, std::memory_order_release, std::memory_order_relaxed))
        {
            break;
        }
    }
}

Timestamp RangeWatermarkTracker::getCurrentWatermark() const
{
    return Timestamp(currentWatermark_.load(std::memory_order_acquire));
}

}
