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

#include <Watermark/WatermarkGapThrottle.hpp>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <ranges>
#include <thread>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <Watermark/MultiOriginWatermarkProcessor.hpp>
#include <BackpressureManager.hpp>
#include <ErrorHandling.hpp>
#include <scope_guard.hpp>

namespace NES
{

namespace
{
/// Only `start` and `stop` wait, and an update is short.
void acquire(std::atomic<bool>& busy)
{
    while (busy.exchange(true))
    {
        std::this_thread::yield();
    }
}
}

WatermarkGapThrottle::WatermarkGapThrottle(std::vector<OriginId> inputOrigins, const uint64_t maxWatermarkGap)
    : inputOrigins(std::move(inputOrigins)), maxWatermarkGap(maxWatermarkGap)
{
    PRECONDITION(this->inputOrigins.size() > 1, "A watermark gap throttle requires several input origins");
    PRECONDITION(maxWatermarkGap > 0, "A watermark gap throttle requires a maximum watermark gap greater than 0");
}

void WatermarkGapThrottle::start(BackpressureVoter voter)
{
    acquire(busy);
    SCOPE_EXIT
    {
        busy.store(false);
    };
    if (!this->voter)
    {
        this->voter = std::move(voter);
    }
}

void WatermarkGapThrottle::stop()
{
    acquire(busy);
    SCOPE_EXIT
    {
        busy.store(false);
    };
    voter.reset();
    held.clear();
}

void WatermarkGapThrottle::onWatermarkMoved(const MultiOriginWatermarkProcessor& watermarks)
{
    requested.store(true);
    while (requested.load())
    {
        if (busy.exchange(true))
        {
            return; /// the holder reads `requested` again after it releases `busy`
        }
        SCOPE_EXIT
        {
            busy.store(false);
        };
        while (requested.exchange(false))
        {
            update(watermarks);
        }
    }
}

void WatermarkGapThrottle::update(const MultiOriginWatermarkProcessor& watermarks)
{
    /// Watermarks may move before `start` and after `stop`.
    if (!voter)
    {
        return;
    }
    /// Read every watermark once.
    /// Watermarks move concurrently, and a second read could find the slowest origin ahead of the minimum.
    /// That would withdraw its release without a new one and leave the window without a wait.
    std::vector<std::pair<OriginId, uint64_t>> snapshot;
    snapshot.reserve(inputOrigins.size());
    for (const auto origin : inputOrigins)
    {
        snapshot.emplace_back(origin, watermarks.getCurrentWatermark(origin).getRawValue());
    }
    const auto slowest = std::ranges::min(snapshot | std::views::values);

    std::vector<std::pair<OriginId, bool>> changes;
    for (const auto& [origin, watermark] : snapshot)
    {
        const auto current = held.find(origin);
        /// An origin that is done is waited on: its sources are done and cannot be blocked anyway.
        const bool done = watermark == Timestamp::INFINITE_VALUE;
        const auto gap = watermark - slowest;
        const bool wasHeld = current != held.end() && current->second;
        const bool hold = !done && (wasHeld ? gap > maxWatermarkGap / 2 : gap > maxWatermarkGap);
        if (current == held.end() || current->second != hold)
        {
            changes.emplace_back(origin, hold);
        }
    }
    /// Cast the new releases first, so the window always waits on some input while its votes change.
    std::ranges::stable_partition(changes, [](const auto& change) { return !change.second; });

    for (const auto& [origin, hold] : changes)
    {
        if (hold)
        {
            voter->applyBackpressureTo(origin);
            NES_DEBUG("Watermark gap: hold origin {}, slowest at {}", origin, slowest);
        }
        else
        {
            voter->releaseBackpressureTo(origin);
        }
        held.insert_or_assign(origin, hold);
    }
}

}
