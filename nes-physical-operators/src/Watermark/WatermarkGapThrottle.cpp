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
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <ranges>
#include <span>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <Watermark/MultiOriginWatermarkProcessor.hpp>
#include <BackpressureManager.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

WatermarkGapThrottle::WatermarkGapThrottle(std::vector<OriginId> inputOrigins, const uint64_t maxWatermarkGap)
    : inputOrigins(std::move(inputOrigins))
    , maxWatermarkGap(maxWatermarkGap)
    , holds(this->inputOrigins.size())
    , latest(this->inputOrigins.size(), Timestamp(Timestamp::INITIAL_VALUE))
    , held(this->inputOrigins.size())
    , shouldHold(this->inputOrigins.size(), false)
    , snapshot(this->inputOrigins.size(), Timestamp(Timestamp::INITIAL_VALUE))
{
    PRECONDITION(this->inputOrigins.size() > 1, "A watermark gap throttle requires several input origins");
    PRECONDITION(maxWatermarkGap > 0, "A watermark gap throttle requires a maximum watermark gap greater than 0");
}

void WatermarkGapThrottle::start(InternalBackpressureVoter voter)
{
    const std::scoped_lock lock(mutex);
    PRECONDITION(!this->voter, "A watermark gap throttle accepts only one start");
    this->voter = std::move(voter);
}

void WatermarkGapThrottle::stop()
{
    const std::scoped_lock lock(mutex);
    voter.reset();
}

bool WatermarkGapThrottle::mayChangeVotes(
    const OriginId updated, const Timestamp origin, const Timestamp slowest, const bool originAdvanced, const bool slowestAdvanced) const
{
    if (slowestAdvanced)
    {
        return true;
    }
    if (!originAdvanced || origin.getRawValue() - slowest.getRawValue() <= maxWatermarkGap)
    {
        return false;
    }
    /// The origin is more than the gap ahead. If the throttle holds it already, only its end changes the vote.
    const auto index = std::ranges::find(inputOrigins, updated) - inputOrigins.begin();
    return origin.getRawValue() == Timestamp::INFINITE_VALUE || !holds[index].load(std::memory_order::relaxed);
}

void WatermarkGapThrottle::onWatermarksMoved(const std::span<const Timestamp> watermarks)
{
    const std::scoped_lock lock(mutex);
    updateVotes(watermarks);
}

void WatermarkGapThrottle::onWatermarksMoved(const MultiOriginWatermarkProcessor& watermarks)
{
    const std::scoped_lock lock(mutex);
    watermarks.getCurrentWatermarks(snapshot);
    updateVotes(snapshot);
}

void WatermarkGapThrottle::updateVotes(const std::span<const Timestamp> watermarks)
{
    PRECONDITION(
        watermarks.size() == inputOrigins.size(),
        "The throttle tracks {} input origins, but received {} watermarks",
        inputOrigins.size(),
        watermarks.size());
    PRECONDITION(voter.has_value(), "The watermark of an input origin moved outside of start and stop");
    for (auto&& [current, received] : std::views::zip(latest, watermarks))
    {
        current = std::max(current, received);
    }
    /// The slowest origin comes from the same values as the gaps, so it has a gap of 0 and is always released.
    const auto slowest = std::ranges::min(latest).getRawValue();

    for (size_t index = 0; index < inputOrigins.size(); ++index)
    {
        const auto watermark = latest[index].getRawValue();
        /// An origin that is done is waited on: its sources are done and cannot be blocked anyway.
        const bool done = watermark == Timestamp::INFINITE_VALUE;
        const auto gap = watermark - slowest;
        const bool wasHeld = held[index] == true;
        shouldHold[index] = !done && (wasHeld ? gap > maxWatermarkGap / 2 : gap > maxWatermarkGap);
    }

    /// Cast the new releases first, so the window always waits on some input while its votes change.
    for (const bool hold : {false, true})
    {
        for (size_t index = 0; index < inputOrigins.size(); ++index)
        {
            if (shouldHold[index] != hold || held[index] == hold)
            {
                continue;
            }
            const auto origin = inputOrigins[index];
            if (hold)
            {
                voter->applyBackpressureTo(origin);
                NES_DEBUG("Watermark gap: hold origin {}, slowest at {}", origin, slowest);
                bpstat::event("hold", origin.getRawValue(), latest[index].getRawValue(), slowest);
            }
            else
            {
                voter->releaseBackpressureTo(origin);
                bpstat::event("release", origin.getRawValue(), latest[index].getRawValue(), slowest);
            }
            held[index] = hold;
            holds[index].store(hold, std::memory_order::relaxed);
        }
    }
}

}
