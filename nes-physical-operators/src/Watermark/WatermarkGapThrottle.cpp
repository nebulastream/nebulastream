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
    /// Destroying the voter withdraws all its votes.
    voter.reset();
    votes.clear();
}

void WatermarkGapThrottle::onWatermarkMoved(const MultiOriginWatermarkProcessor& watermarks)
{
    /// All operations on `requested` and `busy` are sequentially consistent, see `busy`.
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
    const auto slowest = std::ranges::min(
        inputOrigins | std::views::transform([&](const auto origin) { return watermarks.getCurrentWatermark(origin).getRawValue(); }));

    for (const auto origin : inputOrigins)
    {
        const auto watermark = watermarks.getCurrentWatermark(origin).getRawValue();
        const auto current = votes.contains(origin) ? votes.at(origin) : SoftVote::NoOpinion;
        const auto gap = watermark - slowest;
        auto next = current;
        if (watermark != Timestamp::INFINITE_VALUE && gap == 0)
        {
            next = SoftVote::Release;
        }
        else if (watermark != Timestamp::INFINITE_VALUE && gap > maxWatermarkGap)
        {
            next = SoftVote::Apply;
        }
        else if (watermark == Timestamp::INFINITE_VALUE || gap <= maxWatermarkGap / 2 || current == SoftVote::Release)
        {
            next = SoftVote::NoOpinion;
        }
        if (next == current)
        {
            continue;
        }

        switch (next)
        {
            case SoftVote::Apply:
                voter->applyBackpressureTo(origin);
                votes.insert_or_assign(origin, next);
                NES_DEBUG("Watermark gap: apply backpressure to origin {} at {}, slowest at {}", origin, watermark, slowest);
                break;
            case SoftVote::Release:
                voter->releaseBackpressureTo(origin);
                votes.insert_or_assign(origin, next);
                break;
            case SoftVote::NoOpinion:
                voter->withdrawVote(origin);
                votes.erase(origin);
                NES_DEBUG("Watermark gap: withdraw vote on origin {} at {}, slowest at {}", origin, watermark, slowest);
                break;
        }
    }
}

}
