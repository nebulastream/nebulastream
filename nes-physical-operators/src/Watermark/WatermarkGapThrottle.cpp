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
#include <mutex>
#include <optional>
#include <ranges>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <Watermark/MultiOriginWatermarkProcessor.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

WatermarkGapThrottle::WatermarkGapThrottle(SourcesOfInputOrigins sourcesOfInputOrigins)
    : sourcesOfInputOrigins(std::move(sourcesOfInputOrigins))
{
}

void WatermarkGapThrottle::start(std::optional<WatermarkBackpressure> backpressure)
{
    const auto allSources = sourcesOfInputOrigins | std::views::values | std::views::join;
    if (backpressure && sourcesOfInputOrigins.size() > 1
        && std::ranges::all_of(allSources, [&](const auto source) { return backpressure->controller.controls(source); }))
    {
        const std::scoped_lock lock(mutex);
        /// Replacing the holder would release all throttled sources, as a new holder starts without holding any pressure.
        if (this->backpressure)
        {
            return;
        }
        this->backpressure = std::move(backpressure);
        enabled.store(true, std::memory_order_release);
    }
}

void WatermarkGapThrottle::stop()
{
    const std::scoped_lock lock(mutex);
    enabled.store(false, std::memory_order_release);
    /// Destroying the holder releases all pressure it holds.
    backpressure.reset();
    throttledInputOrigins.clear();
    throttledSources.clear();
}

void WatermarkGapThrottle::onWatermarkMoved(const MultiOriginWatermarkProcessor& watermarks)
{
    if (!enabled.load(std::memory_order_acquire))
    {
        return;
    }
    requested.store(true, std::memory_order_release);
    while (requested.load(std::memory_order_acquire))
    {
        const std::unique_lock lock(mutex, std::try_to_lock);
        if (!lock.owns_lock())
        {
            return; /// the holder sees the request after releasing the lock at the latest
        }
        while (requested.exchange(false, std::memory_order_acq_rel))
        {
            update(watermarks);
        }
    }
}

void WatermarkGapThrottle::update(const MultiOriginWatermarkProcessor& watermarks)
{
    /// A thread that saw the throttle enabled may get the mutex only after `stop`.
    if (!backpressure)
    {
        return;
    }
    auto& [controller, maxWatermarkGap] = *backpressure;
    watermarks.getCurrentWatermarkPerOrigin(watermarkSnapshot);
    const auto [slowestOrigin, slowest]
        = std::ranges::min(watermarkSnapshot, {}, [](const auto& originAndWatermark) { return originAndWatermark.second; });

    /// Releasing at half the gap avoids toggling on every buffer.
    for (const auto& [origin, watermark] : watermarkSnapshot)
    {
        const auto gap = watermark.getRawValue() - slowest.getRawValue();
        if (watermark.getRawValue() == Timestamp::INFINITE_VALUE || gap <= maxWatermarkGap / 2)
        {
            throttledInputOrigins.erase(origin);
        }
        else if (gap > maxWatermarkGap)
        {
            throttledInputOrigins.insert(origin);
        }
    }

    /// Sources that drive the slowest origin are never throttled, which guarantees progress even if a source drives several origins.
    const auto& sourcesOfSlowest = sourcesOfInputOrigins.at(slowestOrigin);
    sourcesToThrottle.clear();
    for (const auto origin : throttledInputOrigins)
    {
        for (const auto source : sourcesOfInputOrigins.at(origin))
        {
            if (!std::ranges::contains(sourcesOfSlowest, source))
            {
                sourcesToThrottle.insert(source);
            }
        }
    }

    for (const auto source : throttledSources)
    {
        if (!sourcesToThrottle.contains(source) && controller.releasePressure(source))
        {
            NES_DEBUG("Released watermark backpressure on source {}: slowest origin {} at {}", source, slowestOrigin, slowest);
        }
    }
    for (const auto source : sourcesToThrottle)
    {
        if (controller.applyPressure(source))
        {
            NES_DEBUG("Applied watermark backpressure on source {}: slowest origin {} at {}", source, slowestOrigin, slowest);
        }
    }
    /// Swapping keeps both allocations for the next update.
    std::swap(throttledSources, sourcesToThrottle);
}

}
