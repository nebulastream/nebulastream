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
#include <chrono>
#include <iostream>
#include <unordered_map>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <ranges>
#include <unordered_set>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <BackpressureChannel.hpp>
#include <PipelineExecutionContext.hpp>
#include <WindowBasedOperatorHandler.hpp>

namespace NES
{

/// Extends a window-based operator handler with backpressure on the sources of input origins whose watermark is more than
/// `maxWatermarkGap` ahead of the slowest input origin, and releases them once the origin is within half of the gap.
/// This bounds the state that is kept until windows can be triggered.
/// Without this wrapper, a handler has no backpressure and pays nothing for it, see makeWindowOperatorHandler.
template <typename Handler>
class WithWatermarkBackpressure final : public Handler
{
public:
    template <typename... Args>
    explicit WithWatermarkBackpressure(const uint64_t maxWatermarkGap, Args&&... args)
        : Handler(std::forward<Args>(args)...), maxWatermarkGap(maxWatermarkGap)
    {
    }

    /// Temporary diagnostics: how often and for how long each source was throttled.
    ~WithWatermarkBackpressure() override
    {
        const auto now = std::chrono::steady_clock::now();
        for (const auto& [source, since] : throttleStart)
        {
            stats[source].blocked += now - since;
        }
        for (const auto& [source, stat] : stats)
        {
            std::cerr << "[WatermarkBackpressure] source " << source << ": throttled " << stat.applied << " times, blocked "
                      << std::chrono::duration_cast<std::chrono::milliseconds>(stat.blocked).count() << " ms, evaluations " << evaluations
                      << "\n";
        }
    }

    void start(PipelineExecutionContext& pipelineExecutionContext) override
    {
        Handler::start(pipelineExecutionContext);

        auto controller = pipelineExecutionContext.getBackpressureController();
        const auto allSources = this->sourcesOfInputOrigins | std::views::values | std::views::join;
        if (controller && this->sourcesOfInputOrigins.size() > 1
            && std::ranges::all_of(allSources, [&](const auto source) { return controller->controls(source); }))
        {
            const std::scoped_lock lock(mutex);
            backpressureController = std::move(controller);
        }
    }

    /// Overrides the already virtual checkAndTriggerWindows, so that backpressure adds no further virtual call.
    void checkAndTriggerWindows(const BufferMetaData& bufferMetaData, PipelineExecutionContext* pipelineCtx) override
    {
        const auto newGlobalWatermark = this->updateBuildWatermark(bufferMetaData);
        /// The gap between the origins also changes when the global watermark does not advance.
        applyBackpressure();
        this->triggerWindows(newGlobalWatermark, pipelineCtx);
    }

private:
    void applyBackpressure()
    {
        const std::scoped_lock lock(mutex);
        if (!backpressureController)
        {
            return;
        }
        ++evaluations;
        const auto watermarks = this->watermarkProcessorBuild->getCurrentWatermarkPerOrigin();
        const auto [slowestOrigin, slowest]
            = std::ranges::min(watermarks, {}, [](const auto& originAndWatermark) { return originAndWatermark.second; });

        /// Releasing at half the gap avoids toggling on every buffer.
        for (const auto& [origin, watermark] : watermarks)
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
        const auto& sourcesOfSlowest = this->sourcesOfInputOrigins.at(slowestOrigin);
        std::unordered_set<OriginId> sourcesToThrottle;
        for (const auto origin : throttledInputOrigins)
        {
            for (const auto source : this->sourcesOfInputOrigins.at(origin))
            {
                if (!std::ranges::contains(sourcesOfSlowest, source))
                {
                    sourcesToThrottle.insert(source);
                }
            }
        }

        for (const auto source : throttledSources)
        {
            if (!sourcesToThrottle.contains(source) && backpressureController->releasePressure(source))
            {
                stats[source].blocked += std::chrono::steady_clock::now() - throttleStart[source];
                throttleStart.erase(source);
                NES_DEBUG("Released watermark backpressure on source {}: slowest origin {} at {}", source, slowestOrigin, slowest);
            }
        }
        for (const auto source : sourcesToThrottle)
        {
            if (backpressureController->applyPressure(source))
            {
                ++stats[source].applied;
                throttleStart[source] = std::chrono::steady_clock::now();
                NES_DEBUG("Applied watermark backpressure on source {}: slowest origin {} at {}", source, slowestOrigin, slowest);
            }
        }
        throttledSources = std::move(sourcesToThrottle);
    }

    const uint64_t maxWatermarkGap;
    /// Guards the controller and the throttled origins and sources.
    std::mutex mutex;
    std::optional<BackpressureController> backpressureController;
    std::unordered_set<OriginId> throttledInputOrigins;
    std::unordered_set<OriginId> throttledSources;
    struct ThrottleStat
    {
        uint64_t applied = 0;
        std::chrono::steady_clock::duration blocked{};
    };
    std::unordered_map<OriginId, ThrottleStat> stats;
    std::unordered_map<OriginId, std::chrono::steady_clock::time_point> throttleStart;
    uint64_t evaluations = 0;
};

/// A `maxWatermarkGap` of 0 disables watermark backpressure, the handler is then used as is.
template <typename Handler, typename... Args>
std::shared_ptr<Handler> makeWindowOperatorHandler(const uint64_t maxWatermarkGap, Args&&... args)
{
    if (maxWatermarkGap == 0)
    {
        return std::make_shared<Handler>(std::forward<Args>(args)...);
    }
    return std::make_shared<WithWatermarkBackpressure<Handler>>(maxWatermarkGap, std::forward<Args>(args)...);
}

}
