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
#include <mutex>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Time/Timestamp.hpp>
#include <Watermark/MultiOriginWatermarkProcessor.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

/// Maps every input origin of a window-based operator to the origins of the sources of this worker whose data drives it.
using SourcesOfInputOrigins = std::unordered_map<OriginId, std::vector<OriginId>>;

/// Backpressures the sources of input origins whose watermark runs more than the maximum watermark gap ahead of the slowest input
/// origin, and releases them once the slowest origin has caught up to half the gap.
/// All member functions are thread-safe; `onWatermarkMoved` never blocks.
class WatermarkGapThrottle
{
public:
    explicit WatermarkGapThrottle(SourcesOfInputOrigins sourcesOfInputOrigins);

    /// Enables throttling if `backpressure` is set, there are several input origins and `backpressure` controls all their sources.
    /// Starting again while enabled keeps the current throttle.
    void start(std::optional<WatermarkBackpressure> backpressure);

    /// Disables throttling and releases all sources this throttle holds back.
    void stop();

    /// Call whenever the watermark of an input origin moved. A thread that finds another thread updating leaves a request for it instead
    /// of waiting, and the updating thread repeats the update until no request is left, so the last watermark change is always applied,
    /// e.g., the final watermark that releases all throttled sources.
    void onWatermarkMoved(const MultiOriginWatermarkProcessor& watermarks);

private:
    /// Requires `mutex`.
    void update(const MultiOriginWatermarkProcessor& watermarks);

    const SourcesOfInputOrigins sourcesOfInputOrigins;
    /// Set once `backpressure` is engaged, so operators without watermark gap backpressure never take the mutex.
    std::atomic<bool> enabled{false};
    /// Set by a thread that found the mutex taken; the thread holding it repeats the update.
    std::atomic<bool> requested{false};

    /// Guards all members below.
    std::mutex mutex;
    std::optional<WatermarkBackpressure> backpressure;
    std::unordered_set<OriginId> throttledInputOrigins;
    std::unordered_set<OriginId> throttledSources;
    /// Scratch space of `update`, kept to avoid allocating on every update.
    std::vector<std::pair<OriginId, Timestamp>> watermarkSnapshot;
    std::unordered_set<OriginId> sourcesToThrottle;
};

}
