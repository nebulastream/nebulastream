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
/// All member functions are thread-safe; concurrent watermark notifications are coalesced.
class WatermarkGapThrottle
{
public:
    explicit WatermarkGapThrottle(SourcesOfInputOrigins sourcesOfInputOrigins);

    /// Enables throttling if `backpressure` is set, there are several input origins with at least one source, and `backpressure` controls
    /// all their sources. Lowering maps every input origin to no sources for a window that must not throttle.
    /// Starting again while enabled keeps the current throttle.
    void start(std::optional<WatermarkBackpressure> backpressure);

    /// Disables throttling and releases all sources this throttle holds back.
    void stop();

    /// Call whenever an input origin's watermark moved, always with this handler's same watermark processor. A concurrent caller
    /// records pending work and returns; the elected updater drains it before atomically releasing ownership. The updater may wait
    /// for the mutex and perform several updates, so this does not guarantee bounded caller latency.
    void onWatermarkMoved(const MultiOriginWatermarkProcessor& watermarks);

private:
    /// Requires `mutex`.
    void update(const MultiOriginWatermarkProcessor& watermarks);

    const SourcesOfInputOrigins sourcesOfInputOrigins;
    /// Set once `backpressure` is engaged, so operators without watermark gap backpressure never take the mutex.
    std::atomic<bool> enabled{false};
    static constexpr unsigned BUSY = 1;
    static constexpr unsigned PENDING = 2;
    /// Ownership and pending work share one atomic modification order: idle (0), BUSY, or BUSY | PENDING.
    std::atomic<unsigned> workState{0};

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
