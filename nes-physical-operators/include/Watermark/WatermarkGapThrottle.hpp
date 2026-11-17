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
#include <optional>
#include <unordered_map>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Watermark/MultiOriginWatermarkProcessor.hpp>
#include <BackpressureManager.hpp>

namespace NES
{

/// Votes for backpressure on the input origins of a window-based operator, based on the gap between their watermarks:
///  - The slowest input origin: release, as the operator waits for it.
///  - An input origin more than `maxWatermarkGap` ahead of the slowest: apply. It keeps the vote until the gap is at most half of
///    `maxWatermarkGap`, which avoids toggling on every buffer.
///  - All other input origins, and input origins that are done: no opinion.
/// The backpressure manager translates the votes for an input origin to the sources that drive it.
/// All member functions are thread-safe; `onWatermarkMoved` never blocks.
class WatermarkGapThrottle
{
public:
    /// Requires several input origins and a `maxWatermarkGap` greater than 0.
    WatermarkGapThrottle(std::vector<OriginId> inputOrigins, uint64_t maxWatermarkGap);

    /// Starts voting with `voter`. Starting again while started keeps the current voter, as a new voter starts without any votes.
    void start(BackpressureVoter voter);

    /// Withdraws all votes. Notifications until the next `start` have no effect.
    void stop();

    /// Call whenever the watermark of an input origin moved. A thread that finds another thread updating leaves a request for it instead
    /// of waiting, and the updating thread repeats the update until no request is left, so the last watermark change is always applied,
    /// e.g., the final watermark that withdraws all votes.
    void onWatermarkMoved(const MultiOriginWatermarkProcessor& watermarks);

private:
    /// Requires `busy`.
    void update(const MultiOriginWatermarkProcessor& watermarks);

    const std::vector<OriginId> inputOrigins;
    const uint64_t maxWatermarkGap;
    /// Set by every notification. The thread holding `busy` repeats the update until it is clear.
    std::atomic<bool> requested{false};

    /// Guards all members below. Only `start` and `stop` wait for it; `onWatermarkMoved` leaves a request instead.
    /// `requested` and `busy` must use sequentially consistent operations: a notifier writes `requested` and then reads `busy`, while the
    /// holder writes `busy` and then reads `requested`. With weaker orders, both can read the old value, and the notification is lost.
    /// A mutex does not work here, as `try_lock` may fail spuriously.
    std::atomic<bool> busy{false};
    /// Set between `start` and `stop`.
    std::optional<BackpressureVoter> voter;
    /// The current vote for every input origin with an opinion.
    std::unordered_map<OriginId, SoftVote> votes;
};

}
