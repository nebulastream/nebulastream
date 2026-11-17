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

/// Votes for backpressure on the input origins of a window-based operator, based on the gap between their watermarks.
/// The throttle applies to an input origin that is more than `maxWatermarkGap` ahead of the slowest.
/// It holds that origin until the gap is at most half of `maxWatermarkGap`, so the vote does not toggle on every buffer.
/// It releases every other input origin, so the operator always waits on at least its slowest input origin.
/// All member functions are thread-safe, and `onWatermarkMoved` never blocks.
class WatermarkGapThrottle
{
public:
    /// Requires several input origins and a `maxWatermarkGap` greater than 0.
    WatermarkGapThrottle(std::vector<OriginId> inputOrigins, uint64_t maxWatermarkGap);

    /// Starting again while started keeps the current voter, as a new voter has no votes.
    void start(BackpressureVoter voter);

    /// Withdraws all votes.
    /// Notifications until the next `start` have no effect.
    void stop();

    /// Call whenever the watermark of an input origin moved.
    /// A thread that finds another thread updating leaves a request for it instead of waiting.
    /// The updating thread repeats the update until no request is left, so the last watermark change is always applied.
    void onWatermarkMoved(const MultiOriginWatermarkProcessor& watermarks);

private:
    /// Requires `busy`.
    void update(const MultiOriginWatermarkProcessor& watermarks);

    const std::vector<OriginId> inputOrigins;
    const uint64_t maxWatermarkGap;
    /// Set by every notification.
    /// The thread holding `busy` repeats the update until it is clear.
    std::atomic<bool> requested{false};

    /// Guards all members below.
    /// `requested` and `busy` must use sequentially consistent operations.
    /// A notifier writes `requested` and then reads `busy`, while the holder writes `busy` and then reads `requested`.
    /// With weaker orders, both can read the old value, and the notification is lost.
    /// NOTE: A mutex does not work here, as `try_lock` may fail spuriously.
    std::atomic<bool> busy{false};
    /// Set between `start` and `stop`.
    std::optional<BackpressureVoter> voter;
    /// Whether the throttle holds each input origin it has voted on.
    std::unordered_map<OriginId, bool> held;
};

}
