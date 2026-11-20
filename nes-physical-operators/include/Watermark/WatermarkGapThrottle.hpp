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
#include <atomic>
#include <cstdint>
#include <initializer_list>
#include <mutex>
#include <optional>
#include <span>
#include <variant>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Time/Timestamp.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <BackpressureManager.hpp>

namespace NES
{

/// Returns `maxWatermarkGap`, or 0 (no throttle) if one of the time characteristics of the operator is ingestion time.
/// The throttle works only if a held source keeps its watermark, so that the slowest input origin can catch up.
/// With event time, the watermark comes from the data, and a blocked source does not advance it.
/// With ingestion time, the watermark is the wall clock at the time the source emits a buffer.
/// A blocked source continues to fall behind the wall clock, and its next buffer is ahead by more than the gap again.
/// The gap then comes from the wall clock and from how often the inputs deliver a watermark, e.g., one time per slide for the output
/// of a window. Backpressure cannot close this gap. It would only starve the source.
inline uint64_t
watermarkGapFor(const uint64_t maxWatermarkGap, const std::initializer_list<Windowing::BoundTimeCharacteristic> characteristics)
{
    const bool usesIngestionTime = std::ranges::any_of(
        characteristics,
        [](const auto& characteristic) { return std::holds_alternative<Windowing::IngestionTimeCharacteristic>(characteristic); });
    return usesIngestionTime ? 0 : maxWatermarkGap;
}

class MultiOriginWatermarkProcessor;

/// Votes for backpressure on the input origins of a window-based operator, based on the gap between their watermarks.
/// The throttle applies to an input origin that is more than `maxWatermarkGap` ahead of the slowest.
/// It holds that origin until the gap is at most half of `maxWatermarkGap`, so the vote does not toggle on every buffer.
/// It releases every other input origin, so the operator always waits on at least its slowest input origin.
/// All member functions are thread-safe.
class WatermarkGapThrottle
{
public:
    /// Requires several input origins and a `maxWatermarkGap` greater than 0.
    WatermarkGapThrottle(std::vector<OriginId> inputOrigins, uint64_t maxWatermarkGap);

    /// Call once, before the first notification.
    void start(InternalBackpressureVoter voter);

    /// Withdraws all votes.
    /// Call once, after the last notification.
    void stop();

    /// Returns false if no vote can change after an update of the watermark of the input origin `updated`.
    /// A vote changes only if the slowest input origin advanced, which can release a held origin,
    /// or if the updated origin advanced to more than the gap ahead of the slowest, which can hold it.
    /// An origin that the throttle already holds cannot be held again, so its updates only count when it is done.
    /// `origin` and `slowest` must come from one read, so that `origin` is not behind `slowest`.
    /// The function takes no lock. It can read a hold that a concurrent update is about to release.
    /// The throttle then misses one hold, and the next update of that origin casts it.
    [[nodiscard]] bool
    mayChangeVotes(OriginId updated, Timestamp origin, Timestamp slowest, bool originAdvanced, bool slowestAdvanced) const;

    /// Updates the votes from `watermarks`, the watermark of every input origin in the order of the constructor.
    /// Call it between `start` and `stop`.
    /// Snapshots may arrive in any order: the throttle keeps the largest watermark it received for each input origin.
    void onWatermarksMoved(std::span<const Timestamp> watermarks);

    /// Updates the votes from the current watermarks of `watermarks`, which must track the input origins in the order of the constructor.
    /// Reads them into a member while it has the lock, so the caller does not allocate a snapshot.
    void onWatermarksMoved(const MultiOriginWatermarkProcessor& watermarks);

private:
    /// Requires the lock.
    void updateVotes(std::span<const Timestamp> watermarks);

    const std::vector<OriginId> inputOrigins;
    const uint64_t maxWatermarkGap;
    /// For each input origin, whether the throttle holds it, for `mayChangeVotes`, which takes no lock.
    /// Written with the lock, together with `held`.
    std::vector<std::atomic<bool>> holds;

    /// Guards all members below.
    std::mutex mutex;
    /// Set between `start` and `stop`.
    std::optional<InternalBackpressureVoter> voter;
    /// For each input origin, the largest watermark the throttle received.
    /// Watermarks only grow, so an older snapshot cannot move a vote back.
    std::vector<Timestamp> latest;
    /// For each input origin, whether the throttle holds it.
    /// Empty until the throttle votes on the origin for the first time.
    std::vector<std::optional<bool>> held;
    /// Scratch space of `onWatermarksMoved`, kept as members so an update does not allocate.
    std::vector<bool> shouldHold;
    std::vector<Timestamp> snapshot;
};

}
