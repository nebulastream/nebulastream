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
#include <cstdint>
#include <memory>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Sequencing/NonBlockingMonotonicSeqQueue.hpp>
#include <Sequencing/SequenceData.hpp>
#include <Time/Timestamp.hpp>

namespace NES
{

/// @brief A multi origin version of the lock free watermark processor.
class MultiOriginWatermarkProcessor
{
public:
    explicit MultiOriginWatermarkProcessor(const std::vector<OriginId>& origins);
    static std::shared_ptr<MultiOriginWatermarkProcessor> create(const std::vector<OriginId>& origins);

    /// @brief Updates the watermark timestamp and origin and emits the current watermark.
    [[nodiscard]] Timestamp updateWatermark(Timestamp ts, SequenceData sequenceData, OriginId origin) const;

    /// @brief Returns the current watermark across all origins
    [[nodiscard]] Timestamp getCurrentWatermark() const;

    /// @brief Returns the current watermark of a single origin
    [[nodiscard]] Timestamp getCurrentWatermark(OriginId origin) const;

    std::string getCurrentStatus();

    /// The progress of one origin: all sequence numbers up to `sequenceNumber` have been seen and `watermark` is its watermark.
    struct OriginState
    {
        OriginId origin{INVALID_ORIGIN_ID};
        SequenceNumber::Underlying sequenceNumber;
        Timestamp::Underlying watermark;
    };

    /// Returns the progress of every origin, used to migrate the state of an operator (ISQD).
    [[nodiscard]] std::vector<OriginState> getState() const;
    /// Restores the progress of the given origins. Must not be called concurrently with any other method.
    void restoreState(const std::vector<OriginState>& state) const;

private:
    const std::vector<OriginId> origins;
    std::vector<std::shared_ptr<Sequencing::NonBlockingMonotonicSeqQueue<uint64_t>>> watermarkProcessors;
};

}
