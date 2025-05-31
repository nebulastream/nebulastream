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

    std::string getCurrentStatus();

private:
    const std::vector<OriginId> origins;
    std::vector<std::shared_ptr<NES::Sequencing::NonBlockingMonotonicSeqQueue<uint64_t>>> watermarkProcessors;
};

}
