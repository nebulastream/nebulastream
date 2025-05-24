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
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <sstream>
#include <string>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Sequencing/NonBlockingMonotonicSeqQueue.hpp>
#include <Sequencing/SequenceData.hpp>
#include <Time/Timestamp.hpp>
#include <Watermark/MultiOriginWatermarkProcessor.hpp>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>

namespace NES
{

MultiOriginWatermarkProcessor::MultiOriginWatermarkProcessor(const std::vector<OriginId>& origins) : origins(origins)
{
    for (const auto& _ : origins)
    {
        watermarkProcessors.emplace_back(std::make_shared<Sequencing::NonBlockingMonotonicSeqQueue<uint64_t>>());
    }
};

std::shared_ptr<MultiOriginWatermarkProcessor> MultiOriginWatermarkProcessor::create(const std::vector<OriginId>& origins)
{
    return std::make_shared<MultiOriginWatermarkProcessor>(origins);
}

Timestamp MultiOriginWatermarkProcessor::updateWatermark(const Timestamp ts, const SequenceData& sequenceData, OriginId origin)
{
    bool found = false;
    for (size_t originIndex = 0; originIndex < origins.size(); ++originIndex)
    {
        if (sequenceData.lastChunk)
        {
            const auto now = std::chrono::high_resolution_clock::now();
            const auto ingestionTimeTicks
                = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count());

            const auto seqNumbersIngestionTimeLocked = seqNumbersIngestionTime.wlock();
            (*seqNumbersIngestionTimeLocked)[{origin, SequenceNumber(sequenceData.sequenceNumber)}] = ingestionTimeTicks;
        }

        if (origins[originIndex] == origin)
        {
            watermarkProcessors[originIndex]->emplace(sequenceData, ts.getRawValue());
            found = true;
            break;
        }
    }
    INVARIANT(
        found,
        "update watermark for non existing origin={} number of origins size={} ids={}",
        origin,
        origins.size(),
        fmt::join(origins, ","));
    return getCurrentWatermark();
}

std::string MultiOriginWatermarkProcessor::getCurrentStatus() const
{
    std::stringstream ss;
    for (size_t originIndex = 0; originIndex < origins.size(); ++originIndex)
    {
        ss << " id=" << origins[originIndex] << " watermark=" << watermarkProcessors[originIndex]->getCurrentValue();
    }
    return ss.str();
}

Timestamp MultiOriginWatermarkProcessor::getCurrentWatermark() const
{
    auto minimalWatermark = UINT64_MAX;
    for (const auto& wt : watermarkProcessors)
    {
        minimalWatermark = std::min(minimalWatermark, wt->getCurrentValue());
    }
    return Timestamp(minimalWatermark);
}

std::vector<std::pair<uint64_t, Timestamp::Underlying>> MultiOriginWatermarkProcessor::getIngestionTimesForWatermarks(
    const OriginId origin, const uint64_t numGapsAllowed, const uint64_t maxNumSeqNumbers) const
{
    std::vector<std::pair<uint64_t, Timestamp::Underlying>> ingestionTimesForWatermarks;
    for (size_t originIndex = 0; originIndex < origins.size(); ++originIndex)
    {
        if (origins[originIndex] == origin)
        {
            const auto& nextSequenceNumbers
                = watermarkProcessors[originIndex]->getNextSequenceNumbersAndValues(numGapsAllowed, maxNumSeqNumbers);
            ingestionTimesForWatermarks.reserve(nextSequenceNumbers.size());

            const auto seqNumbersIngestionTimeLocked = seqNumbersIngestionTime.rlock();
            for (const auto& [seqNumber, timestamp] : nextSequenceNumbers)
            {
                const auto ingestionTime = seqNumbersIngestionTimeLocked->at({origin, SequenceNumber(seqNumber)});
                ingestionTimesForWatermarks.emplace_back(ingestionTime, timestamp);
            }
            break;
        }
    }
    return ingestionTimesForWatermarks;
}

}
