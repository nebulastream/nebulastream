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
#include <Sequencing/RangeWatermarkTracker.hpp>
#include <Sequencing/SequenceRange.hpp>
#include <Time/Timestamp.hpp>
#include <Watermark/MultiOriginWatermarkProcessor.hpp>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>

namespace NES
{

MultiOriginWatermarkProcessor::MultiOriginWatermarkProcessor(const std::vector<OriginId>& origins) : origins(origins)
{
    watermarkTrackers.reserve(origins.size());
    for (size_t i = 0; i < origins.size(); ++i)
    {
        watermarkTrackers.push_back(std::make_unique<RangeWatermarkTracker>());
    }
};

std::shared_ptr<MultiOriginWatermarkProcessor> MultiOriginWatermarkProcessor::create(const std::vector<OriginId>& origins)
{
    return std::make_shared<MultiOriginWatermarkProcessor>(origins);
}

std::string MultiOriginWatermarkProcessor::getCurrentStatus()
{
    std::stringstream ss;
    for (size_t originIndex = 0; originIndex < origins.size(); ++originIndex)
    {
        ss << " id=" << origins[originIndex] << " watermark=" << watermarkTrackers[originIndex]->getCurrentWatermark().getRawValue();
    }
    return ss.str();
}

Timestamp MultiOriginWatermarkProcessor::updateWatermark(Timestamp ts, const SequenceRange& sequenceRange, OriginId origin) const
{
    bool found = false;
    for (size_t originIndex = 0; originIndex < origins.size(); ++originIndex)
    {
        if (origins[originIndex] == origin)
        {
            watermarkTrackers[originIndex]->insert(sequenceRange, ts);
            found = true;
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

Timestamp MultiOriginWatermarkProcessor::getCurrentWatermark() const
{
    auto minimalWatermark = UINT64_MAX;
    for (const auto& tracker : watermarkTrackers)
    {
        minimalWatermark = std::min(minimalWatermark, tracker->getCurrentWatermark().getRawValue());
    }
    return Timestamp(minimalWatermark);
}

}
