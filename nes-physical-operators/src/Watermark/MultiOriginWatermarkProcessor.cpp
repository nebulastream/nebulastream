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
#include <Watermark/MultiOriginWatermarkProcessor.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <ranges>
#include <span>
#include <sstream>
#include <string>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Sequencing/NonBlockingMonotonicSeqQueue.hpp>
#include <Sequencing/SequenceData.hpp>
#include <Time/Timestamp.hpp>
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

MultiOriginWatermarkProcessor::WatermarkUpdate
MultiOriginWatermarkProcessor::updateWatermark(Timestamp ts, SequenceData sequenceData, OriginId origin) const
{
    bool found = false;
    bool advanced = false;
    for (size_t originIndex = 0; originIndex < origins.size(); ++originIndex)
    {
        if (origins[originIndex] == origin)
        {
            advanced |= watermarkProcessors[originIndex]->emplace(sequenceData, ts.getRawValue());
            found = true;
        }
    }
    INVARIANT(
        found,
        "update watermark for non existing origin={} number of origins size={} ids={}",
        origin,
        origins.size(),
        fmt::join(origins, ","));
    auto global = UINT64_MAX;
    auto originWatermark = UINT64_MAX;
    for (const auto& [currentOrigin, processor] : std::views::zip(origins, watermarkProcessors))
    {
        const auto value = processor->getCurrentValue();
        global = std::min(global, value);
        if (currentOrigin == origin)
        {
            originWatermark = value;
        }
    }
    return {.global = Timestamp(global), .origin = Timestamp(originWatermark), .originAdvanced = advanced};
}

std::string MultiOriginWatermarkProcessor::getCurrentStatus()
{
    std::stringstream ss;
    for (size_t originIndex = 0; originIndex < origins.size(); ++originIndex)
    {
        ss << " id=" << origins[originIndex] << " watermark=" << watermarkProcessors[originIndex]->getCurrentValue();
    }
    return ss.str();
}

Timestamp MultiOriginWatermarkProcessor::getCurrentWatermark(const OriginId origin) const
{
    for (const auto& [currentOrigin, processor] : std::views::zip(origins, watermarkProcessors))
    {
        if (currentOrigin == origin)
        {
            return Timestamp(processor->getCurrentValue());
        }
    }
    INVARIANT(false, "watermark requested for non existing origin={}", origin);
    std::unreachable();
}

void MultiOriginWatermarkProcessor::getCurrentWatermarks(const std::span<Timestamp> out) const
{
    PRECONDITION(
        out.size() == origins.size(),
        "The output has space for {} watermarks, but the processor tracks {} origins",
        out.size(),
        origins.size());
    for (auto&& [target, processor] : std::views::zip(out, watermarkProcessors))
    {
        target = Timestamp(processor->getCurrentValue());
    }
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

}
