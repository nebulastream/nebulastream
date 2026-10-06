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

Timestamp MultiOriginWatermarkProcessor::updateWatermark(Timestamp ts, SequenceData sequenceData, OriginId origin) const
{
    bool found = false;
    for (size_t originIndex = 0; originIndex < origins.size(); ++originIndex)
    {
        if (origins[originIndex] == origin)
        {
            watermarkProcessors[originIndex]->emplace(sequenceData, ts.getRawValue());
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

std::string MultiOriginWatermarkProcessor::getCurrentStatus()
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

std::vector<MultiOriginWatermarkProcessor::OriginSnapshot> MultiOriginWatermarkProcessor::snapshotContiguous() const
{
    std::vector<OriginSnapshot> result;
    result.reserve(origins.size());
    for (size_t index = 0; index < origins.size(); ++index)
    {
        const auto snapshot = watermarkProcessors[index]->snapshotContiguous();
        result.push_back({origins[index], snapshot.sequence, snapshot.value});
    }
    return result;
}

void MultiOriginWatermarkProcessor::restoreContiguous(const std::vector<OriginSnapshot>& snapshot)
{
    INVARIANT(snapshot.size() == origins.size(), "Watermark origin count changed during migration");
    for (size_t index = 0; index < origins.size(); ++index)
    {
        INVARIANT(snapshot[index].origin == origins[index], "Watermark origin changed during migration");
        watermarkProcessors[index]->restoreContiguous({snapshot[index].sequence, snapshot[index].watermark});
    }
}

}
