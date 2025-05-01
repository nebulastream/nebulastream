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

#include <iostream>
#include <map>
#include <numeric>
#include <Execution/Operators/SliceStore/WindowTriggerPredictor/AbstractWindowTriggerPredictor.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Time/Timestamp.hpp>

namespace NES::Runtime::Execution
{

class WatermarkBasedWindowTriggerPredictor final : public AbstractWindowTriggerPredictor
{
public:
    void updateWatermark(const OriginId streamId, Timestamp timestamp, SequenceNumber sequenceNumber)
    {
        globalWatermark = std::max(globalWatermark, timestamp);
        perStreamWatermarks[streamId] = std::max(perStreamWatermarks[streamId], timestamp);
        sequenceNumbers.insert({streamId, sequenceNumber});
        historicalData.push_back({timestamp, sequenceNumber});
    }

    std::vector<SequenceNumber> identifyGaps(const OriginId streamId) const
    {
        if (!sequenceNumbers.contains(streamId))
        {
            return {};
        }
        std::vector<SequenceNumber> sortedSeqNumbers;
        auto [begin, end] = sequenceNumbers.equal_range(streamId);
        for (auto it = begin; it != end; ++it) {
            sortedSeqNumbers.push_back(it->second);
        }
        std::ranges::sort(sortedSeqNumbers);

        std::vector<SequenceNumber> gaps;
        for (auto i = 1UL; i < sortedSeqNumbers.back().getRawValue(); ++i)
        {
            if (std::ranges::find(sortedSeqNumbers, SequenceNumber(i)) == sortedSeqNumbers.end())
            {
                gaps.push_back(SequenceNumber(i));
            }
        }
        return gaps;
    }

    Timestamp predictArrivalTime(const SequenceNumber missingSlice) const
    {
        std::vector<Timestamp> arrivalTimes;
        for (const auto& [timestamp, sequenceNumber] : historicalData)
        {
            if (sequenceNumber == missingSlice)
            {
                arrivalTimes.push_back(timestamp);
            }
        }
        if (!arrivalTimes.empty())
        {
            const int sum = std::accumulate(arrivalTimes.begin(), arrivalTimes.end(), 0);
            return Timestamp(sum / arrivalTimes.size());
        }
        return Timestamp(std::numeric_limits<int>::max()); // Return a large value if no data is available
    }

    Timestamp getEstimatedTimestamp(const OriginId streamId, const SequenceNumber sequenceNumber) const
    {
        auto gaps = identifyGaps(streamId);
        for (const auto& gap : gaps)
        {
            if (gap == sequenceNumber)
            {
                return predictArrivalTime(gap);
            }
        }
        return Timestamp(std::numeric_limits<int>::max()); // Return a large value if the slice is not missing
    }

    void processSlice(const OriginId streamId, const Timestamp timestamp, const SequenceNumber sequenceNumber)
    {
        updateWatermark(streamId, timestamp, sequenceNumber);
        const Timestamp estimatedTimestamp = getEstimatedTimestamp(streamId, sequenceNumber);
        std::cout << "Estimated timestamp for slice " << sequenceNumber << ": " << estimatedTimestamp << std::endl;
    }

private:
    Timestamp globalWatermark = Timestamp(0);
    std::map<OriginId, Timestamp> perStreamWatermarks;
    std::multimap<OriginId, SequenceNumber> sequenceNumbers;
    std::vector<std::pair<Timestamp, SequenceNumber>> historicalData;
};

}
