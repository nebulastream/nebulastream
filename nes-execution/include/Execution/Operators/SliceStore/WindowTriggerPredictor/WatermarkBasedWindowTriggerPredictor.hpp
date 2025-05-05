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
#include <set>
#include <Execution/Operators/SliceStore/WindowTriggerPredictor/AbstractWindowTriggerPredictor.hpp>
#include <boost/range/numeric.hpp>

namespace NES::Runtime::Execution
{

class WatermarkBasedWindowTriggerPredictor final : public AbstractWindowTriggerPredictor
{
public:
    void processSlice(const OriginId originId, const Timestamp timestamp, const SequenceNumber sequenceNumber) override
    {
        updateWatermark(originId, timestamp, sequenceNumber);
        const Timestamp estimatedTimestamp = getEstimatedTimestamp(originId, sequenceNumber);
        std::cout << "Estimated timestamp for slice " << sequenceNumber << ": " << estimatedTimestamp << '\n';
    }

    [[nodiscard]] Timestamp getEstimatedTimestamp(const OriginId originId, const SequenceNumber sequenceNumber) const override
    {
        const auto gaps = identifyGaps(originId);
        for (const auto& gap : gaps)
        {
            if (gap == sequenceNumber)
            {
                return predictArrivalTime(gap);
            }
        }
        return Timestamp(std::numeric_limits<int>::max()); // Return a large value if the slice is not missing
    }

private:
    void updateWatermark(const OriginId originId, Timestamp timestamp, SequenceNumber sequenceNumber)
    {
        globalWatermark = std::max(globalWatermark, timestamp);
        perOriginWatermarks.insert_or_assign(originId, std::max(perOriginWatermarks.at(originId), timestamp));
        sequenceNumbers[originId].emplace(sequenceNumber);
        historicalData.emplace_back(timestamp, sequenceNumber);
    }

    [[nodiscard]] std::vector<SequenceNumber> identifyGaps(const OriginId originId) const
    {
        if (!sequenceNumbers.contains(originId))
        {
            return {};
        }

        const auto sequenceNumbersForOriginId = sequenceNumbers.find(originId)->second;
        const auto lastSequenceNumberAsInt = sequenceNumbersForOriginId.end()->getRawValue();

        std::vector<SequenceNumber> gaps;
        for (auto i = 1UL; i < lastSequenceNumberAsInt; ++i)
        {
            if (std::ranges::find(sequenceNumbersForOriginId, SequenceNumber(i)) == sequenceNumbersForOriginId.end())
            {
                gaps.emplace_back(i);
            }
        }

        return gaps;
    }

    [[nodiscard]] Timestamp predictArrivalTime(const SequenceNumber missingSlice) const
    {
        std::vector<Timestamp::Underlying> arrivalTimes;
        for (const auto& [timestamp, sequenceNumber] : historicalData)
        {
            if (sequenceNumber == missingSlice)
            {
                arrivalTimes.emplace_back(timestamp.getRawValue());
            }
        }
        if (!arrivalTimes.empty())
        {
            const auto sum = boost::accumulate(arrivalTimes, 0);
            return Timestamp(sum / arrivalTimes.size());
        }
        return Timestamp(std::numeric_limits<int>::max()); // Return a large value if no data is available
    }

    Timestamp globalWatermark = Timestamp(0);
    std::map<OriginId, Timestamp> perOriginWatermarks;
    std::map<OriginId, std::multiset<SequenceNumber>> sequenceNumbers;
    std::vector<std::pair<Timestamp, SequenceNumber>> historicalData;
};

}
