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

#include <Operators/LogicalOperators/StatisticCollection/Statistics/Synopses/CountMinStatistic.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Statistics/ProbeExpression.hpp>
#include <Util/Logger/Logger.hpp>
#include <cstring>
#include <string>
#include <vector>
#include <numeric>
#include <fmt/format.h>

namespace NES::Statistic {

StatisticPtr CountMinStatistic::create(const Windowing::TimeMeasure& startTs,
                                       const Windowing::TimeMeasure& endTs,
                                       uint64_t observedTuples,
                                       uint64_t width,
                                       uint64_t depth,
                                       const std::string_view countMinDataString) {
    // We just store the CountMin data as a string. So we have to copy the bytes from the string to a vector
    // We do this by first creating a large enough vector and then copy the bytes
    std::vector<uint64_t> countMinData(width * depth);
    std::memcpy(countMinData.data(), countMinDataString.data(), countMinDataString.size());

    return std::make_shared<CountMinStatistic>(CountMinStatistic(startTs, endTs, observedTuples,
                                                                 width, depth, countMinData));
}

CountMinStatistic::CountMinStatistic(const Windowing::TimeMeasure& startTs,
                                     const Windowing::TimeMeasure& endTs,
                                     uint64_t observedTuples,
                                     uint64_t width,
                                     uint64_t depth,
                                     const std::vector<uint64_t>& countMinData)
    : SynopsesStatistic(startTs, endTs, observedTuples), width(width), depth(depth), countMinData(countMinData) {}

StatisticValue<> CountMinStatistic::getStatisticValue(const ProbeExpression&) const {
    // TODO will be done in issue #4689
    NES_NOT_IMPLEMENTED();
}

bool CountMinStatistic::equal(const Statistic& other) const {
    if (other.instanceOf<CountMinStatistic>()) {
        auto otherCountMinStatistic = other.as<const CountMinStatistic>();
        return startTs.equals(otherCountMinStatistic->startTs) && endTs.equals(otherCountMinStatistic->endTs) &&
            observedTuples == otherCountMinStatistic->observedTuples && width == otherCountMinStatistic->width &&
            depth == otherCountMinStatistic->depth && countMinData == otherCountMinStatistic->countMinData;
    }
    return false;
}

std::string CountMinStatistic::toString() const {
    std::ostringstream oss;
    oss << "CountMin( ";
    oss << "startTs: " << startTs.toString() << " ";
    oss << "endTs: " << endTs.toString() << " ";
    oss << "observedTuples: " << observedTuples << " ";
    oss << "width: " << width << " ";
    oss << "depth: " << depth << " ";
    oss << fmt::format("countMinData({}) = {}", countMinData.size(), fmt::join(countMinData, ", ")) << " ";
    oss << ")";
    return oss.str();
}

}// namespace NES::Statistic