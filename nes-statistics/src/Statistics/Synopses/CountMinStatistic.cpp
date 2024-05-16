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

#include <Common/ValueTypes/ValueType.hpp>
#include <Expressions/ConstantValueExpressionNode.hpp>
#include <Expressions/FieldAccessExpressionNode.hpp>
#include <Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <StatisticCollection/StatisticProbeHandling/ProbeExpression.hpp>
#include <Statistics/StatisticUtil.hpp>
#include <Statistics/Synopses/CountMinStatistic.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>
#include <cstring>
#include <fmt/format.h>
#include <numeric>
#include <string>
#include <vector>

namespace NES::Statistic {

StatisticPtr CountMinStatistic::create(const Windowing::TimeMeasure& startTs,
                                       const Windowing::TimeMeasure& endTs,
                                       uint64_t observedTuples,
                                       uint64_t width,
                                       uint64_t depth,
                                       uint64_t numberOfBitsInKey,
                                       const std::string_view countMinDataString) {
    // We just store the CountMin data as a string. So we have to copy the bytes from the string to a vector
    // We do this by first creating a large enough vector and then copy the bytes
    std::vector<uint64_t> countMinData(width * depth, 0);
    std::memcpy(countMinData.data(), countMinDataString.data(), sizeof(uint64_t) * width * depth);

    return std::make_shared<CountMinStatistic>(
        CountMinStatistic(startTs, endTs, observedTuples, width, depth, numberOfBitsInKey, countMinData));
}

StatisticPtr CountMinStatistic::createInit(const Windowing::TimeMeasure& startTs,
                                           const Windowing::TimeMeasure& endTs,
                                           uint64_t width,
                                           uint64_t depth,
                                           uint64_t numberOfBitsInKey) {
    // Initializing the underlying count min data to all 0 and setting the observed tuples to 0
    std::vector<uint64_t> countMinData(width * depth, 0);
    constexpr auto observedTuples = 0;
    return std::make_shared<CountMinStatistic>(
        CountMinStatistic(startTs, endTs, observedTuples, width, depth, numberOfBitsInKey, countMinData));
}

void CountMinStatistic::update(uint64_t row, uint64_t col) {
    // 1. Calculating the position, as we store a 2-d array in a 1-d vector.
    const auto pos = row * width + col;

    // 2. Incrementing the counter at the previously computed position
    countMinData[pos] += 1;
}

CountMinStatistic::CountMinStatistic(const Windowing::TimeMeasure& startTs,
                                     const Windowing::TimeMeasure& endTs,
                                     uint64_t observedTuples,
                                     uint64_t width,
                                     uint64_t depth,
                                     uint64_t numberOfBitsInKey,
                                     const std::vector<uint64_t>& countMinData)
    : SynopsesStatistic(startTs, endTs, observedTuples), width(width), depth(depth), numberOfBitsInKey(numberOfBitsInKey),
      countMinData(countMinData) {}

bool CountMinStatistic::equal(const Statistic& other) const {
    if (other.instanceOf<CountMinStatistic>()) {
        auto otherCountMinStatistic = other.as<const CountMinStatistic>();
        return startTs.equals(otherCountMinStatistic->startTs) && endTs.equals(otherCountMinStatistic->endTs)
            && observedTuples == otherCountMinStatistic->observedTuples && width == otherCountMinStatistic->width
            && depth == otherCountMinStatistic->depth && numberOfBitsInKey == otherCountMinStatistic->numberOfBitsInKey
            && countMinData == otherCountMinStatistic->countMinData;
    }
    return false;
}

std::string CountMinStatistic::toString() const {
    std::ostringstream oss;
    oss << "CountMin(";
    oss << "startTs: " << startTs.toString() << " ";
    oss << "endTs: " << endTs.toString() << " ";
    oss << "observedTuples: " << observedTuples << " ";
    oss << "width: " << width << " ";
    oss << "depth: " << depth << " ";
    oss << "numberOfBitsInKey: " << numberOfBitsInKey << " ";
    oss << ")";
    return oss.str();
}

uint64_t CountMinStatistic::getWidth() const { return width; }

uint64_t CountMinStatistic::getDepth() const { return depth; }

uint64_t CountMinStatistic::getNumberOfBitsInKeyOffset() const { return numberOfBitsInKey; }

std::string CountMinStatistic::getCountMinDataAsString() const {
    const auto dataSizeBytes = countMinData.size() * sizeof(uint64_t);
    std::string countMinStr(dataSizeBytes, 0);
    std::memcpy(countMinStr.data(), countMinData.data(), dataSizeBytes);
    return countMinStr;
}

void CountMinStatistic::merge(const SynopsesStatistic& other) {
    // 1. We can only merge the same synopsis type
    if (!other.instanceOf<CountMinStatistic>()) {
        NES_ERROR("Other is not of type CountMinStatistic. Therefore, we skipped merging them together");
        return;
    }

    // 2. We can only merge statistics with the same period
    if (!startTs.equals(other.getStartTs()) || !endTs.equals(other.getEndTs())) {
        NES_ERROR("Can not merge HyperLogLog statistics with different periods");
        return;
    }

    // 3. We can only merge a count min sketch with the same dimensions
    auto otherCountMinStatistic = other.as<const CountMinStatistic>();
    if (depth != otherCountMinStatistic->depth || width != otherCountMinStatistic->width) {
        NES_ERROR("Can not combine sketches <{},{}> and <{},{}>, as they do not share the same dimensions",
                  width,
                  depth,
                  otherCountMinStatistic->width,
                  otherCountMinStatistic->depth);
        return;
    }

    // 4. We merge the count min counters by simply adding them up
    for (auto i = 0_u64; i < countMinData.size(); ++i) {
        countMinData[i] += otherCountMinStatistic->countMinData[i];
    }

    // 5. Increment the observedTuples
    observedTuples += otherCountMinStatistic->observedTuples;
}

void* CountMinStatistic::getStatisticData() const {
    // We have to cast away the const, as Nautilus currently only translates void* to a MemRef
    return const_cast<uint64_t*>(countMinData.data());
}

}// namespace NES::Statistic
