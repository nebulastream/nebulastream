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
#include <Operators/Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Operators/Expressions/ConstantValueExpressionNode.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Statistics/ProbeExpression.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Statistics/Synopses/CountMinStatistic.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Statistics/StatisticUtil.hpp>
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

    return std::make_shared<CountMinStatistic>(CountMinStatistic(startTs, endTs, observedTuples,
                                                                 width, depth, numberOfBitsInKey, countMinData));
}

StatisticPtr CountMinStatistic::createInit(const Windowing::TimeMeasure& startTs,
                                           const Windowing::TimeMeasure& endTs,
                                           uint64_t width,
                                           uint64_t depth,
                                           uint64_t numberOfBitsInKey) {
    // Initializing the underlying count min data to all 0 and setting the observed tuples to 0
    std::vector<uint64_t> countMinData(width * depth, 0);
    constexpr auto observedTuples = 0;
    return std::make_shared<CountMinStatistic>(CountMinStatistic(startTs, endTs, observedTuples,
                                                                 width, depth, numberOfBitsInKey,
                                                                 countMinData));
}

void CountMinStatistic::update(uint64_t row, uint64_t col) {
    // 1. Incrementing the observed tuples as we have seen one more
    observedTuples += 1;

    // 2. Calculating the position, as we store a 2-d array in a 1-d vector.
    const auto pos = row * width + col;

    // 3. Incrementing the counter at the previously computed position
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
      countMinData(countMinData) {
}

StatisticValue<> CountMinStatistic::getStatisticValue(const ProbeExpression& probeExpression) const {
    const auto expression = probeExpression.getProbeExpression();
    if (!expression->instanceOf<EqualsExpressionNode>()) {
        NES_THROW_RUNTIME_ERROR("For now, we can only get the statistic for a FieldAssignmentExpressionNode!");
    }
    const auto equalExpression = expression->as<EqualsExpressionNode>();
    const auto leftExpr = equalExpression->getLeft();
    const auto rightExpr = equalExpression->getRight();
    // We expect that one expression is a constant value and the other a field access expression
    if (!((leftExpr->instanceOf<ConstantValueExpressionNode>() && rightExpr->instanceOf<FieldAccessExpressionNode>()) ||
            (leftExpr->instanceOf<FieldAccessExpressionNode>() && rightExpr->instanceOf<ConstantValueExpressionNode>()))) {
        NES_THROW_RUNTIME_ERROR("For now, we expect that one expression is a constant value and the other a field access expression!");
    }

    // Getting the constant value and the field access
    const auto constantExpression = leftExpr->instanceOf<ConstantValueExpressionNode>() ? leftExpr : rightExpr;
    const auto constantValue = constantExpression->as<ConstantValueExpressionNode>()->getConstantValue();
    const auto basicValue = constantValue->as<BasicValue>();

    // Iterating over each row. Calculating the column and then taking the minimum over all rows
    uint64_t minCount = UINT64_MAX;
    for (auto row = 0_u64; row < depth; ++row) {
        const auto hashValue = StatisticUtil::getH3HashValue(*basicValue, row, depth, numberOfBitsInKey);
        const auto column = hashValue % width;
        minCount = std::min(minCount, countMinData[row * width + column]);
    }


    return StatisticValue<>(minCount, startTs, endTs);
}

bool CountMinStatistic::equal(const Statistic& other) const {
    if (other.instanceOf<CountMinStatistic>()) {
        auto otherCountMinStatistic = other.as<const CountMinStatistic>();
        return startTs.equals(otherCountMinStatistic->startTs) && endTs.equals(otherCountMinStatistic->endTs) &&
            observedTuples == otherCountMinStatistic->observedTuples && width == otherCountMinStatistic->width &&
            depth == otherCountMinStatistic->depth && numberOfBitsInKey == otherCountMinStatistic->numberOfBitsInKey &&
            countMinData == otherCountMinStatistic->countMinData;
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
    oss << fmt::format("countMinData({}) = {}", countMinData.size(), fmt::join(countMinData, ", "));
    oss << ")";
    return oss.str();
}

uint64_t CountMinStatistic::getWidth() const { return width; }

uint64_t CountMinStatistic::getDepth() const { return depth; }

uint64_t CountMinStatistic::getNumberOfBitsInKeyOffset() const { return numberOfBitsInKey; }

std::string CountMinStatistic::getCountMinDataAsString() const {
    const auto dataSizeBytes = countMinData.size() * sizeof(uint64_t);
    std::string countMinStr;
    countMinStr.resize(dataSizeBytes);
    std::memcpy(countMinStr.data(), countMinData.data(), dataSizeBytes);
    return countMinStr;
}

void CountMinStatistic::merge(const SynopsesStatistic& other) {
    // 1. We can only merge the same synopsis type
    if (!other.instanceOf<CountMinStatistic>()) {
        NES_ERROR("Other is not of type CountMinStatistic. Therefore, we skipped merging them together");
        return;
    }

    // 2. We can only merge a count min sketch with the same dimensions
    auto otherCountMinStatistic = other.as<const CountMinStatistic>();
    if (depth != otherCountMinStatistic->depth || width != otherCountMinStatistic->width) {
        NES_ERROR("Can not combine sketches <{},{}> and <{},{}>, as they do not share the same dimensions", width, depth, otherCountMinStatistic->width, otherCountMinStatistic->depth);
        return;
    }

    // 3. We merge the count min counters by simply adding them up
    for (auto i = 0_u64; i < countMinData.size(); ++i) {
        countMinData[i] += otherCountMinStatistic->countMinData[i];
    }

    // 4. Increment the observedTuples
    observedTuples += otherCountMinStatistic->observedTuples;
}

}// namespace NES::Statistic