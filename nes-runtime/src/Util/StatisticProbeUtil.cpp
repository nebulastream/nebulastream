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

#include <Common/ValueTypes/BasicValue.hpp>
#include <Expressions/ConstantValueExpressionNode.hpp>
#include <Expressions/FieldAccessExpressionNode.hpp>
#include <Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Statistics/StatisticUtil.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StatisticProbeUtil.hpp>
#include <Util/StdInt.hpp>
namespace NES::Statistic {

StatisticValue<> StatisticProbeUtil::probeCountMin(const CountMinStatistic& countMinStatistic,
                                                   const ProbeExpression& probeExpression) {
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
    const auto depth = countMinStatistic.getDepth();
    const auto width = countMinStatistic.getWidth();
    const auto numberOfBitsInKey = countMinStatistic.getNumberOfBitsInKeyOffset();
    const auto startTs = countMinStatistic.getStartTs();
    const auto endTs = countMinStatistic.getEndTs();
    auto* countMinData = static_cast<uint64_t*>(countMinStatistic.getStatisticData());
    uint64_t minCount = UINT64_MAX;
    for (auto row = 0_u64; row < depth; ++row) {
        const auto hashValue = StatisticUtil::getH3HashValue(*basicValue, row, depth, numberOfBitsInKey);
        const auto column = hashValue % width;
        minCount = std::min(minCount, countMinData[row * width + column]);
    }

    return StatisticValue<>(minCount, startTs, endTs);
}

StatisticValue<> StatisticProbeUtil::probeReservoirSample(const ReservoirSampleStatistic& reservoirSampleStatistic,
                                                          const ProbeExpression& probeExpression,
                                                          const Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayout) {
    /* For now, we only allow a simple EqualExpression that will count how many fields are equal to a specific value.
     * Later on, we plan on merging the probe of each statistic into Nautilus giving us more flexibility.
     * As soon as we have this, we will support the same expressions as in our query engine.
     */
    const auto expression = probeExpression.getProbeExpression();
    const auto equalExpression = expression->as<EqualsExpressionNode>();
    const auto leftExpr = equalExpression->getLeft();
    const auto rightExpr = equalExpression->getRight();
    // We expect that one expression is a constant value and the other a field access expression
    if (!((leftExpr->instanceOf<ConstantValueExpressionNode>() && rightExpr->instanceOf<FieldAccessExpressionNode>()) ||
          (leftExpr->instanceOf<FieldAccessExpressionNode>() && rightExpr->instanceOf<ConstantValueExpressionNode>()))) {
        NES_THROW_RUNTIME_ERROR("For now, we expect that one expression is a constant value and the other a field access expression!");
    }

    // Getting the constant value
    const auto constantExpression = leftExpr->instanceOf<ConstantValueExpressionNode>() ? leftExpr : rightExpr;
    const auto constantValue = constantExpression->as<ConstantValueExpressionNode>()->getConstantValue();
    const auto basicValue = constantValue->as<BasicValue>();

    // Getting the field access expression
    const auto fieldAccessExpression = leftExpr->instanceOf<FieldAccessExpressionNode>() ? leftExpr : rightExpr;
    const auto qualifierNameWithSeperator = memoryLayout->getSchema()->getQualifierNameForSystemGeneratedFieldsWithSeparator();
    const auto fieldName = qualifierNameWithSeperator + fieldAccessExpression->as<FieldAccessExpressionNode>()->getFieldName();


    // Iterating over the sample and counting how many elements are equal to the constant value
    const auto sampleSize = reservoirSampleStatistic.getSampleSize();
    const auto observedTuples = reservoirSampleStatistic.getObservedTuples();
    const auto reservoirSpace = reservoirSampleStatistic.getReservoirSpace();
    const auto startTs = reservoirSampleStatistic.getStartTs();
    const auto endTs = reservoirSampleStatistic.getEndTs();
    uint64_t count = 0;
    for (auto i = 0_u64; i < sampleSize; i++) {
        const auto curFieldOffset = memoryLayout->getFieldOffset(i, fieldName);
        if (!curFieldOffset.has_value()) {
            NES_ERROR("Can not field a field offset for {} in {}. Will skip this sample!", fieldName, memoryLayout->getSchema()->toString());
            continue;
        }

        const auto curFieldPosition = reservoirSpace + curFieldOffset.value();
        if (StatisticUtil::compareFieldWithBasicValue(curFieldPosition, *basicValue)) {
            count++;
        }
    }

    // Adapt the count to the number of observed tuples
    count = count * (observedTuples / sampleSize);

    return StatisticValue<>(count, startTs, endTs);
}
StatisticValue<> StatisticProbeUtil::probeHyperLogLog(HyperLogLogStatistic& hyperLogLogStatistic,
                                                      const ProbeExpression&) {
    hyperLogLogStatistic.performEstimation();
    const auto estimate = hyperLogLogStatistic.getEstimate();
    const auto startTs = hyperLogLogStatistic.getStartTs();
    const auto endTs = hyperLogLogStatistic.getEndTs();
    // If the estimate is not calculated yet, we throw an exception
    #ifdef __APPLE__
    if (std::isnan(estimate)) {
        NES_THROW_RUNTIME_ERROR("HyperLogLogStatistic has no estimate value. Please call performEstimation() first.");
    }
    #else
    if (isnanl(estimate)) {
        NES_THROW_RUNTIME_ERROR("HyperLogLogStatistic has no estimate value. Please call performEstimation() first.");
    }
    #endif
    return StatisticValue<>(estimate, startTs, endTs);
}

StatisticValue<> StatisticProbeUtil::probeDDSketch(const DDSketchStatistic& statistic, const ProbeExpression& probeExpression) {
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
    const double quantile = std::stod(basicValue->value);

    // Getting the value for the quantile from the DD-Sketch
    const auto value = statistic.getValueAtQuantile(quantile);
    return StatisticValue<>(value, statistic.getStartTs(), statistic.getEndTs());
}

StatisticValue<> StatisticProbeUtil::probeEquiWidthHistogram(const EquiWidthHistogramStatistic& equiWidthHistogramStatistic,
                                                             const ProbeExpression& probeExpression) {
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
    const double value = std::stod(basicValue->value);

    // Getting the count for the value from the EquiWidthHistogram
    const auto count = equiWidthHistogramStatistic.getCountForValue(value);
    return StatisticValue<>(count, equiWidthHistogramStatistic.getStartTs(), equiWidthHistogramStatistic.getEndTs());
}


} // namespace NES::Statistic