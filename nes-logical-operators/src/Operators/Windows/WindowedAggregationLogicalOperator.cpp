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

#include <Operators/Windows/WindowedAggregationLogicalOperator.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Serialization/FunctionSerializationUtil.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <WindowTypes/Types/SlidingWindow.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <WindowTypes/Types/TumblingWindow.hpp>
#include <WindowTypes/Types/WindowType.hpp>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <SerializableOperator.pb.h>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

WindowedAggregationLogicalOperator::WindowedAggregationLogicalOperator(
    std::vector<FieldAccessLogicalFunction> groupingKey,
    std::vector<std::shared_ptr<WindowAggregationLogicalFunction>> aggregationFunctions,
    std::shared_ptr<Windowing::WindowType> windowType)
{
    data.aggregationFunctions = std::move(aggregationFunctions);
    data.windowType = std::move(windowType);
    data.groupingKey = std::move(groupingKey);
}

std::string_view WindowedAggregationLogicalOperator::getName() const noexcept
{
    return NAME;
}

std::string WindowedAggregationLogicalOperator::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        auto windowType = getWindowType();
        auto windowAggregation = getWindowAggregation();
        return fmt::format(
            "WINDOW AGGREGATION(opId: {}, {}, window type: {})",
            id,
            fmt::join(std::views::transform(windowAggregation, [](const auto& agg) { return agg->toString(); }), ", "),
            windowType->toString());
    }
    auto windowAggregation = getWindowAggregation();
    return fmt::format(
        "WINDOW AGG({})", fmt::join(std::views::transform(windowAggregation, [](const auto& agg) { return agg->getName(); }), ", "));
}

LogicalOperator WindowedAggregationLogicalOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
{
    auto copy = *this;
    INVARIANT(!inputSchemas.empty(), "WindowAggregation should have at least one input");

    const auto& firstSchema = inputSchemas[0];
    for (const auto& schema : inputSchemas)
    {
        if (schema != firstSchema)
        {
            throw CannotInferSchema("All input schemas must be equal for WindowAggregation operator");
        }
    }

    std::vector<std::shared_ptr<WindowAggregationLogicalFunction>> newFunctions;
    for (const auto& agg : getWindowAggregation())
    {
        agg->inferStamp(firstSchema);
        newFunctions.push_back(agg);
    }
    copy.data.aggregationFunctions = newFunctions;

    copy.data.windowType->inferStamp(firstSchema);
    copy.inputSchemas = inputSchemas;
    copy.outputSchema = Schema{copy.outputSchema.memoryLayoutType};

    if (auto* timeWindow = dynamic_cast<Windowing::TimeBasedWindowType*>(getWindowType().get()))
    {
        const auto& newQualifierForSystemField = firstSchema.getQualifierNameForSystemGeneratedFieldsWithSeparator();

        copy.data.windowMetaData.windowStartFieldName = newQualifierForSystemField + "start";
        copy.data.windowMetaData.windowEndFieldName = newQualifierForSystemField + "end";
        copy.outputSchema.addField(copy.data.windowMetaData.windowStartFieldName, DataType::Type::UINT64);
        copy.outputSchema.addField(copy.data.windowMetaData.windowEndFieldName, DataType::Type::UINT64);
    }
    else
    {
        throw CannotInferSchema("Unsupported window type {}", getWindowType()->toString());
    }

    if (isKeyed())
    {
        auto keys = getGroupingKeys();
        auto newKeys = std::vector<FieldAccessLogicalFunction>();
        for (auto& key : keys)
        {
            auto newKey = key.withInferredDataType(firstSchema).get<FieldAccessLogicalFunction>();
            newKeys.push_back(newKey);
            copy.outputSchema.addField(newKey.getFieldName(), newKey.getDataType());
        }
        copy.data.groupingKey = newKeys;
    }
    for (const auto& agg : copy.data.aggregationFunctions)
    {
        copy.outputSchema.addField(agg->asField.getFieldName(), agg->asField.getDataType());
    }
    return copy;
}

bool WindowedAggregationLogicalOperator::isKeyed() const
{
    return !data.groupingKey.empty();
}

std::vector<std::shared_ptr<WindowAggregationLogicalFunction>> WindowedAggregationLogicalOperator::getWindowAggregation() const
{
    return data.aggregationFunctions;
}

void WindowedAggregationLogicalOperator::setWindowAggregation(std::vector<std::shared_ptr<WindowAggregationLogicalFunction>> wa)
{
    data.aggregationFunctions = std::move(wa);
}

std::shared_ptr<Windowing::WindowType> WindowedAggregationLogicalOperator::getWindowType() const
{
    return data.windowType;
}

void WindowedAggregationLogicalOperator::setWindowType(std::shared_ptr<Windowing::WindowType> wt)
{
    data.windowType = std::move(wt);
}

std::vector<FieldAccessLogicalFunction> WindowedAggregationLogicalOperator::getGroupingKeys() const
{
    return data.groupingKey;
}

std::string WindowedAggregationLogicalOperator::getWindowStartFieldName() const
{
    return data.windowMetaData.windowStartFieldName;
}

std::string WindowedAggregationLogicalOperator::getWindowEndFieldName() const
{
    return data.windowMetaData.windowEndFieldName;
}

const WindowMetaData& WindowedAggregationLogicalOperator::getWindowMetaData() const
{
    return data.windowMetaData;
}

}
