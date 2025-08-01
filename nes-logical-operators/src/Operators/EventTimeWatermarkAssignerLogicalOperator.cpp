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

#include <Operators/EventTimeWatermarkAssignerLogicalOperator.hpp>

#include <algorithm>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/TimeUnit.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Serialization/FunctionSerializationUtil.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <SerializableOperator.pb.h>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

EventTimeWatermarkAssignerLogicalOperator::EventTimeWatermarkAssignerLogicalOperator(
    LogicalFunction onField, const Windowing::TimeUnit& unit)
    : data(std::move(onField), unit)
{
}

std::string_view EventTimeWatermarkAssignerLogicalOperator::getName() const noexcept
{
    return NAME;
}

LogicalFunction EventTimeWatermarkAssignerLogicalOperator::getOnField() const {
    return data.onField.value();
}

Windowing::TimeUnit EventTimeWatermarkAssignerLogicalOperator::getUnit() const {
    return data.unit;
}

std::string EventTimeWatermarkAssignerLogicalOperator::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        std::string inputOriginIdsStr;
        if (!inputOriginIds.empty())
        {
            inputOriginIdsStr = fmt::format(", inputOriginIds: [{}]", fmt::join(inputOriginIds, ", "));
        }
        return fmt::format(
            "EVENT_TIME_WATERMARK_ASSIGNER(opId: {}, onField: {}, unit: {}, inputSchema: {}{})",
            id,
            data.onField.value().explain(verbosity),
            data.unit.getMillisecondsConversionMultiplier(),
            inputSchemas.front(),
            inputOriginIdsStr);
    }
    return "WATERMARK_ASSIGNER(Event time)";
}


LogicalOperator EventTimeWatermarkAssignerLogicalOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
{
    auto copy = *this;
    PRECONDITION(inputSchemas.size() == 1, "Watermark assigner should have only one input");
    const auto& inputSchema = inputSchemas[0];
    copy.data.onField = data.onField.value().withInferredDataType(inputSchema);
    copy.inputSchemas = inputSchemas;
    copy.outputSchema = inputSchema;
    return copy;
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterEventTimeWatermarkAssignerLogicalOperator(LogicalOperatorRegistryArguments arguments)
{
    return EventTimeWatermarkAssignerLogicalOperator();
}
}
