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

#include <TemporalSequenceAggregationLogicalFunction.hpp>

#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <fmt/format.h>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>

#include <utility>
#include <variant>
#include <DataTypes/DataType.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <folly/hash/Hash.h>
#include <AggregationLogicalFunctionRegistry.hpp>

namespace NES
{
TemporalSequenceAggregationLogicalFunction::TemporalSequenceAggregationLogicalFunction(AggregationFieldAccess inputFunction)
    : inputFunction(std::move(inputFunction))
{
    aggregateType = DataTypeProvider::provideDataType("TemporalPointSequence", DataType::NULLABLE::NOT_NULLABLE);
}

std::string_view TemporalSequenceAggregationLogicalFunction::getName() noexcept
{
    return NAME;
}

DataType TemporalSequenceAggregationLogicalFunction::getAggregateType() const
{
    return aggregateType;
}

bool TemporalSequenceAggregationLogicalFunction::shallIncludeNullValues() noexcept
{
    return false;
}

AggregationFieldAccess TemporalSequenceAggregationLogicalFunction::getInputFunction() const
{
    return inputFunction;
}

std::string TemporalSequenceAggregationLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Short)
    {
        return fmt::format("{}()", NAME);
    }
    auto inputExplain = std::visit([verbosity](const auto& input) { return input->explain(verbosity); }, inputFunction);
    return fmt::format("{}({})", NAME, inputExplain);
}

bool TemporalSequenceAggregationLogicalFunction::operator==(const TemporalSequenceAggregationLogicalFunction& other) const
{
    return inputFunction == other.inputFunction;
}

TemporalSequenceAggregationLogicalFunction
TemporalSequenceAggregationLogicalFunction::withInferredType(const Schema<Field, Unordered>& schema) const
{
    auto newInputFunction = inferFieldAccess(inputFunction, schema);
    if (newInputFunction->getDataType().structName != "TemporalPoint")
    {
        throw UnsupportedQuery(
            "{} is only supported on TemporalPoint values, but field {} has type {}.",
            NAME,
            newInputFunction->getField().getLastName(),
            newInputFunction->getDataType());
    }
    TemporalSequenceAggregationLogicalFunction newAgg{newInputFunction};
    return newAgg;
}

Reflected Reflector<TemporalSequenceAggregationLogicalFunction>::operator()(
    const TemporalSequenceAggregationLogicalFunction& function, const ReflectionContext& context) const
{
    return context.reflect(function.getInputFunction());
}

TemporalSequenceAggregationLogicalFunction
Unreflector<TemporalSequenceAggregationLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    return TemporalSequenceAggregationLogicalFunction{context.unreflect<AggregationFieldAccess>(reflected)};
}

AggregationLogicalFunctionRegistryReturnType
TemporalSequenceAggregationLogicalFunction::create(AggregationLogicalFunctionRegistryArguments arguments)
{
    if (arguments.on.size() != 1)
    {
        throw CannotDeserialize("TemporalSequenceAggregationLogicalFunction requires exactly one field, but got {}", arguments.on.size());
    }
    return TemporalSequenceAggregationLogicalFunction{arguments.on.at(0)};
}
}

size_t std::hash<NES::TemporalSequenceAggregationLogicalFunction>::operator()(
    const NES::TemporalSequenceAggregationLogicalFunction& aggregationFunction) const noexcept
{
    return folly::hash::hash_combine(aggregationFunction.getInputFunction(), NES::TemporalSequenceAggregationLogicalFunction::getName());
}
