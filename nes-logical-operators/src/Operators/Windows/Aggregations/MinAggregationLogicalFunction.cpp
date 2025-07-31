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

#include <Operators/Windows/Aggregations/MinAggregationLogicalFunction.hpp>

#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <DataTypes/DataType.hpp>
#include <DataTypes/SchemaBase.hpp>
#include <DataTypes/SchemaBaseFwd.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <folly/hash/Hash.h>
#include <AggregationLogicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
MinAggregationLogicalFunction::MinAggregationLogicalFunction(AggregationFieldAccess inputFunction)
    : inputFunction(inputFunction), aggregateType(std::visit([](const auto& input) { return input->getDataType(); }, inputFunction))
{
}

MinAggregationLogicalFunction::MinAggregationLogicalFunction(AggregationFieldAccess inputFunction, DataType aggregateType)
    : inputFunction(std::move(inputFunction)), aggregateType(aggregateType)
{
}

bool MinAggregationLogicalFunction::shallIncludeNullValues() noexcept
{
    return true;
}

std::string_view MinAggregationLogicalFunction::getName() const noexcept
{
    return NAME;
}

DataType MinAggregationLogicalFunction::getAggregateType() const
{
    return aggregateType;
}

AggregationFieldAccess MinAggregationLogicalFunction::getInputFunction() const
{
    return inputFunction;
}

std::string MinAggregationLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Short)
    {
        return fmt::format("{}()", NAME);
    }
    auto inputExplain = std::visit([verbosity](const auto& input) { return input->explain(verbosity); }, inputFunction);
    return fmt::format("{}({})", NAME, inputExplain);
}

bool MinAggregationLogicalFunction::operator==(const MinAggregationLogicalFunction& other) const
{
    return inputFunction == other.inputFunction && aggregateType == other.aggregateType;
}

MinAggregationLogicalFunction MinAggregationLogicalFunction::withInferredType(const Schema<Field, Unordered>& schema) const
{
    auto newInputFunction = inferFieldAccess(inputFunction, schema);
    if (!newInputFunction->getDataType().isNumeric())
    {
        throw CannotInferStamp("Cannot calculate minimum value on non-numeric function {}.", *newInputFunction);
    }
    return MinAggregationLogicalFunction{newInputFunction, newInputFunction->getDataType()};
}

Reflected MinAggregationLogicalFunction::reflect() const
{
    return NES::reflect(this);
}

Reflected Reflector<MinAggregationLogicalFunction>::operator()(const MinAggregationLogicalFunction& function) const
{
    return reflect(function.getInputFunction());
}

MinAggregationLogicalFunction
Unreflector<MinAggregationLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    return MinAggregationLogicalFunction{context.unreflect<AggregationFieldAccess>(reflected)};
}

AggregationLogicalFunctionRegistryReturnType
AggregationLogicalFunctionGeneratedRegistrar::RegisterMinAggregationLogicalFunction(AggregationLogicalFunctionRegistryArguments arguments)
{
    if (arguments.on.size() != 1)
    {
        throw CannotDeserialize("MinAggregationLogicalFunction requires exactly one field, but got {}", arguments.on.size());
    }
    return MinAggregationLogicalFunction{arguments.on.at(0)};
}
}

size_t
std::hash<NES::MinAggregationLogicalFunction>::operator()(const NES::MinAggregationLogicalFunction& aggregationFunction) const noexcept
{
    return folly::hash::hash_combine(aggregationFunction.getInputFunction(), aggregationFunction.getName());
}
