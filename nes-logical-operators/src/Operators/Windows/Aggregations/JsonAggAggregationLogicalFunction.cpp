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

#include <Operators/Windows/Aggregations/JsonAggAggregationLogicalFunction.hpp>

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
JsonAggAggregationLogicalFunction::JsonAggAggregationLogicalFunction(AggregationFieldAccess inputFunction)
    : inputFunction(std::move(inputFunction))
{
}

std::string_view JsonAggAggregationLogicalFunction::getName() noexcept
{
    return NAME;
}

DataType JsonAggAggregationLogicalFunction::getAggregateType() const
{
    return DataTypeProvider::provideDataType(DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE);
}

bool JsonAggAggregationLogicalFunction::shallIncludeNullValues() noexcept
{
    return true;
}

AggregationFieldAccess JsonAggAggregationLogicalFunction::getInputFunction() const
{
    return inputFunction;
}

std::string JsonAggAggregationLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Short)
    {
        return fmt::format("{}()", NAME);
    }
    auto inputExplain = std::visit([verbosity](const auto& input) { return input->explain(verbosity); }, inputFunction);
    return fmt::format("{}({})", NAME, inputExplain);
}

bool JsonAggAggregationLogicalFunction::operator==(const JsonAggAggregationLogicalFunction& other) const
{
    return inputFunction == other.inputFunction;
}

JsonAggAggregationLogicalFunction JsonAggAggregationLogicalFunction::withInferredType(const Schema<Field, Unordered>& schema) const
{
    auto newInputFunction = inferFieldAccess(inputFunction, schema);
    if (!newInputFunction->getDataType().isType(DataType::Type::VARSIZED) || newInputFunction->getDataType().nullable)
    {
        throw UnsupportedQuery(
            "JSON_AGG is only supported on NOT NULL VARSIZED fields, but field {} has type {}{}",
            newInputFunction->getField().getLastName(),
            magic_enum::enum_name(newInputFunction->getDataType().type),
            newInputFunction->getDataType().nullable ? " (nullable)" : "");
    }
    return JsonAggAggregationLogicalFunction{newInputFunction};
}

Reflected Reflector<JsonAggAggregationLogicalFunction>::operator()(
    const JsonAggAggregationLogicalFunction& function, const ReflectionContext& context) const
{
    return context.reflect(function.getInputFunction());
}

JsonAggAggregationLogicalFunction
Unreflector<JsonAggAggregationLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    return JsonAggAggregationLogicalFunction{context.unreflect<AggregationFieldAccess>(reflected)};
}

AggregationLogicalFunctionRegistryReturnType
JsonAggAggregationLogicalFunction::create(AggregationLogicalFunctionRegistryArguments arguments)
{
    if (arguments.on.size() != 1)
    {
        throw CannotDeserialize("JsonAggAggregationLogicalFunction requires exactly one field, but got {}", arguments.on.size());
    }
    return JsonAggAggregationLogicalFunction{arguments.on.at(0)};
}
}

size_t std::hash<NES::JsonAggAggregationLogicalFunction>::operator()(
    const NES::JsonAggAggregationLogicalFunction& aggregationFunction) const noexcept
{
    return folly::hash::hash_combine(aggregationFunction.getInputFunction(), NES::JsonAggAggregationLogicalFunction::getName());
}
