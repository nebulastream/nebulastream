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

#include <Operators/Windows/Aggregations/SumAggregationLogicalFunction.hpp>

#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <variant>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Functions/UnboundFieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/Overloaded.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <folly/hash/Hash.h>
#include <magic_enum/magic_enum.hpp>
#include <AggregationLogicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{
Identifier fieldNameOf(const AggregationFieldAccess& inputFunction)
{
    return std::visit(
        Overloaded{
            [](const TypedLogicalFunction<UnboundFieldAccessLogicalFunction>& unbound) { return unbound->getFieldName(); },
            [](const TypedLogicalFunction<FieldAccessLogicalFunction>& bound) { return bound->getField().getLastName(); }},
        inputFunction);
}
}

SumAggregationLogicalFunction::SumAggregationLogicalFunction(AggregationFieldAccess inputFunction)
    : inputFunction(inputFunction)
    , aggregateType(
          inferFromInput(std::visit([](const auto& input) { return input->getDataType(); }, inputFunction), fieldNameOf(inputFunction)))
{
}

SumAggregationLogicalFunction::SumAggregationLogicalFunction(AggregationFieldAccess inputFunction, const DataType aggregateType)
    : inputFunction(std::move(inputFunction)), aggregateType(aggregateType)
{
}

bool SumAggregationLogicalFunction::shallIncludeNullValues() noexcept
{
    return true;
}

std::string_view SumAggregationLogicalFunction::getName() const noexcept
{
    return NAME;
}

/// Widens the result type to avoid overflows when summing many narrow values (#1694). The SQL standard (ISO/IEC 9075-2, <sum>)
/// requires a numeric result with implementation-defined precision at least that of the argument. Lacking NUMERIC/INT128, we follow
/// PostgreSQL: integers are promoted to the widest available integer type (sum(int2/int4) -> int8), while floats keep their type
/// (sum(real) -> real) since they saturate to infinity instead of wrapping around. Sums exceeding the 64-bit integer range still
/// wrap around silently.
DataType SumAggregationLogicalFunction::inferFromInput(const DataType inputType, const Identifier& inputFieldName)
{
    if (inputType.type == DataType::Type::UNDEFINED)
    {
        return DataTypeProvider::provideDataType(
            DataType::Type::UNDEFINED, inputType.nullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE);
    }
    if (inputType.isSignedInteger())
    {
        return DataTypeProvider::provideDataType(
            DataType::Type::INT64, inputType.nullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE);
    }
    if (inputType.isInteger())
    {
        return DataTypeProvider::provideDataType(
            DataType::Type::UINT64, inputType.nullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE);
    }
    if (inputType.isFloat())
    {
        return inputType;
    }
    throw UnsupportedQuery(
        "{} is only supported on numeric fields, but field {} has type {}", NAME, inputFieldName, magic_enum::enum_name(inputType.type));
}

SumAggregationLogicalFunction SumAggregationLogicalFunction::withInferredType(const Schema<Field, Unordered>& schema) const
{
    const auto newInputFunction = inferFieldAccess(inputFunction, schema);
    const DataType outputType = inferFromInput(newInputFunction->getDataType(), newInputFunction->getField().getLastName());
    return SumAggregationLogicalFunction{newInputFunction, outputType};
}

DataType SumAggregationLogicalFunction::getAggregateType() const
{
    return aggregateType;
}

AggregationFieldAccess SumAggregationLogicalFunction::getInputFunction() const
{
    return inputFunction;
}

std::string SumAggregationLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Short)
    {
        return fmt::format("{}()", NAME);
    }
    auto inputExplain = std::visit([verbosity](const auto& input) { return input->explain(verbosity); }, inputFunction);
    return fmt::format("{}({})", NAME, inputExplain);
}

bool SumAggregationLogicalFunction::operator==(const SumAggregationLogicalFunction& other) const
{
    return inputFunction == other.inputFunction && aggregateType == other.aggregateType;
}

Reflected
Reflector<SumAggregationLogicalFunction>::operator()(const SumAggregationLogicalFunction& function, const ReflectionContext& context) const
{
    return context.reflect(function.getInputFunction());
}

SumAggregationLogicalFunction
Unreflector<SumAggregationLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    return SumAggregationLogicalFunction{context.unreflect<AggregationFieldAccess>(reflected)};
}

AggregationLogicalFunctionRegistryReturnType SumAggregationLogicalFunction::create(AggregationLogicalFunctionRegistryArguments arguments)
{
    if (arguments.on.size() != 1)
    {
        throw InvalidLogicalFunctionArgument("SumAggregationLogicalFunction requires exactly one field, but got {}", arguments.on.size());
    }
    return SumAggregationLogicalFunction{arguments.on.at(0)};
}
}

size_t
std::hash<NES::SumAggregationLogicalFunction>::operator()(const NES::SumAggregationLogicalFunction& aggregationFunction) const noexcept
{
    return folly::hash::hash_combine(aggregationFunction.getInputFunction(), aggregationFunction.getName());
}
