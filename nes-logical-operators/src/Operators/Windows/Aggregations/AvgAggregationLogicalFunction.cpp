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

#include <Operators/Windows/Aggregations/AvgAggregationLogicalFunction.hpp>

#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
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
AvgAggregationLogicalFunction::AvgAggregationLogicalFunction(AggregationFieldAccess inputFunction) : inputFunction(std::move(inputFunction))
{
}

DataType AvgAggregationLogicalFunction::getAggregateType() const
{
    return DataTypeProvider::provideDataType(
        finalAggregateStampType, nullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE);
}

bool AvgAggregationLogicalFunction::shallIncludeNullValues() noexcept
{
    return true;
}

AggregationFieldAccess AvgAggregationLogicalFunction::getInputFunction() const
{
    return inputFunction;
}

std::string_view AvgAggregationLogicalFunction::getName() noexcept
{
    return NAME;
}

std::string AvgAggregationLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Short)
    {
        return fmt::format("{}()", NAME);
    }
    auto inputExplain = std::visit([verbosity](const auto& input) { return input->explain(verbosity); }, inputFunction);
    return fmt::format("{}({})", NAME, inputExplain);
}

bool AvgAggregationLogicalFunction::operator==(const AvgAggregationLogicalFunction& other) const
{
    return inputFunction == other.inputFunction;
}

AvgAggregationLogicalFunction AvgAggregationLogicalFunction::withInferredType(const Schema<Field, Unordered>& schema) const
{
    /// We first infer the dataType of the input field and set the output dataType as the same.
    const auto newInputFunction = inferFieldAccess(inputFunction, schema);
    if (!newInputFunction->getDataType().isNumeric())
    {
        throw CannotInferStamp("Cannot calculate average over non-numeric function.", *newInputFunction);
    }
    auto newAggFunction = AvgAggregationLogicalFunction{newInputFunction};
    newAggFunction.nullable = newInputFunction->getDataType().nullable;
    return newAggFunction;
}

Reflected AvgAggregationLogicalFunction::reflect() const
{
    return NES::reflect(this);
}

Reflected Reflector<AvgAggregationLogicalFunction>::operator()(const AvgAggregationLogicalFunction& function) const
{
    return reflect(function.getInputFunction());
}

AvgAggregationLogicalFunction
Unreflector<AvgAggregationLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    return AvgAggregationLogicalFunction{context.unreflect<AggregationFieldAccess>(reflected)};
}

AggregationLogicalFunctionRegistryReturnType
AggregationLogicalFunctionGeneratedRegistrar::RegisterAvgAggregationLogicalFunction(AggregationLogicalFunctionRegistryArguments arguments)
{
    if (arguments.on.size() != 1)
    {
        throw CannotDeserialize("AvgAggregationLogicalFunction requires exactly one fields, but got {}", arguments.on.size());
    }
    return AvgAggregationLogicalFunction{arguments.on.at(0)};
}
}

size_t
std::hash<NES::AvgAggregationLogicalFunction>::operator()(const NES::AvgAggregationLogicalFunction& aggregationFunction) const noexcept
{
    return folly::hash::hash_combine(aggregationFunction.getInputFunction(), NES::AvgAggregationLogicalFunction::getName());
}
