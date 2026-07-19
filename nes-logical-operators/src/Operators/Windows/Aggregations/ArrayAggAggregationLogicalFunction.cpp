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

#include <Operators/Windows/Aggregations/ArrayAggAggregationLogicalFunction.hpp>

#include <cstddef>
#include <string>
#include <string_view>
#include <utility>
#include <variant>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
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
ArrayAggAggregationLogicalFunction::ArrayAggAggregationLogicalFunction(AggregationFieldAccess inputFunction)
    : inputFunction(std::move(inputFunction))
{
}

std::string_view ArrayAggAggregationLogicalFunction::getName() noexcept
{
    return NAME;
}

DataType ArrayAggAggregationLogicalFunction::getAggregateType() const
{
    return DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
}

bool ArrayAggAggregationLogicalFunction::shallIncludeNullValues() noexcept
{
    return false;
}

AggregationFieldAccess ArrayAggAggregationLogicalFunction::getInputFunction() const
{
    return inputFunction;
}

std::string ArrayAggAggregationLogicalFunction::explain(const ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Short)
    {
        return fmt::format("{}()", NAME);
    }
    const auto inputExplain = std::visit([verbosity](const auto& input) { return input->explain(verbosity); }, inputFunction);
    return fmt::format("{}({})", NAME, inputExplain);
}

bool ArrayAggAggregationLogicalFunction::operator==(const ArrayAggAggregationLogicalFunction& other) const
{
    return inputFunction == other.inputFunction;
}

ArrayAggAggregationLogicalFunction
ArrayAggAggregationLogicalFunction::withInferredType(const Schema<Field, Unordered>& schema) const
{
    auto newInputFunction = inferFieldAccess(inputFunction, schema);
    const auto inputType = newInputFunction->getDataType();
    if (inputType.type == DataType::Type::VARSIZED)
    {
        throw CannotInferStamp("ARRAY_AGG requires a fixed-size input field, but got {}.", inputType);
    }
    return ArrayAggAggregationLogicalFunction{newInputFunction};
}

Reflected Reflector<ArrayAggAggregationLogicalFunction>::operator()(
    const ArrayAggAggregationLogicalFunction& function, const ReflectionContext& context) const
{
    return context.reflect(function.getInputFunction());
}

ArrayAggAggregationLogicalFunction
Unreflector<ArrayAggAggregationLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    return ArrayAggAggregationLogicalFunction{context.unreflect<AggregationFieldAccess>(reflected)};
}

AggregationLogicalFunctionRegistryReturnType AggregationLogicalFunctionGeneratedRegistrar::RegisterArrayAggAggregationLogicalFunction(
    AggregationLogicalFunctionRegistryArguments arguments)
{
    if (arguments.on.size() != 1)
    {
        throw CannotDeserialize("ArrayAggAggregationLogicalFunction requires exactly one field, but got {}", arguments.on.size());
    }
    return ArrayAggAggregationLogicalFunction{arguments.on.at(0)};
}
}

size_t std::hash<NES::ArrayAggAggregationLogicalFunction>::operator()(
    const NES::ArrayAggAggregationLogicalFunction& aggregationFunction) const noexcept
{
    return folly::hash::hash_combine(
        aggregationFunction.getInputFunction(), NES::ArrayAggAggregationLogicalFunction::getName());
}
