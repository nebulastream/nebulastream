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

#include <Operators/Windows/Aggregations/CountDistinctAggregationLogicalFunction.hpp>

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
CountDistinctAggregationLogicalFunction::CountDistinctAggregationLogicalFunction(AggregationFieldAccess inputFunction)
    : inputFunction(std::move(inputFunction))
{
}

std::string_view CountDistinctAggregationLogicalFunction::getName() noexcept
{
    return NAME;
}

/// COUNT(DISTINCT x) counts the distinct non-null values of x; it is 0, not NULL, when there are none.
DataType CountDistinctAggregationLogicalFunction::getAggregateType()
{
    return DataTypeProvider::provideDataType(finalAggregateStampType);
}

bool CountDistinctAggregationLogicalFunction::shallIncludeNullValues() noexcept
{
    return false;
}

AggregationFieldAccess CountDistinctAggregationLogicalFunction::getInputFunction() const
{
    return inputFunction;
}

std::string CountDistinctAggregationLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Short)
    {
        return fmt::format("{}()", NAME);
    }
    auto inputExplain = std::visit([verbosity](const auto& input) { return input->explain(verbosity); }, inputFunction);
    return fmt::format("{}({})", NAME, inputExplain);
}

bool CountDistinctAggregationLogicalFunction::operator==(const CountDistinctAggregationLogicalFunction& other) const
{
    return inputFunction == other.inputFunction;
}

CountDistinctAggregationLogicalFunction CountDistinctAggregationLogicalFunction::withInferredType(const Schema<Field, Unordered>& schema) const
{
    return CountDistinctAggregationLogicalFunction{inferFieldAccess(inputFunction, schema)};
}

Reflected Reflector<CountDistinctAggregationLogicalFunction>::operator()(
    const CountDistinctAggregationLogicalFunction& function, const ReflectionContext& context) const
{
    return context.reflect(function.getInputFunction());
}

CountDistinctAggregationLogicalFunction
Unreflector<CountDistinctAggregationLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    return CountDistinctAggregationLogicalFunction{context.unreflect<AggregationFieldAccess>(reflected)};
}

AggregationLogicalFunctionRegistryReturnType CountDistinctAggregationLogicalFunction::create(AggregationLogicalFunctionRegistryArguments arguments)
{
    if (arguments.on.size() != 1)
    {
        throw CannotDeserialize("CountDistinctAggregationLogicalFunction requires exactly one field, but got {}", arguments.on.size());
    }
    return CountDistinctAggregationLogicalFunction{arguments.on.at(0)};
}
}

size_t std::hash<NES::CountDistinctAggregationLogicalFunction>::operator()(
    const NES::CountDistinctAggregationLogicalFunction& aggregationFunction) const noexcept
{
    return folly::hash::hash_combine(aggregationFunction.getInputFunction(), NES::CountDistinctAggregationLogicalFunction::getName());
}
