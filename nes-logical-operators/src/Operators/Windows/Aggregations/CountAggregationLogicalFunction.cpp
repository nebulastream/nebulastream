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

#include <Operators/Windows/Aggregations/CountAggregationLogicalFunction.hpp>

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
/// COUNT(*) includes null values (counts all rows), COUNT(fieldName) does not
CountAggregationLogicalFunction::CountAggregationLogicalFunction(AggregationFieldAccess inputFunction, const bool includeNullValues)
    : inputFunction(std::move(inputFunction)), includeNullValues(includeNullValues)
{
}

bool CountAggregationLogicalFunction::shallIncludeNullValues() const noexcept
{
    return includeNullValues;
}

std::string_view CountAggregationLogicalFunction::getName() const noexcept
{
    return NAME;
}

DataType CountAggregationLogicalFunction::getAggregateType()
{
    return DataTypeProvider::provideDataType(finalAggregateStampType);
}

AggregationFieldAccess CountAggregationLogicalFunction::getInputFunction() const
{
    return inputFunction;
}

std::string CountAggregationLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Short)
    {
        return fmt::format("{}()", NAME);
    }
    auto inputExplain = std::visit([verbosity](const auto& input) { return input->explain(verbosity); }, inputFunction);
    return fmt::format("{}({})", NAME, inputExplain);
}

bool CountAggregationLogicalFunction::operator==(const CountAggregationLogicalFunction& other) const
{
    return inputFunction == other.inputFunction;
}

Reflected CountAggregationLogicalFunction::reflect() const
{
    return NES::reflect(this);
}

CountAggregationLogicalFunction CountAggregationLogicalFunction::withInferredType(const Schema<Field, Unordered>& schema) const
{
    if (includeNullValues)
    {
        /// COUNT(*) does not refer to a specific field; use the first field of the schema as a placeholder
        PRECONDITION(schema.size() > 1, "Schema needs to have one field at least for CountAggregationLogicalFunction");
        const auto& firstField = *schema.begin();
        auto newInputFunction = TypedLogicalFunction<FieldAccessLogicalFunction>{FieldAccessLogicalFunction{firstField}};
        return CountAggregationLogicalFunction{newInputFunction, includeNullValues};
    }
    auto newInputFunction = inferFieldAccess(inputFunction, schema);
    return CountAggregationLogicalFunction{newInputFunction, includeNullValues};
}

namespace detail
{
struct ReflectedCountAggregationLogicalFunction
{
    AggregationFieldAccess inputFunction;
    bool includeNullValues;
};
}

Reflected Reflector<CountAggregationLogicalFunction>::operator()(const CountAggregationLogicalFunction& function) const
{
    return reflect(detail::ReflectedCountAggregationLogicalFunction{
        .inputFunction = function.getInputFunction(), .includeNullValues = function.shallIncludeNullValues()});
}

CountAggregationLogicalFunction
Unreflector<CountAggregationLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [inputFunction, includeNullValues] = context.unreflect<detail::ReflectedCountAggregationLogicalFunction>(reflected);
    return CountAggregationLogicalFunction{std::move(inputFunction), includeNullValues};
}

AggregationLogicalFunctionRegistryReturnType
AggregationLogicalFunctionGeneratedRegistrar::RegisterCountAggregationLogicalFunction(AggregationLogicalFunctionRegistryArguments arguments)
{
    if (arguments.on.size() != 1)
    {
        throw CannotDeserialize("CountAggregationLogicalFunction requires exactly one field, but got {}", arguments.on.size());
    }
    return CountAggregationLogicalFunction{arguments.on.at(0), arguments.includeNullValues};
}
}

size_t
std::hash<NES::CountAggregationLogicalFunction>::operator()(const NES::CountAggregationLogicalFunction& aggregationFunction) const noexcept
{
    return folly::hash::hash_combine(aggregationFunction.getInputFunction(), aggregationFunction.getName());
}
