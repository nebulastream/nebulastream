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

#include <Operators/Windows/Aggregations/MaxAggregationLogicalFunction.hpp>

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Functions/UnboundFieldAccessLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Schema/Schema.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <folly/hash/Hash.h>
#include <AggregationLogicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
MaxAggregationLogicalFunction::MaxAggregationLogicalFunction(AggregationFieldAccess inputFunction)
    : inputFunction(inputFunction), aggregateType(std::visit([](const auto& input) { return input->getDataType(); }, inputFunction))
{
}

MaxAggregationLogicalFunction::MaxAggregationLogicalFunction(AggregationFieldAccess inputFunction, DataType aggregateType)
    : inputFunction(std::move(inputFunction)), aggregateType(aggregateType)
{
}

bool MaxAggregationLogicalFunction::shallIncludeNullValues() noexcept
{
    return true;
}

std::string_view MaxAggregationLogicalFunction::getName() const noexcept
{
    return NAME;
}

DataType MaxAggregationLogicalFunction::getAggregateType() const
{
    return aggregateType;
}

AggregationFieldAccess MaxAggregationLogicalFunction::getInputFunction() const
{
    return inputFunction;
}

std::string MaxAggregationLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Short)
    {
        return fmt::format("{}()", NAME);
    }
    auto inputExplain = std::visit([verbosity](const auto& input) { return input->explain(verbosity); }, inputFunction);
    return fmt::format("{}({})", NAME, inputExplain);
}

bool MaxAggregationLogicalFunction::operator==(const MaxAggregationLogicalFunction& other) const
{
    return inputFunction == other.inputFunction && aggregateType == other.aggregateType;
}

MaxAggregationLogicalFunction MaxAggregationLogicalFunction::withInferredType(const Schema<Field, Unordered>& schema) const
{
    auto newInputFunction = inferFieldAccess(inputFunction, schema);

    if (!newInputFunction->getDataType().isNumeric())
    {
        throw CannotInferStamp("Cannot calculate maximum value on non-numeric function {}.", *newInputFunction);
    }
    return MaxAggregationLogicalFunction{newInputFunction, newInputFunction->getDataType()};
}

Reflected MaxAggregationLogicalFunction::reflect() const
{
    return NES::reflect(this);
}

Reflected Reflector<MaxAggregationLogicalFunction>::operator()(const MaxAggregationLogicalFunction& function) const
{
    return reflect(function.getInputFunction());
}

MaxAggregationLogicalFunction
Unreflector<MaxAggregationLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    return MaxAggregationLogicalFunction{context.unreflect<AggregationFieldAccess>(reflected)};
}

AggregationLogicalFunctionRegistryReturnType
AggregationLogicalFunctionGeneratedRegistrar::RegisterMaxAggregationLogicalFunction(AggregationLogicalFunctionRegistryArguments arguments)
{
    if (arguments.on.size() != 1)
    {
        throw CannotDeserialize("MaxAggregationLogicalFunction requires exactly one fields, but got {}", arguments.on.size());
    }
    return MaxAggregationLogicalFunction{arguments.on.at(0)};
}
}

size_t
std::hash<NES::MaxAggregationLogicalFunction>::operator()(const NES::MaxAggregationLogicalFunction& aggregationFunction) const noexcept
{
    return folly::hash::hash_combine(aggregationFunction.getInputFunction(), aggregationFunction.getName());
}
