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

#include <Operators/Windows/Aggregations/MedianAggregationLogicalFunction.hpp>

#include <memory>
#include <string>
#include <string_view>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Functions/UnboundFieldAccessLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Schema/Schema.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>

#include <utility>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/Reflection.hpp>
#include <folly/hash/Hash.h>
#include <AggregationLogicalFunctionRegistry.hpp>

namespace NES
{
MedianAggregationLogicalFunction::MedianAggregationLogicalFunction(AggregationFieldAccess inputFunction) : inputFunction(inputFunction)
{
}

std::string_view MedianAggregationLogicalFunction::getName() const noexcept
{
    return NAME;
}

DataType MedianAggregationLogicalFunction::getAggregateType() const
{
    return DataTypeProvider::provideDataType(finalAggregateStampType);
}

bool MedianAggregationLogicalFunction::shallIncludeNullValues() noexcept
{
    return true;
}

AggregationFieldAccess MedianAggregationLogicalFunction::getInputFunction() const
{
    return inputFunction;
}

std::string MedianAggregationLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Short)
    {
        return fmt::format("{}()", NAME);
    }
    auto inputExplain = std::visit([verbosity](const auto& input) { return input->explain(verbosity); }, inputFunction);
    return fmt::format("{}({})", NAME, inputExplain);
}

bool MedianAggregationLogicalFunction::operator==(const MedianAggregationLogicalFunction& other) const
{
    return inputFunction == other.inputFunction;
}

MedianAggregationLogicalFunction MedianAggregationLogicalFunction::withInferredType(const Schema<Field, Unordered>& schema) const
{
    auto newInputFunction = inferFieldAccess(inputFunction, schema);
    if (!newInputFunction->getDataType().isNumeric())
    {
        throw CannotInferStamp("Cannot calculate median over non numeric fields.", newInputFunction->getDataType().isNumeric());
    }
    return MedianAggregationLogicalFunction{newInputFunction};
}

Reflected MedianAggregationLogicalFunction::reflect() const
{
    return NES::reflect(this);
}

Reflected Reflector<MedianAggregationLogicalFunction>::operator()(const MedianAggregationLogicalFunction& function) const
{
    return reflect(function.getInputFunction());
}

MedianAggregationLogicalFunction
Unreflector<MedianAggregationLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    return MedianAggregationLogicalFunction{context.unreflect<AggregationFieldAccess>(reflected)};
}

AggregationLogicalFunctionRegistryReturnType AggregationLogicalFunctionGeneratedRegistrar::RegisterMedianAggregationLogicalFunction(
    AggregationLogicalFunctionRegistryArguments arguments)
{
    if (arguments.on.size() != 1)
    {
        throw CannotDeserialize("MedianAggregationLogicalFunction requires exactly one field, but got {}", arguments.on.size());
    }
    return MedianAggregationLogicalFunction{arguments.on.at(0)};
}
}

size_t std::hash<NES::MedianAggregationLogicalFunction>::operator()(
    const NES::MedianAggregationLogicalFunction& aggregationFunction) const noexcept
{
    return folly::hash::hash_combine(aggregationFunction.getInputFunction(), aggregationFunction.getName());
}
