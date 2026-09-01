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

#include <Operators/Windows/Aggregations/ReservoirSampleAggregationLogicalFunction.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
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
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <folly/hash/Hash.h>
#include <ErrorHandling.hpp>

namespace NES
{

ReservoirSampleAggregationLogicalFunction::ReservoirSampleAggregationLogicalFunction(const uint64_t sampleSize, const uint64_t seed)
    : ReservoirSampleAggregationLogicalFunction(
          TypedLogicalFunction<UnboundFieldAccessLogicalFunction>{
              UnboundFieldAccessLogicalFunction{Identifier::parse(std::string{PlaceholderFieldName})}},
          sampleSize,
          seed)
{
}

ReservoirSampleAggregationLogicalFunction::ReservoirSampleAggregationLogicalFunction(
    AggregationFieldAccess inputFunction, const uint64_t sampleSize, const uint64_t seed)
    : inputFunction(std::move(inputFunction)), sampleSize(sampleSize), seed(seed)
{
}

std::string_view ReservoirSampleAggregationLogicalFunction::getName() const noexcept
{
    return NAME;
}

DataType ReservoirSampleAggregationLogicalFunction::getAggregateType()
{
    return DataTypeProvider::provideDataType(finalAggregateStampType);
}

bool ReservoirSampleAggregationLogicalFunction::shallIncludeNullValues() noexcept
{
    return true;
}

AggregationFieldAccess ReservoirSampleAggregationLogicalFunction::getInputFunction() const
{
    return inputFunction;
}

uint64_t ReservoirSampleAggregationLogicalFunction::getSampleSize() const
{
    return sampleSize;
}

uint64_t ReservoirSampleAggregationLogicalFunction::getSeed() const
{
    return seed;
}

std::string ReservoirSampleAggregationLogicalFunction::explain(const ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Short)
    {
        return fmt::format("{}({})", NAME, sampleSize);
    }
    return fmt::format("{}(sampleSize: {}, seed: {})", NAME, sampleSize, seed);
}

bool ReservoirSampleAggregationLogicalFunction::operator==(const ReservoirSampleAggregationLogicalFunction& other) const
{
    return inputFunction == other.inputFunction and sampleSize == other.sampleSize and seed == other.seed;
}

ReservoirSampleAggregationLogicalFunction
ReservoirSampleAggregationLogicalFunction::withInferredType(const Schema<Field, Unordered>& schema) const
{
    PRECONDITION(schema.size() >= 1, "A reservoir sample needs at least one input field");
    const auto placeholderField
        = std::ranges::min_element(schema, {}, [](const auto& field) { return fmt::format("{}", field.getFullyQualifiedName()); });
    const TypedLogicalFunction<FieldAccessLogicalFunction> newInputFunction{FieldAccessLogicalFunction{*placeholderField}};
    return ReservoirSampleAggregationLogicalFunction{newInputFunction, sampleSize, seed};
}

namespace detail
{
struct ReflectedReservoirSampleAggregationLogicalFunction
{
    AggregationFieldAccess inputFunction;
    uint64_t sampleSize;
    uint64_t seed;
};
}

Reflected Reflector<ReservoirSampleAggregationLogicalFunction>::operator()(
    const ReservoirSampleAggregationLogicalFunction& function, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedReservoirSampleAggregationLogicalFunction{
        .inputFunction = function.getInputFunction(), .sampleSize = function.getSampleSize(), .seed = function.getSeed()});
}

ReservoirSampleAggregationLogicalFunction Unreflector<ReservoirSampleAggregationLogicalFunction>::operator()(
    const Reflected& reflected, const ReflectionContext& context) const
{
    auto [inputFunction, sampleSize, seed]
        = context.unreflect<detail::ReflectedReservoirSampleAggregationLogicalFunction>(reflected);
    return ReservoirSampleAggregationLogicalFunction{std::move(inputFunction), sampleSize, seed};
}

}

size_t std::hash<NES::ReservoirSampleAggregationLogicalFunction>::operator()(
    const NES::ReservoirSampleAggregationLogicalFunction& aggregationFunction) const noexcept
{
    return folly::hash::hash_combine(
        aggregationFunction.getInputFunction(),
        aggregationFunction.getName(),
        aggregationFunction.getSampleSize(),
        aggregationFunction.getSeed());
}
