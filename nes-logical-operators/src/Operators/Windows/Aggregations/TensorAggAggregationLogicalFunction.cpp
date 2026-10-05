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

#include <Operators/Windows/Aggregations/TensorAggAggregationLogicalFunction.hpp>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <folly/hash/Hash.h>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{
std::string explainFields(const std::vector<AggregationFieldAccess>& functions, const ExplainVerbosity verbosity)
{
    std::vector<std::string> explained;
    explained.reserve(functions.size());
    for (const auto& function : functions)
    {
        explained.push_back(std::visit([verbosity](const auto& input) { return input->explain(verbosity); }, function));
    }
    return fmt::format("{}", fmt::join(explained, ", "));
}

void validateLayout(const size_t numberOfValues, const size_t numberOfIndices, const std::vector<uint64_t>& shape)
{
    if (numberOfValues == 0 || numberOfIndices == 0)
    {
        throw CannotInferStamp("TENSOR_AGG requires at least one value field and one index field.");
    }
    const bool valueAxisListed = shape.size() == numberOfIndices + 1 && shape.back() == numberOfValues;
    const bool valueAxisOmitted = shape.size() == numberOfIndices && numberOfValues == 1;
    if (!valueAxisListed && !valueAxisOmitted)
    {
        throw CannotInferStamp(
            "TENSOR_AGG with {} index field(s) and {} value field(s) requires SHAPE to list {} axes followed by the value axis of "
            "length {}, but got SHAPE ({}).",
            numberOfIndices,
            numberOfValues,
            numberOfIndices,
            numberOfValues,
            fmt::join(shape, ", "));
    }

    uint64_t numberOfTensorValues = 1;
    for (const auto dimension : shape)
    {
        if (dimension == 0)
        {
            throw CannotInferStamp("TENSOR_AGG requires every axis of SHAPE ({}) to be at least 1.", fmt::join(shape, ", "));
        }
        if (numberOfTensorValues > TensorAggAggregationLogicalFunction::MAX_NUMBER_OF_VALUES / dimension)
        {
            throw CannotInferStamp(
                "TENSOR_AGG SHAPE ({}) exceeds the limit of {} values per tensor.",
                fmt::join(shape, ", "),
                TensorAggAggregationLogicalFunction::MAX_NUMBER_OF_VALUES);
        }
        numberOfTensorValues *= dimension;
    }
}
}

TensorAggAggregationLogicalFunction::TensorAggAggregationLogicalFunction(
    std::vector<AggregationFieldAccess> valueFunctions,
    std::vector<AggregationFieldAccess> indexFunctions,
    std::vector<uint64_t> shape,
    const float defaultValue)
    : valueFunctions(std::move(valueFunctions))
    , indexFunctions(std::move(indexFunctions))
    , shape(std::move(shape))
    , defaultValue(defaultValue)
{
}

std::string_view TensorAggAggregationLogicalFunction::getName() noexcept
{
    return NAME;
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
DataType TensorAggAggregationLogicalFunction::getAggregateType() const
{
    return DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
}

bool TensorAggAggregationLogicalFunction::shallIncludeNullValues() noexcept
{
    return false;
}

AggregationFieldAccess TensorAggAggregationLogicalFunction::getInputFunction() const
{
    return valueFunctions.front();
}

std::vector<AggregationFieldAccess> TensorAggAggregationLogicalFunction::getInputFunctions() const
{
    auto inputFunctions = valueFunctions;
    inputFunctions.insert(inputFunctions.end(), indexFunctions.begin(), indexFunctions.end());
    return inputFunctions;
}

const std::vector<AggregationFieldAccess>& TensorAggAggregationLogicalFunction::getValueFunctions() const
{
    return valueFunctions;
}

const std::vector<AggregationFieldAccess>& TensorAggAggregationLogicalFunction::getIndexFunctions() const
{
    return indexFunctions;
}

const std::vector<uint64_t>& TensorAggAggregationLogicalFunction::getShape() const
{
    return shape;
}

float TensorAggAggregationLogicalFunction::getDefaultValue() const
{
    return defaultValue;
}

std::string TensorAggAggregationLogicalFunction::explain(const ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Short)
    {
        return fmt::format("{}()", NAME);
    }
    return fmt::format(
        "{}({} AT ({}) SHAPE ({}) DEFAULT {})",
        NAME,
        explainFields(valueFunctions, verbosity),
        explainFields(indexFunctions, verbosity),
        fmt::join(shape, ", "),
        defaultValue);
}

bool TensorAggAggregationLogicalFunction::operator==(const TensorAggAggregationLogicalFunction& other) const
{
    return valueFunctions == other.valueFunctions && indexFunctions == other.indexFunctions && shape == other.shape
        && defaultValue == other.defaultValue;
}

TensorAggAggregationLogicalFunction TensorAggAggregationLogicalFunction::withInferredType(const Schema<Field, Unordered>& schema) const
{
    validateLayout(valueFunctions.size(), indexFunctions.size(), shape);

    std::vector<AggregationFieldAccess> inferredValues;
    for (const auto& valueFunction : valueFunctions)
    {
        auto value = inferFieldAccess(valueFunction, schema);
        if (!value->getDataType().isNumeric())
        {
            throw CannotInferStamp(
                "TENSOR_AGG requires numeric value fields, but {} is {}.", value->getField().getLastName(), value->getDataType());
        }
        inferredValues.emplace_back(value);
    }

    std::vector<AggregationFieldAccess> inferredIndices;
    for (const auto& indexFunction : indexFunctions)
    {
        auto index = inferFieldAccess(indexFunction, schema);
        if (!index->getDataType().isInteger())
        {
            throw CannotInferStamp(
                "TENSOR_AGG requires integer index fields, but {} is {}.", index->getField().getLastName(), index->getDataType());
        }
        inferredIndices.emplace_back(index);
    }
    return TensorAggAggregationLogicalFunction{std::move(inferredValues), std::move(inferredIndices), shape, defaultValue};
}

namespace detail
{
struct ReflectedTensorAggAggregationLogicalFunction
{
    std::vector<AggregationFieldAccess> valueFunctions;
    std::vector<AggregationFieldAccess> indexFunctions;
    std::vector<uint64_t> shape;
    float defaultValue;
};
}

Reflected Reflector<TensorAggAggregationLogicalFunction>::operator()(
    const TensorAggAggregationLogicalFunction& function, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedTensorAggAggregationLogicalFunction{
        .valueFunctions = function.getValueFunctions(),
        .indexFunctions = function.getIndexFunctions(),
        .shape = function.getShape(),
        .defaultValue = function.getDefaultValue()});
}

TensorAggAggregationLogicalFunction
Unreflector<TensorAggAggregationLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [valueFunctions, indexFunctions, shape, defaultValue]
        = context.unreflect<detail::ReflectedTensorAggAggregationLogicalFunction>(reflected);
    return TensorAggAggregationLogicalFunction{std::move(valueFunctions), std::move(indexFunctions), std::move(shape), defaultValue};
}
}

size_t std::hash<NES::TensorAggAggregationLogicalFunction>::operator()(
    const NES::TensorAggAggregationLogicalFunction& aggregationFunction) const noexcept
{
    auto hash = std::hash<std::string_view>{}(NES::TensorAggAggregationLogicalFunction::getName());
    for (const auto& function : aggregationFunction.getInputFunctions())
    {
        hash = folly::hash::hash_combine(hash, function);
    }
    for (const auto dimension : aggregationFunction.getShape())
    {
        hash = folly::hash::hash_combine(hash, dimension);
    }
    return folly::hash::hash_combine(hash, aggregationFunction.getDefaultValue());
}
