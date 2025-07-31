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

#include <memory>
#include <string>
#include <string_view>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
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
#include "DataTypes/DataType.hpp"
#include "DataTypes/DataTypeProvider.hpp"

namespace NES
{
SumAggregationLogicalFunction::SumAggregationLogicalFunction(AggregationFieldAccess inputFunction)
    : inputFunction(inputFunction), aggregateType(std::visit([](const auto& input) { return input->getDataType(); }, inputFunction))
{
}

SumAggregationLogicalFunction::SumAggregationLogicalFunction(AggregationFieldAccess inputFunction, const DataType aggregateType)
    : inputFunction(std::move(inputFunction)), aggregateType(aggregateType) {
}

bool SumAggregationLogicalFunction::shallIncludeNullValues() noexcept
{
    return true;
}

std::string_view SumAggregationLogicalFunction::getName() const noexcept
{
    return NAME;
}

DataType SumAggregationLogicalFunction::inferFromInput(const DataType inputType)
{
    if (inputType.type == DataType::Type::UNDEFINED)
    {
        return DataTypeProvider::provideDataType(DataType::Type::UNDEFINED);
    }
    if (inputType.isSignedInteger())
    {
        return DataTypeProvider::provideDataType(DataType::Type::INT64);
    }
    if (inputType.isInteger())
    {
        return DataTypeProvider::provideDataType(DataType::Type::UINT64);
    }
    if (inputType.isFloat())
    {
        return DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
    }
    throw CannotInferStamp("aggregations on non numeric fields is not supported.", inputType.isNumeric());
}

SumAggregationLogicalFunction SumAggregationLogicalFunction::withInferredType(const Schema<Field, Unordered>& schema) const
{
    const auto newInputFunction = inferFieldAccess(inputFunction, schema);
    const DataType outputType = newInputFunction->getDataType();
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

Reflected SumAggregationLogicalFunction::reflect() const
{
    return NES::reflect(this);
}

Reflected Reflector<SumAggregationLogicalFunction>::operator()(const SumAggregationLogicalFunction& function) const
{
    return reflect(function.getInputFunction());
}

SumAggregationLogicalFunction
Unreflector<SumAggregationLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    return SumAggregationLogicalFunction{context.unreflect<AggregationFieldAccess>(reflected)};
}

AggregationLogicalFunctionRegistryReturnType
AggregationLogicalFunctionGeneratedRegistrar::RegisterSumAggregationLogicalFunction(AggregationLogicalFunctionRegistryArguments arguments)
{
    if (arguments.on.size() != 1)
    {
        throw CannotDeserialize("SumAggregationLogicalFunction requires exactly one field, but got {}", arguments.on.size());
    }
    return SumAggregationLogicalFunction{arguments.on.at(0)};
}
}

size_t
std::hash<NES::SumAggregationLogicalFunction>::operator()(const NES::SumAggregationLogicalFunction& aggregationFunction) const noexcept
{
    return folly::hash::hash_combine(aggregationFunction.getInputFunction(), aggregationFunction.getName());
}
