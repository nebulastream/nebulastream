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

#include <memory>
#include <string>
#include <string_view>
#include <utility>
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

namespace NES
{
AvgAggregationLogicalFunction::AvgAggregationLogicalFunction(AggregationFieldAccess inputFunction) : inputFunction(std::move(inputFunction))
{
}

DataType AvgAggregationLogicalFunction::getAggregateType() const
{
    return DataTypeProvider::provideDataType(finalAggregateStampType);
}

bool AvgAggregationLogicalFunction::shallIncludeNullValues() noexcept
{
    return true;
}

AggregationFieldAccess AvgAggregationLogicalFunction::getInputFunction() const
{
    return inputFunction;
}

std::string_view AvgAggregationLogicalFunction::getName() const noexcept
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
    /// TODO cast the on field to 64 bit wide types to avoid overlflows, see old code

    return AvgAggregationLogicalFunction{newInputFunction};


    // Null handling

    /// As we are performing essentially a sum and a count, we need to cast the sum to either uint64_t, int64_t or double to avoid overflow
    if (this->getOnField().getDataType().isInteger())
    {
        if (this->getOnField().getDataType().isSignedInteger())
        {
            newOnField = newOnField.withDataType(DataTypeProvider::provideDataType(
                DataType::Type::INT64,
                newOnField.getDataType().nullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE));
        }
        else
        {
            newOnField = newOnField.withDataType(DataTypeProvider::provideDataType(
                DataType::Type::UINT64,
                newOnField.getDataType().nullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE));
        }
    }
    else
    {
        newOnField = newOnField.withDataType(DataTypeProvider::provideDataType(
            DataType::Type::FLOAT64,
            newOnField.getDataType().nullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE));
    }

    ///Set fully qualified name for the as Field
    const auto onFieldName = newOnField.getAs<FieldAccessLogicalFunction>()->getFieldName();
    const auto asFieldName = this->getAsField().getFieldName();
    const auto attributeNameResolver = onFieldName.substr(0, onFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);

    std::string newAsFieldName;
    ///If on and as field name are different then append the attribute name resolver from on field to the as field
    if (asFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) == std::string::npos)
    {
        newAsFieldName = attributeNameResolver + asFieldName;
    }
    else
    {
        const auto fieldName = asFieldName.substr(asFieldName.find_last_of(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
        newAsFieldName = attributeNameResolver + fieldName;
    }

    const auto newFinalAggregateStamp = DataTypeProvider::provideDataType(
        DataType::Type::FLOAT64, newOnField.getDataType().nullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE);
    return this->withOnField(newOnField.getAs<FieldAccessLogicalFunction>().get())
        .withFinalAggregateStamp(newFinalAggregateStamp)
        .withAsField(this->getAsField().withFieldName(newAsFieldName).withDataType(newFinalAggregateStamp))
        .withInputStamp(newOnField.getDataType());
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
    return folly::hash::hash_combine(aggregationFunction.getInputFunction(), aggregationFunction.getName());
}
