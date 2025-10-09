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
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <AggregationLogicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{
AvgAggregationLogicalFunction::AvgAggregationLogicalFunction(const FieldAccessLogicalFunction& field)
    : WindowAggregationLogicalFunction(
          field.getDataType(),
          DataTypeProvider::provideDataType(partialAggregateStampType),
          DataTypeProvider::provideDataType(finalAggregateStampType),
          field)
{
}

AvgAggregationLogicalFunction::AvgAggregationLogicalFunction(
    const FieldAccessLogicalFunction& field, const FieldAccessLogicalFunction& asField)
    : WindowAggregationLogicalFunction(
          field.getDataType(),
          DataTypeProvider::provideDataType(partialAggregateStampType),
          DataTypeProvider::provideDataType(finalAggregateStampType),
          field,
          asField)
{
}

std::string_view AvgAggregationLogicalFunction::getName() const noexcept
{
    return NAME;
}

void AvgAggregationLogicalFunction::inferStamp(const Schema& schema)
{
    /// We first infer the dataType of the input field and set the output dataType as the same.
    auto newOnField = this->getOnField().withInferredDataType(schema).get<FieldAccessLogicalFunction>();
    if (not newOnField.getDataType().isNumeric())
    {
        throw CannotDeserialize("aggregations on non numeric fields is not supported.");
    }

    /// As we are performing essentially a sum and a count, we need to cast the sum to either uint64_t, int64_t or double to avoid overflow
    if (this->getOnField().getDataType().isInteger())
    {
        if (this->getOnField().getDataType().isSignedInteger())
        {
            newOnField
                = newOnField.withDataType(DataTypeProvider::provideDataType(DataType::Type::INT64)).get<FieldAccessLogicalFunction>();
        }
        else
        {
            newOnField
                = newOnField.withDataType(DataTypeProvider::provideDataType(DataType::Type::UINT64)).get<FieldAccessLogicalFunction>();
        }
    }
    else
    {
        newOnField = newOnField.withDataType(DataTypeProvider::provideDataType(DataType::Type::FLOAT64)).get<FieldAccessLogicalFunction>();
    }

    ///Set fully qualified name for the as Field
    const auto onFieldName = newOnField.getFieldName();
    const auto asFieldName = this->getAsField().getFieldName();

    const auto attributeNameResolver = onFieldName.substr(0, onFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
    ///If on and as field name are different then append the attribute name resolver from on field to the as field
    if (asFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) == std::string::npos)
    {
        this->setAsField(this->getAsField().withFieldName(attributeNameResolver + asFieldName).get<FieldAccessLogicalFunction>());
    }
    else
    {
        const auto fieldName = asFieldName.substr(asFieldName.find_last_of(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
        this->setAsField(this->getAsField().withFieldName(attributeNameResolver + fieldName).get<FieldAccessLogicalFunction>());
    }
    auto newAsField = this->getAsField().withDataType(getFinalAggregateStamp());
    this->setAsField(newAsField.get<FieldAccessLogicalFunction>());
    this->setOnField(newOnField);
    this->setInputStamp(newOnField.getDataType());
}

SerializableAggregationFunction AvgAggregationLogicalFunction::serialize() const
{
    SerializableAggregationFunction serializedAggregationFunction;
    serializedAggregationFunction.set_type(NAME);

    auto onFieldFuc = SerializableFunction();
    onFieldFuc.CopyFrom(this->getOnField().serialize());

    auto asFieldFuc = SerializableFunction();
    asFieldFuc.CopyFrom(this->getAsField().serialize());

    serializedAggregationFunction.mutable_as_field()->CopyFrom(asFieldFuc);
    serializedAggregationFunction.mutable_on_field()->CopyFrom(onFieldFuc);
    return serializedAggregationFunction;
}

AggregationLogicalFunctionRegistryReturnType
AggregationLogicalFunctionGeneratedRegistrar::RegisterAvgAggregationLogicalFunction(AggregationLogicalFunctionRegistryArguments arguments)
{
    if (arguments.fields.size() != 2)
    {
        throw CannotDeserialize("AvgAggregationLogicalFunction requires exactly two fields, but got {}", arguments.fields.size());
    }
    return std::make_shared<AvgAggregationLogicalFunction>(arguments.fields[0], arguments.fields[1]);
}
}
