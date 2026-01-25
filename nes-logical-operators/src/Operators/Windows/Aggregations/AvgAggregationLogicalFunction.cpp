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
AvgAggregationLogicalFunction::AvgAggregationLogicalFunction(
    DataType inputStamp, DataType partialAggregateStamp, DataType finalAggregateStamp, const FieldAccessLogicalFunction& onField)
    : AvgAggregationLogicalFunction(
          std::move(inputStamp), std::move(partialAggregateStamp), std::move(finalAggregateStamp), onField, onField)
{
}

AvgAggregationLogicalFunction::AvgAggregationLogicalFunction(
    DataType inputStamp,
    DataType partialAggregateStamp,
    DataType finalAggregateStamp,
    FieldAccessLogicalFunction onField,
    FieldAccessLogicalFunction asField)
    : inputStamp(std::move(inputStamp))
    , partialAggregateStamp(std::move(partialAggregateStamp))
    , finalAggregateStamp(std::move(finalAggregateStamp))
    , onField(std::move(onField))
    , asField(std::move(asField))
{
}

AvgAggregationLogicalFunction::AvgAggregationLogicalFunction(const FieldAccessLogicalFunction& field)
    : AvgAggregationLogicalFunction(
          field.getDataType(),
          DataTypeProvider::provideDataType(partialAggregateStampType),
          DataTypeProvider::provideDataType(finalAggregateStampType),
          field)
{
}

AvgAggregationLogicalFunction::AvgAggregationLogicalFunction(
    const FieldAccessLogicalFunction& field, const FieldAccessLogicalFunction& asField)
    : AvgAggregationLogicalFunction(
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
    auto newOnField = this->getOnField().withInferredDataType(schema);
    if (not newOnField.getDataType().isNumeric())
    {
        throw CannotDeserialize("aggregations on non numeric fields is not supported.");
    }

    /// As we are performing essentially a sum and a count, we need to cast the sum to either uint64_t, int64_t or double to avoid overflow
    if (this->getOnField().getDataType().isInteger())
    {
        if (this->getOnField().getDataType().isSignedInteger())
        {
            newOnField = newOnField.withDataType(DataTypeProvider::provideDataType(DataType::Type::INT64));
        }
        else
        {
            newOnField = newOnField.withDataType(DataTypeProvider::provideDataType(DataType::Type::UINT64));
        }
    }
    else
    {
        newOnField = newOnField.withDataType(DataTypeProvider::provideDataType(DataType::Type::FLOAT64));
    }

    ///Set fully qualified name for the as Field
    const auto onFieldName = newOnField.getAs<FieldAccessLogicalFunction>().get().getFieldName();
    const auto asFieldName = this->getAsField().getFieldName();

    const auto attributeNameResolver = onFieldName.substr(0, onFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
    ///If on and as field name are different then append the attribute name resolver from on field to the as field
    if (asFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) == std::string::npos)
    {
        this->setAsField(this->getAsField().withFieldName(attributeNameResolver + asFieldName));
    }
    else
    {
        const auto fieldName = asFieldName.substr(asFieldName.find_last_of(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
        this->setAsField(this->getAsField().withFieldName(attributeNameResolver + fieldName));
    }
    auto newAsField = this->getAsField().withDataType(getFinalAggregateStamp());
    this->setAsField(newAsField);
    this->setOnField(newOnField.getAs<FieldAccessLogicalFunction>().get());
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
    return std::make_shared<WindowAggregationLogicalFunction>(AvgAggregationLogicalFunction(arguments.fields[0], arguments.fields[1]));
}

std::string AvgAggregationLogicalFunction::toString() const
{
    return fmt::format("WindowAggregation: onField={} asField={}", onField, asField);
}

DataType AvgAggregationLogicalFunction::getInputStamp() const
{
    return inputStamp;
}

DataType AvgAggregationLogicalFunction::getPartialAggregateStamp() const
{
    return partialAggregateStamp;
}

DataType AvgAggregationLogicalFunction::getFinalAggregateStamp() const
{
    return finalAggregateStamp;
}

FieldAccessLogicalFunction AvgAggregationLogicalFunction::getOnField() const
{
    return onField;
}

FieldAccessLogicalFunction AvgAggregationLogicalFunction::getAsField() const
{
    return asField;
}

void AvgAggregationLogicalFunction::setInputStamp(DataType inputStamp)
{
    this->inputStamp = std::move(inputStamp);
}

void AvgAggregationLogicalFunction::setPartialAggregateStamp(DataType partialAggregateStamp)
{
    this->partialAggregateStamp = std::move(partialAggregateStamp);
}

void AvgAggregationLogicalFunction::setFinalAggregateStamp(DataType finalAggregateStamp)
{
    this->finalAggregateStamp = std::move(finalAggregateStamp);
}

void AvgAggregationLogicalFunction::setOnField(FieldAccessLogicalFunction onField)
{
    this->onField = std::move(onField);
}

void AvgAggregationLogicalFunction::setAsField(FieldAccessLogicalFunction asField)
{
    this->asField = std::move(asField);
}

bool AvgAggregationLogicalFunction::operator==(
    const AvgAggregationLogicalFunction& otherAvgAggregationLogicalFunction) const
{
    return this->getName() == otherAvgAggregationLogicalFunction.getName()
        && this->onField == otherAvgAggregationLogicalFunction.onField
        && this->asField == otherAvgAggregationLogicalFunction.asField;
}
}
