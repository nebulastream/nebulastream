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

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <DataTypes/DataType.hpp>
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
CountAggregationLogicalFunction::CountAggregationLogicalFunction(
    DataType inputStamp, DataType partialAggregateStamp, DataType finalAggregateStamp, const FieldAccessLogicalFunction& onField)
    : CountAggregationLogicalFunction(
          std::move(inputStamp), std::move(partialAggregateStamp), std::move(finalAggregateStamp), onField, onField)
{
}

CountAggregationLogicalFunction::CountAggregationLogicalFunction(
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
CountAggregationLogicalFunction::CountAggregationLogicalFunction(const FieldAccessLogicalFunction& field)
    : CountAggregationLogicalFunction(
          DataTypeProvider::provideDataType(inputAggregateStampType),
          DataTypeProvider::provideDataType(partialAggregateStampType),
          DataTypeProvider::provideDataType(finalAggregateStampType),
          field)
{
}

CountAggregationLogicalFunction::CountAggregationLogicalFunction(FieldAccessLogicalFunction field, FieldAccessLogicalFunction asField)
    : CountAggregationLogicalFunction(
          DataTypeProvider::provideDataType(inputAggregateStampType),
          DataTypeProvider::provideDataType(partialAggregateStampType),
          DataTypeProvider::provideDataType(finalAggregateStampType),
          std::move(field),
          std::move(asField))
{
}

std::string_view CountAggregationLogicalFunction::getName() const noexcept
{
    return NAME;
}

void CountAggregationLogicalFunction::inferStamp(const Schema& schema)
{
    if (const auto sourceNameQualifier = schema.getSourceNameQualifier())
    {
        const auto attributeNameResolver = sourceNameQualifier.value() + std::string(Schema::ATTRIBUTE_NAME_SEPARATOR);
        const auto asFieldName = this->getAsField().getFieldName();

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

        /// a count aggregation is always on an uint 64
        this->setOnField(this->getOnField().withDataType(DataTypeProvider::provideDataType(DataType::Type::UINT64)));
        this->setAsField(this->getAsField().withDataType(DataTypeProvider::provideDataType(DataType::Type::UINT64)));
    }
    else
    {
        throw CannotInferSchema("Schema lacked source name qualifier: {}", schema);
    }
}

SerializableAggregationFunction CountAggregationLogicalFunction::serialize() const
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
AggregationLogicalFunctionGeneratedRegistrar::RegisterCountAggregationLogicalFunction(AggregationLogicalFunctionRegistryArguments arguments)
{
    if (arguments.fields.size() != 2)
    {
        throw CannotDeserialize("CountAggregationLogicalFunction requires exactly two fields, but got {}", arguments.fields.size());
    }
    return std::make_shared<WindowAggregationLogicalFunction>(CountAggregationLogicalFunction(arguments.fields[0], arguments.fields[1]));
}

std::string CountAggregationLogicalFunction::toString() const
{
    return fmt::format("WindowAggregation: onField={} asField={}", onField, asField);
}

DataType CountAggregationLogicalFunction::getInputStamp() const
{
    return inputStamp;
}

DataType CountAggregationLogicalFunction::getPartialAggregateStamp() const
{
    return partialAggregateStamp;
}

DataType CountAggregationLogicalFunction::getFinalAggregateStamp() const
{
    return finalAggregateStamp;
}

FieldAccessLogicalFunction CountAggregationLogicalFunction::getOnField() const
{
    return onField;
}

FieldAccessLogicalFunction CountAggregationLogicalFunction::getAsField() const
{
    return asField;
}

void CountAggregationLogicalFunction::setInputStamp(DataType inputStamp)
{
    this->inputStamp = std::move(inputStamp);
}

void CountAggregationLogicalFunction::setPartialAggregateStamp(DataType partialAggregateStamp)
{
    this->partialAggregateStamp = std::move(partialAggregateStamp);
}

void CountAggregationLogicalFunction::setFinalAggregateStamp(DataType finalAggregateStamp)
{
    this->finalAggregateStamp = std::move(finalAggregateStamp);
}

void CountAggregationLogicalFunction::setOnField(FieldAccessLogicalFunction onField)
{
    this->onField = std::move(onField);
}

void CountAggregationLogicalFunction::setAsField(FieldAccessLogicalFunction asField)
{
    this->asField = std::move(asField);
}

bool CountAggregationLogicalFunction::operator==(
    const CountAggregationLogicalFunction& otherCountAggregationLogicalFunction) const
{
    return this->getName() == otherCountAggregationLogicalFunction.getName()
        && this->onField == otherCountAggregationLogicalFunction.onField
        && this->asField == otherCountAggregationLogicalFunction.asField;
}
}

