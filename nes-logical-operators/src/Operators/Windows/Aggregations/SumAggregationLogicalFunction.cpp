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
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <AggregationLogicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{
SumAggregationLogicalFunction::SumAggregationLogicalFunction(
    DataType inputStamp, DataType partialAggregateStamp, DataType finalAggregateStamp, const FieldAccessLogicalFunction& onField)
    : SumAggregationLogicalFunction(
          std::move(inputStamp), std::move(partialAggregateStamp), std::move(finalAggregateStamp), onField, onField)
{
}

SumAggregationLogicalFunction::SumAggregationLogicalFunction(
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

SumAggregationLogicalFunction::SumAggregationLogicalFunction(const FieldAccessLogicalFunction& field)
    : SumAggregationLogicalFunction(field.getDataType(), field.getDataType(), field.getDataType(), field)
{
}

SumAggregationLogicalFunction::SumAggregationLogicalFunction(
    const FieldAccessLogicalFunction& field, const FieldAccessLogicalFunction& asField)
    : SumAggregationLogicalFunction(field.getDataType(), field.getDataType(), field.getDataType(), field, asField)
{
}

std::string_view SumAggregationLogicalFunction::getName() const noexcept
{
    return NAME;
}

void SumAggregationLogicalFunction::inferStamp(const Schema& schema)
{
    /// We first infer the dataType of the input field and set the output dataType as the same.
    this->setOnField(this->getOnField().withInferredDataType(schema).getAs<FieldAccessLogicalFunction>().get());
    if (not this->getOnField().getDataType().isNumeric())
    {
        throw CannotDeserialize("aggregations on non numeric fields is not supported, but got {}", this->getOnField().getDataType());
    }

    ///Set fully qualified name for the as Field
    const auto onFieldName = this->getOnField().getFieldName();
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
    this->setInputStamp(this->getOnField().getDataType());
    this->setFinalAggregateStamp(this->getOnField().getDataType());
    this->setAsField(this->getAsField().withDataType(this->getFinalAggregateStamp()));
}

SerializableAggregationFunction SumAggregationLogicalFunction::serialize() const
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
AggregationLogicalFunctionGeneratedRegistrar::RegisterSumAggregationLogicalFunction(AggregationLogicalFunctionRegistryArguments arguments)
{
    if (arguments.fields.size() != 2)
    {
        throw CannotDeserialize("SumAggregationLogicalFunction requires exactly two fields, but got {}", arguments.fields.size());
    }
    return std::make_shared<WindowAggregationLogicalFunction>(SumAggregationLogicalFunction(arguments.fields[0], arguments.fields[1]));
}

std::string SumAggregationLogicalFunction::toString() const
{
    return fmt::format("WindowAggregation: onField={} asField={}", onField, asField);
}

DataType SumAggregationLogicalFunction::getInputStamp() const
{
    return inputStamp;
}

DataType SumAggregationLogicalFunction::getPartialAggregateStamp() const
{
    return partialAggregateStamp;
}

DataType SumAggregationLogicalFunction::getFinalAggregateStamp() const
{
    return finalAggregateStamp;
}

FieldAccessLogicalFunction SumAggregationLogicalFunction::getOnField() const
{
    return onField;
}

FieldAccessLogicalFunction SumAggregationLogicalFunction::getAsField() const
{
    return asField;
}

void SumAggregationLogicalFunction::setInputStamp(DataType inputStamp)
{
    this->inputStamp = std::move(inputStamp);
}

void SumAggregationLogicalFunction::setPartialAggregateStamp(DataType partialAggregateStamp)
{
    this->partialAggregateStamp = std::move(partialAggregateStamp);
}

void SumAggregationLogicalFunction::setFinalAggregateStamp(DataType finalAggregateStamp)
{
    this->finalAggregateStamp = std::move(finalAggregateStamp);
}

void SumAggregationLogicalFunction::setOnField(FieldAccessLogicalFunction onField)
{
    this->onField = std::move(onField);
}

void SumAggregationLogicalFunction::setAsField(FieldAccessLogicalFunction asField)
{
    this->asField = std::move(asField);
}

bool SumAggregationLogicalFunction::operator==(
    const SumAggregationLogicalFunction& otherSumAggregationLogicalFunction) const
{
    return this->getName() == otherSumAggregationLogicalFunction.getName()
        && this->onField == otherSumAggregationLogicalFunction.onField
        && this->asField == otherSumAggregationLogicalFunction.asField;
}
}

