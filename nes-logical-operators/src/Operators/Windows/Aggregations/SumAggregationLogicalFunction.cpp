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
#include <utility>
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <AggregationLogicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

SumAggregationLogicalFunction::SumAggregationLogicalFunction(const FieldAccessLogicalFunction& onField, FieldAccessLogicalFunction asField)
    : inputStamp(onField.getDataType())
    , partialAggregateStamp(onField.getDataType())
    , finalAggregateStamp(onField.getDataType())
    , onField(onField)
    , asField(std::move(asField))
{
}

SumAggregationLogicalFunction::SumAggregationLogicalFunction(const FieldAccessLogicalFunction& asField)
    : inputStamp(asField.getDataType())
    , partialAggregateStamp(asField.getDataType())
    , finalAggregateStamp(asField.getDataType())
    , onField(asField)
    , asField(asField)
{
}

std::string_view SumAggregationLogicalFunction::getName() const noexcept
{
    return NAME;
}

SumAggregationLogicalFunction SumAggregationLogicalFunction::withInferredStamp(const Schema& schema) const
{
    /// We first infer the dataType of the input field and set the output dataType as the same.
    auto newOnField = this->getOnField().withInferredDataType(schema).getAs<FieldAccessLogicalFunction>().get();
    if (not newOnField.getDataType().isNumeric())
    {
        throw CannotDeserialize("aggregations on non numeric fields is not supported, but got {}", newOnField.getDataType());
    }

    ///Set fully qualified name for the as Field
    const auto onFieldName = newOnField.getFieldName();
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
    auto newAsField = this->getAsField().withFieldName(newAsFieldName).withDataType(newOnField.getDataType());
    return this->withOnField(newOnField)
        .withInputStamp(newOnField.getDataType())
        .withFinalAggregateStamp(newOnField.getDataType())
        .withAsField(newAsField);
}

Reflected SumAggregationLogicalFunction::reflect() const
{
    return NES::reflect(this);
}

Reflected Reflector<SumAggregationLogicalFunction>::operator()(const SumAggregationLogicalFunction& function) const
{
    return reflect(detail::ReflectedSumAggregationLogicalFunction{.onField = function.getOnField(), .asField = function.getAsField()});
}

SumAggregationLogicalFunction Unreflector<SumAggregationLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [onField, asField] = unreflect<detail::ReflectedSumAggregationLogicalFunction>(reflected);
    return SumAggregationLogicalFunction{onField, asField};
}

AggregationLogicalFunctionRegistryReturnType
AggregationLogicalFunctionGeneratedRegistrar::RegisterSumAggregationLogicalFunction(AggregationLogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return std::make_shared<WindowAggregationLogicalFunction>(unreflect<SumAggregationLogicalFunction>(arguments.reflected));
    }
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

SumAggregationLogicalFunction SumAggregationLogicalFunction::withInputStamp(DataType inputStamp) const
{
    auto copy = *this;
    copy.inputStamp = std::move(inputStamp);
    return copy;
}

SumAggregationLogicalFunction SumAggregationLogicalFunction::withPartialAggregateStamp(DataType partialAggregateStamp) const
{
    auto copy = *this;
    copy.partialAggregateStamp = std::move(partialAggregateStamp);
    return copy;
}

SumAggregationLogicalFunction SumAggregationLogicalFunction::withFinalAggregateStamp(DataType finalAggregateStamp) const
{
    auto copy = *this;
    copy.finalAggregateStamp = std::move(finalAggregateStamp);
    return copy;
}

SumAggregationLogicalFunction SumAggregationLogicalFunction::withOnField(FieldAccessLogicalFunction onField) const
{
    auto copy = *this;
    copy.onField = std::move(onField);
    return copy;
}

SumAggregationLogicalFunction SumAggregationLogicalFunction::withAsField(FieldAccessLogicalFunction asField) const
{
    auto copy = *this;
    copy.asField = std::move(asField);
    return copy;
}

bool SumAggregationLogicalFunction::operator==(const SumAggregationLogicalFunction& otherSumAggregationLogicalFunction) const
{
    return this->getName() == otherSumAggregationLogicalFunction.getName() && this->onField == otherSumAggregationLogicalFunction.onField
        && this->asField == otherSumAggregationLogicalFunction.asField;
}
}
