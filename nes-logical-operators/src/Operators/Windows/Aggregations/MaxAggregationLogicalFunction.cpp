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

#include <Operators/Windows/Aggregations/MaxAggregationLogicalFunction.hpp>

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
MaxAggregationLogicalFunction::MaxAggregationLogicalFunction(const FieldAccessLogicalFunction& field)
    : inputStamp(field.getDataType())
    , partialAggregateStamp(field.getDataType())
    , finalAggregateStamp(field.getDataType())
    , onField(field)
    , asField(field)
{
}

MaxAggregationLogicalFunction::MaxAggregationLogicalFunction(const FieldAccessLogicalFunction& field, FieldAccessLogicalFunction asField)
    : inputStamp(field.getDataType())
    , partialAggregateStamp(field.getDataType())
    , finalAggregateStamp(field.getDataType())
    , onField(field)
    , asField(std::move(asField))
{
}

std::string_view MaxAggregationLogicalFunction::getName() noexcept
{
    return NAME;
}

std::string MaxAggregationLogicalFunction::toString() const
{
    return fmt::format("WindowAggregation: onField={} asField={}", onField, asField);
}

MaxAggregationLogicalFunction MaxAggregationLogicalFunction::withInferredStamp(const Schema& schema) const
{
    /// We first infer the dataType of the input field and set the output dataType as the same.
    auto newOnField = this->getOnField().withInferredDataType(schema).getAs<FieldAccessLogicalFunction>().get();
    if (not newOnField.getDataType().isNumeric())
    {
        throw CannotDeserialize("aggregations on non numeric fields is not supported, but got {}", newOnField.getDataType());
    }

    ///Set fully qualified name for the as Field
    auto onFieldName = newOnField.getFieldName();
    auto asFieldName = this->getAsField().getFieldName();

    const auto attributeNameResolver = onFieldName.substr(0, onFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
    ///If on and as field name are different then append the attribute name resolver from on field to the as field
    std::string newAsFieldName;
    if (asFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) == std::string::npos)
    {
        newAsFieldName = attributeNameResolver + asFieldName;
    }
    else
    {
        auto fieldName = asFieldName.substr(asFieldName.find_last_of(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
        newAsFieldName = attributeNameResolver + fieldName;
    }
    auto newAsField = this->getAsField().withFieldName(newAsFieldName).withDataType(newOnField.getDataType());
    return this->withOnField(newOnField)
        .withInputStamp(newOnField.getDataType())
        .withFinalAggregateStamp(newOnField.getDataType())
        .withAsField(newAsField);
}

Reflected MaxAggregationLogicalFunction::reflect() const
{
    return NES::reflect(this);
}

Reflected Reflector<MaxAggregationLogicalFunction>::operator()(const MaxAggregationLogicalFunction& function) const
{
    return reflect(detail::ReflectedMaxAggregationLogicalFunction{.onField = function.getOnField(), .asField = function.getAsField()});
}

MaxAggregationLogicalFunction Unreflector<MaxAggregationLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [onField, asField] = unreflect<detail::ReflectedMaxAggregationLogicalFunction>(reflected);
    return MaxAggregationLogicalFunction{onField, asField};
}

AggregationLogicalFunctionRegistryReturnType
AggregationLogicalFunctionGeneratedRegistrar::RegisterMaxAggregationLogicalFunction(AggregationLogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return std::make_shared<WindowAggregationLogicalFunction>(unreflect<MaxAggregationLogicalFunction>(arguments.reflected));
    }
    if (arguments.fields.size() != 2)
    {
        throw CannotDeserialize("MaxAggregationLogicalFunction requires exactly two fields, but got {}", arguments.fields.size());
    }
    return std::make_shared<WindowAggregationLogicalFunction>(MaxAggregationLogicalFunction(arguments.fields[0], arguments.fields[1]));
}

DataType MaxAggregationLogicalFunction::getInputStamp() const
{
    return inputStamp;
}

DataType MaxAggregationLogicalFunction::getPartialAggregateStamp() const
{
    return partialAggregateStamp;
}

DataType MaxAggregationLogicalFunction::getFinalAggregateStamp() const
{
    return finalAggregateStamp;
}

FieldAccessLogicalFunction MaxAggregationLogicalFunction::getOnField() const
{
    return onField;
}

FieldAccessLogicalFunction MaxAggregationLogicalFunction::getAsField() const
{
    return asField;
}

MaxAggregationLogicalFunction MaxAggregationLogicalFunction::withInputStamp(DataType inputStamp) const
{
    auto copy = *this;
    copy.inputStamp = std::move(inputStamp);
    return copy;
}

MaxAggregationLogicalFunction MaxAggregationLogicalFunction::withPartialAggregateStamp(DataType partialAggregateStamp) const
{
    auto copy = *this;
    copy.partialAggregateStamp = std::move(partialAggregateStamp);
    return copy;
}

MaxAggregationLogicalFunction MaxAggregationLogicalFunction::withFinalAggregateStamp(DataType finalAggregateStamp) const
{
    auto copy = *this;
    copy.finalAggregateStamp = std::move(finalAggregateStamp);
    return copy;
}

MaxAggregationLogicalFunction MaxAggregationLogicalFunction::withOnField(FieldAccessLogicalFunction onField) const
{
    auto copy = *this;
    copy.onField = std::move(onField);
    return copy;
}

MaxAggregationLogicalFunction MaxAggregationLogicalFunction::withAsField(FieldAccessLogicalFunction asField) const
{
    auto copy = *this;
    copy.asField = std::move(asField);
    return copy;
}

bool MaxAggregationLogicalFunction::operator==(const MaxAggregationLogicalFunction& otherMaxAggregationLogicalFunction) const
{
    return this->onField == otherMaxAggregationLogicalFunction.onField && this->asField == otherMaxAggregationLogicalFunction.asField;
}
}
