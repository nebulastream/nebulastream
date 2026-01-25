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

#include <Operators/Windows/Aggregations/MinAggregationLogicalFunction.hpp>

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
MinAggregationLogicalFunction::MinAggregationLogicalFunction(const FieldAccessLogicalFunction& field)
    : inputStamp(field.getDataType())
    , partialAggregateStamp(field.getDataType())
    , finalAggregateStamp(field.getDataType())
    , onField(field)
    , asField(field)
{
}

MinAggregationLogicalFunction::MinAggregationLogicalFunction(const FieldAccessLogicalFunction& field, FieldAccessLogicalFunction asField)
    : inputStamp(field.getDataType())
    , partialAggregateStamp(field.getDataType())
    , finalAggregateStamp(field.getDataType())
    , onField(field)
    , asField(std::move(asField))
{
}

std::string_view MinAggregationLogicalFunction::getName() const noexcept
{
    return NAME;
}

MinAggregationLogicalFunction MinAggregationLogicalFunction::withInferredStamp(const Schema& schema) const
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

Reflected MinAggregationLogicalFunction::reflect() const
{
    return NES::reflect(this);
}

Reflected Reflector<MinAggregationLogicalFunction>::operator()(const MinAggregationLogicalFunction& function) const
{
    return reflect(detail::ReflectedMinAggregationLogicalFunction{.onField = function.getOnField(), .asField = function.getAsField()});
}

MinAggregationLogicalFunction Unreflector<MinAggregationLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [onField, asField] = unreflect<detail::ReflectedMinAggregationLogicalFunction>(reflected);
    return MinAggregationLogicalFunction{onField, asField};
}

AggregationLogicalFunctionRegistryReturnType
AggregationLogicalFunctionGeneratedRegistrar::RegisterMinAggregationLogicalFunction(AggregationLogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return std::make_shared<WindowAggregationLogicalFunction>(unreflect<MinAggregationLogicalFunction>(arguments.reflected));
    }
    if (arguments.fields.size() != 2)
    {
        throw CannotDeserialize("MinAggregationLogicalFunction requires exactly two fields, but got {}", arguments.fields.size());
    }
    return std::make_shared<WindowAggregationLogicalFunction>(MinAggregationLogicalFunction(arguments.fields[0], arguments.fields[1]));
}

std::string MinAggregationLogicalFunction::toString() const
{
    return fmt::format("WindowAggregation: onField={} asField={}", onField, asField);
}

DataType MinAggregationLogicalFunction::getInputStamp() const
{
    return inputStamp;
}

DataType MinAggregationLogicalFunction::getPartialAggregateStamp() const
{
    return partialAggregateStamp;
}

DataType MinAggregationLogicalFunction::getFinalAggregateStamp() const
{
    return finalAggregateStamp;
}

FieldAccessLogicalFunction MinAggregationLogicalFunction::getOnField() const
{
    return onField;
}

FieldAccessLogicalFunction MinAggregationLogicalFunction::getAsField() const
{
    return asField;
}

MinAggregationLogicalFunction MinAggregationLogicalFunction::withInputStamp(DataType inputStamp) const
{
    auto copy = *this;
    copy.inputStamp = std::move(inputStamp);
    return copy;
}

MinAggregationLogicalFunction MinAggregationLogicalFunction::withPartialAggregateStamp(DataType partialAggregateStamp) const
{
    auto copy = *this;
    copy.partialAggregateStamp = std::move(partialAggregateStamp);
    return copy;
}

MinAggregationLogicalFunction MinAggregationLogicalFunction::withFinalAggregateStamp(DataType finalAggregateStamp) const
{
    auto copy = *this;
    copy.finalAggregateStamp = std::move(finalAggregateStamp);
    return copy;
}

MinAggregationLogicalFunction MinAggregationLogicalFunction::withOnField(FieldAccessLogicalFunction onField) const
{
    auto copy = *this;
    copy.onField = std::move(onField);
    return copy;
}

MinAggregationLogicalFunction MinAggregationLogicalFunction::withAsField(FieldAccessLogicalFunction asField) const
{
    auto copy = *this;
    copy.asField = std::move(asField);
    return copy;
}

bool MinAggregationLogicalFunction::operator==(const MinAggregationLogicalFunction& otherMinAggregationLogicalFunction) const
{
    return this->getName() == otherMinAggregationLogicalFunction.getName() && this->onField == otherMinAggregationLogicalFunction.onField
        && this->asField == otherMinAggregationLogicalFunction.asField;
}
}
