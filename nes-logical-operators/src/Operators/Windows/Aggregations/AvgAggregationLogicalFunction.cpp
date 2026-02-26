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
#include <DataTypes/DataTypeProvider.hpp>
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
AvgAggregationLogicalFunction::AvgAggregationLogicalFunction(const FieldAccessLogicalFunction& field)
    : inputStamp(field.getDataType())
    , partialAggregateStamp(DataTypeProvider::provideDataType(partialAggregateStampType))
    , finalAggregateStamp(DataTypeProvider::provideDataType(finalAggregateStampType))
    , onField(field)
    , asField(field)
{
}

AvgAggregationLogicalFunction::AvgAggregationLogicalFunction(const FieldAccessLogicalFunction& onField, FieldAccessLogicalFunction asField)
    : inputStamp(onField.getDataType())
    , partialAggregateStamp(DataTypeProvider::provideDataType(partialAggregateStampType))
    , finalAggregateStamp(DataTypeProvider::provideDataType(finalAggregateStampType))
    , onField(onField)
    , asField(std::move(asField))
{
}

std::string_view AvgAggregationLogicalFunction::getName() noexcept
{
    return NAME;
}

AvgAggregationLogicalFunction AvgAggregationLogicalFunction::withInferredStamp(const Schema& schema) const
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
    auto newAsField = this->getAsField().withFieldName(newAsFieldName).withDataType(getFinalAggregateStamp());
    return this->withAsField(newAsField)
        .withOnField(newOnField.getAs<FieldAccessLogicalFunction>().get())
        .withInputStamp(newOnField.getDataType());
}

Reflected AvgAggregationLogicalFunction::reflect() const
{
    return NES::reflect(this);
}

Reflected Reflector<AvgAggregationLogicalFunction>::operator()(const AvgAggregationLogicalFunction& function) const
{
    return reflect(detail::ReflectedAvgAggregationLogicalFunction{.onField = function.getOnField(), .asField = function.getAsField()});
}

AvgAggregationLogicalFunction Unreflector<AvgAggregationLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [onField, asField] = unreflect<detail::ReflectedAvgAggregationLogicalFunction>(reflected);
    return AvgAggregationLogicalFunction{onField, asField};
}

AggregationLogicalFunctionRegistryReturnType
AggregationLogicalFunctionGeneratedRegistrar::RegisterAvgAggregationLogicalFunction(AggregationLogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return std::make_shared<WindowAggregationLogicalFunction>(unreflect<AvgAggregationLogicalFunction>(arguments.reflected));
    }

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

AvgAggregationLogicalFunction AvgAggregationLogicalFunction::withInputStamp(DataType inputStamp) const
{
    auto copy = *this;
    copy.inputStamp = std::move(inputStamp);
    return copy;
}

AvgAggregationLogicalFunction AvgAggregationLogicalFunction::withPartialAggregateStamp(DataType partialAggregateStamp) const
{
    auto copy = *this;
    copy.partialAggregateStamp = std::move(partialAggregateStamp);
    return copy;
}

AvgAggregationLogicalFunction AvgAggregationLogicalFunction::withFinalAggregateStamp(DataType finalAggregateStamp) const
{
    auto copy = *this;
    copy.finalAggregateStamp = std::move(finalAggregateStamp);
    return copy;
}

AvgAggregationLogicalFunction AvgAggregationLogicalFunction::withOnField(FieldAccessLogicalFunction onField) const
{
    auto copy = *this;
    copy.onField = std::move(onField);
    return copy;
}

AvgAggregationLogicalFunction AvgAggregationLogicalFunction::withAsField(FieldAccessLogicalFunction asField) const
{
    auto copy = *this;
    copy.asField = std::move(asField);
    return copy;
}

bool AvgAggregationLogicalFunction::operator==(const AvgAggregationLogicalFunction& otherAvgAggregationLogicalFunction) const
{
    return this->onField == otherAvgAggregationLogicalFunction.onField && this->asField == otherAvgAggregationLogicalFunction.asField;
}
}
