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
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <AggregationLogicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{
CountAggregationLogicalFunction::CountAggregationLogicalFunction(const FieldAccessLogicalFunction& field)
    : inputStamp(DataTypeProvider::provideDataType(inputAggregateStampType))
    , partialAggregateStamp(DataTypeProvider::provideDataType(partialAggregateStampType))
    , finalAggregateStamp(DataTypeProvider::provideDataType(finalAggregateStampType))
    , onField(field)
    , asField(field)
{
}

CountAggregationLogicalFunction::CountAggregationLogicalFunction(FieldAccessLogicalFunction field, FieldAccessLogicalFunction asField)
    : inputStamp(DataTypeProvider::provideDataType(inputAggregateStampType))
    , partialAggregateStamp(DataTypeProvider::provideDataType(partialAggregateStampType))
    , finalAggregateStamp(DataTypeProvider::provideDataType(finalAggregateStampType))
    , onField(std::move(field))
    , asField(std::move(asField))
{
}

std::string_view CountAggregationLogicalFunction::getName() const noexcept
{
    return NAME;
}

Reflected CountAggregationLogicalFunction::reflect() const
{
    return NES::reflect(this);
}

CountAggregationLogicalFunction CountAggregationLogicalFunction::withInferredStamp(const Schema& schema) const
{
    if (const auto sourceNameQualifier = schema.getSourceNameQualifier())
    {
        const auto attributeNameResolver = sourceNameQualifier.value() + std::string(Schema::ATTRIBUTE_NAME_SEPARATOR);
        const auto asFieldName = this->getAsField().getFieldName();

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

        /// a count aggregation is always on an uint 64
        auto newAsField = this->getAsField().withFieldName(newAsFieldName);

        return this->withOnField(this->getOnField().withDataType(DataTypeProvider::provideDataType(DataType::Type::UINT64)))
            .withAsField(newAsField.withDataType(DataTypeProvider::provideDataType(DataType::Type::UINT64)));
    }
    else
    {
        throw CannotInferSchema("Schema lacked source name qualifier: {}", schema);
    }
}

Reflected Reflector<CountAggregationLogicalFunction>::operator()(const CountAggregationLogicalFunction& function) const
{
    return reflect(detail::ReflectedCountAggregationLogicalFunction{.onField = function.getOnField(), .asField = function.getAsField()});
}

CountAggregationLogicalFunction Unreflector<CountAggregationLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [onField, asField] = unreflect<detail::ReflectedCountAggregationLogicalFunction>(reflected);

    if (!onField.has_value() || !asField.has_value())
    {
        throw CannotDeserialize("CountAggregationLogicalFunction is missing onField/asField function");
    }

    return {onField.value(), asField.value()};
}

AggregationLogicalFunctionRegistryReturnType
AggregationLogicalFunctionGeneratedRegistrar::RegisterCountAggregationLogicalFunction(AggregationLogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return std::make_shared<WindowAggregationLogicalFunction>(unreflect<CountAggregationLogicalFunction>(arguments.reflected));
    }

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

CountAggregationLogicalFunction CountAggregationLogicalFunction::withInputStamp(DataType inputStamp) const
{
    auto copy = *this;
    copy.inputStamp = std::move(inputStamp);
    return copy;
}

CountAggregationLogicalFunction CountAggregationLogicalFunction::withPartialAggregateStamp(DataType partialAggregateStamp) const
{
    auto copy = *this;
    copy.partialAggregateStamp = std::move(partialAggregateStamp);
    return copy;
}

CountAggregationLogicalFunction CountAggregationLogicalFunction::withFinalAggregateStamp(DataType finalAggregateStamp) const
{
    auto copy = *this;
    copy.finalAggregateStamp = std::move(finalAggregateStamp);
    return copy;
}

CountAggregationLogicalFunction CountAggregationLogicalFunction::withOnField(FieldAccessLogicalFunction onField) const
{
    auto copy = *this;
    copy.onField = std::move(onField);
    return copy;
}

CountAggregationLogicalFunction CountAggregationLogicalFunction::withAsField(FieldAccessLogicalFunction asField) const
{
    auto copy = *this;
    copy.asField = std::move(asField);
    return copy;
}

bool CountAggregationLogicalFunction::operator==(const CountAggregationLogicalFunction& otherCountAggregationLogicalFunction) const
{
    return this->getName() == otherCountAggregationLogicalFunction.getName()
        && this->onField.getFieldName() == otherCountAggregationLogicalFunction.onField.getFieldName()
        && this->asField.getFieldName() == otherCountAggregationLogicalFunction.asField.getFieldName();
}
}
