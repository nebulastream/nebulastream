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

#include <Operators/Windows/Aggregations/MedianAggregationLogicalFunction.hpp>

#include <memory>
#include <string>
#include <string_view>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <ErrorHandling.hpp>

#include <utility>
#include <Util/Reflection.hpp>
#include <AggregationLogicalFunctionRegistry.hpp>

namespace NES
{
MedianAggregationLogicalFunction::MedianAggregationLogicalFunction(const FieldAccessLogicalFunction& field)
    : WindowAggregationLogicalFunction(
          field.getDataType(),
          DataTypeProvider::provideDataType(partialAggregateStampType),
          DataTypeProvider::provideDataType(finalAggregateStampType),
          field)
{
}

MedianAggregationLogicalFunction::MedianAggregationLogicalFunction(
    const FieldAccessLogicalFunction& field, FieldAccessLogicalFunction asField)
    : WindowAggregationLogicalFunction(
          field.getDataType(),
          DataTypeProvider::provideDataType(partialAggregateStampType),
          DataTypeProvider::provideDataType(finalAggregateStampType),
          field,
          std::move(asField))
{
}

std::string_view MedianAggregationLogicalFunction::getName() const noexcept
{
    return NAME;
}

void MedianAggregationLogicalFunction::inferStamp(const Schema& schema)
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
    this->setFinalAggregateStamp(DataTypeProvider::provideDataType(DataType::Type::FLOAT64));
    this->setAsField(this->getAsField().withDataType(getFinalAggregateStamp()));
}

Reflected MedianAggregationLogicalFunction::reflect() const
{
    return NES::reflect(this);
}

Reflected Reflector<MedianAggregationLogicalFunction>::operator()(const MedianAggregationLogicalFunction& function) const
{
    return reflect(detail::ReflectedMedianAggregationLogicalFunction{.onField = function.getOnField(), .asField = function.getAsField()});
}

MedianAggregationLogicalFunction
Unreflector<MedianAggregationLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [onField, asField] = context.unreflect<detail::ReflectedMedianAggregationLogicalFunction>(reflected);
    return MedianAggregationLogicalFunction{onField, asField};
}

AggregationLogicalFunctionRegistryReturnType AggregationLogicalFunctionGeneratedRegistrar::RegisterMedianAggregationLogicalFunction(
    AggregationLogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return std::make_shared<MedianAggregationLogicalFunction>(
            ReflectionContext{}.unreflect<MedianAggregationLogicalFunction>(arguments.reflected));
    }
    if (arguments.fields.size() != 2)
    {
        throw CannotDeserialize("MedianAggregationLogicalFunction requires exactly two fields, but got {}", arguments.fields.size());
    }
    return std::make_shared<MedianAggregationLogicalFunction>(arguments.fields[0], arguments.fields[1]);
}
}
