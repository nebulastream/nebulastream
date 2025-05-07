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

#include <memory>
#include <LogicalFunctions/Function.hpp>
#include <SerializableFunction.pb.h>
#include <Common/DataTypes/DataTypeProvider.hpp>
#include <Common/DataTypes/Numeric.hpp>
#include <AggregationFunctionRegistry.hpp>
#include <LogicalOperators/Windows/Aggregations/MedianAggregationFunction.hpp>

namespace NES::Logical
{
MedianAggregationFunction::MedianAggregationFunction(const FieldAccessFunction& field)
    : WindowAggregationFunction(
          field.getStamp(),
          DataTypeProvider::provideDataType(LogicalType::UNDEFINED),
          DataTypeProvider::provideDataType(LogicalType::FLOAT64),
          field)
{
}
MedianAggregationFunction::MedianAggregationFunction(FieldAccessFunction field, FieldAccessFunction asField)
    : WindowAggregationFunction(
          field.getStamp(),
          DataTypeProvider::provideDataType(LogicalType::UNDEFINED),
          DataTypeProvider::provideDataType(LogicalType::FLOAT64),
          field,
          asField)
{
}

std::shared_ptr<WindowAggregationFunction>
MedianAggregationFunction::create(const FieldAccessFunction& onField, const FieldAccessFunction& asField)
{
    return std::make_shared<MedianAggregationFunction>(onField, asField);
}

std::shared_ptr<WindowAggregationFunction> MedianAggregationFunction::create(FieldAccessFunction onField)
{
    return std::make_shared<MedianAggregationFunction>(onField);
}

std::string_view MedianAggregationFunction::getName() const noexcept
{
    return NAME;
}

void MedianAggregationFunction::inferStamp(const Schema& schema)
{
    /// We first infer the stamp of the input field and set the output stamp as the same.
    onField = onField.withInferredStamp(schema).get<FieldAccessFunction>();
    INVARIANT(dynamic_cast<const Numeric*>(onField.getStamp().get()), "aggregations on non numeric fields is not supported, but got {}", onField.getStamp()->toString());

    ///Set fully qualified name for the as Field
    const auto onFieldName = onField.getFieldName();
    const auto asFieldName = asField.getFieldName();

    const auto attributeNameResolver = onFieldName.substr(0, onFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
    ///If on and as field name are different then append the attribute name resolver from on field to the as field
    if (asFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) == std::string::npos)
    {
        asField = asField.withFieldName(attributeNameResolver + asFieldName).get<FieldAccessFunction>();
    }
    else
    {
        const auto fieldName = asFieldName.substr(asFieldName.find_last_of(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
        asField = asField.withFieldName(attributeNameResolver + fieldName).get<FieldAccessFunction>();
    }
    inputStamp = onField.getStamp();
    finalAggregateStamp = DataTypeProvider::provideDataType(LogicalType::FLOAT64);
    asField = asField.withStamp(getFinalAggregateStamp()).get<FieldAccessFunction>();
}

NES::SerializableAggregationFunction MedianAggregationFunction::serialize() const
{
    NES::SerializableAggregationFunction serializedAggregationFunction;
    serializedAggregationFunction.set_type(NAME);

    auto onFieldFuc = SerializableFunction();
    onFieldFuc.CopyFrom(onField.serialize());

    auto asFieldFuc = SerializableFunction();
    asFieldFuc.CopyFrom(asField.serialize());

    serializedAggregationFunction.mutable_as_field()->CopyFrom(asFieldFuc);
    serializedAggregationFunction.mutable_on_field()->CopyFrom(onFieldFuc);
    return serializedAggregationFunction;
}

AggregationFunctionRegistryReturnType AggregationFunctionGeneratedRegistrar::RegisterMedianAggregationFunction(AggregationFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.fields.size() == 2, "MedianAggregationFunction requires exactly two fields, but got {}", arguments.fields.size());
    return MedianAggregationFunction::create(arguments.fields[0], arguments.fields[1]);
}
}
