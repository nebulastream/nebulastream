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
#include <string>
#include <string_view>
#include <utility>
#include <API/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/Windows/Aggregations/CountAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <AggregationLogicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <SerializableVariantDescriptor.pb.h>
#include <Common/DataTypes/DataTypeProvider.hpp>

namespace NES
{
CountAggregationLogicalFunction::CountAggregationLogicalFunction(const FieldAccessLogicalFunction& field)
    : WindowAggregationLogicalFunction(
          DataTypeProvider::provideDataType(inputAggregateStampType),
          DataTypeProvider::provideDataType(partialAggregateStampType),
          DataTypeProvider::provideDataType(finalAggregateStampType),
          field)
{
}
CountAggregationLogicalFunction::CountAggregationLogicalFunction(FieldAccessLogicalFunction field, FieldAccessLogicalFunction asField)
    : WindowAggregationLogicalFunction(
          DataTypeProvider::provideDataType(inputAggregateStampType),
          DataTypeProvider::provideDataType(partialAggregateStampType),
          DataTypeProvider::provideDataType(finalAggregateStampType),
          std::move(field),
          std::move(asField))
{
}

std::shared_ptr<WindowAggregationLogicalFunction>
CountAggregationLogicalFunction::create(const FieldAccessLogicalFunction& onField, const FieldAccessLogicalFunction& asField)
{
    return std::make_shared<CountAggregationLogicalFunction>(onField, asField);
}

std::shared_ptr<WindowAggregationLogicalFunction> CountAggregationLogicalFunction::create(const FieldAccessLogicalFunction& onField)
{
    return std::make_shared<CountAggregationLogicalFunction>(onField);
}

std::string_view CountAggregationLogicalFunction::getName() const noexcept
{
    return NAME;
}

void CountAggregationLogicalFunction::inferStamp(const Schema& schema)
{
    const auto attributeNameResolver = schema.getSourceNameQualifier() + Schema::ATTRIBUTE_NAME_SEPARATOR;
    const auto asFieldName = asField.getFieldName();

    ///If on and as field name are different then append the attribute name resolver from on field to the as field
    if (asFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) == std::string::npos)
    {
        asField = asField.withFieldName(attributeNameResolver + asFieldName).get<FieldAccessLogicalFunction>();
    }
    else
    {
        const auto fieldName = asFieldName.substr(asFieldName.find_last_of(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
        asField = asField.withFieldName(attributeNameResolver + fieldName).get<FieldAccessLogicalFunction>();
    }

    /// a count aggregation is always on an uint 64
    this->onField = onField.withDataType(DataTypeProvider::provideDataType(LogicalType::UINT64)).get<FieldAccessLogicalFunction>();
    this->asField = asField.withDataType(DataTypeProvider::provideDataType(LogicalType::UINT64)).get<FieldAccessLogicalFunction>();
}

NES::SerializableAggregationFunction CountAggregationLogicalFunction::serialize() const
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

AggregationLogicalFunctionRegistryReturnType
AggregationLogicalFunctionGeneratedRegistrar::RegisterCountAggregationLogicalFunction(AggregationLogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(
        arguments.fields.size() == 2, "CountAggregationLogicalFunction requires exactly two fields, but got {}", arguments.fields.size());
    return CountAggregationLogicalFunction::create(arguments.fields[0], arguments.fields[1]);
}
}
