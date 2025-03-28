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
#include <utility>
#include <API/Schema.hpp>
#include <Abstract/LogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/CountAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Util/Common.hpp>
#include <ErrorHandling.hpp>
#include <SerializableFunction.pb.h>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>

namespace NES
{

CountAggregationLogicalFunction::CountAggregationLogicalFunction(const FieldAccessLogicalFunction& field)
    : WindowAggregationLogicalFunction(
          DataTypeProvider::provideDataType(LogicalType::UINT64),
          DataTypeProvider::provideDataType(LogicalType::UINT64),
          DataTypeProvider::provideDataType(LogicalType::UINT64),
          field)
{
    this->aggregationType = Type::Count;
}
CountAggregationLogicalFunction::CountAggregationLogicalFunction(FieldAccessLogicalFunction field, FieldAccessLogicalFunction asField)
    : WindowAggregationLogicalFunction(
          DataTypeProvider::provideDataType(LogicalType::UINT64),
          DataTypeProvider::provideDataType(LogicalType::UINT64),
          DataTypeProvider::provideDataType(LogicalType::UINT64),
          field,
          asField)
{
    this->aggregationType = Type::Count;
}

std::unique_ptr<WindowAggregationLogicalFunction>
CountAggregationLogicalFunction::create(const FieldAccessLogicalFunction& onField, const FieldAccessLogicalFunction& asField)
{
    return std::make_unique<CountAggregationLogicalFunction>(std::move(onField), std::move(asField));
}

std::unique_ptr<WindowAggregationLogicalFunction> CountAggregationLogicalFunction::create(FieldAccessLogicalFunction onField)
{
    return std::make_unique<CountAggregationLogicalFunction>(onField);
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
    auto newOnfield = onField.withStamp(DataTypeProvider::provideDataType(LogicalType::UINT64));
    auto newAsField = asField.withStamp(onField.getStamp().clone());
    this->onField = newOnfield.get<FieldAccessLogicalFunction>();
    this->asField = newAsField.get<FieldAccessLogicalFunction>();
}

std::unique_ptr<WindowAggregationLogicalFunction> CountAggregationLogicalFunction::clone()
{
    return std::make_unique<CountAggregationLogicalFunction>(onField, asField);
}

NES::SerializableAggregationFunction CountAggregationLogicalFunction::serialize() const
{
    NES::SerializableAggregationFunction serializedAggregationFunction;
    serializedAggregationFunction.set_type(NAME);

    auto* onFieldFuc = new SerializableFunction();
    onFieldFuc->CopyFrom(onField.serialize());

    auto* asFieldFuc = new SerializableFunction();
    asFieldFuc->CopyFrom(asField.serialize());

    serializedAggregationFunction.set_allocated_as_field(asFieldFuc);
    serializedAggregationFunction.set_allocated_on_field(onFieldFuc);
    return serializedAggregationFunction;
}

}
