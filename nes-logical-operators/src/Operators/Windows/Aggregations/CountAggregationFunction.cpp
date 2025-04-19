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
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/Windows/Aggregations/CountAggregationFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationFunction.hpp>
#include <Util/Common.hpp>
#include <ErrorHandling.hpp>
#include <SerializableFunction.pb.h>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>

namespace NES
{

CountAggregationFunction::CountAggregationFunction(std::unique_ptr<FieldAccessLogicalFunction> field)
    : WindowAggregationFunction(
          DataTypeProvider::provideDataType(LogicalType::UINT64),
          DataTypeProvider::provideDataType(LogicalType::UINT64),
          DataTypeProvider::provideDataType(LogicalType::UINT64),
          std::move(field))
{
    this->aggregationType = Type::Count;
}
CountAggregationFunction::CountAggregationFunction(std::unique_ptr<LogicalFunction> field, std::unique_ptr<LogicalFunction> asField)
    : WindowAggregationFunction(
          DataTypeProvider::provideDataType(LogicalType::UINT64),
          DataTypeProvider::provideDataType(LogicalType::UINT64),
          DataTypeProvider::provideDataType(LogicalType::UINT64),
          std::move(field),
          std::move(asField))
{
    this->aggregationType = Type::Count;
}

std::unique_ptr<WindowAggregationFunction>
CountAggregationFunction::create(std::unique_ptr<FieldAccessLogicalFunction> onField, std::unique_ptr<FieldAccessLogicalFunction> asField)
{
    return std::make_unique<CountAggregationFunction>(std::move(onField), std::move(asField));
}

std::unique_ptr<WindowAggregationFunction> CountAggregationFunction::create(std::unique_ptr<LogicalFunction> onField)
{
    if (!dynamic_cast<FieldAccessLogicalFunction*>(onField.get()))
    {
        throw DifferentFieldTypeExpected("Query: window key has to be a FieldAccessFunction but it was " + onField->toString());
    }
    std::unique_ptr<FieldAccessLogicalFunction> fieldAccess(static_cast<FieldAccessLogicalFunction*>(onField.release()));
    return std::make_unique<CountAggregationFunction>(std::move(fieldAccess));
}

void CountAggregationFunction::inferStamp(const Schema& schema)
{
    const auto attributeNameResolver = schema.getSourceNameQualifier() + Schema::ATTRIBUTE_NAME_SEPARATOR;
    const auto asFieldName = dynamic_cast<FieldAccessLogicalFunction*>(asField.get())->getFieldName();

    ///If on and as field name are different then append the attribute name resolver from on field to the as field
    if (asFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) == std::string::npos)
    {
        dynamic_cast<FieldAccessLogicalFunction*>(asField.get())->setFieldName(attributeNameResolver + asFieldName);
    }
    else
    {
        const auto fieldName = asFieldName.substr(asFieldName.find_last_of(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
        dynamic_cast<FieldAccessLogicalFunction*>(asField.get())->setFieldName(attributeNameResolver + fieldName);
    }

    /// a count aggregation is always on an uint 64
    onField->setStamp(DataTypeProvider::provideDataType(LogicalType::UINT64));
    asField->setStamp(onField->getStamp().clone());
}

std::unique_ptr<WindowAggregationFunction> CountAggregationFunction::clone()
{
    return std::make_unique<CountAggregationFunction>(onField->clone(), asField->clone());
}

NES::SerializableAggregationFunction CountAggregationFunction::serialize() const
{
    NES::SerializableAggregationFunction serializedAggregationFunction;
    serializedAggregationFunction.set_type(NAME);

    auto* onFieldFuc = new SerializableFunction();
    FunctionSerializationUtil::serializeFunction(onField, onFieldFuc);

    auto* asFieldFuc = new SerializableFunction();
    FunctionSerializationUtil::serializeFunction(asField, asFieldFuc);

    serializedAggregationFunction.set_allocated_as_field(asFieldFuc);
    serializedAggregationFunction.set_allocated_on_field(onFieldFuc);
    return serializedAggregationFunction;
}

}
