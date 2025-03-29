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
#include <Operators/Windows/Aggregations/CountAggregationFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationFunction.hpp>
#include "Common/DataTypes/DataType.hpp"
#include "Common/DataTypes/DataTypeFactory.hpp"
#include "API/Schema.hpp"
#include <ErrorHandling.hpp>
#include "Functions/FieldAccessLogicalFunction.hpp"
#include "Functions/FunctionSerializationUtil.hpp"
#include "Functions/LogicalFunction.hpp"
#include "SerializableAggregationFunction.pb.h"
#include "Util/Common.hpp"

namespace NES::Windowing
{

CountAggregationFunction::CountAggregationFunction(const std::shared_ptr<FieldAccessLogicalFunction> field)
    : WindowAggregationFunction(field)
{
    this->aggregationType = Type::Count;
}
CountAggregationFunction::CountAggregationFunction(std::shared_ptr<LogicalFunction> field, std::shared_ptr<LogicalFunction> asField)
    : WindowAggregationFunction(std::move(field), std::move(asField))
{
    this->aggregationType = Type::Count;
}

std::shared_ptr<WindowAggregationFunction>
CountAggregationFunction::create(std::shared_ptr<FieldAccessLogicalFunction> onField, std::shared_ptr<FieldAccessLogicalFunction> asField)
{
    return std::make_shared<CountAggregationFunction>(CountAggregationFunction(std::move(onField), std::move(asField)));
}

std::shared_ptr<WindowAggregationFunction> CountAggregationFunction::on(const std::shared_ptr<LogicalFunction>& onField)
{
    if (!NES::Util::instanceOf<FieldAccessLogicalFunction>(onField))
    {
        throw DifferentFieldTypeExpected("Query: window key has to be an FieldAccessFunction but it was a  {}", *onField);
    }
    auto fieldAccess = NES::Util::as<FieldAccessLogicalFunction>(onField);
    return std::make_shared<CountAggregationFunction>(CountAggregationFunction(fieldAccess));
}

void CountAggregationFunction::inferStamp(const Schema& schema)
{
    const auto attributeNameResolver = schema.getSourceNameQualifier() + Schema::ATTRIBUTE_NAME_SEPARATOR;
    const auto asFieldName = NES::Util::as<FieldAccessLogicalFunction>(asField)->getFieldName();

    ///If on and as field name are different then append the attribute name resolver from on field to the as field
    if (asFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) == std::string::npos)
    {
        NES::Util::as<FieldAccessLogicalFunction>(asField)->updateFieldName(attributeNameResolver + asFieldName);
    }
    else
    {
        const auto fieldName = asFieldName.substr(asFieldName.find_last_of(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
        NES::Util::as<FieldAccessLogicalFunction>(asField)->updateFieldName(attributeNameResolver + fieldName);
    }

    /// a count aggregation is always on an uint 64
    onField->setStamp(DataTypeProvider::provideDataType(LogicalType::UINT64));
    asField->setStamp(onField->getStamp());
}

std::shared_ptr<WindowAggregationFunction> CountAggregationFunction::clone()
{
    return std::make_shared<CountAggregationFunction>(CountAggregationFunction(this->onField->clone(), this->asField->clone()));
}

std::shared_ptr<DataType> CountAggregationFunction::getInputStamp()
{
    return DataTypeProvider::provideDataType(LogicalType::UINT64);
}
std::shared_ptr<DataType> CountAggregationFunction::getPartialAggregateStamp()
{
    return DataTypeProvider::provideDataType(LogicalType::UINT64);
}
std::shared_ptr<DataType> CountAggregationFunction::getFinalAggregateStamp()
{
    return DataTypeProvider::provideDataType(LogicalType::UINT64);
}

NES::SerializableAggregationFunction CountAggregationFunction::serialize() const
{
    NES::SerializableAggregationFunction serializedAggregationFunction;
    serializedAggregationFunction.set_type(NAME);

    auto *onFieldFuc = new SerializableFunction();
    FunctionSerializationUtil::serializeFunction(onField, onFieldFuc);

    auto *asFieldFuc = new SerializableFunction();
    FunctionSerializationUtil::serializeFunction(asField, asFieldFuc);

    serializedAggregationFunction.set_allocated_as_field(asFieldFuc);
    serializedAggregationFunction.set_allocated_on_field(onFieldFuc);
    return serializedAggregationFunction;
}

}
