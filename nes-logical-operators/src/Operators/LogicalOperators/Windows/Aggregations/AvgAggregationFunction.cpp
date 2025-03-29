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
#include <Operators/LogicalOperators/Windows/Aggregations/AvgAggregationFunction.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/WindowAggregationFunction.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>
#include <Common/DataTypes/Numeric.hpp>
#include <SerializableAggregationFunction.pb.h>
#include <Functions/FunctionSerializationUtil.hpp>

namespace NES::Windowing
{

AvgAggregationFunction::AvgAggregationFunction(std::shared_ptr<FieldAccessLogicalFunction> field) : WindowAggregationFunction(field)
{
    this->aggregationType = Type::Avg;
}
AvgAggregationFunction::AvgAggregationFunction(std::shared_ptr<LogicalFunction> field, std::shared_ptr<LogicalFunction> asField)
    : WindowAggregationFunction(field, asField)
{
    this->aggregationType = Type::Avg;
}

std::shared_ptr<WindowAggregationFunction>
AvgAggregationFunction::create(std::shared_ptr<FieldAccessLogicalFunction> onField, std::shared_ptr<FieldAccessLogicalFunction> asField)
{
    return std::make_shared<AvgAggregationFunction>(AvgAggregationFunction(std::move(onField), std::move(asField)));
}

std::shared_ptr<WindowAggregationFunction> AvgAggregationFunction::on(const std::shared_ptr<LogicalFunction>& onField)
{
    if (!NES::Util::instanceOf<FieldAccessLogicalFunction>(onField))
    {
        NES_ERROR("Query: window key has to be an FieldAccessFunction but it was a  {}", *onField);
    }
    auto fieldAccess = NES::Util::as<FieldAccessLogicalFunction>(onField);
    return std::make_shared<AvgAggregationFunction>(AvgAggregationFunction(fieldAccess));
}

std::shared_ptr<WindowAggregationFunction> AvgAggregationFunction::clone()
{
    return std::make_shared<AvgAggregationFunction>(AvgAggregationFunction(this->onField->clone(), this->asField->clone()));
}

void AvgAggregationFunction::inferStamp(const Schema& schema)
{
    /// We first infer the stamp of the input field and set the output stamp as the same.
    onField->inferStamp(schema);
    if (!NES::Util::instanceOf<Numeric>(onField->getStamp()))
    {
        NES_FATAL_ERROR("AvgAggregationFunction: aggregations on non numeric fields is not supported.");
    }
    ///Set fully qualified name for the as Field
    auto onFieldName = NES::Util::as<FieldAccessLogicalFunction>(onField)->getFieldName();
    auto asFieldName = NES::Util::as<FieldAccessLogicalFunction>(asField)->getFieldName();

    const auto attributeNameResolver = onFieldName.substr(0, onFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
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
    asField->setStamp(getFinalAggregateStamp());
}

std::shared_ptr<DataType> AvgAggregationFunction::getInputStamp()
{
    return onField->getStamp();
}
std::shared_ptr<DataType> AvgAggregationFunction::getPartialAggregateStamp()
{
    return DataTypeProvider::provideDataType(LogicalType::UNDEFINED);
}
std::shared_ptr<DataType> AvgAggregationFunction::getFinalAggregateStamp()
{
    return DataTypeProvider::provideDataType(LogicalType::FLOAT64);
}

NES::SerializableAggregationFunction AvgAggregationFunction::serialize() const
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
