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
#include <Functions/NodeFunction.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/AvgAggregationDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/WindowAggregationDescriptor.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>
#include <Common/DataTypes/Integer.hpp>
#include <Common/DataTypes/Numeric.hpp>

namespace NES::Windowing
{

AvgAggregationDescriptor::AvgAggregationDescriptor(const std::shared_ptr<NodeFunctionFieldAccess>& field)
    : WindowAggregationDescriptor(field)
{
    this->aggregationType = Type::Avg;
}

AvgAggregationDescriptor::AvgAggregationDescriptor(const std::shared_ptr<NodeFunction>& field, const std::shared_ptr<NodeFunction>& asField)
    : WindowAggregationDescriptor(field, asField)
{
    this->aggregationType = Type::Avg;
}

std::shared_ptr<WindowAggregationDescriptor>
AvgAggregationDescriptor::create(std::shared_ptr<NodeFunctionFieldAccess> onField, std::shared_ptr<NodeFunctionFieldAccess> asField)
{
    return std::make_shared<AvgAggregationDescriptor>(AvgAggregationDescriptor(std::move(onField), std::move(asField)));
}

std::shared_ptr<WindowAggregationDescriptor> AvgAggregationDescriptor::on(const std::shared_ptr<NodeFunction>& onField)
{
    if (!NES::Util::instanceOf<NodeFunctionFieldAccess>(onField))
    {
        NES_ERROR("Query: window key has to be an FieldAccessFunction but it was a  {}", *onField);
    }
    const auto fieldAccess = NES::Util::as<NodeFunctionFieldAccess>(onField);
    return std::make_shared<AvgAggregationDescriptor>(AvgAggregationDescriptor(fieldAccess));
}

void AvgAggregationDescriptor::inferStamp(const Schema& schema)
{
    /// We first infer the stamp of the input field and set the output stamp as the same.
    onField->inferStamp(schema);
    if (!NES::Util::instanceOf<Numeric>(onField->getStamp()))
    {
        NES_FATAL_ERROR("AvgAggregationDescriptor: aggregations on non numeric fields is not supported.");
    }

    /// As we are performing essentially a sum and a count, we need to cast the sum to either uint64_t, int64_t or double to avoid overflow
    if (const auto integerDataType = NES::Util::as_if<Integer>(onField->getStamp()); integerDataType)
    {
        if (integerDataType->getIsSigned())
        {
            onField->setStamp(DataTypeProvider::provideDataType(LogicalType::INT64));
        }
        else
        {
            onField->setStamp(DataTypeProvider::provideDataType(LogicalType::UINT64));
        }
    }
    else
    {
        onField->setStamp(DataTypeProvider::provideDataType(LogicalType::FLOAT64));
    }


    ///Set fully qualified name for the as Field
    const auto onFieldName = NES::Util::as<NodeFunctionFieldAccess>(onField)->getFieldName();
    const auto asFieldName = NES::Util::as<NodeFunctionFieldAccess>(asField)->getFieldName();

    const auto attributeNameResolver = onFieldName.substr(0, onFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
    ///If on and as field name are different then append the attribute name resolver from on field to the as field
    if (asFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) == std::string::npos)
    {
        NES::Util::as<NodeFunctionFieldAccess>(asField)->updateFieldName(attributeNameResolver + asFieldName);
    }
    else
    {
        const auto fieldName = asFieldName.substr(asFieldName.find_last_of(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
        NES::Util::as<NodeFunctionFieldAccess>(asField)->updateFieldName(attributeNameResolver + fieldName);
    }
    asField->setStamp(getFinalAggregateStamp());
}

std::shared_ptr<WindowAggregationDescriptor> AvgAggregationDescriptor::copy()
{
    return std::make_shared<AvgAggregationDescriptor>(AvgAggregationDescriptor(this->onField->deepCopy(), this->asField->deepCopy()));
}

std::shared_ptr<DataType> AvgAggregationDescriptor::getInputStamp()
{
    return onField->getStamp();
}

std::shared_ptr<DataType> AvgAggregationDescriptor::getPartialAggregateStamp()
{
    return DataTypeProvider::provideDataType(LogicalType::UNDEFINED);
}

std::shared_ptr<DataType> AvgAggregationDescriptor::getFinalAggregateStamp()
{
    return DataTypeProvider::provideDataType(LogicalType::FLOAT64);
}

}
