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
#include <Operators/LogicalOperators/Windows/Aggregations/MaxAggregationDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/WindowAggregationDescriptor.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/Numeric.hpp>


namespace NES::Windowing
{

MaxAggregationDescriptor::MaxAggregationDescriptor(std::shared_ptr<FieldAccessLogicalFunction> field) : WindowAggregationDescriptor(field)
{
    this->aggregationType = Type::Max;
}

MaxAggregationDescriptor::MaxAggregationDescriptor(std::shared_ptr<LogicalFunction> field, std::shared_ptr<LogicalFunction> asField)
    : WindowAggregationDescriptor(field, asField)
{
    this->aggregationType = Type::Max;
}

std::shared_ptr<WindowAggregationDescriptor>
MaxAggregationDescriptor::create(std::shared_ptr<FieldAccessLogicalFunction> onField, std::shared_ptr<FieldAccessLogicalFunction> asField)
{
    return std::make_shared<MaxAggregationDescriptor>(std::move(onField), std::move(asField));
}

std::shared_ptr<WindowAggregationDescriptor> MaxAggregationDescriptor::on(const std::shared_ptr<LogicalFunction>& keyFunction)
{
    if (!NES::Util::instanceOf<FieldAccessLogicalFunction>(onField))
    {
        NES_ERROR("Query: window key has to be an FieldAccessFunction but it was a  {}", *onField);
    }
    const auto fieldAccess = NES::Util::as<FieldAccessLogicalFunction>(onField);
    return std::make_shared<MaxAggregationDescriptor>(MaxAggregationDescriptor(fieldAccess));
}

void MaxAggregationDescriptor::inferStamp(const Schema& schema)
{
    /// We first infer the stamp of the input field and set the output stamp as the same.
    onField->inferStamp(schema);
    if (!NES::Util::instanceOf<Numeric>(onField->getStamp()))
    {
        NES_FATAL_ERROR("MaxAggregationDescriptor: aggregations on non numeric fields is not supported.");
    }

    ///Set fully qualified name for the as Field
    const auto onFieldName = NES::Util::as<FieldAccessLogicalFunction>(onField)->getFieldName();
    const auto asFieldName = NES::Util::as<FieldAccessLogicalFunction>(asField)->getFieldName();

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

std::shared_ptr<WindowAggregationDescriptor> MaxAggregationDescriptor::clone()
{
    return std::make_shared<MaxAggregationDescriptor>(MaxAggregationDescriptor(this->onField->clone(), this->asField->clone()));
}

std::shared_ptr<DataType> MaxAggregationDescriptor::getInputStamp()
{
    return onField->getStamp();
}

std::shared_ptr<DataType> MaxAggregationDescriptor::getPartialAggregateStamp()
{
    return onField->getStamp();
}

std::shared_ptr<DataType> MaxAggregationDescriptor::getFinalAggregateStamp()
{
    return onField->getStamp();
}
}
