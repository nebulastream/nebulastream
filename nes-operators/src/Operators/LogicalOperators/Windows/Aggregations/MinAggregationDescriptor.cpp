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
#include <Operators/LogicalOperators/Windows/Aggregations/MinAggregationDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/WindowAggregationDescriptor.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/Numeric.hpp>

namespace NES::Windowing
{

MinAggregationDescriptor::MinAggregationDescriptor(const std::shared_ptr<NodeFunctionFieldAccess>& field)
    : WindowAggregationDescriptor(field)
{
    this->aggregationType = Type::Min;
}

MinAggregationDescriptor::MinAggregationDescriptor(const std::shared_ptr<NodeFunction>& field, const std::shared_ptr<NodeFunction>& asField)
    : WindowAggregationDescriptor(field, asField)
{
    this->aggregationType = Type::Min;
}

std::shared_ptr<WindowAggregationDescriptor>
MinAggregationDescriptor::create(std::shared_ptr<NodeFunctionFieldAccess> onField, std::shared_ptr<NodeFunctionFieldAccess> asField)
{
    return std::make_shared<MinAggregationDescriptor>(MinAggregationDescriptor(std::move(onField), std::move(asField)));
}

std::shared_ptr<WindowAggregationDescriptor> MinAggregationDescriptor::on(const std::shared_ptr<NodeFunction>& onField)
{
    if (!NES::Util::instanceOf<NodeFunctionFieldAccess>(onField))
    {
        NES_ERROR("Query: window key has to be an FieldAccessFunction but it was a  {}", *onField);
    }
    const auto fieldAccess = NES::Util::as<NodeFunctionFieldAccess>(onField);
    return std::make_shared<MinAggregationDescriptor>(MinAggregationDescriptor(fieldAccess));
}

std::shared_ptr<DataType> MinAggregationDescriptor::getInputStamp()
{
    return onField->getStamp();
}

std::shared_ptr<DataType> MinAggregationDescriptor::getPartialAggregateStamp()
{
    return onField->getStamp();
}

std::shared_ptr<DataType> MinAggregationDescriptor::getFinalAggregateStamp()
{
    return onField->getStamp();
}

void MinAggregationDescriptor::inferStamp(const Schema& schema)
{
    /// We first infer the stamp of the input field and set the output stamp as the same.
    onField->inferStamp(schema);
    if (!NES::Util::instanceOf<Numeric>(onField->getStamp()))
    {
        NES_FATAL_ERROR("MinAggregationDescriptor: aggregations on non numeric fields is not supported.");
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

std::shared_ptr<WindowAggregationDescriptor> MinAggregationDescriptor::copy()
{
    return std::make_shared<MinAggregationDescriptor>(MinAggregationDescriptor(this->onField->deepCopy(), this->asField->deepCopy()));
}

}
