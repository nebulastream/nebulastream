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

#include <utility>
#include <API/Schema.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/SumAggregationDescriptor.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/Numeric.hpp>


namespace NES::Windowing
{

SumAggregationDescriptor::SumAggregationDescriptor(NodeFunctionFieldAccessPtr field) : WindowAggregationDescriptor(field)
{
    this->aggregationType = Type::Sum;
}
SumAggregationDescriptor::SumAggregationDescriptor(NodeFunctionPtr field, NodeFunctionPtr asField)
    : WindowAggregationDescriptor(field, asField)
{
    this->aggregationType = Type::Sum;
}

WindowAggregationDescriptorPtr SumAggregationDescriptor::create(NodeFunctionFieldAccessPtr onField, NodeFunctionFieldAccessPtr asField)
{
    return std::make_shared<SumAggregationDescriptor>(SumAggregationDescriptor(std::move(onField), std::move(asField)));
}

WindowAggregationDescriptorPtr SumAggregationDescriptor::on(const NodeFunctionPtr& keyFunction)
{
    if (!NES::Util::instanceOf<NodeFunctionFieldAccess>(keyFunction))
    {
        NES_ERROR("Query: window key has to be an FieldAccessFunction but it was a  {}", *keyFunction);
    }
    auto fieldAccess = NES::Util::as<NodeFunctionFieldAccess>(keyFunction);
    return std::make_shared<SumAggregationDescriptor>(SumAggregationDescriptor(fieldAccess));
}

void SumAggregationDescriptor::inferStamp(const Schema& schema)
{
    /// We first infer the stamp of the input field and set the output stamp as the same.
    onField->inferStamp(schema);
    if (!NES::Util::instanceOf<Numeric>(onField->getStamp()))
    {
        NES_FATAL_ERROR("SumAggregationDescriptor: aggregations on non numeric fields is not supported.");
    }

    ///Set fully qualified name for the as Field
    auto onFieldName = NES::Util::as<NodeFunctionFieldAccess>(onField)->getFieldName();
    auto asFieldName = NES::Util::as<NodeFunctionFieldAccess>(asField)->getFieldName();

    auto attributeNameResolver = onFieldName.substr(0, onFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
    ///If on and as field name are different then append the attribute name resolver from on field to the as field
    if (asFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) == std::string::npos)
    {
        NES::Util::as<NodeFunctionFieldAccess>(asField)->updateFieldName(attributeNameResolver + asFieldName);
    }
    else
    {
        auto fieldName = asFieldName.substr(asFieldName.find_last_of(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
        NES::Util::as<NodeFunctionFieldAccess>(asField)->updateFieldName(attributeNameResolver + fieldName);
    }
    asField->setStamp(getFinalAggregateStamp());
}
WindowAggregationDescriptorPtr SumAggregationDescriptor::copy()
{
    return std::make_shared<SumAggregationDescriptor>(SumAggregationDescriptor(this->onField->deepCopy(), this->asField->deepCopy()));
}

DataTypePtr SumAggregationDescriptor::getInputStamp()
{
    return onField->getStamp();
}
DataTypePtr SumAggregationDescriptor::getPartialAggregateStamp()
{
    return onField->getStamp();
}
DataTypePtr SumAggregationDescriptor::getFinalAggregateStamp()
{
    return onField->getStamp();
}

}
