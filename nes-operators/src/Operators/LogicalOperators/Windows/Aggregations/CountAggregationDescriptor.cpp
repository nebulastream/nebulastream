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
#include <Operators/LogicalOperators/Windows/Aggregations/CountAggregationDescriptor.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES::Windowing
{

CountAggregationDescriptor::CountAggregationDescriptor(NodeFunctionFieldAccessPtr field) : WindowAggregationDescriptor(field)
{
    this->aggregationType = Type::Count;
}
CountAggregationDescriptor::CountAggregationDescriptor(NodeFunctionPtr field, NodeFunctionPtr asField)
    : WindowAggregationDescriptor(field, asField)
{
    this->aggregationType = Type::Count;
}

WindowAggregationDescriptorPtr CountAggregationDescriptor::create(NodeFunctionFieldAccessPtr onField, NodeFunctionFieldAccessPtr asField)
{
    return std::make_shared<CountAggregationDescriptor>(CountAggregationDescriptor(std::move(onField), std::move(asField)));
}

WindowAggregationDescriptorPtr CountAggregationDescriptor::on()
{
    auto countField = NodeFunctionFieldAccess::create("count");
    return std::make_shared<CountAggregationDescriptor>(CountAggregationDescriptor(countField->as<NodeFunctionFieldAccess>()));
}

void CountAggregationDescriptor::inferStamp(SchemaPtr schema)
{
    auto attributeNameResolver = schema->getSourceNameQualifier() + Schema::ATTRIBUTE_NAME_SEPARATOR;
    auto asFieldName = asField->as<NodeFunctionFieldAccess>()->getFieldName();

    ///If on and as field name are different then append the attribute name resolver from on field to the as field
    if (asFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) == std::string::npos)
    {
        asField->as<NodeFunctionFieldAccess>()->updateFieldName(attributeNameResolver + asFieldName);
    }
    else
    {
        auto fieldName = asFieldName.substr(asFieldName.find_last_of(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
        asField->as<NodeFunctionFieldAccess>()->updateFieldName(attributeNameResolver + fieldName);
    }

    /// a count aggregation is always on an uint 64
    onField->setStamp(DataTypeFactory::createUInt64());
    asField->setStamp(onField->getStamp());
}

WindowAggregationDescriptorPtr CountAggregationDescriptor::copy()
{
    return std::make_shared<CountAggregationDescriptor>(CountAggregationDescriptor(this->onField->deepCopy(), this->asField->deepCopy()));
}
DataTypePtr CountAggregationDescriptor::getInputStamp()
{
    return DataTypeFactory::createUInt64();
}
DataTypePtr CountAggregationDescriptor::getPartialAggregateStamp()
{
    return DataTypeFactory::createUInt64();
}
DataTypePtr CountAggregationDescriptor::getFinalAggregateStamp()
{
    return DataTypeFactory::createUInt64();
}

} /// namespace NES::Windowing
