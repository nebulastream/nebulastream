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
#include <Functions/FunctionNode.hpp>
#include <Functions/FieldAccessFunctionNode.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/MinAggregationDescriptor.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>


namespace NES::Windowing
{

MinAggregationDescriptor::MinAggregationDescriptor(FieldAccessFunctionNodePtr field) : WindowAggregationDescriptor(field)
{
    this->aggregationType = Type::Min;
}
MinAggregationDescriptor::MinAggregationDescriptor(FunctionNodePtr field, FunctionNodePtr asField)
    : WindowAggregationDescriptor(field, asField)
{
    this->aggregationType = Type::Min;
}

WindowAggregationDescriptorPtr MinAggregationDescriptor::create(FieldAccessFunctionNodePtr onField, FieldAccessFunctionNodePtr asField)
{
    return std::make_shared<MinAggregationDescriptor>(MinAggregationDescriptor(std::move(onField), std::move(asField)));
}

WindowAggregationDescriptorPtr MinAggregationDescriptor::on(const FunctionNodePtr& keyFunction)
{
    if (!NES::Util::instanceOf<FieldAccessFunctionNode>(keyFunction))
    {
        NES_ERROR("Query: window key has to be an FieldAccessFunction but it was a  {}", keyFunction->toString());
    }
    auto fieldAccess = NES::Util::as<FieldAccessFunctionNode>(keyFunction);
    return std::make_shared<MinAggregationDescriptor>(MinAggregationDescriptor(fieldAccess));
}

DataTypePtr MinAggregationDescriptor::getInputStamp()
{
    return onField->getStamp();
}
DataTypePtr MinAggregationDescriptor::getPartialAggregateStamp()
{
    return onField->getStamp();
}
DataTypePtr MinAggregationDescriptor::getFinalAggregateStamp()
{
    return onField->getStamp();
}

void MinAggregationDescriptor::inferStamp(SchemaPtr schema)
{
    /// We first infer the stamp of the input field and set the output stamp as the same.
    onField->inferStamp(schema);
    if (!onField->getStamp()->isNumeric())
    {
        NES_FATAL_ERROR("MinAggregationDescriptor: aggregations on non numeric fields is not supported.");
    }

    ///Set fully qualified name for the as Field
    auto onFieldName = NES::Util::as<FieldAccessFunctionNode>(onField)->getFieldName();
    auto asFieldName = NES::Util::as<FieldAccessFunctionNode>(asField)->getFieldName();

    auto attributeNameResolver = onFieldName.substr(0, onFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
    ///If on and as field name are different then append the attribute name resolver from on field to the as field
    if (asFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) == std::string::npos)
    {
        NES::Util::as<FieldAccessFunctionNode>(asField)->updateFieldName(attributeNameResolver + asFieldName);
    }
    else
    {
        auto fieldName = asFieldName.substr(asFieldName.find_last_of(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
        NES::Util::as<FieldAccessFunctionNode>(asField)->updateFieldName(attributeNameResolver + fieldName);
    }
    asField->setStamp(onField->getStamp());
}
WindowAggregationDescriptorPtr MinAggregationDescriptor::copy()
{
    return std::make_shared<MinAggregationDescriptor>(MinAggregationDescriptor(this->onField->copy(), this->asField->copy()));
}

} /// namespace NES::Windowing
