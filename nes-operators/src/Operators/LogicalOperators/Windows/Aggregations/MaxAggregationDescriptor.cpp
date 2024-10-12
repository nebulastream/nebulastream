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
#include <Operators/LogicalOperators/Windows/Aggregations/MaxAggregationDescriptor.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>


namespace NES::Windowing
{

MaxAggregationDescriptor::MaxAggregationDescriptor(FieldAccessFunctionNodePtr field) : WindowAggregationDescriptor(field)
{
    this->aggregationType = Type::Max;
}

MaxAggregationDescriptor::MaxAggregationDescriptor(FunctionNodePtr field, FunctionNodePtr asField)
    : WindowAggregationDescriptor(field, asField)
{
    this->aggregationType = Type::Max;
}

WindowAggregationDescriptorPtr MaxAggregationDescriptor::create(FieldAccessFunctionNodePtr onField, FieldAccessFunctionNodePtr asField)
{
    return std::make_shared<MaxAggregationDescriptor>(std::move(onField), std::move(asField));
}

WindowAggregationDescriptorPtr MaxAggregationDescriptor::on(const FunctionNodePtr& keyFunction)
{
    if (!NES::Util::instanceOf<FieldAccessFunctionNode>(keyFunction))
    {
        NES_ERROR("Query: window key has to be an FieldAccessFunction but it was a  {}", keyFunction->toString());
    }
    auto fieldAccess = NES::Util::as<FieldAccessFunctionNode>(keyFunction);
    return std::make_shared<MaxAggregationDescriptor>(MaxAggregationDescriptor(fieldAccess));
}

void MaxAggregationDescriptor::inferStamp(SchemaPtr schema)
{
    /// We first infer the stamp of the input field and set the output stamp as the same.
    onField->inferStamp(schema);
    if (!onField->getStamp()->isNumeric())
    {
        NES_FATAL_ERROR("MaxAggregationDescriptor: aggregations on non numeric fields is not supported.");
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

WindowAggregationDescriptorPtr MaxAggregationDescriptor::copy()
{
    return std::make_shared<MaxAggregationDescriptor>(this->onField->copy(), this->asField->copy());
}

DataTypePtr MaxAggregationDescriptor::getInputStamp()
{
    return onField->getStamp();
}

DataTypePtr MaxAggregationDescriptor::getPartialAggregateStamp()
{
    return onField->getStamp();
}

DataTypePtr MaxAggregationDescriptor::getFinalAggregateStamp()
{
    return onField->getStamp();
}
} /// namespace NES::Windowing
