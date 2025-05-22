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
#include <Operators/LogicalOperators/Windows/Aggregations/CountAggregationDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/WindowAggregationDescriptor.hpp>
#include <Util/Common.hpp>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>

namespace NES::Windowing
{

CountAggregationDescriptor::CountAggregationDescriptor(const std::shared_ptr<NodeFunctionFieldAccess>& field)
    : WindowAggregationDescriptor(field)
{
    this->aggregationType = Type::Count;
}

CountAggregationDescriptor::CountAggregationDescriptor(
    const std::shared_ptr<NodeFunction>& field, const std::shared_ptr<NodeFunction>& asField)
    : WindowAggregationDescriptor(field, asField)
{
    this->aggregationType = Type::Count;
}

std::shared_ptr<WindowAggregationDescriptor>
CountAggregationDescriptor::create(std::shared_ptr<NodeFunctionFieldAccess> onField, std::shared_ptr<NodeFunctionFieldAccess> asField)
{
    return std::make_shared<CountAggregationDescriptor>(CountAggregationDescriptor(std::move(onField), std::move(asField)));
}

std::shared_ptr<WindowAggregationDescriptor> CountAggregationDescriptor::on(const std::shared_ptr<NodeFunction>& keyFunction)
{
    if (!NES::Util::instanceOf<NodeFunctionFieldAccess>(keyFunction))
    {
        throw DifferentFieldTypeExpected("Query: window key has to be an FieldAccessFunction but it was a  {}", *keyFunction);
    }
    auto fieldAccess = NES::Util::as<NodeFunctionFieldAccess>(keyFunction);
    return std::make_shared<CountAggregationDescriptor>(CountAggregationDescriptor(fieldAccess));
}

void CountAggregationDescriptor::inferStamp(const Schema& schema)
{
    const auto attributeNameResolver = schema.getSourceNameQualifier() + Schema::ATTRIBUTE_NAME_SEPARATOR;
    const auto asFieldName = NES::Util::as<NodeFunctionFieldAccess>(asField)->getFieldName();

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

    /// a count aggregation is always on an uint 64
    onField->setStamp(DataTypeProvider::provideDataType(LogicalType::UINT64));
    asField->setStamp(onField->getStamp());
}

std::shared_ptr<WindowAggregationDescriptor> CountAggregationDescriptor::copy()
{
    return std::make_shared<CountAggregationDescriptor>(CountAggregationDescriptor(this->onField->deepCopy(), this->asField->deepCopy()));
}

std::shared_ptr<DataType> CountAggregationDescriptor::getInputStamp()
{
    return DataTypeProvider::provideDataType(LogicalType::UINT64);
}

std::shared_ptr<DataType> CountAggregationDescriptor::getPartialAggregateStamp()
{
    return DataTypeProvider::provideDataType(LogicalType::UINT64);
}

std::shared_ptr<DataType> CountAggregationDescriptor::getFinalAggregateStamp()
{
    return DataTypeProvider::provideDataType(LogicalType::UINT64);
}

}
