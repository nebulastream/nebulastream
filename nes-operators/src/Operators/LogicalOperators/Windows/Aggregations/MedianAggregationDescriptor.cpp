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
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/NodeFunction.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/MedianAggregationDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/WindowAggregationDescriptor.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>


namespace NES::Windowing
{

MedianAggregationDescriptor::MedianAggregationDescriptor(const std::shared_ptr<NodeFunctionFieldAccess>& field)
    : WindowAggregationDescriptor(field)
{
    this->aggregationType = Type::Median;
}
MedianAggregationDescriptor::MedianAggregationDescriptor(
    const std::shared_ptr<NodeFunction>& field, const std::shared_ptr<NodeFunction>& asField)
    : WindowAggregationDescriptor(field, asField)
{
    this->aggregationType = Type::Median;
}

std::shared_ptr<WindowAggregationDescriptor>
MedianAggregationDescriptor::create(std::shared_ptr<NodeFunctionFieldAccess> onField, std::shared_ptr<NodeFunctionFieldAccess> asField)
{
    return std::make_shared<MedianAggregationDescriptor>(MedianAggregationDescriptor(std::move(onField), std::move(asField)));
}

std::shared_ptr<WindowAggregationDescriptor> MedianAggregationDescriptor::on(const std::shared_ptr<NodeFunction>& onField)
{
    if (!NES::Util::instanceOf<NodeFunctionFieldAccess>(onField))
    {
        NES_ERROR("Query: window key has to be an FieldAccessFunction but it was a  {}", *onField);
    }
    const auto fieldAccess = NES::Util::as<NodeFunctionFieldAccess>(onField);
    return std::make_shared<MedianAggregationDescriptor>(MedianAggregationDescriptor(fieldAccess));
}

void MedianAggregationDescriptor::inferStamp(const Schema& schema)
{
    /// We first infer the stamp of the input field and set the output stamp as the same.
    onField->inferStamp(schema);
    if (not onField->getStamp().isNumeric())
    {
        NES_FATAL_ERROR("MedianAggregationDescriptor: aggregations on non numeric fields is not supported.");
    }
    ///Set fully qualified name for the as Field
    const auto onFieldName = NES::Util::as<NodeFunctionFieldAccess>(onField)->getFieldName();
    const auto asFieldName = NES::Util::as<NodeFunctionFieldAccess>(asField)->getFieldName();

    ///If on and as field name are different then append the attribute name resolver from on field to the as field
    ///
    //Logic adapted from previous inferStamp versions, should be revisited in the future
    IdentifierList prefix{};
    if (std::ranges::size(onFieldName) > 1)
    {
        prefix = *std::ranges::begin(onFieldName);
    }
    NES::Util::as<NodeFunctionFieldAccess>(asField)->updateFieldName(prefix + asFieldName);
    asField->setStamp(getFinalAggregateStamp());
}
std::shared_ptr<WindowAggregationDescriptor> MedianAggregationDescriptor::copy()
{
    return std::make_shared<MedianAggregationDescriptor>(MedianAggregationDescriptor(this->onField->deepCopy(), this->asField->deepCopy()));
}

DataType MedianAggregationDescriptor::getInputStamp()
{
    return onField->getStamp();
}
DataType MedianAggregationDescriptor::getPartialAggregateStamp()
{
    return DataTypeProvider::provideDataType(DataType::Type::UNDEFINED);
}
DataType MedianAggregationDescriptor::getFinalAggregateStamp()
{
    return DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
}

}
