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

#include <Operators/LogicalOperators/Windows/Aggregations/Synopsis/Sample/ReservoirSampleDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>
#include <Common/DataTypes/Numeric.hpp>

namespace NES::Windowing
{

ReservoirSampleDescriptor::ReservoirSampleDescriptor(const std::shared_ptr<NodeFunctionFieldAccess>& onField)
    : WindowAggregationDescriptor(onField)
{
    this->aggregationType = Type::Reservoir;
    this->reservoirSize = RESERVOIR_SIZE;
}
ReservoirSampleDescriptor::ReservoirSampleDescriptor(
    const std::shared_ptr<NodeFunction>& onField, const std::shared_ptr<NodeFunction>& asField)
    : WindowAggregationDescriptor(onField, asField)
{
    this->aggregationType = Type::Reservoir;
    this->reservoirSize = RESERVOIR_SIZE;
}

std::shared_ptr<WindowAggregationDescriptor>
ReservoirSampleDescriptor::create(std::shared_ptr<NodeFunctionFieldAccess> onField, std::shared_ptr<NodeFunctionFieldAccess> asField)
{
    return std::make_shared<ReservoirSampleDescriptor>(ReservoirSampleDescriptor(std::move(onField), std::move(asField)));
}

std::shared_ptr<WindowAggregationDescriptor> ReservoirSampleDescriptor::on(const std::shared_ptr<NodeFunction>& onField)
{
    // TODO(nikla44): use NodeFunction that gets parameter from query instead of accessing fields
    if (!NES::Util::instanceOf<NodeFunctionFieldAccess>(onField))
    {
        NES_ERROR("Query: window key has to be an FieldAccessFunction but it was a  {}", *onField);
    }
    const auto fieldAccess = NES::Util::as<NodeFunctionFieldAccess>(onField);
    return std::make_shared<ReservoirSampleDescriptor>(ReservoirSampleDescriptor(fieldAccess));
}

void ReservoirSampleDescriptor::inferStamp(const Schema& schema)
{
    /// We first infer the stamp of the input field and set the output stamp as the same.
    onField->inferStamp(schema);
    if (!NES::Util::instanceOf<Numeric>(onField->getStamp()))
    {
        NES_FATAL_ERROR("ReservoirSampleDescriptor: aggregations on non numeric fields is not supported.");
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

std::shared_ptr<WindowAggregationDescriptor> ReservoirSampleDescriptor::copy()
{
    return std::make_shared<ReservoirSampleDescriptor>(ReservoirSampleDescriptor(this->onField->deepCopy(), this->asField->deepCopy()));
}

std::shared_ptr<DataType> ReservoirSampleDescriptor::getInputStamp()
{
    return onField->getStamp();
}

std::shared_ptr<DataType> ReservoirSampleDescriptor::getPartialAggregateStamp()
{
    return DataTypeProvider::provideDataType(LogicalType::UNDEFINED);
}

std::shared_ptr<DataType> ReservoirSampleDescriptor::getFinalAggregateStamp()
{
    return DataTypeProvider::provideDataType(LogicalType::VARSIZED);
}

uint64_t ReservoirSampleDescriptor::getReservoirSize() const
{
    return reservoirSize;
}

}
