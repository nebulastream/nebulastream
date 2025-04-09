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

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <API/Schema.hpp>
#include <Functions/NodeFunction.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/Synopsis/Sample/ReservoirSampleDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/WindowAggregationDescriptor.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>
#include <Common/DataTypes/Numeric.hpp>
#include <Common/DataTypes/VariableSizedDataType.hpp>

namespace NES::Windowing
{

ReservoirSampleDescriptor::ReservoirSampleDescriptor(const std::shared_ptr<NodeFunctionFieldAccess>& onField, uint64_t reservoirSize)
    : WindowAggregationDescriptor(onField, NodeFunctionFieldAccess::create("reservoir")), reservoirSize(reservoirSize)
{
    this->aggregationType = Type::Reservoir;
}
ReservoirSampleDescriptor::ReservoirSampleDescriptor(
    const std::shared_ptr<NodeFunctionFieldAccess>& onField,
    const std::shared_ptr<NodeFunctionFieldAccess>& asField,
    uint64_t reservoirSize)
    : WindowAggregationDescriptor(onField, asField), reservoirSize(reservoirSize)
{
    this->aggregationType = Type::Reservoir;
}

std::shared_ptr<WindowAggregationDescriptor> ReservoirSampleDescriptor::create(
    std::shared_ptr<NodeFunctionFieldAccess> onField, std::shared_ptr<NodeFunctionFieldAccess> asField, uint64_t reservoirSize)
{
    return std::make_shared<ReservoirSampleDescriptor>(ReservoirSampleDescriptor(std::move(onField), std::move(asField), reservoirSize));
}

std::shared_ptr<WindowAggregationDescriptor>
ReservoirSampleDescriptor::on(const std::shared_ptr<NodeFunction>& onField, uint64_t reservoirSize)
{
    INVARIANT(
        NES::Util::instanceOf<NodeFunctionFieldAccess>(onField),
        "Query: window key has to be an NodeFunctionFieldAccess but it was a {}",
        *onField);
    const auto fieldAccess = NES::Util::as<NodeFunctionFieldAccess>(onField);
    return std::make_shared<ReservoirSampleDescriptor>(ReservoirSampleDescriptor(fieldAccess, reservoirSize));
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
    const auto asFieldName = NES::Util::as<NodeFunctionFieldAccess>(asField)->getFieldName();

    const auto attributeNameResolver = "stream$";
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
    return std::make_shared<ReservoirSampleDescriptor>(ReservoirSampleDescriptor(
        NES::Util::as<NodeFunctionFieldAccess>(this->onField->deepCopy()),
        NES::Util::as<NodeFunctionFieldAccess>(this->asField->deepCopy()),
        this->reservoirSize));
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
