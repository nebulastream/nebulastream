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

#include <Operators/Windows/Aggregations/Synopsis/Sample/ReservoirSampleLogicalFunction.hpp>

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <AggregationLogicalFunctionRegistry.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

ReservoirSampleLogicalFunction::ReservoirSampleLogicalFunction(const FieldAccessLogicalFunction& onField, uint64_t reservoirSize)
    : WindowAggregationLogicalFunction(
          onField.getDataType(),
          DataTypeProvider::provideDataType(partialAggregateStampType),
          DataTypeProvider::provideDataType(finalAggregateStampType),
          onField)
    , reservoirSize(reservoirSize)
{
}
ReservoirSampleLogicalFunction::ReservoirSampleLogicalFunction(
    const FieldAccessLogicalFunction& onField, const FieldAccessLogicalFunction& asField, uint64_t reservoirSize)
    : WindowAggregationLogicalFunction(
          onField.getDataType(),
          DataTypeProvider::provideDataType(partialAggregateStampType),
          DataTypeProvider::provideDataType(finalAggregateStampType),
          onField,
          asField)
    , reservoirSize(reservoirSize)
{
}

std::shared_ptr<WindowAggregationLogicalFunction> ReservoirSampleLogicalFunction::create(
    const FieldAccessLogicalFunction& onField, const FieldAccessLogicalFunction& asField, uint64_t reservoirSize)
{
    return std::make_shared<ReservoirSampleLogicalFunction>(onField, asField, reservoirSize);
}

std::shared_ptr<WindowAggregationLogicalFunction>
ReservoirSampleLogicalFunction::create(const FieldAccessLogicalFunction& onField, uint64_t reservoirSize)
{
    return std::make_shared<ReservoirSampleLogicalFunction>(onField, reservoirSize);
}

std::string_view ReservoirSampleLogicalFunction::getName() const noexcept
{
    return NAME;
}

void ReservoirSampleLogicalFunction::inferStamp(const Schema& schema)
{
    // TODO use schema!
    (void)schema;
    /// TODO We probably don't need to do most of this, especially if we remove the onField, which we should!
    /// We first infer the dataType of the input field and set the output dataType as the same.
    ///Set fully qualified name for the as Field
    const auto onFieldName = onField.getFieldName();
    const auto asFieldName = asField.getFieldName();

    const auto attributeNameResolver = "stream$";
    ///If on and as field name are different then append the attribute name resolver from on field to the as field
    if (asFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) == std::string::npos)
    {
        asField = asField.withFieldName(attributeNameResolver + asFieldName).get<FieldAccessLogicalFunction>();
    }
    else
    {
        const auto fieldName = asFieldName.substr(asFieldName.find_last_of(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
        asField = asField.withFieldName(attributeNameResolver + fieldName).get<FieldAccessLogicalFunction>();
    }
    inputStamp = onField.getDataType();
    finalAggregateStamp = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
    asField = asField.withDataType(getFinalAggregateStamp()).get<FieldAccessLogicalFunction>();
}

NES::SerializableAggregationFunction ReservoirSampleLogicalFunction::serialize() const
{
    /// TODO Adapt to reservoir sample
    NES::SerializableAggregationFunction serializedAggregationFunction;
    serializedAggregationFunction.set_type(NAME);

    auto onFieldFuc = SerializableFunction();
    onFieldFuc.CopyFrom(onField.serialize());

    auto asFieldFuc = SerializableFunction();
    asFieldFuc.CopyFrom(asField.serialize());

    serializedAggregationFunction.mutable_as_field()->CopyFrom(asFieldFuc);
    serializedAggregationFunction.mutable_on_field()->CopyFrom(onFieldFuc);
    return serializedAggregationFunction;
}

AggregationLogicalFunctionRegistryReturnType
AggregationLogicalFunctionGeneratedRegistrar::RegisterReservoirSampleAggregationLogicalFunction(
    AggregationLogicalFunctionRegistryArguments arguments)
{
    (void)arguments;
    PRECONDITION(false, "Not yet used. TODO Implement when NES/main has it.");
    // PRECONDITION(
    //     arguments.fields.size() == 2, "ReservoirSampleLogicalFunction requires exactly two fields, but got {}", arguments.fields.size());
    // auto reservoirSizeString = arguments.fields[2].getFieldName();
    // uint64_t reservoirSize;
    // auto parseResult = std::from_chars(reservoirSizeString.data(), reservoirSizeString.data() + reservoirSizeString.size(), reservoirSize);
    // INVARIANT(parseResult.ec == std::errc(), "Failed to parse reservoir size: {}", reservoirSizeString);
    // return ReservoirSampleLogicalFunction::create(arguments.fields[0], arguments.fields[1], reservoirSize);
}

uint64_t ReservoirSampleLogicalFunction::getReservoirSize() const
{
    return reservoirSize;
}

}
