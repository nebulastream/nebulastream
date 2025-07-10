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
#include <string>
#include <string_view>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/TemporalSequenceAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <AggregationLogicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

TemporalSequenceAggregationLogicalFunction::TemporalSequenceAggregationLogicalFunction(
    const FieldAccessLogicalFunction& lonField,
    const FieldAccessLogicalFunction& latField,
    const FieldAccessLogicalFunction& timestampField)
    : WindowAggregationLogicalFunction(
          DataTypeProvider::provideDataType(inputAggregateStampType),
          DataTypeProvider::provideDataType(partialAggregateStampType),
          DataTypeProvider::provideDataType(finalAggregateStampType),
          lonField)  // Use lonField as the primary "onField" for base class
    , lonField(lonField)
    , latField(latField)
    , timestampField(timestampField)
{
}

TemporalSequenceAggregationLogicalFunction::TemporalSequenceAggregationLogicalFunction(
    const FieldAccessLogicalFunction& lonField,
    const FieldAccessLogicalFunction& latField,
    const FieldAccessLogicalFunction& timestampField,
    const FieldAccessLogicalFunction& asField)
    : WindowAggregationLogicalFunction(
          DataTypeProvider::provideDataType(inputAggregateStampType),
          DataTypeProvider::provideDataType(partialAggregateStampType),
          DataTypeProvider::provideDataType(finalAggregateStampType),
          lonField,
          asField)
    , lonField(lonField)
    , latField(latField) 
    , timestampField(timestampField)
{
}

std::shared_ptr<WindowAggregationLogicalFunction> TemporalSequenceAggregationLogicalFunction::create(
    const FieldAccessLogicalFunction& lonField,
    const FieldAccessLogicalFunction& latField,
    const FieldAccessLogicalFunction& timestampField)
{
    return std::make_shared<TemporalSequenceAggregationLogicalFunction>(lonField, latField, timestampField);
}

std::shared_ptr<WindowAggregationLogicalFunction> TemporalSequenceAggregationLogicalFunction::create(
    const FieldAccessLogicalFunction& lonField,
    const FieldAccessLogicalFunction& latField,
    const FieldAccessLogicalFunction& timestampField,
    const FieldAccessLogicalFunction& asField)
{
    return std::make_shared<TemporalSequenceAggregationLogicalFunction>(lonField, latField, timestampField, asField);
}

std::string_view TemporalSequenceAggregationLogicalFunction::getName() const noexcept
{
    return NAME;
}

void TemporalSequenceAggregationLogicalFunction::inferStamp(const Schema& schema)
{
    // Infer data types for all three fields
    auto newLonField = lonField.withInferredDataType(schema).get<FieldAccessLogicalFunction>();
    auto newLatField = latField.withInferredDataType(schema).get<FieldAccessLogicalFunction>();
    auto newTimestampField = timestampField.withInferredDataType(schema).get<FieldAccessLogicalFunction>();

    // Validate field types - longitude and latitude must be floating point numbers
    INVARIANT(newLonField.getDataType().isFloat(), "TemporalSequence longitude field must be FLOAT32 or FLOAT64, but got {}", newLonField.getDataType());
    INVARIANT(newLatField.getDataType().isFloat(), "TemporalSequence latitude field must be FLOAT32 or FLOAT64, but got {}", newLatField.getDataType());
    
    // Timestamp must be an integer type (for Unix timestamps)
    INVARIANT(newTimestampField.getDataType().isInteger(), "TemporalSequence timestamp field must be integer type, but got {}", newTimestampField.getDataType());

    // Update field references
    lonField = newLonField;
    latField = newLatField;
    timestampField = newTimestampField;
    
    // Update base class onField to longitude field
    onField = newLonField;
    
    // Set the result type to VARSIZED (MEOS trajectory)
    auto newAsField = asField.withDataType(DataTypeProvider::provideDataType(finalAggregateStampType));
    asField = newAsField.get<FieldAccessLogicalFunction>();
    
    // Update input stamp based on the primary field (longitude)
    inputStamp = newLonField.getDataType();
}

NES::SerializableAggregationFunction TemporalSequenceAggregationLogicalFunction::serialize() const
{
    NES::SerializableAggregationFunction serializedAggregationFunction;
    serializedAggregationFunction.set_type(NAME);

    // Serialize the longitude field as the primary "on_field"
    auto lonFieldSerialized = SerializableFunction();
    lonFieldSerialized.CopyFrom(lonField.serialize());
    serializedAggregationFunction.mutable_on_field()->CopyFrom(lonFieldSerialized);

    // Serialize the result field 
    auto asFieldSerialized = SerializableFunction();
    asFieldSerialized.CopyFrom(asField.serialize());
    serializedAggregationFunction.mutable_as_field()->CopyFrom(asFieldSerialized);

    // Store additional fields in custom configuration
    // Note: In a complete implementation, you'd extend the protobuf to support multiple fields
    // For now, we'll document the limitation that full serialization requires protobuf extension
    
    return serializedAggregationFunction;
}

// Registry integration - this function will be called by the generated registrar
AggregationLogicalFunctionRegistryReturnType
AggregationLogicalFunctionGeneratedRegistrar::RegisterTemporalSequenceAggregationLogicalFunction(
    AggregationLogicalFunctionRegistryArguments arguments)
{
    // TemporalSequence requires 4 fields: [lon, lat, timestamp, as_field]
    // Or 3 fields if as_field is auto-generated: [lon, lat, timestamp]
    
    PRECONDITION(arguments.fields.size() == 3 || arguments.fields.size() == 4, 
        "TemporalSequenceAggregationLogicalFunction requires 3 or 4 fields (lon, lat, timestamp[, as_field]), but got {}",
        arguments.fields.size());
        
    if (arguments.fields.size() == 4)
    {
        // [lon, lat, timestamp, as_field]
        return TemporalSequenceAggregationLogicalFunction::create(
            arguments.fields[0], arguments.fields[1], arguments.fields[2], arguments.fields[3]);
    }
    else
    {
        // [lon, lat, timestamp] - auto-generate as_field
        return TemporalSequenceAggregationLogicalFunction::create(
            arguments.fields[0], arguments.fields[1], arguments.fields[2]);
    }
}

} // namespace NES
 