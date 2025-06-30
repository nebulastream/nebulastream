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

#pragma once

#include <memory>
#include <string_view>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

/**
 * @brief TemporalSequence aggregation logical function that collects spatial-temporal points
 * into a MEOS TemporalSequence and returns it as serialized VARSIZED data.
 * 
 * This function takes three field inputs:
 * - Longitude field (FLOAT64)
 * - Latitude field (FLOAT64) 
 * - Timestamp field (UINT64 or timestamp)
 * 
 * Output: UINT64 containing point count (simplified proof-of-concept)
 * 
 * Usage in SQL:
 * SELECT TEMPORAL_SEQUENCE(longitude, latitude, timestamp) AS trajectory
 * FROM stream 
 * WINDOW TUMBLING(SIZE 1 HOUR)
 */
class TemporalSequenceAggregationLogicalFunction : public WindowAggregationLogicalFunction
{
public:
    // Factory methods for creating the function
    static std::shared_ptr<WindowAggregationLogicalFunction> create(
        const FieldAccessLogicalFunction& lonField,
        const FieldAccessLogicalFunction& latField,
        const FieldAccessLogicalFunction& timestampField);
    
    static std::shared_ptr<WindowAggregationLogicalFunction> create(
        const FieldAccessLogicalFunction& lonField,
        const FieldAccessLogicalFunction& latField,
        const FieldAccessLogicalFunction& timestampField,
        const FieldAccessLogicalFunction& asField);

    // Constructors
    TemporalSequenceAggregationLogicalFunction(
        const FieldAccessLogicalFunction& lonField,
        const FieldAccessLogicalFunction& latField,
        const FieldAccessLogicalFunction& timestampField);
        
    TemporalSequenceAggregationLogicalFunction(
        const FieldAccessLogicalFunction& lonField,
        const FieldAccessLogicalFunction& latField,
        const FieldAccessLogicalFunction& timestampField,
        const FieldAccessLogicalFunction& asField);

    ~TemporalSequenceAggregationLogicalFunction() override = default;

    // Required WindowAggregationLogicalFunction methods
    void inferStamp(const Schema& schema) override;
    [[nodiscard]] NES::SerializableAggregationFunction serialize() const override;
    [[nodiscard]] std::string_view getName() const noexcept override;

    // Getters for field access functions
    [[nodiscard]] const FieldAccessLogicalFunction& getLonField() const { return lonField; }
    [[nodiscard]] const FieldAccessLogicalFunction& getLatField() const { return latField; }
    [[nodiscard]] const FieldAccessLogicalFunction& getTimestampField() const { return timestampField; }

private:
    static constexpr std::string_view NAME = "TemporalSequence";
    static constexpr DataType::Type inputAggregateStampType = DataType::Type::UNDEFINED;  // Multi-field input
    static constexpr DataType::Type partialAggregateStampType = DataType::Type::VARSIZED;   // Trajectory state 
    static constexpr DataType::Type finalAggregateStampType = DataType::Type::VARSIZED;     // MEOS trajectory result

    // Field access functions for spatial-temporal coordinates
    FieldAccessLogicalFunction lonField;       // Longitude field
    FieldAccessLogicalFunction latField;       // Latitude field  
    FieldAccessLogicalFunction timestampField; // Timestamp field
};

} // namespace NES 