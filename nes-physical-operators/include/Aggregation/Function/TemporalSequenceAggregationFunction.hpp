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

#include <cstddef>
#include <memory>
#include <string>
#include <vector>
#include <Aggregation/Function/AggregationFunction.hpp>
#include <DataTypes/DataType.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <val_concepts.hpp>

// Simplified trajectory aggregation
// Tracks trajectory bounds and statistics instead of collecting all points
// Memory layout: count(8) + minLon(8) + maxLon(8) + minLat(8) + maxLat(8) + firstTs(8) + lastTs(8)

namespace NES
{

/**
 * @brief Simplified trajectory aggregation function that tracks spatial-temporal bounds
 * and returns trajectory summary data.
 * 
 * This function expects input records with:
 * - longitude (double)
 * - latitude (double) 
 * - timestamp (int64 or timestamp type)
 * 
 * Output: Trajectory summary containing bounds and statistics
 * 
 * Proof-of-concept Features:
 * - Tracks min/max longitude and latitude bounds
 * - Counts total number of points
 * - Records first and last timestamps
 * - Returns simple JSON summary format
 * - Foundation for incremental MEOS integration
 */
class TemporalSequenceAggregationFunction : public AggregationFunction
{
public:
    TemporalSequenceAggregationFunction(
        DataType inputType,
        DataType resultType,
        PhysicalFunction inputFunction,
        Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier,
        PhysicalFunction lonFunction,
        PhysicalFunction latFunction,
        PhysicalFunction timestampFunction,
        std::shared_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider> memProviderPagedVector);

    void lift(
        const nautilus::val<AggregationState*>& aggregationState,
        PipelineMemoryProvider& pipelineMemoryProvider,
        const Nautilus::Record& record) override;

    void combine(
        nautilus::val<AggregationState*> aggregationState1,
        nautilus::val<AggregationState*> aggregationState2,
        PipelineMemoryProvider& pipelineMemoryProvider) override;

    Nautilus::Record lower(
        nautilus::val<AggregationState*> aggregationState, 
        PipelineMemoryProvider& pipelineMemoryProvider) override;

    void reset(
        nautilus::val<AggregationState*> aggregationState, 
        PipelineMemoryProvider& pipelineMemoryProvider) override;

    void cleanup(nautilus::val<AggregationState*> aggregationState) override;

    [[nodiscard]] size_t getSizeOfStateInBytes() const override;

    ~TemporalSequenceAggregationFunction() override = default;

private:
    PhysicalFunction lonFunction;       // Function to extract longitude
    PhysicalFunction latFunction;       // Function to extract latitude  
    PhysicalFunction timestampFunction; // Function to extract timestamp
    std::shared_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider> memProviderPagedVector;
};

} 