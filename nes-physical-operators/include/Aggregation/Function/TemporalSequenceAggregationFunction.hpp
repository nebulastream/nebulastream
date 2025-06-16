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
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <val_concepts.hpp>

// Forward declare MEOS types - but we need to use raw pointers due to incomplete types
namespace MEOS {
    class Meos;
}

// MEOS temporal state structure definition
// Using raw pointers to avoid incomplete type issues with unique_ptr
struct MEOSTemporalState {
    std::unique_ptr<MEOS::Meos> meosWrapper;
    std::vector<void*> temporalInstants;  // Will store MEOS::Meos::TemporalInstant*
    void* temporalSequence;               // Will store MEOS::Meos::TemporalSequence*
    bool isInitialized;
    size_t pointCount;
    
    MEOSTemporalState();
    ~MEOSTemporalState();
};

namespace NES
{

/**
 * @brief Aggregation function that collects spatial-temporal points into a MEOS TemporalSequence
 * and returns it as serialized VARSIZED data.
 * 
 * This function expects input records with:
 * - longitude (double)
 * - latitude (double) 
 * - timestamp (int64 or timestamp type)
 * 
 * Output: VARSIZED data containing serialized TemporalSequence in MF-JSON format
 * 
 * MEOS Integration Features:
 * - Uses actual MEOS TemporalInstant objects for each point
 * - Builds MEOS TemporalSequence from accumulated points
 * - Supports MF-JSON serialization format
 * - Handles MEOS temporal operations (intersection, distance, etc.)
 * - Provides fallback mechanisms for error handling
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
        PhysicalFunction timestampFunction);

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
    PhysicalFunction lonFunction;   // Function to extract longitude
    PhysicalFunction latFunction;   // Function to extract latitude  
    PhysicalFunction timestampFunction; // Function to extract timestamp

    // Legacy state structure for backward compatibility
    struct TemporalPointState {
        std::vector<double> longitudes;
        std::vector<double> latitudes;
        std::vector<int64_t> timestamps;
        size_t count;
    };

    // MEOS-integrated helper functions
    std::string serializeTemporalSequenceWithMEOS(MEOSTemporalState* state) const;
    std::string createSimpleTemporalSequenceString(MEOSTemporalState* state) const;
    void updateTemporalSequence(MEOSTemporalState* state);
    void accumulatePointSimple(MEOSTemporalState* state, double longitude, double latitude, int64_t timestamp);
    
    // Memory management functions
    static MEOSTemporalState* getMEOSStateFromMemory(nautilus::val<AggregationState*> aggregationState);

    // Legacy helper functions (kept for compatibility)
    std::string serializeTemporalSequence(const TemporalPointState& state) const;
    static TemporalPointState* getStateFromMemory(nautilus::val<AggregationState*> aggregationState);
};

} 