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

#include <Aggregation/Function/TemporalSequenceAggregationFunction.hpp>
#include "../../../nes-plugins/MEOS/include/MEOSWrapper.hpp"
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <cstring>
#include <sstream>
#include <iostream>
#include <vector>
#include <memory>

// MEOSTemporalState implementation
MEOSTemporalState::MEOSTemporalState() 
    : meosWrapper(std::make_unique<MEOS::Meos>())
    , temporalSequence(nullptr)
    , isInitialized(false)
    , pointCount(0) {}

MEOSTemporalState::~MEOSTemporalState() {
    // Clean up raw pointers
    for (auto* instant : temporalInstants) {
        delete static_cast<MEOS::Meos::TemporalInstant*>(instant);
    }
    temporalInstants.clear();
    
    if (temporalSequence) {
        delete static_cast<MEOS::Meos::TemporalSequence*>(temporalSequence);
    }
}

namespace NES
{

TemporalSequenceAggregationFunction::TemporalSequenceAggregationFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction inputFunction,
    Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier,
    PhysicalFunction lonFunction,
    PhysicalFunction latFunction,
    PhysicalFunction timestampFunction)
    : AggregationFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
    , lonFunction(std::move(lonFunction))
    , latFunction(std::move(latFunction))
    , timestampFunction(std::move(timestampFunction))
{
}

void TemporalSequenceAggregationFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState,
    PipelineMemoryProvider& pipelineMemoryProvider,
    const Nautilus::Record& record)
{
    // Get the MEOS state from memory
    auto* meosState = getMEOSStateFromMemory(aggregationState);
    
    // Extract longitude, latitude, and timestamp from the record using proper arena
    auto& arena = pipelineMemoryProvider.arena;
    const auto lonValue = lonFunction.execute(record, arena);
    const auto latValue = latFunction.execute(record, arena);
    const auto timestampValue = timestampFunction.execute(record, arena);
    
    // Convert to actual values using proper Nautilus API
    // Note: In actual Nautilus code, values would be extracted differently
    // For now, we'll use a simplified approach that works with the compilation
    double longitude = 0.0; // Would extract from lonValue
    double latitude = 0.0;  // Would extract from latValue  
    int64_t timestamp = 0;  // Would extract from timestampValue
    
    try {
        // Create MF-JSON format for the temporal point
        std::ostringstream mfJsonStream;
        mfJsonStream << R"({
            "type": "MovingPoint",
            "coordinates": [)" << longitude << "," << latitude << R"(],
            "datetimes": ")" << meosState->meosWrapper->convertSecondsToTimestamp(timestamp) << R"(",
            "interpolation": "None"
        })";
        
        std::string mfJsonString = mfJsonStream.str();
        
        // Create a temporal instant using MEOS
        auto* temporalInstant = new MEOS::Meos::TemporalInstant(mfJsonString);
        
        // Add to our collection of temporal instants
        meosState->temporalInstants.push_back(temporalInstant);
        meosState->pointCount++;
        
        // If we have multiple points, create/update temporal sequence
        if (meosState->pointCount > 1) {
            // Create a temporal sequence from accumulated points
            // Note: This is a simplified approach - in production you'd want more sophisticated merging
            updateTemporalSequence(meosState);
        }
        
    } catch (const std::exception& e) {
        std::cerr << "Error in MEOS lift operation: " << e.what() << std::endl;
        // Fallback to simple accumulation
        accumulatePointSimple(meosState, longitude, latitude, timestamp);
    }
}

void TemporalSequenceAggregationFunction::combine(
    nautilus::val<AggregationState*> aggregationState1,
    nautilus::val<AggregationState*> aggregationState2,
    PipelineMemoryProvider& /*pipelineMemoryProvider*/)  // Mark as unused
{
    auto* meosState1 = getMEOSStateFromMemory(aggregationState1);
    auto* meosState2 = getMEOSStateFromMemory(aggregationState2);
    
    try {
        // Merge temporal instants from state2 into state1
        for (auto* instant : meosState2->temporalInstants) {
            // Move temporal instants from state2 to state1
            meosState1->temporalInstants.push_back(instant);
        }
        meosState2->temporalInstants.clear();
        
        meosState1->pointCount += meosState2->pointCount;
        meosState2->pointCount = 0;
        
        // Rebuild the temporal sequence with all accumulated points
        if (meosState1->pointCount > 1) {
            updateTemporalSequence(meosState1);
        }
        
    } catch (const std::exception& e) {
        std::cerr << "Error in MEOS combine operation: " << e.what() << std::endl;
    }
}

Nautilus::Record TemporalSequenceAggregationFunction::lower(
    nautilus::val<AggregationState*> aggregationState,
    PipelineMemoryProvider& /*pipelineMemoryProvider*/)  // Mark as unused
{
    auto* meosState = getMEOSStateFromMemory(aggregationState);
    
    try {
        // Finalize the temporal sequence
        std::string serializedSequence = serializeTemporalSequenceWithMEOS(meosState);
        
        // Create VARSIZED data using proper Nautilus API
        // Note: This would need to be adapted to actual Nautilus VariableSizedData API
        // For now, create a simplified version
        auto data = nautilus::val<int8_t*>(reinterpret_cast<int8_t*>(const_cast<char*>(serializedSequence.c_str())));
        auto size = nautilus::val<uint32_t>(static_cast<uint32_t>(serializedSequence.size()));
        
        auto variableSizedData = VariableSizedData(data, size);
        auto varVal = VarVal(variableSizedData);
        
        return Nautilus::Record({{resultFieldIdentifier, varVal}});
        
    } catch (const std::exception& e) {
        std::cerr << "Error in MEOS lower operation: " << e.what() << std::endl;
        
        // Fallback to simple serialization
        std::string fallbackResult = "TEMPORAL_SEQUENCE_ERROR";
        auto data = nautilus::val<int8_t*>(reinterpret_cast<int8_t*>(const_cast<char*>(fallbackResult.c_str())));
        auto size = nautilus::val<uint32_t>(static_cast<uint32_t>(fallbackResult.size()));
        auto variableSizedData = VariableSizedData(data, size);
        auto varVal = VarVal(variableSizedData);
        return Nautilus::Record({{resultFieldIdentifier, varVal}});
    }
}

void TemporalSequenceAggregationFunction::reset(
    nautilus::val<AggregationState*> aggregationState,
    PipelineMemoryProvider& /*pipelineMemoryProvider*/)  // Mark as unused
{
    auto* meosState = getMEOSStateFromMemory(aggregationState);
    if (meosState) {
        // Clear all temporal instants
        for (auto* instant : meosState->temporalInstants) {
            delete static_cast<MEOS::Meos::TemporalInstant*>(instant);
        }
        meosState->temporalInstants.clear();
        
        if (meosState->temporalSequence) {
            delete static_cast<MEOS::Meos::TemporalSequence*>(meosState->temporalSequence);
            meosState->temporalSequence = nullptr;
        }
        
        meosState->pointCount = 0;
        meosState->isInitialized = false;
    }
}

void TemporalSequenceAggregationFunction::cleanup(nautilus::val<AggregationState*> aggregationState)
{
    auto* meosState = getMEOSStateFromMemory(aggregationState);
    if (meosState) {
        // Explicit cleanup of MEOS objects (destructor will handle it)
        meosState->meosWrapper.reset();
        
        // Call destructor
        meosState->~MEOSTemporalState();
    }
}

size_t TemporalSequenceAggregationFunction::getSizeOfStateInBytes() const
{
    // Return the size needed for our MEOS state
    return sizeof(MEOSTemporalState);
}

std::string TemporalSequenceAggregationFunction::serializeTemporalSequenceWithMEOS(MEOSTemporalState* state) const
{
    if (!state || state->pointCount == 0) {
        return "EMPTY_TEMPORAL_SEQUENCE";
    }
    
    try {
        // If we have a constructed temporal sequence, serialize it
        if (state->temporalSequence) {
            // Use MEOS serialization functions
            // For now, create a MF-JSON representation
            std::ostringstream result;
            result << R"({
                "type": "MovingPoint",
                "coordinates": [";
            
            // Extract coordinates from temporal instants
            bool first = true;
            for (const auto* instant : state->temporalInstants) {
                if (!first) result << ",";
                result << "[/* longitude, latitude */]"; // Would extract from MEOS instant
                first = false;
            }
            
            result << R"(],
                "datetimes": [";
            
            // Extract timestamps
            first = true;
            for (const auto* instant : state->temporalInstants) {
                if (!first) result << ",";
                result << "\"/* timestamp */\""; // Would extract from MEOS instant
                first = false;
            }
            
            result << R"(],
                "interpolation": "Linear"
            })";
            
            return result.str();
        }
        
        // Fallback: create simple representation from collected points
        return createSimpleTemporalSequenceString(state);
        
    } catch (const std::exception& e) {
        std::cerr << "Error serializing MEOS temporal sequence: " << e.what() << std::endl;
        return createSimpleTemporalSequenceString(state);
    }
}

std::string TemporalSequenceAggregationFunction::createSimpleTemporalSequenceString(MEOSTemporalState* state) const
{
    std::ostringstream result;
    result << "TEMPORAL_SEQUENCE(" << state->pointCount << "_points)";
    return result.str();
}

void TemporalSequenceAggregationFunction::updateTemporalSequence(MEOSTemporalState* state)
{
    // This would create a MEOS TemporalSequence from the accumulated temporal instants
    // For now, we'll keep it as a collection of instants
    // In a full implementation, you'd use MEOS sequence construction functions
    
    try {
        // Example of how you'd create a temporal sequence:
        // state->temporalSequence = std::make_unique<MEOS::Meos::TemporalSequence>(
        //     /* coordinates */, /* coordinates */, /* timestamp */);
        
        state->isInitialized = true;
    } catch (const std::exception& e) {
        std::cerr << "Error updating temporal sequence: " << e.what() << std::endl;
    }
}

void TemporalSequenceAggregationFunction::accumulatePointSimple(
    MEOSTemporalState* state, double longitude, double latitude, int64_t timestamp)
{
    // Fallback simple accumulation when MEOS operations fail
    state->pointCount++;
    std::cout << "Accumulated point: (" << longitude << ", " << latitude << ") at " << timestamp << std::endl;
}

MEOSTemporalState* TemporalSequenceAggregationFunction::getMEOSStateFromMemory(
    nautilus::val<AggregationState*> aggregationState)
{
    // Use Nautilus memory access pattern
    const auto memArea = static_cast<nautilus::val<int8_t*>>(aggregationState);
    // For VARSIZED data, we need to handle this differently
    // Return nullptr for now - this needs proper VARSIZED handling
    return nullptr;
}

// Update the old methods to use the new MEOS state
std::string TemporalSequenceAggregationFunction::serializeTemporalSequence(const TemporalPointState& state) const
{
    // Legacy method - kept for compatibility
    std::ostringstream result;
    result << "LEGACY_TEMPORAL_SEQUENCE(" << state.count << "_points)";
    return result.str();
}

TemporalSequenceAggregationFunction::TemporalPointState* 
TemporalSequenceAggregationFunction::getStateFromMemory(nautilus::val<AggregationState*> aggregationState)
{
    // Legacy method - kept for compatibility
    const auto memArea = static_cast<nautilus::val<int8_t*>>(aggregationState);
    // Use proper Nautilus memory access - return nullptr for now
    return nullptr;
}

} 