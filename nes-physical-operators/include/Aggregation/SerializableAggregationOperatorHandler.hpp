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

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/DataStructures/OffsetBasedHashMap.hpp>
#include <Nautilus/DataStructures/OffsetHashMapWrapper.hpp>
#include <Nautilus/State/Reflection/StateDefinitions.hpp>
#include <Nautilus/State/Serialization/StateSerializer.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <WindowBasedOperatorHandler.hpp>

namespace NES
{

using TupleBuffer = Memory::TupleBuffer;
using AbstractBufferProvider = Memory::AbstractBufferProvider;

/// This struct models the information for a serializable aggregation window trigger
/// Now uses the same HashMap interface pattern as EmittedAggregationWindow
struct EmittedSerializableAggregationWindow
{
    WindowInfo windowInfo;
    Nautilus::Interface::HashMap* finalHashMap;
    uint64_t numberOfHashMaps;
    Nautilus::Interface::HashMap** hashMaps; // Pointer to array of hashmap interface pointers
};

class SerializableAggregationOperatorHandler final : public WindowBasedOperatorHandler
{
public:
    explicit SerializableAggregationOperatorHandler(
        const std::vector<OriginId>& inputOrigins,
        OriginId outputOriginId,
        std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore);

    explicit SerializableAggregationOperatorHandler(
        const std::vector<OriginId>& inputOrigins,
        OriginId outputOriginId,
        std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore,
        const State::AggregationState& initialState);

    // Serialization support - the key feature of this handler
    TupleBuffer serialize(AbstractBufferProvider* bufferProvider) const;
    
    // Capture current live slice/window state into state_ prior to serialization
    void captureState();

    // Rehydrate slice store and slice-local hash maps from state_ after deserialization
    void rehydrateFromState();
    
    static std::shared_ptr<SerializableAggregationOperatorHandler> 
    deserialize(const TupleBuffer& buffer, 
               const std::vector<OriginId>& inputOrigins,
               OriginId outputOriginId,
               std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore);

    // Access for PhysicalOperators - provides offset-based hashmaps via wrapper
    // This is for direct wrapper access when needed by build operator
    DataStructures::OffsetHashMapWrapper* getOffsetHashMapWrapper(WorkerThreadId threadId);
    const DataStructures::OffsetHashMapWrapper* getOffsetHashMapWrapper(WorkerThreadId threadId) const;
    
    // State access
    const State::AggregationState& getState() const { return state_; }
    State::AggregationState& getState() { return state_; }
    
    // WindowBasedOperatorHandler interface implementation
    [[nodiscard]] std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>
    getCreateNewSlicesFunction(const CreateNewSlicesArguments& newSlicesArguments) const override;

protected:
    void triggerSlices(
        const std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>& slicesAndWindowInfo,
        PipelineExecutionContext* pipelineCtx) override;


private:
    void initializeState();
    void initializeHashMaps();
    void initializeHashMapState(State::AggregationState::HashMap& hashMapState);
    std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>
    createOffsetBasedSlices(const CreateNewSlicesArguments& args) const;

    State::AggregationState state_;
    mutable std::vector<std::unique_ptr<DataStructures::OffsetHashMapWrapper>> hashMapWrappers_;
    bool initialized_{false};
};

} // namespace NES
