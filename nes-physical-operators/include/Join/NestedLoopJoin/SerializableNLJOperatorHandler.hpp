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
#include <Join/StreamJoinOperatorHandler.hpp>
#include <Nautilus/State/Reflection/StateDefinitions.hpp>
#include <Nautilus/State/Serialization/StateSerializer.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>

namespace NES {

using TupleBuffer = Memory::TupleBuffer;
using AbstractBufferProvider = Memory::AbstractBufferProvider;

class SerializableNLJOperatorHandler final : public StreamJoinOperatorHandler {
public:
    SerializableNLJOperatorHandler(
        const std::vector<OriginId>& inputOrigins,
        OriginId outputOriginId,
        std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore);

    SerializableNLJOperatorHandler(
        const std::vector<OriginId>& inputOrigins,
        OriginId outputOriginId,
        std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore,
        const State::JoinState& initialState);

    // State accessors
    const State::JoinState& getState() const { return state_; }
    State::JoinState& getState() { return state_; }

    // Serialization
    TupleBuffer serialize(AbstractBufferProvider* bufferProvider) const {
        return NES::Serialization::StateSerializer<State::JoinState>::serialize(state_, bufferProvider);
    }
    static std::shared_ptr<SerializableNLJOperatorHandler> deserialize(
        const TupleBuffer& buffer,
        const std::vector<OriginId>& inputOrigins,
        OriginId outputOriginId,
        std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore);

    // Slice creation (NLJ uses NLJSlice directly)
    [[nodiscard]] std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>
    getCreateNewSlicesFunction(const CreateNewSlicesArguments& newSlicesArguments) const override;

    // Snapshot/rehydrate hooks (to be implemented later)
    void captureState();
    void rehydrateFromState();

private:
    void emitSlicesToProbe(
        Slice& sliceLeft,
        Slice& sliceRight,
        const WindowInfo& windowInfo,
        const SequenceData& sequenceData,
        PipelineExecutionContext* pipelineCtx) override;
        
    State::JoinState state_{};
};

} // namespace NES
