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
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/NESStrongTypeRef.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Nautilus/Interface/TimestampRef.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Time/Timestamp.hpp>
#include <nautilus/val_concepts.hpp>
#include <nautilus/val_ptr.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <OperatorState.hpp>
#include <PipelineExecutionContext.hpp>
#include <function.hpp>
#include <val.hpp>

namespace NES
{

struct PipelineMemoryProvider
{
    explicit PipelineMemoryProvider(const nautilus::val<Arena*>& arena, const nautilus::val<AbstractBufferProvider*>& bufferProvider)
        : arena(arena), bufferProvider(bufferProvider)
    {
    }

    explicit PipelineMemoryProvider(ArenaRef arena, const nautilus::val<AbstractBufferProvider*>& bufferProvider)
        : arena(std::move(arena)), bufferProvider(bufferProvider)
    {
    }

    ArenaRef arena;
    nautilus::val<AbstractBufferProvider*> bufferProvider;
};

enum class OpenReturnState : uint8_t
{
    CONTINUE,
    REPEAT,
};

struct ExecutionContext final
{
    explicit ExecutionContext(const nautilus::val<PipelineExecutionContext*>& pipelineContext, const nautilus::val<Arena*>& arena);

    void setLocalOperatorState(OperatorId operatorId, std::unique_ptr<OperatorState> state);
    OperatorState* getLocalState(OperatorId operatorId);

    [[nodiscard]] nautilus::val<OperatorHandler*> getGlobalOperatorHandler(OperatorHandlerId handlerIndex) const;
    [[nodiscard]] nautilus::val<TupleBuffer*> allocateBuffer() const;
    [[nodiscard]] nautilus::val<int8_t*> allocateMemory(const nautilus::val<size_t>& sizeInBytes);

    void emitBuffer(const RecordBuffer& buffer) const;

    void setOpenReturnState(OpenReturnState openReturnState);
    [[nodiscard]] OpenReturnState getOpenReturnState() const;

    const nautilus::val<PipelineExecutionContext*> pipelineContext;
    nautilus::val<WorkerThreadId> workerThreadId;
    PipelineMemoryProvider pipelineMemoryProvider;
    nautilus::val<OriginId> originId;
    nautilus::val<Timestamp> watermarkTs;
    nautilus::val<Timestamp> currentTs;
    nautilus::val<SequenceNumber> sequenceNumber;
    nautilus::val<ChunkNumber> chunkNumber;
    nautilus::val<bool> lastChunk;

private:
    std::unordered_map<OperatorId, std::unique_ptr<OperatorState>> localStateMap;
    OpenReturnState openReturnState{OpenReturnState::CONTINUE};
};

}
