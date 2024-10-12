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
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/WorkerContext.hpp>


#include <cstdint>
#include <memory>
#include <unordered_map>
#include <Execution/Operators/OperatorState.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/NESStrongTypeRef.hpp>

namespace NES::Runtime::Execution
{
using namespace Nautilus;
class RecordBuffer;

namespace Operators
{
class Operator;
class OperatorState;
} /// namespace Operators

/**
 * The execution context manages state of operators within a pipeline and provides access to some global functionality.
 * We differentiate between local and global operator state.
 * Local operator state lives throughout one pipeline invocation. It gets initialized in the open call and cleared in the close call.
 * Global operator state lives throughout the whole existence of the pipeline. It gets initialized in the setup call and cleared in the terminate call.
 */
class ExecutionContext final
{
public:
    ExecutionContext(const nautilus::val<WorkerContext*>& workerContext, const nautilus::val<PipelineExecutionContext*>& pipelineContext);

    /**
     * @brief Set local operator state that keeps state in a single pipeline invocation.
     * @param op reference to the operator to identify the state.
     * @param state operator state.
     */
    void setLocalOperatorState(const Operators::Operator* op, std::unique_ptr<Operators::OperatorState> state);
    Operators::OperatorState* getLocalState(const Operators::Operator* op);

    nautilus::val<OperatorHandler*> getGlobalOperatorHandler(uint64_t handlerIndex);
    nautilus::val<WorkerThreadId> getWorkerThreadId();
    nautilus::val<Memory::TupleBuffer*> allocateBuffer();

    /// Emit a record buffer to the next pipeline or sink
    void emitBuffer(const RecordBuffer& buffer);

    const nautilus::val<PipelineExecutionContext*>& getPipelineContext() const;
    const nautilus::val<WorkerContext*>& getWorkerContext() const;

    /// Returns the current origin id. This is set in the scan.
    const nautilus::val<uint64_t>& getOriginId() const;
    void setOriginId(const nautilus::val<uint64_t>& origin);


    /// Returns the current origin id. This is set in the scan.
    const nautilus::val<uint64_t>& getWatermarkTs() const;
    void setWatermarkTs(const nautilus::val<uint64_t>& uint64_t);

    /// Returns the current sequence number id. This is set in the scan.
    const nautilus::val<uint64_t>& getSequenceNumber() const;
    void setSequenceNumber(const nautilus::val<uint64_t>& sequenceNumber);

    /// Returns the current chunk number. This is set in the scan.
    const nautilus::val<uint64_t>& getChunkNumber() const;
    void setChunkNumber(const nautilus::val<uint64_t>& chunkNumber);
    const nautilus::val<bool>& getLastChunk() const;

    /// Removes the sequence state for the current <OrigindId, uint64_t>
    void removeSequenceState() const;

    /// Checks if all chunks have been seen
    nautilus::val<bool> isLastChunk() const;

    /// Returns the next chunk number for the emitted sequence numbers
    nautilus::val<uint64_t> getNextChunkNr() const;
    void setLastChunk(const nautilus::val<bool>& isLastChunk);


    /// Returns the current time stamp ts. This is set by a time function
    const nautilus::val<uint64_t>& getCurrentTs() const;
    void setCurrentTs(const nautilus::val<uint64_t>& ts);

private:
    std::unordered_map<const Operators::Operator*, std::unique_ptr<Operators::OperatorState>> localStateMap;
    nautilus::val<WorkerContext*> workerContext;
    nautilus::val<PipelineExecutionContext*> pipelineContext;
    nautilus::val<uint64_t> origin;
    nautilus::val<uint64_t> watermarkTs;
    nautilus::val<uint64_t> currentTs;
    nautilus::val<uint64_t> sequenceNumber;
    nautilus::val<uint64_t> chunkNumber;
    nautilus::val<bool> lastChunk;
};

}
