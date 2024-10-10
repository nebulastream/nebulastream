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
}

/// The execution context manages state of operators within a pipeline and provides access to some global functionality.
/// We differentiate between local and global operator state.
/// Local operator state lives throughout one pipeline invocation. It gets initialized in the open call and cleared in the close call.
/// Global operator state lives throughout the whole existence of the pipeline. It gets initialized in the setup call and cleared in the terminate call.
struct ExecutionContext final
{
    ExecutionContext(const nautilus::val<int8_t*>& workerContext, const nautilus::val<int8_t*>& pipelineContext);

    void setLocalOperatorState(const Operators::Operator* op, std::unique_ptr<Operators::OperatorState> state);
    Operators::OperatorState* getLocalState(const Operators::Operator* op);

    nautilus::val<int8_t*> getGlobalOperatorHandler(uint64_t handlerIndex);
    nautilus::val<WorkerThreadId> getWorkerThreadId();
    nautilus::val<int8_t*> allocateBuffer();

    /// Emit a record buffer to the next pipeline or sink
    void emitBuffer(const RecordBuffer& rb);

    /// Removes the sequence state for the current <OrigindId, uint64_t>
    void removeSequenceState() const;

    /// Checks if all chunks have been seen
    nautilus::val<bool> isLastChunk() const;

    /// Returns the next chunk number for the emitted sequence numbers
    nautilus::val<uint64_t> getNextChunkNr() const;

    std::unordered_map<const Operators::Operator*, std::unique_ptr<Operators::OperatorState>> localStateMap;
    const nautilus::val<int8_t*> workerContext;
    const nautilus::val<int8_t*> pipelineContext;
    nautilus::val<uint64_t> originId; /// Stores the current origin id of the incoming tuple buffer. This is set in the scan.
    nautilus::val<uint64_t> watermarkTs; /// Stores the watermark timestamp of the incoming tuple buffer. This is set in the scan.
    nautilus::val<uint64_t> currentTs; /// Stores the current time stamp. This is set by a time function
    nautilus::val<uint64_t> sequenceNumber; /// Stores the sequence number id of the incoming tuple buffer. This is set in the scan.
    nautilus::val<uint64_t> chunkNumber; /// Stores the chunk number of the incoming tuple buffer. This is set in the scan.
    nautilus::val<bool> lastChunk;
};

}
