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
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Nautilus/Interface/TimestampRef.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <nautilus/val_concepts.hpp>
#include <nautilus/val_ptr.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES::Runtime::Execution
{
using namespace Nautilus;

namespace Operators
{
class Operator;
class OperatorState;
}

/// The execution context provides access to functionality, such as emitting a record buffer to the next pipeline or sink as well
/// as access to operator states from the nautilus-runtime.
/// We differentiate between local and global operator state.
/// Local operator state lives throughout one pipeline invocation, i.e., over one tuple buffer. It gets initialized in the open call and cleared in the close call.
/// An example is to store the pointer to the current window in a local state so that the window can be accessed when processing the next tuple.
/// Global operator state lives throughout the whole existence of the pipeline. It gets initialized in the setup call and cleared in the terminate call.
/// As the pipeline exists as long as the query exists, the global operator state exists as long as the query runs.
/// An example is to store the windows of a window operator in the global state so that the windows can be accessed in the next pipeline invocation.
struct ExecutionContext final
{
    explicit ExecutionContext(const nautilus::val<PipelineExecutionContext*>& pipelineContext);

    void setLocalOperatorState(const Operators::Operator* op, std::unique_ptr<Operators::OperatorState> state);
    Operators::OperatorState* getLocalState(const Operators::Operator* op);

    [[nodiscard]] nautilus::val<OperatorHandler*> getGlobalOperatorHandler(uint64_t handlerIndex) const;
    [[nodiscard]] nautilus::val<WorkerThreadId> getWorkerThreadId() const;
    [[nodiscard]] nautilus::val<Memory::TupleBuffer*> allocateBuffer() const;
    const nautilus::val<PipelineExecutionContext*>& getPipelineContext() const;


    /// Emit a record buffer to the successor pipeline(s) or sink(s)
    void emitBuffer(const RecordBuffer& buffer) const;

    std::unordered_map<const Operators::Operator*, std::unique_ptr<Operators::OperatorState>> localStateMap;
    const nautilus::val<PipelineExecutionContext*> pipelineContext;
    nautilus::val<OriginId> originId; /// Stores the current origin id of the incoming tuple buffer. This is set in the scan.
    nautilus::val<Timestamp> watermarkTs; /// Stores the watermark timestamp of the incoming tuple buffer. This is set in the scan.
    nautilus::val<Timestamp> currentTs; /// Stores the current time stamp. This is set by a time function
    nautilus::val<SequenceNumber> sequenceNumber; /// Stores the sequence number id of the incoming tuple buffer. This is set in the scan.
    nautilus::val<ChunkNumber> chunkNumber; /// Stores the chunk number of the incoming tuple buffer. This is set in the scan.
    nautilus::val<bool> lastChunk;
};

}
