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
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES
{
class PipelineExecutionContext
{
public:
    /// Policy whether an emitted TupleBuffer is able to process immediately or
    /// needs to be dispatched as a new Task.
    enum class ContinuationPolicy : uint8_t
    {
        POSSIBLE, /// It is possible for the emitted tuple buffer to be processed immediately. This is not a guarantee that that will happen
        NEVER, /// The tuple buffer should never be processed immediately
        REPEAT /// Put the same task back into the task queue
    };

    virtual ~PipelineExecutionContext() = default;

    /// Returns success, if the buffer was emitted successfully.
    bool emitBuffer(const Memory::TupleBuffer& buffer) { return emitBuffer(buffer, ContinuationPolicy::POSSIBLE); };

    /// Please be aware of how you are setting the continuation policy, as this will/can lead to deadlocks and no progress in our system.
    /// We advise to use ContinuationPolicy::POSSIBLE, as this will ensure no deadlock arising.
    /// Returns success, if the buffer was emitted successfully.
    virtual bool emitBuffer(const Memory::TupleBuffer&, ContinuationPolicy) = 0;
    virtual Memory::TupleBuffer allocateTupleBuffer() = 0;
    [[nodiscard]] virtual WorkerThreadId getId() const = 0;
    [[nodiscard]] virtual uint64_t getNumberOfWorkerThreads() const = 0;
    [[nodiscard]] virtual std::shared_ptr<Memory::AbstractBufferProvider> getBufferManager() const = 0;
    [[nodiscard]] virtual PipelineId getPipelineId() const = 0;

    /// TODO #30 Remove OperatorHandler from the pipeline execution context
    virtual std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& getOperatorHandlers() = 0;
    virtual void setOperatorHandlers(std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>&) = 0;

    bool formattingTask = false;
};
}
