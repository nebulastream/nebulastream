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
#include <functional>
#include <memory>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <QueryId.hpp>
#include <Task.hpp>

namespace NES
{
struct RunningQueryPlanNode;
class RunningSource;

class QueryLifetimeController
{
public:
    virtual ~QueryLifetimeController() = default;
    virtual void initializeSourceFailure(QueryId, OriginId, std::weak_ptr<RunningSource>, Exception exception) = 0;
    virtual void initializeSourceStop(QueryId, OriginId, std::weak_ptr<RunningSource>) = 0;
};

class WorkEmitter
{
public:
    virtual ~WorkEmitter() = default;

    /// Creates and submits a new Task targeting the `target` node.
    /// If the `potentiallyProcessTheWorkInPlace` parameter is set, the task may be executed in place if it cannot be submitted to the task queue.
    /// This function may return false if the `potentiallyProcessTheWorkInPlace` parameter is disabled and the task cannot be submitted.
    virtual bool emitWork(
        QueryId,
        const std::shared_ptr<RunningQueryPlanNode>& target,
        TupleBuffer,
        TaskCallback,
        PipelineExecutionContext::ContinuationPolicy continuationPolicy)
        = 0;

    /// Batched-admission variant of emitWork for the source-emit path (source/io threads only): builds the WorkTask
    /// exactly like emitWork and writes it into the admission queue, but does NOT publish it (no worker can dequeue it
    /// yet). Call publishAdmittedWork(N) once after queueing a batch of N to release all N permits in a single burst,
    /// waking up to N parked workers at once. pendingTasks is incremented at build time here as in emitWork, so every
    /// task built MUST eventually be published (the source's batch accumulator flushes on batch-full, on end-of-stream,
    /// and unconditionally in its destructor) -- an unpublished task would strand pendingTasks and hang a graceful stop.
    /// Returns false only if canceled.
    virtual bool emitWorkUnpublished(QueryId, const std::shared_ptr<RunningQueryPlanNode>& target, TupleBuffer, TaskCallback) = 0;

    /// Release the permits for `count` tasks previously queued via emitWorkUnpublished (one burst notify).
    virtual void publishAdmittedWork(size_t count) = 0;

    virtual void emitPipelineStart(QueryId, const std::shared_ptr<RunningQueryPlanNode>&, TaskCallback) = 0;
    virtual void emitPendingPipelineStop(QueryId, std::shared_ptr<RunningQueryPlanNode>, TaskCallback) = 0;
    virtual void emitPipelineStop(QueryId, std::unique_ptr<RunningQueryPlanNode>, TaskCallback) = 0;
};
}
