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
        Memory::TupleBuffer,
        BaseTask::onComplete,
        BaseTask::onFailure,
        PipelineExecutionContext::ContinuationPolicy continuationPolicy)
        = 0;
    virtual void emitPipelineStart(QueryId, const std::shared_ptr<RunningQueryPlanNode>&, BaseTask::onComplete, BaseTask::onFailure) = 0;
    virtual void emitPendingPipelineStop(QueryId, std::shared_ptr<RunningQueryPlanNode>, BaseTask::onComplete, BaseTask::onFailure) = 0;
    virtual void emitPipelineStop(QueryId, std::unique_ptr<RunningQueryPlanNode>, BaseTask::onComplete, BaseTask::onFailure) = 0;
};
}
