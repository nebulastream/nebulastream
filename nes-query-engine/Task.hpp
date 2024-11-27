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

#include <chrono>
#include <functional>
#include <memory>
#include <utility>
#include <variant>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Runtime/PinnedBuffer.hpp>
#include <Runtime/FloatingBuffer.hpp>
#include <ErrorHandling.hpp>
#include <ExecutableQueryPlan.hpp>
#include <Interfaces.hpp>
#include <RunningQueryPlan.hpp>

namespace NES::Runtime
{
class QueryCatalog;

class BaseTask
{
public:
    BaseTask() = default;
    BaseTask(QueryId queryId, std::function<void()> onCompletion, std::function<void(Exception)> onError)
        : queryId(queryId), onCompletion(std::move(onCompletion)), onError(std::move(onError))
    {
    }
    void complete() const
    {
        if (onCompletion)
        {
            onCompletion();
        }
    }
    void fail(Exception exception) const
    {
        if (onError)
        {
            onError(std::move(exception));
        }
    }

    QueryId queryId = INVALID<QueryId>;

private:
    std::chrono::high_resolution_clock::time_point creation = std::chrono::high_resolution_clock::now();
    std::function<void()> onCompletion = [] {};
    std::function<void(Exception)> onError = [](const Exception&) {};
};

struct WorkTask : BaseTask
{
    WorkTask(
        QueryId queryId,
        PipelineId pipelineId,
        std::weak_ptr<RunningQueryPlanNode> pipeline,
        std::variant<Memory::FloatingBuffer, Memory::RepinBufferFuture> buf,
        WorkEmitter::onComplete complete,
        WorkEmitter::onFailure failure)
        : BaseTask(queryId, std::move(complete), std::move(failure))
        , pipeline(std::move(pipeline))
        , pipelineId(pipelineId)
        , buf(buf)
    {
    }

    WorkTask() = default;
    std::weak_ptr<RunningQueryPlanNode> pipeline;
    PipelineId pipelineId = INVALID<PipelineId>;
    std::variant<Memory::FloatingBuffer, Memory::RepinBufferFuture> buf;
};

struct StartPipelineTask : BaseTask
{
    StartPipelineTask(
        QueryId queryId,
        PipelineId pipelineId,
        std::function<void()> onCompletion,
        std::function<void(Exception)> onError,
        std::weak_ptr<RunningQueryPlanNode> pipeline)
        : BaseTask(std::move(queryId), std::move(onCompletion), std::move(onError))
        , pipeline(std::move(pipeline))
        , pipelineId(std::move(pipelineId))
    {
    }

    StartPipelineTask() = default;
    std::weak_ptr<RunningQueryPlanNode> pipeline;
    PipelineId pipelineId = INVALID<PipelineId>;
};

struct StopPipelineTask : BaseTask
{
    explicit StopPipelineTask(
        QueryId queryId, std::unique_ptr<RunningQueryPlanNode> pipeline, WorkEmitter::onComplete complete, WorkEmitter::onFailure failure)
        : BaseTask(queryId, std::move(complete), std::move(failure)), pipeline(std::move(pipeline))
    {
    }
    StopPipelineTask() = default;
    std::unique_ptr<RunningQueryPlanNode> pipeline;
};


struct StopSourceTask : BaseTask
{
    StopSourceTask() = default;
    StopSourceTask(
        QueryId queryId, std::weak_ptr<RunningSource> target, WorkEmitter::onComplete onComplete, WorkEmitter::onFailure onFailure)
        : BaseTask(queryId, std::move(onComplete), std::move(onFailure)), target(std::move(target))
    {
    }
    std::weak_ptr<RunningSource> target;
};

struct FailSourceTask : BaseTask
{
    FailSourceTask() : exception("", 0) { }
    FailSourceTask(
        QueryId queryId,
        std::weak_ptr<RunningSource> target,
        Exception exception,
        WorkEmitter::onComplete onComplete,
        WorkEmitter::onFailure onFailure)
        : BaseTask(queryId, std::move(onComplete), std::move(onFailure)), target(std::move(target)), exception(std::move(exception))
    {
    }

    std::weak_ptr<RunningSource> target;
    Exception exception;
};

struct StopQueryTask : BaseTask
{
    StopQueryTask(
        QueryId queryId, std::weak_ptr<QueryCatalog> catalog, std::function<void()> onCompletion, std::function<void(Exception)> onError)
        : BaseTask(std::move(queryId), std::move(onCompletion), std::move(onError)), catalog(std::move(catalog))
    {
    }

    StopQueryTask() = default;
    std::weak_ptr<QueryCatalog> catalog;
};

struct StartQueryTask : BaseTask
{
    StartQueryTask(
        QueryId queryId,
        std::unique_ptr<ExecutableQueryPlan> queryPlan,
        std::weak_ptr<QueryCatalog> catalog,
        std::function<void()> onCompletion,
        std::function<void(Exception)> onError)
        : BaseTask(std::move(queryId), std::move(onCompletion), std::move(onError))
        , queryPlan(std::move(queryPlan))
        , catalog(std::move(catalog))
    {
    }

    StartQueryTask() = default;
    std::unique_ptr<ExecutableQueryPlan> queryPlan;
    std::weak_ptr<QueryCatalog> catalog;
};

struct PendingPipelineStopTask : BaseTask
{
    PendingPipelineStopTask(
        QueryId queryId,
        std::shared_ptr<RunningQueryPlanNode> pipeline,
        size_t attempts,
        std::function<void()> onCompletion,
        std::function<void(Exception)> onError)
        : BaseTask(std::move(queryId), std::move(onCompletion), std::move(onError)), attempts(attempts), pipeline(std::move(pipeline))
    {
    }

    size_t attempts;
    std::shared_ptr<RunningQueryPlanNode> pipeline;
};

using Task = std::variant<
    WorkTask,
    StopQueryTask,
    StartQueryTask,
    FailSourceTask,
    StopSourceTask,
    PendingPipelineStopTask,
    StopPipelineTask,
    StartPipelineTask>;

inline void completeTask(const Task& task)
{
    std::visit([](auto& specificTask) { return specificTask.complete(); }, task);
}

inline void failTask(const Task& task, Exception exception)
{
    std::visit([&](auto& specificTask) { return specificTask.fail(std::move(exception)); }, task);
}

}
