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

#include <Task.hpp>

#include <cstddef>
#include <memory>
#include <tuple>
#include <utility>
#include <variant>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <EngineLogger.hpp>
#include <ErrorHandling.hpp>
#include <ExecutableQueryPlan.hpp>
#include <RunningQueryPlan.hpp>

namespace NES
{


TaskCallback::OnComplete::OnComplete(onComplete callback) : callback(std::move(callback))
{
}

TaskCallback::OnSuccess::OnSuccess(onSuccess callback) : callback(std::move(callback))
{
}

TaskCallback::OnFailure::OnFailure(onFailure callback) : callback(std::move(callback))
{
}

void TaskCallback::callOnComplete()
{
    if (onCompleteCallback)
    {
        ENGINE_LOG_DEBUG("TaskCallback::callOnComplete");
        onCompleteCallback();
    }
}

void TaskCallback::callOnSuccess()
{
    if (onSuccessCallback)
    {
        ENGINE_LOG_DEBUG("TaskCallback::callOnSuccess");
        onSuccessCallback();
    }
}

void TaskCallback::callOnFailure(Exception exception)
{
    if (onFailureCallback)
    {
        ENGINE_LOG_ERROR("TaskCallback::callOnFailure");
        onFailureCallback(std::move(exception));
    }
}

std::tuple<TaskCallback::OnComplete, TaskCallback::OnFailure, TaskCallback::OnSuccess> TaskCallback::take() &&
{
    auto callbacks = std::make_tuple(
        OnComplete{std::move(onCompleteCallback)}, OnFailure{std::move(onFailureCallback)}, OnSuccess{std::move(onSuccessCallback)});
    this->onCompleteCallback = {};
    this->onSuccessCallback = {};
    this->onFailureCallback = {};
    return callbacks;
}

void TaskCallback::processArgs(OnComplete onComplete)
{
    onCompleteCallback = std::move(onComplete.callback);
}

void TaskCallback::processArgs(OnSuccess onSuccess)
{
    onSuccessCallback = std::move(onSuccess.callback);
}

void TaskCallback::processArgs(OnFailure onFailure)
{
    onFailureCallback = std::move(onFailure.callback);
}

BaseTask::BaseTask(LocalQueryId queryId, TaskCallback callback) : queryId(queryId), callback(std::move(callback))
{
}

void BaseTask::complete()
{
    callback.callOnComplete();
}

void BaseTask::succeed()
{
    callback.callOnSuccess();
}

void BaseTask::fail(Exception exception)
{
    callback.callOnFailure(std::move(exception));
}

WorkTask::WorkTask(
    LocalQueryId queryId, PipelineId pipelineId, std::weak_ptr<RunningQueryPlanNode> pipeline, TupleBuffer buf, TaskCallback callback)
    : BaseTask(queryId, std::move(callback)), pipeline(std::move(pipeline)), pipelineId(pipelineId), buf(std::move(buf))
{
}

StartPipelineTask::StartPipelineTask(
    LocalQueryId queryId, PipelineId pipelineId, TaskCallback callback, std::weak_ptr<RunningQueryPlanNode> pipeline)
    : BaseTask(std::move(queryId), std::move(callback)), pipeline(std::move(pipeline)), pipelineId(std::move(pipelineId))
{
}

StopPipelineTask::StopPipelineTask(LocalQueryId queryId, std::unique_ptr<RunningQueryPlanNode> pipeline, TaskCallback callback) noexcept
    : BaseTask(queryId, std::move(callback)), pipeline(std::move(pipeline))
{
}

StopSourceTask::StopSourceTask(LocalQueryId queryId, std::weak_ptr<RunningSource> target, size_t attempts, TaskCallback callback)
    : BaseTask(queryId, std::move(callback)), attempts(attempts), target(std::move(target))
{
}

FailSourceTask::FailSourceTask() : exception(nullptr)
{
}

FailSourceTask::FailSourceTask(LocalQueryId queryId, std::weak_ptr<RunningSource> target, Exception exception, TaskCallback callback)
    : BaseTask(queryId, std::move(callback)), target(std::move(target)), exception(std::make_unique<Exception>(std::move(exception)))
{
}

StopQueryTask::StopQueryTask(LocalQueryId queryId, std::weak_ptr<QueryCatalog> catalog, TaskCallback callback)
    : BaseTask(std::move(queryId), std::move(callback)), catalog(std::move(catalog))
{
}

StartQueryTask::StartQueryTask(
    LocalQueryId queryId, std::unique_ptr<ExecutableQueryPlan> queryPlan, std::weak_ptr<QueryCatalog> catalog, TaskCallback callback)
    : BaseTask(std::move(queryId), std::move(callback)), queryPlan(std::move(queryPlan)), catalog(std::move(catalog))
{
}

PendingPipelineStopTask::PendingPipelineStopTask(
    LocalQueryId queryId, std::shared_ptr<RunningQueryPlanNode> pipeline, size_t attempts, TaskCallback callback)
    : BaseTask(std::move(queryId), std::move(callback)), attempts(attempts), pipeline(std::move(pipeline))
{
}

void succeedTask(Task& task)
{
    std::visit([](auto& specificTask) { return specificTask.succeed(); }, task);
}

void completeTask(Task& task)
{
    std::visit([](auto& specificTask) { return specificTask.complete(); }, task);
}

void failTask(Task& task, Exception exception)
{
    std::visit([&exception](auto& specificTask) { return specificTask.fail(std::move(exception)); }, task);
}

}
