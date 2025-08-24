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
#include <tuple>
#include <utility>
#include <variant>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <absl/functional/any_invocable.h>
#include <cpptrace/from_current.hpp>
#include <EngineLogger.hpp>
#include <ErrorHandling.hpp>
#include <ExecutableQueryPlan.hpp>

namespace NES
{
/// Forward refs to not expose them via nes-query-engine
struct RunningQueryPlanNode;
class RunningSource;
class QueryCatalog;

/// It is possible to register callbacks which are invoked after task execution.
/// The complete callback is invoked regardless of the outcome of the task (even throwing an exception)
/// The success callback is invoked if the task succeeds. It is not invoked if the task throws an exception,
///     but could be skipped for tasks that are deemed unsuccessful, even without an exception.
/// The failure callback is invoked if the task fails with an exception.
/// Callbacks are move only, thus every callback is invoked at most once.
class TaskCallback
{
public:
    using onComplete = absl::AnyInvocable<void()>;
    using onSuccess = absl::AnyInvocable<void()>;
    using onFailure = absl::AnyInvocable<void(Exception)>;

    /// Helper structs that wrap the callbacks
    struct OnComplete
    {
        onComplete callback;

        explicit OnComplete(onComplete callback) : callback(std::move(callback)) { }
    };

    struct OnSuccess
    {
        onSuccess callback;

        explicit OnSuccess(onSuccess callback) : callback(std::move(callback)) { }
    };

    struct OnFailure
    {
        onFailure callback;

        explicit OnFailure(onFailure callback) : callback(std::move(callback)) { }
    };

    TaskCallback() = default;

    /// Variadic template constructor
    template <typename... Args>
    explicit TaskCallback(Args&&... args)
    {
        (processArgs(std::forward<Args>(args)), ...);
    }

    void callOnComplete()
    {
        if (onCompleteCallback)
        {
            ENGINE_LOG_DEBUG("TaskCallback::callOnComplete");
            onCompleteCallback();
        }
    }

    void callOnSuccess()
    {
        if (onSuccessCallback)
        {
            ENGINE_LOG_DEBUG("TaskCallback::callOnSuccess");
            onSuccessCallback();
        }
    }

    void callOnFailure(Exception exception)
    {
        if (onFailureCallback)
        {
            ENGINE_LOG_ERROR("TaskCallback::callOnFailure");
            onFailureCallback(std::move(exception));
        }
    }

    [[nodiscard]] std::tuple<onComplete, onFailure, onSuccess> take() &&
    {
        auto callbacks = std::make_tuple(std::move(onCompleteCallback), std::move(onFailureCallback), std::move(onSuccessCallback));
        this->onCompleteCallback = {};
        this->onSuccessCallback = {};
        this->onFailureCallback = {};
        return callbacks;
    }

private:
    /// Process OnComplete tag and callback
    void processArgs(OnComplete onComplete) { onCompleteCallback = std::move(onComplete.callback); }

    /// Process OnSuccess tag and callback
    void processArgs(OnSuccess onSuccess) { onSuccessCallback = std::move(onSuccess.callback); }

    /// Process OnFailure tag and callback
    void processArgs(OnFailure onFailure) { onFailureCallback = std::move(onFailure.callback); }

    onComplete onCompleteCallback;
    onSuccess onSuccessCallback;
    onFailure onFailureCallback;
};

class BaseTask
{
public:
    BaseTask() = default;

    BaseTask(QueryId queryId, TaskCallback callback) : queryId(queryId), callback(std::move(callback)) { }

    void complete() { callback.callOnComplete(); }

    void succeed() { callback.callOnSuccess(); }

    void fail(Exception exception) { callback.callOnFailure(std::move(exception)); }

    QueryId queryId = INVALID<QueryId>;
    TaskCallback callback;

private:
    /// No need for onSuccessCalled and onErrorCalled since TaskCallback handles this
};

struct WorkTask : BaseTask
{
    WorkTask(QueryId queryId, PipelineId pipelineId, std::weak_ptr<RunningQueryPlanNode> pipeline, TupleBuffer buf, TaskCallback callback)
        : BaseTask(queryId, std::move(callback)), pipeline(std::move(pipeline)), pipelineId(pipelineId), buf(std::move(buf))
    {
    }

    WorkTask() = default;
    std::weak_ptr<RunningQueryPlanNode> pipeline;
    PipelineId pipelineId = INVALID<PipelineId>;
    TupleBuffer buf;
};

struct StartPipelineTask : BaseTask
{
    StartPipelineTask(QueryId queryId, PipelineId pipelineId, TaskCallback callback, std::weak_ptr<RunningQueryPlanNode> pipeline)
        : BaseTask(std::move(queryId), std::move(callback)), pipeline(std::move(pipeline)), pipelineId(std::move(pipelineId))
    {
    }

    std::weak_ptr<RunningQueryPlanNode> pipeline;
    PipelineId pipelineId = INVALID<PipelineId>;
};

struct StopPipelineTask : BaseTask
{
    explicit StopPipelineTask(QueryId queryId, std::unique_ptr<RunningQueryPlanNode> pipeline, TaskCallback callback) noexcept;
    std::unique_ptr<RunningQueryPlanNode> pipeline;
};

struct StopSourceTask : BaseTask
{
    StopSourceTask() = default;

    StopSourceTask(QueryId queryId, std::weak_ptr<RunningSource> target, TaskCallback callback)
        : BaseTask(queryId, std::move(callback)), target(std::move(target))
    {
    }

    std::weak_ptr<RunningSource> target;
};

struct FailSourceTask : BaseTask
{
    FailSourceTask() : exception("", 0) { }

    FailSourceTask(QueryId queryId, std::weak_ptr<RunningSource> target, const Exception& exception, TaskCallback callback)
        : BaseTask(queryId, std::move(callback)), target(std::move(target)), exception(std::move(exception))
    {
    }

    std::weak_ptr<RunningSource> target;
    Exception exception;
};

struct StopQueryTask : BaseTask
{
    StopQueryTask(QueryId queryId, std::weak_ptr<QueryCatalog> catalog, TaskCallback callback)
        : BaseTask(std::move(queryId), std::move(callback)), catalog(std::move(catalog))
    {
    }

    std::weak_ptr<QueryCatalog> catalog;
};

struct StartQueryTask : BaseTask
{
    StartQueryTask(
        QueryId queryId, std::unique_ptr<ExecutableQueryPlan> queryPlan, std::weak_ptr<QueryCatalog> catalog, TaskCallback callback)
        : BaseTask(std::move(queryId), std::move(callback)), queryPlan(std::move(queryPlan)), catalog(std::move(catalog))
    {
    }

    std::unique_ptr<ExecutableQueryPlan> queryPlan;
    std::weak_ptr<QueryCatalog> catalog;
};

struct PendingPipelineStopTask : BaseTask
{
    PendingPipelineStopTask(QueryId queryId, std::shared_ptr<RunningQueryPlanNode> pipeline, size_t attempts, TaskCallback callback)
        : BaseTask(std::move(queryId), std::move(callback)), attempts(attempts), pipeline(std::move(pipeline))
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

inline void succeedTask(Task& task)
{
    std::visit([](auto& specificTask) { return specificTask.succeed(); }, task);
}

inline void completeTask(Task& task)
{
    std::visit([](auto& specificTask) { return specificTask.complete(); }, task);
}

inline void failTask(Task& task, Exception exception)
{
    std::visit([&](auto& specificTask) { return specificTask.fail(std::move(exception)); }, task);
}

void handleTask(const auto& handler, Task task)
{
    CPPTRACE_TRY
    {
        /// if handler returns true, the task has been successfully handled, which implies that the task has not been moved
        if (std::visit(handler, task))
        {
            succeedTask(task);
        }
    }
    CPPTRACE_CATCH(const Exception& exception)
    {
        failTask(task, exception);
    }
    CPPTRACE_CATCH_ALT(...)
    {
        NES_ERROR("Worker thread produced unknown exception during processing");
        tryLogCurrentException();
        failTask(task, wrapExternalException());
    }
    completeTask(task);
}

}
