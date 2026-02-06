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
#include <memory>
#include <tuple>
#include <utility>
#include <variant>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TypeTraits.hpp>
#include <absl/functional/any_invocable.h>
#include <cpptrace/from_current.hpp>
#include <ErrorHandling.hpp>
#include <ExecutableQueryPlan.hpp>
#include <QueryId.hpp>

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

        explicit OnComplete(onComplete callback);
    };

    struct OnSuccess
    {
        onSuccess callback;

        explicit OnSuccess(onSuccess callback);
    };

    struct OnFailure
    {
        onFailure callback;

        explicit OnFailure(onFailure callback);
    };

    TaskCallback() = default;

    /// Variadic template constructor
    template <typename... Args>
    explicit TaskCallback(Args&&... args)
    {
        static_assert(UniqueTypesIgnoringCVRef<Args...>, "Cannot use the same callback multiple times");
        (processArgs(std::forward<Args>(args)), ...);
    }

    void callOnComplete();

    void callOnSuccess();

    void callOnFailure(Exception exception);

    [[nodiscard]] std::tuple<OnComplete, OnFailure, OnSuccess> take() &&;

private:
    /// Process OnComplete tag and callback
    void processArgs(OnComplete onComplete);

    /// Process OnSuccess tag and callback
    void processArgs(OnSuccess onSuccess);

    /// Process OnFailure tag and callback
    void processArgs(OnFailure onFailure);

    onComplete onCompleteCallback;
    onSuccess onSuccessCallback;
    onFailure onFailureCallback;
};

class BaseTask
{
public:
    BaseTask() = default;

    BaseTask(QueryId queryId, TaskCallback callback);

    void complete();

    void succeed();

    void fail(Exception exception);

    QueryId queryId;
    TaskCallback callback;

private:
    /// No need for onSuccessCalled and onErrorCalled since TaskCallback handles this
};

struct WorkTask : BaseTask
{
    WorkTask(QueryId queryId, PipelineId pipelineId, std::weak_ptr<RunningQueryPlanNode> pipeline, TupleBuffer buf, TaskCallback callback);

    WorkTask() = default;
    std::weak_ptr<RunningQueryPlanNode> pipeline;
    PipelineId pipelineId = INVALID<PipelineId>;
    TupleBuffer buf;
};

struct StartPipelineTask : BaseTask
{
    StartPipelineTask(QueryId queryId, PipelineId pipelineId, TaskCallback callback, std::weak_ptr<RunningQueryPlanNode> pipeline);

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

    StopSourceTask(QueryId queryId, std::weak_ptr<RunningSource> target, size_t attempts, TaskCallback callback);

    size_t attempts;
    std::weak_ptr<RunningSource> target;
};

struct FailSourceTask : BaseTask
{
    FailSourceTask();

    FailSourceTask(QueryId queryId, std::weak_ptr<RunningSource> target, Exception exception, TaskCallback callback);

    std::weak_ptr<RunningSource> target;
    std::unique_ptr<Exception> exception;
};

struct StopQueryTask : BaseTask
{
    StopQueryTask(QueryId queryId, std::weak_ptr<QueryCatalog> catalog, TaskCallback callback);

    std::weak_ptr<QueryCatalog> catalog;
};

struct StartQueryTask : BaseTask
{
    StartQueryTask(
        QueryId queryId, std::unique_ptr<ExecutableQueryPlan> queryPlan, std::weak_ptr<QueryCatalog> catalog, TaskCallback callback);

    std::unique_ptr<ExecutableQueryPlan> queryPlan;
    std::weak_ptr<QueryCatalog> catalog;
};

struct PendingPipelineStopTask : BaseTask
{
    PendingPipelineStopTask(QueryId queryId, std::shared_ptr<RunningQueryPlanNode> pipeline, size_t attempts, TaskCallback callback);

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

void succeedTask(Task& task);

void completeTask(Task& task);

void failTask(Task& task, Exception exception);

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
