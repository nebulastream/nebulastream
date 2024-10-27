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
#include <type_traits>
#include <utility>
#include <variant>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ErrorHandling.hpp>
#include <Interfaces.hpp>
#include <RunningQueryPlan.hpp>

namespace NES::Runtime
{
class OperationBase
{
public:
    OperationBase() = default;
    OperationBase(QueryId queryId, std::function<void()> on_completion, std::function<void(Exception)> on_error)
        : queryId(queryId), onCompletion(std::move(on_completion)), onError(std::move(on_error))
    {
    }
    void complete() const
    {
        if (onCompletion)
        {
            onCompletion();
        }
    }
    void fail(Exception e) const
    {
        if (onError)
        {
            onError(std::move(e));
        }
    }

    QueryId queryId = INVALID<QueryId>;

private:
    std::chrono::high_resolution_clock::time_point creation = std::chrono::high_resolution_clock::now();
    std::function<void()> onCompletion = [] {};
    std::function<void(Exception)> onError = [](Exception) {};
};

struct Task : OperationBase
{
    Task(
        QueryId queryId,
        std::weak_ptr<RunningQueryPlanNode> pipeline,
        Memory::TupleBuffer buf,
        WorkEmitter::onComplete complete = []() {},
        WorkEmitter::onFailure failure = [](Exception) {})
        : OperationBase(queryId, std::move(complete), std::move(failure)), pipeline(std::move(pipeline)), buf(std::move(buf))
    {
    }
    Task() = default;
    std::weak_ptr<RunningQueryPlanNode> pipeline;
    Memory::TupleBuffer buf;
};

struct SetupPipeline : OperationBase
{
    explicit SetupPipeline(
        QueryId queryId, std::weak_ptr<RunningQueryPlanNode> pipeline, WorkEmitter::onComplete complete, WorkEmitter::onFailure failure)
        : OperationBase(queryId, std::move(complete), std::move(failure)), pipeline(std::move(pipeline))
    {
    }
    SetupPipeline() = default;
    std::weak_ptr<RunningQueryPlanNode> pipeline;
};

struct StopPipeline : OperationBase
{
    explicit StopPipeline(
        QueryId queryId, std::unique_ptr<RunningQueryPlanNode> pipeline, WorkEmitter::onComplete complete, WorkEmitter::onFailure failure)
        : OperationBase(queryId, std::move(complete), std::move(failure)), pipeline(std::move(pipeline))
    {
    }
    StopPipeline() = default;
    std::unique_ptr<RunningQueryPlanNode> pipeline;
};


struct StopSource : OperationBase
{
    StopSource() = default;
    StopSource(
        QueryId queryId,
        std::weak_ptr<RunningSource> target,
        WorkEmitter::onComplete onComplete = []() {},
        WorkEmitter::onFailure onFailure = [](Exception) {})
        : OperationBase(queryId, std::move(onComplete), std::move(onFailure)), target(std::move(target))
    {
    }
    std::weak_ptr<RunningSource> target;
};

struct FailSource : OperationBase
{
    FailSource() : exception("", 0) { }
    FailSource(
        QueryId queryId,
        std::weak_ptr<RunningSource> target,
        Exception exception,
        WorkEmitter::onComplete onComplete = []() {},
        WorkEmitter::onFailure onFailure = [](Exception) {})
        : OperationBase(queryId, std::move(onComplete), std::move(onFailure)), target(std::move(target)), exception(std::move(exception))
    {
    }

    std::weak_ptr<RunningSource> target;
    Exception exception;
};

struct Terminate : OperationBase
{
    Terminate(
        QueryId queryId,
        std::weak_ptr<QueryCatalog> catalog,
        std::function<void()> onCompletion = []() {},
        std::function<void(Exception)> onError = [](auto) {})
        : OperationBase(std::move(queryId), std::move(onCompletion), std::move(onError)), catalog(std::move(catalog))
    {
    }

    Terminate() = default;
    std::weak_ptr<QueryCatalog> catalog;
};

struct Start : OperationBase
{
    Start(
        QueryId queryId,
        std::unique_ptr<InstantiatedQueryPlan> qep,
        std::weak_ptr<QueryCatalog> catalog,
        std::function<void()> onCompletion = []() {},
        std::function<void(Exception)> onError = [](auto) {})
        : OperationBase(std::move(queryId), std::move(onCompletion), std::move(onError)), qep(std::move(qep)), catalog(std::move(catalog))
    {
    }

    Start() = default;
    std::unique_ptr<InstantiatedQueryPlan> qep;
    std::weak_ptr<QueryCatalog> catalog;
};

using Operation = std::variant<Task, Terminate, Start, FailSource, StopSource, StopPipeline, SetupPipeline>;
static_assert(std::is_default_constructible_v<Operation>, "Every operation type needs to be able to be default constructed");
static_assert(std::is_move_constructible_v<Operation>, "Every operation type needs to be moveable");

}
