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
#include <cstddef>
#include <variant>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>

namespace NES::Runtime
{
using TaskId = NESStrongType<size_t, struct TaskId_, 0, 1>;
using ChronoClock = std::chrono::system_clock;

struct EventBase
{
    EventBase(WorkerThreadId threadId, QueryId queryId) : threadId(threadId), queryId(queryId) { }

    EventBase() = default;

    ChronoClock::time_point timestamp = ChronoClock::now();
    WorkerThreadId threadId = INVALID<WorkerThreadId>;
    QueryId queryId = INVALID<QueryId>;
};

struct TaskExecutionStart : EventBase
{
    TaskExecutionStart(WorkerThreadId threadId, QueryId queryId, PipelineId pipelineId, TaskId taskId, size_t numberOfTuples)
        : EventBase(threadId, queryId), pipelineId(pipelineId), taskId(taskId), numberOfTuples(numberOfTuples)
    {
    }

    TaskExecutionStart() = default;

    PipelineId pipelineId = INVALID<PipelineId>;
    TaskId taskId = INVALID<TaskId>;
    size_t numberOfTuples;
};

struct TaskEmit : EventBase
{
    TaskEmit(WorkerThreadId threadId, QueryId queryId, PipelineId fromPipeline, PipelineId toPipeline, TaskId taskId, size_t numberOfTuples)
        : EventBase(threadId, queryId), fromPipeline(fromPipeline), toPipeline(toPipeline), taskId(taskId), numberOfTuples(numberOfTuples)
    {
    }

    TaskEmit() = default;

    PipelineId fromPipeline = INVALID<PipelineId>;
    PipelineId toPipeline = INVALID<PipelineId>;
    TaskId taskId = INVALID<TaskId>;
    size_t numberOfTuples{};
};

struct TaskExecutionComplete : EventBase
{
    TaskExecutionComplete(WorkerThreadId threadId, QueryId queryId, PipelineId pipelineId, TaskId taskId)
        : EventBase(threadId, queryId), pipelineId(pipelineId), taskId(taskId)
    {
    }

    TaskExecutionComplete() = default;


    PipelineId pipelineId = INVALID<PipelineId>;
    TaskId taskId = INVALID<TaskId>;
};

struct TaskExpired : EventBase
{
    TaskExpired(WorkerThreadId threadId, QueryId queryId, PipelineId pipelineId, TaskId taskId)
        : EventBase(threadId, queryId), pipelineId(pipelineId), taskId(taskId)
    {
    }

    PipelineId pipelineId = INVALID<PipelineId>;
    TaskId taskId = INVALID<TaskId>;
};

struct QueryStart : EventBase
{
    QueryStart(WorkerThreadId threadId, QueryId queryId) : EventBase(threadId, queryId) { }

    QueryStart() = default;
};

struct QueryStop : EventBase
{
    QueryStop(WorkerThreadId threadId, QueryId queryId) : EventBase(threadId, queryId) { }

    QueryStop() = default;
};

struct PipelineStart : EventBase
{
    PipelineStart(WorkerThreadId threadId, QueryId queryId, PipelineId pipelineId)
        : EventBase(threadId, queryId), pipelineId(pipelineId) { }

    PipelineStart() = default;

    PipelineId pipelineId = INVALID<PipelineId>;
};

struct PipelineStop : EventBase
{
    PipelineStop(WorkerThreadId threadId, QueryId queryId, PipelineId pipeline_id) : EventBase(threadId, queryId), pipelineId(pipeline_id)
    {
    }

    PipelineStop() = default;

    PipelineId pipelineId = INVALID<PipelineId>;
};

using Event
    = std::variant<TaskExecutionStart, TaskEmit, TaskExecutionComplete, TaskExpired, PipelineStart, PipelineStop, QueryStart, QueryStop>;

struct QueryEngineStatisticListener
{
    virtual ~QueryEngineStatisticListener() = default;

    /// This function is called from a WorkerThread!
    /// It should not block, and it has to be thread-safe!
    virtual void onEvent(Event event) = 0;
};

}
