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

namespace NES
{
using TaskId = NESStrongType<size_t, struct TaskId_, 0, 1>;
using ChronoClock = std::chrono::system_clock;

struct EventBase
{
    EventBase(const WorkerThreadId threadId, const QueryId queryId) : threadId(threadId), queryId(queryId) { }
    EventBase() = default;

    ChronoClock::time_point timestamp = ChronoClock::now();
    WorkerThreadId threadId = INVALID<WorkerThreadId>;
    QueryId queryId = INVALID<QueryId>;
};

struct TaskExecutionStart : EventBase
{
    TaskExecutionStart(
        const WorkerThreadId threadId,
        const QueryId queryId,
        const PipelineId pipelineId,
        const TaskId taskId,
        const size_t numberOfTuples,
        const size_t bytesInTupleBuffer)
        : EventBase(threadId, queryId)
        , pipelineId(pipelineId)
        , taskId(taskId)
        , numberOfTuples(numberOfTuples)
        , bytesInTupleBuffer(bytesInTupleBuffer)
    {
    }
    TaskExecutionStart() = default;

    PipelineId pipelineId = INVALID<PipelineId>;
    TaskId taskId = INVALID<TaskId>;
    size_t numberOfTuples;
    size_t bytesInTupleBuffer;
};

struct TaskEmit : EventBase
{
    TaskEmit(
        const WorkerThreadId threadId,
        const QueryId queryId,
        const PipelineId fromPipeline,
        const PipelineId toPipeline,
        const TaskId taskId,
        const size_t numberOfTuples,
        const size_t bytesInTupleBuffer,
        const bool formattingTask)
        : EventBase(threadId, queryId)
        , fromPipeline(fromPipeline)
        , toPipeline(toPipeline)
        , taskId(taskId)
        , numberOfTuples(numberOfTuples)
        , bytesInTupleBuffer(bytesInTupleBuffer)
        , formattingTask(formattingTask)
    {
    }
    TaskEmit() = default;

    PipelineId fromPipeline = INVALID<PipelineId>;
    PipelineId toPipeline = INVALID<PipelineId>;
    TaskId taskId = INVALID<TaskId>;
    size_t numberOfTuples{};
    size_t bytesInTupleBuffer;
    bool formattingTask;
};

struct TaskExecutionComplete : EventBase
{
    TaskExecutionComplete(const WorkerThreadId threadId, const QueryId queryId, const PipelineId pipelineId, const TaskId taskId)
        : EventBase(threadId, queryId), pipelineId(pipelineId), taskId(taskId)
    {
    }
    TaskExecutionComplete() = default;


    PipelineId pipelineId = INVALID<PipelineId>;
    TaskId taskId = INVALID<TaskId>;
};

struct TaskExpired : EventBase
{
    TaskExpired(const WorkerThreadId threadId, const QueryId queryId, const PipelineId pipelineId, const TaskId taskId)
        : EventBase(threadId, queryId), pipelineId(pipelineId), taskId(taskId)
    {
    }

    PipelineId pipelineId = INVALID<PipelineId>;
    TaskId taskId = INVALID<TaskId>;
};

struct QueryStart : EventBase
{
    QueryStart(const WorkerThreadId threadId, const QueryId queryId) : EventBase(threadId, queryId) { }
    QueryStart() = default;
};

struct QueryStop : EventBase
{
    QueryStop(const WorkerThreadId threadId, const QueryId queryId) : EventBase(threadId, queryId) { }
    QueryStop() = default;
};

struct PipelineStart : EventBase
{
    PipelineStart(const WorkerThreadId threadId, const QueryId queryId, const PipelineId pipelineId)
        : EventBase(threadId, queryId), pipelineId(pipelineId)
    {
    }
    PipelineStart() = default;

    PipelineId pipelineId = INVALID<PipelineId>;
};

struct PipelineStop : EventBase
{
    PipelineStop(const WorkerThreadId threadId, const QueryId queryId, const PipelineId pipeline_id)
        : EventBase(threadId, queryId), pipelineId(pipeline_id)
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
