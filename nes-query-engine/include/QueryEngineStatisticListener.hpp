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
#include <chrono>
#include <variant>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>

namespace NES::Runtime
{
using TaskId = NESStrongType<size_t, struct TaskId_, 0, 1>;

struct TaskExecutionStart
{
    WorkerThreadId threadId;
    TaskId taskId;
    size_t numberOfTuples;
    PipelineId pipelineId;
    QueryId queryId;
    std::chrono::system_clock::time_point timestamp;
};

struct TaskEmit
{
    WorkerThreadId threadId;
    TaskId taskId;
    PipelineId fromPipeline;
    PipelineId toPipeline;
    QueryId queryId;
};

struct TaskExecutionComplete
{
    WorkerThreadId threadId;
    TaskId id;
    PipelineId pipelineId;
    QueryId queryId;
    std::chrono::system_clock::time_point timestamp;
};

struct TaskExpired
{
    WorkerThreadId threadId;
    TaskId id;
    PipelineId pipelineId;
    QueryId queryId;
};

struct QueryStart
{
    WorkerThreadId threadId;
    QueryId queryId;
};

struct QueryStop
{
    WorkerThreadId threadId;
    QueryId queryId;
};

struct PipelineStart
{
    WorkerThreadId threadId;
    PipelineId pipelineId;
    QueryId queryId;
};

struct PipelineStop
{
    WorkerThreadId threadId;
    PipelineId pipelineId;
    QueryId queryId;
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
