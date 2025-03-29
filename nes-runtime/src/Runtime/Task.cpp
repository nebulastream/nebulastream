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

#include <utility>
#include <Exceptions/TaskExecutionException.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/ExecutablePipeline.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/ExecutionResult.hpp>
#include <Runtime/Task.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sinks/Sink.hpp>
#include <Util/Core.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES
{

Task::Task(SuccessorExecutablePipeline pipeline, Memory::TupleBuffer buffer, uint64_t taskId)
    : pipeline(std::move(pipeline)), buf(std::move(buffer)), id(taskId)
{
    inputTupleCount = buf.getNumberOfTuples();
}

ExecutionResult Task::operator()(WorkerContextRef workerContext)
{
    /// execute this task.
    /// a task could be an executable pipeline, or a data sink.
    try
    {
        /// Todo: #4040: refactor (use if - else if and get rid of bool in second if)
        if (auto* executablePipeline = std::get_if<std::shared_ptr<ExecutablePipeline>>(&pipeline))
        {
            return (*executablePipeline)->execute(buf, workerContext);
        }
        if (const auto dataSink = std::get_if<std::shared_ptr<NES::Sinks::Sink>>(&pipeline))
        {
            if ((*dataSink)->emitTupleBuffer(buf))
            {
                return ExecutionResult::Ok;
            }
            return ExecutionResult::Error;
        }
        NES_ERROR("Executable pipeline was not of any suitable type");
        return ExecutionResult::Error;
    }
    catch (std::exception const& error)
    {
        throw TaskExecutionException(pipeline, std::string(error.what()));
    }
}

uint64_t Task::getNumberOfTuples() const
{
    return buf.getNumberOfTuples();
}

uint64_t Task::getNumberOfInputTuples() const
{
    return inputTupleCount;
}

bool Task::isReconfiguration() const
{
    if (auto* executablePipeline = std::get_if<std::shared_ptr<ExecutablePipeline>>(&pipeline))
    {
        return (*executablePipeline)->isReconfiguration();
    }
    return false;
}

SuccessorExecutablePipeline Task::getExecutable()
{
    return pipeline;
}

Memory::TupleBuffer const& Task::getBufferRef() const
{
    return buf;
}

bool Task::operator!() const
{
    return pipeline.valueless_by_exception();
}

Task::operator bool() const
{
    return !pipeline.valueless_by_exception();
}

uint64_t Task::getId() const
{
    return id;
}

std::string Task::toString() const
{
    std::stringstream ss;
    ss << "Task: id=" << id;
    if (auto* executablePipeline = std::get_if<std::shared_ptr<ExecutablePipeline>>(&pipeline))
    {
        ss << " execute pipelineId=" << (*executablePipeline)->getPipelineId() << " qepParentId=" << (*executablePipeline)->getQueryId();
    }
    else if (std::holds_alternative<std::shared_ptr<NES::Sinks::Sink>>(pipeline))
    {
        ss << " execute data sink";
    }
    ss << " inputBuffer=" << reinterpret_cast<std::size_t>(buf.getBuffer()) << " inputTuples=" << buf.getNumberOfTuples()
       << " bufferSize=" << buf.getBufferSize() << " watermark=" << buf.getWatermark() << " originID=" << buf.getOriginId();
    return ss.str();
}

}
