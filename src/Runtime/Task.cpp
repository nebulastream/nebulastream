/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/ExecutablePipeline.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/ExecutionResult.hpp>
#include <Runtime/Task.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Util/UtilityFunctions.hpp>
#include <utility>

namespace NES::Runtime {

Task::Task(Execution::SuccessorExecutablePipeline pipeline, TupleBuffer buffer)
    : pipeline(std::move(pipeline)), buf(std::move(buffer)) {
    id = UtilityFunctions::getNextTaskId();
    inputTupleCount = buf.getNumberOfTuples();
}

ExecutionResult Task::operator()(WorkerContextRef workerContext) {
    // execute this task.
    // a task could be a executable pipeline, or a data sink.
    if (auto* executablePipeline = std::get_if<Execution::ExecutablePipelinePtr>(&pipeline)) {
        return (*executablePipeline)->execute(buf, workerContext);
    }
    if (auto* dataSink = std::get_if<DataSinkPtr>(&pipeline)) {
        auto result = (*dataSink)->writeData(buf, workerContext);
        if (result) {
            return ExecutionResult::Ok;
        } else {
            return ExecutionResult::Error;
        }
    } else {
        NES_ERROR("Executable pipeline was not of any suitable type");
        return ExecutionResult::Error;
    }
}

uint64_t Task::getNumberOfTuples() { return buf.getNumberOfTuples(); }
uint64_t Task::getNumberOfInputTuples() { return inputTupleCount; }

bool Task::isReconfiguration() {
    if (auto* executablePipeline = std::get_if<Execution::ExecutablePipelinePtr>(&pipeline)) {
        return (*executablePipeline)->isReconfiguration();
    }
    return false;
}

Execution::SuccessorExecutablePipeline Task::getExecutable() { return pipeline; }

TupleBuffer& Task::getBufferRef() { return buf; }

bool Task::operator!() const { return pipeline.valueless_by_exception(); }

Task::operator bool() const { return !pipeline.valueless_by_exception(); }
uint64_t Task::getId() const { return id; }

std::string Task::toString() {
    std::stringstream ss;
    ss << "Task: id=" << id;
    if (auto* executablePipeline = std::get_if<Execution::ExecutablePipelinePtr>(&pipeline)) {
        ss << " execute pipelineId=" << (*executablePipeline)->getPipelineId()
           << " qepParentId=" << (*executablePipeline)->getQuerySubPlanId();
    } else if (std::holds_alternative<DataSinkPtr>(pipeline)) {
        ss << " execute data sink";
    }
    ss << " inputBuffer=" << reinterpret_cast<std::size_t>(buf.getBuffer()) << " inputTuples=" << buf.getNumberOfTuples()
       << " bufferSize=" << buf.getBufferSize() << " watermark=" << buf.getWatermark() << " originID=" << buf.getOriginId();
    return ss.str();
}

}// namespace NES::Runtime
