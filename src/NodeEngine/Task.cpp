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

#include <NodeEngine/BufferManager.hpp>
#include <NodeEngine/Task.hpp>
#include <NodeEngine/WorkerContext.hpp>
#include <QueryCompiler/PipelineExecutionContext.hpp>
#include <QueryCompiler/PipelineStage.hpp>
#include <Util/UtilityFunctions.hpp>
#include <utility>

namespace NES {

Task::Task(PipelineStagePtr pipeline, TupleBuffer& buffer)
    : pipeline(std::move(pipeline)),
      buf(buffer) {
    id = UtilityFunctions::generateIdInt();
}

Task::Task() : pipeline(nullptr), buf() {
    id = UtilityFunctions::generateIdInt();
}

bool Task::operator()(WorkerContextRef workerContext) {
    return pipeline->execute(buf, workerContext);
}

size_t Task::getNumberOfTuples() {
    return buf.getNumberOfTuples();
}

bool Task::isWatermarkOnly() {
    return buf.getNumberOfTuples() == 0;
}

PipelineStagePtr Task::getPipelineStage() {
    return pipeline;
}

bool Task::operator!() const {
    return pipeline == nullptr;
}

Task::operator bool() const {
    return pipeline != nullptr;
}
uint64_t Task::getId() {
    return id;
}

std::string Task::toString() {
    std::stringstream ss;
    ss << "Task: id=" << id;
    ss << " execute pipelineId=" << pipeline->getPipeStageId() << " qepParentId=" << pipeline->getQepParentId() << " nextPipelineId=" << pipeline->getNextStage();
    ss << " inputBuffer=" << buf.getBuffer() << " inputTuples=" << buf.getNumberOfTuples() << " bufferSize=" << buf.getBufferSize() << " watermark=" << buf.getWatermark();
    return ss.str();
}

}// namespace NES
