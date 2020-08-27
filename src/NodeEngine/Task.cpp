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

bool Task::operator()(WorkerContext&) {
    return pipeline->execute(buf);
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
