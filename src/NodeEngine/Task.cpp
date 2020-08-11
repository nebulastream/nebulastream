#include <NodeEngine/BufferManager.hpp>
#include <NodeEngine/Task.hpp>
#include <NodeEngine/WorkerContext.hpp>
#include <QueryCompiler/PipelineExecutionContext.hpp>
#include <QueryCompiler/PipelineStage.hpp>
#include <utility>

namespace NES {

Task::Task(PipelineStagePtr pipeline, TupleBuffer& buffer)
    : pipeline(std::move(pipeline)),
      buf(buffer) {
    // nop
}

bool Task::operator()(WorkerContext&) {
    return pipeline->execute(buf);
}

size_t Task::getNumberOfTuples() {
    return buf.getNumberOfTuples();
}

bool Task::isWatermarkOnly()
{
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

}// namespace NES
