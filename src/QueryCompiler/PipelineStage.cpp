#include <QueryCompiler/ExecutablePipeline.hpp>
#include <QueryCompiler/QueryExecutionPlan.hpp>
#include <Util/Logger.hpp>
#include <Windows/WindowHandler.hpp>
#include <utility>

namespace NES {

PipelineStage::PipelineStage(uint32_t pipelineStageId,
                             QueryExecutionPlanPtr queryExecutionPlan,
                             ExecutablePipelinePtr executablePipeline)
    : pipelineStageId(pipelineStageId),
      queryExecutionPlan(std::move(queryExecutionPlan)),
      executablePipeline(std::move(executablePipeline)) {}

PipelineStage::PipelineStage(uint32_t pipelineStageId,
                             QueryExecutionPlanPtr queryExecutionPlan,
                             ExecutablePipelinePtr executablePipeline,
                             WindowHandlerPtr windowHandler) : pipelineStageId(pipelineStageId),
                                                               queryExecutionPlan(std::move(queryExecutionPlan)),
                                                               executablePipeline(std::move(executablePipeline)),
                                                               windowHandler(std::move(windowHandler)) {
}

bool PipelineStage::execute(TupleBuffer& inputBuffer,
                            PipelineExecutionContext& context) {
    NES_DEBUG("Execute Pipeline Stage!");
    // only get the window manager and state if the pipeline has a window handler.
    auto windowStage = hasWindowHandler() ? windowHandler->getWindowState() : nullptr;
    auto windowManager = hasWindowHandler() ? windowHandler->getWindowManager() : WindowManagerPtr();
    auto bufferManager = queryExecutionPlan->getBufferManager();
    auto result = executablePipeline->execute(inputBuffer, windowStage, windowManager, context);
    if (result) {
        NES_ERROR("Execution of PipelineStage Failed!");
    }
    return true;
}

PipelineStage::~PipelineStage() = default;

bool PipelineStage::setup() {
    if (hasWindowHandler()) {
        return windowHandler->setup(queryExecutionPlan, pipelineStageId);
    }
    return true;
}

bool PipelineStage::start() {
    if (hasWindowHandler()) {
        return windowHandler->start();
    }
    return true;
}

bool PipelineStage::stop() {
    if (hasWindowHandler()) {
        return windowHandler->stop();
    }
    return true;
}

PipelineStagePtr PipelineStage::getNextStage() {
    return nextStage;
}
void PipelineStage::setNextStage(PipelineStagePtr ptr) {
    this->nextStage = ptr;
}
int PipelineStage::getPipeStageId() {
    return pipelineStageId;
}

bool PipelineStage::hasWindowHandler() {
    return windowHandler != nullptr;
}

PipelineStagePtr createPipelineStage(uint32_t pipelineStageId,
                                     const QueryExecutionPlanPtr& queryExecutionPlanPtr,
                                     const ExecutablePipelinePtr& executablePipeline) {
    return std::make_shared<PipelineStage>(pipelineStageId, queryExecutionPlanPtr, executablePipeline);
}

PipelineStagePtr createPipelineStage(uint32_t pipelineStageId,
                                     const QueryExecutionPlanPtr& queryExecutionPlanPtr,
                                     const ExecutablePipelinePtr& executablePipeline,
                                     const WindowHandlerPtr& windowHandler) {
    return std::make_shared<PipelineStage>(pipelineStageId, queryExecutionPlanPtr, executablePipeline, windowHandler);
}

}// namespace NES