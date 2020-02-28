#include <QueryCompiler/PipelineStage.hpp>
#include <QueryCompiler/ExecutablePipeline.hpp>
#include <Windows/WindowHandler.hpp>
#include <Util/Logger.hpp>
#include <utility>

namespace NES {

PipelineStage::PipelineStage(uint32_t pipelineStageId,
                             QueryExecutionPlanPtr queryExecutionPlanPtr,
                             ExecutablePipelinePtr executablePipeline)
    : pipelineStageId(pipelineStageId),
      queryExecutionPlanPtr(std::move(queryExecutionPlanPtr)),
      executablePipeline(std::move(executablePipeline)) {}

PipelineStage::PipelineStage(uint32_t pipelineStageId,
                             QueryExecutionPlanPtr queryExecutionPlanPtr,
                             ExecutablePipelinePtr executablePipeline,
                             WindowHandlerPtr windowHandler) :
    pipelineStageId(pipelineStageId),
    queryExecutionPlanPtr(std::move(queryExecutionPlanPtr)),
    executablePipeline(std::move(executablePipeline)),
    windowHandler(std::move(windowHandler)) {
}

bool PipelineStage::execute(TupleBufferPtr inputBuffer,
                            TupleBufferPtr outputBuffer) {
    NES_DEBUG("Execute Pipeline Stage!");
    auto windowStage = hasWindowHandler()? windowHandler->getWindowState(): nullptr;
    auto windowManager = hasWindowHandler()? windowHandler->getWindowManager(): WindowManagerPtr();
    auto result = executablePipeline->execute(std::move(inputBuffer), windowStage, windowManager, std::move(outputBuffer));
    if (result) {
        NES_ERROR("Execution of PipelineStage Failed!");
    }
    return true;
}

PipelineStage::~PipelineStage() = default;

void PipelineStage::setup() {
    if (hasWindowHandler()) {
        windowHandler->setup(queryExecutionPlanPtr, pipelineStageId);
    }
}

bool PipelineStage::start() {
    if (hasWindowHandler()) {
        windowHandler->start();
    }
    return true;
}

bool PipelineStage::stop() {
    if (hasWindowHandler()) {
        windowHandler->stop();
    }
    return true;
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

}  // namespace NES