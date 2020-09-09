#include <QueryCompiler/ExecutablePipeline.hpp>
#include <QueryCompiler/QueryExecutionPlan.hpp>
#include <Util/Logger.hpp>
#include <Windows/WindowHandler.hpp>
#include <utility>

namespace NES {

PipelineStage::PipelineStage(
    uint32_t pipelineStageId,
    QuerySubPlanId qepId,
    ExecutablePipelinePtr executablePipeline,
    QueryExecutionContextPtr pipelineExecutionContext,
    PipelineStagePtr nextPipelineStage,
    WindowHandlerPtr windowHandler) : pipelineStageId(pipelineStageId),
                                      qepId(std::move(qepId)),
                                      executablePipeline(std::move(executablePipeline)),
                                      windowHandler(std::move(windowHandler)),
                                      nextStage(std::move(nextPipelineStage)),
                                      pipelineContext(std::move(pipelineExecutionContext)) {
    // nop
    NES_ASSERT(this->executablePipeline && this->pipelineContext, "Wrong pipeline stage argument");
}

bool PipelineStage::execute(TupleBuffer& inputBuffer) {
    NES_DEBUG("Execute Pipeline Stage with id=" << pipelineStageId);
    // only get the window manager and state if the pipeline has a window handler.
    auto windowStage = hasWindowHandler() ? windowHandler->getWindowState() : nullptr;               // TODO Philipp, do we need this check?
    auto windowManager = hasWindowHandler() ? windowHandler->getWindowManager() : WindowManagerPtr();// TODO Philipp, do we need this check?
    return !executablePipeline->execute(inputBuffer, windowStage, windowManager, pipelineContext);
}

bool PipelineStage::setup() {
    if (hasWindowHandler()) {
        return windowHandler->setup(nextStage, pipelineStageId);
    }
    return true;
}

bool PipelineStage::start() {
    if (hasWindowHandler()) {
        NES_DEBUG("PipelineStage::start: windowhandler start");
        return windowHandler->start();
    }
    return true;
}

bool PipelineStage::stop() {
    if (hasWindowHandler()) {
        NES_DEBUG("PipelineStage::stop: windowhandler stop");
        return windowHandler->stop();
    }
    return true;
}

PipelineStagePtr PipelineStage::getNextStage() {
    return nextStage;
}

uint32_t PipelineStage::getPipeStageId() {
    return pipelineStageId;
}

QuerySubPlanId PipelineStage::getQepParentId() const {
    return qepId;
}

bool PipelineStage::hasWindowHandler() {
    return windowHandler != nullptr;
}

PipelineStagePtr PipelineStage::create(
    uint32_t pipelineStageId,
    const QuerySubPlanId querySubPlanId,
    const ExecutablePipelinePtr executablePipeline,
    QueryExecutionContextPtr pipelineContext,
    const PipelineStagePtr nextPipelineStage,
    const WindowHandlerPtr& windowHandler) {

    return std::make_shared<PipelineStage>(pipelineStageId, querySubPlanId, executablePipeline, pipelineContext, nextPipelineStage, windowHandler);
}
}// namespace NES