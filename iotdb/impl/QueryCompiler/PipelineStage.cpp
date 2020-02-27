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
  auto result = this->executablePipeline->
      execute(std::move(inputBuffer),
              windowHandler->getWindowState(),
              windowHandler->getWindowManager(),
              std::move(outputBuffer));
  if (result) {
    NES_ERROR("Execution of PipelineStage Failed!");
  }
  return true;
}

PipelineStage::~PipelineStage() {
}

void PipelineStage::setup() {
  if (this->windowHandler != nullptr) {
    this->windowHandler->setup(this->queryExecutionPlanPtr, this->pipelineStageId);
  }
}

bool PipelineStage::start() {
  if (this->windowHandler != nullptr) {
    this->windowHandler->start();
  }
}

bool PipelineStage::stop() {
  if (this->windowHandler != nullptr) {
    this->windowHandler->stop();
  }
}

PipelineStagePtr createPipelineStage(uint32_t pipelineStageId,
                                     const QueryExecutionPlanPtr &queryExecutionPlanPtr,
                                     const ExecutablePipelinePtr &executablePipeline) {
  return std::make_shared<PipelineStage>(pipelineStageId, queryExecutionPlanPtr, executablePipeline);
}

PipelineStagePtr createPipelineStage(uint32_t pipelineStageId,
                                     const QueryExecutionPlanPtr &queryExecutionPlanPtr,
                                     const ExecutablePipelinePtr &executablePipeline,
                                     const WindowHandlerPtr &windowHandler) {
  return std::make_shared<PipelineStage>(pipelineStageId, queryExecutionPlanPtr, executablePipeline, windowHandler);
}

}  // namespace NES