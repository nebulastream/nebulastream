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

#include <Common/PhysicalTypes/PhysicalTypeFactory.hpp>
#include <NodeEngine/Execution/ExecutablePipeline.hpp>
#include <NodeEngine/Execution/ExecutablePipelineStage.hpp>
#include <NodeEngine/Execution/ExecutableQueryPlan.hpp>
#include <NodeEngine/Execution/PipelineExecutionContext.hpp>
#include <NodeEngine/MemoryLayout/RowLayout.hpp>
#include <Util/Logger.hpp>
#include <Windowing/WindowHandler/AbstractJoinHandler.hpp>
#include <Windowing/WindowHandler/AbstractWindowHandler.hpp>
#include <Windowing/WindowMeasures/TimeMeasure.hpp>
#include <utility>

namespace NES::NodeEngine::Execution {

ExecutablePipeline::ExecutablePipeline(uint32_t pipelineStageId, QuerySubPlanId qepId,
                                       ExecutablePipelineStagePtr executablePipelineStage,
                                       PipelineExecutionContextPtr pipelineExecutionContext, ExecutablePipelinePtr nextPipeline)
    : pipelineStageId(pipelineStageId), qepId(qepId), executablePipelineStage(std::move(executablePipelineStage)),
      nextPipeline(std::move(nextPipeline)), pipelineContext(std::move(pipelineExecutionContext)) {
    // nop
    NES_ASSERT(this->executablePipelineStage && this->pipelineContext, "Wrong pipeline stage argument");
}

bool ExecutablePipeline::execute(TupleBuffer& inputBuffer, WorkerContextRef workerContext) {
    NES_TRACE("Execute Pipeline Stage with id=" << qepId << " originId=" << inputBuffer.getOriginId()
                                                << " stage=" << pipelineStageId);
    uint32_t ret = !executablePipelineStage->execute(inputBuffer, *pipelineContext.get(), workerContext);
    return ret;
}

bool ExecutablePipeline::setup(QueryManagerPtr queryManager, BufferManagerPtr bufferManager) {
    executablePipelineStage->setup(*pipelineContext.get());
    if (hasWindowHandler()) {
        NES_DEBUG("PipelineStage::setup windowHandler");
        return pipelineContext->getWindowHandler()->setup(queryManager, bufferManager, nextPipeline, pipelineStageId, qepId);
    }
    if (hasJoinHandler()) {
        NES_DEBUG("PipelineStage::setup joinHandler");
        return pipelineContext->getJoinHandler()->setup(queryManager, bufferManager, nextPipeline, pipelineStageId, qepId);
    }
    return true;
}

bool ExecutablePipeline::start() {
    executablePipelineStage->start(*pipelineContext.get());
    if (hasWindowHandler()) {
        NES_DEBUG("PipelineStage::start: windowhandler start");
        return pipelineContext->getWindowHandler()->start();
    } else {
        NES_DEBUG("PipelineStage::start: no windowhandler to start");
    }

    if (hasJoinHandler()) {
        NES_DEBUG("PipelineStage::start: joinHandler start");
        return pipelineContext->getJoinHandler()->start();
    } else {
        NES_DEBUG("PipelineStage::start: no join to start");
    }

    return true;
}

bool ExecutablePipeline::stop() {
    executablePipelineStage->stop(*pipelineContext.get());
    if (hasWindowHandler()) {
        NES_DEBUG("PipelineStage::stop: windowhandler stop");
        return pipelineContext->getWindowHandler()->stop();
    } else {
        NES_DEBUG("PipelineStage::stop: no windowhandler to stop");
    }

    if (hasJoinHandler()) {
        NES_DEBUG("PipelineStage::stop: joinHandler stop");
        return pipelineContext->getJoinHandler()->stop();
    } else {
        NES_DEBUG("PipelineStage::stop: no joinHandler to stop");
    }
    return true;
}

ExecutablePipelinePtr ExecutablePipeline::getNextPipeline() { return nextPipeline; }

uint32_t ExecutablePipeline::getPipeStageId() { return pipelineStageId; }

QuerySubPlanId ExecutablePipeline::getQepParentId() const { return qepId; }

bool ExecutablePipeline::hasWindowHandler() { return pipelineContext->getWindowHandler() != nullptr; }

bool ExecutablePipeline::hasJoinHandler() { return pipelineContext->getJoinHandler() != nullptr; }

bool ExecutablePipeline::isReconfiguration() const { return reconfiguration; }

ExecutablePipelinePtr ExecutablePipeline::create(uint32_t pipelineStageId, const QuerySubPlanId querySubPlanId,
                                            ExecutablePipelineStagePtr executablePipelineStage,
                                              PipelineExecutionContextPtr pipelineContext, ExecutablePipelinePtr nextPipeline) {
    return std::make_shared<ExecutablePipeline>(pipelineStageId, querySubPlanId, executablePipelineStage, pipelineContext,
                                                nextPipeline);
}

}// namespace NES::NodeEngine::Execution