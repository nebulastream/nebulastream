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

#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalTypeFactory.hpp>
#include <NodeEngine/MemoryLayout/RowLayout.hpp>
#include <QueryCompiler/ExecutablePipeline.hpp>
#include <QueryCompiler/PipelineExecutionContext.hpp>
#include <QueryCompiler/QueryExecutionPlan.hpp>
#include <Util/Logger.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/WindowMeasures/TimeMeasure.hpp>
#include <Windowing/WindowTypes/WindowType.hpp>
#include <utility>

namespace NES {

PipelineStage::PipelineStage(
    uint32_t pipelineStageId,
    QuerySubPlanId qepId,
    ExecutablePipelinePtr executablePipeline,
    QueryExecutionContextPtr pipelineExecutionContext,
    PipelineStagePtr nextPipelineStage) : pipelineStageId(pipelineStageId),
                                          qepId(qepId),
                                          executablePipeline(std::move(executablePipeline)),
                                          nextStage(std::move(nextPipelineStage)),
                                          pipelineContext(std::move(pipelineExecutionContext)) {
    // nop
    NES_ASSERT(this->executablePipeline && this->pipelineContext, "Wrong pipeline stage argument");
}

bool PipelineStage::execute(TupleBuffer& inputBuffer, WorkerContextRef workerContext) {
    std::stringstream dbgMsg;
    dbgMsg << "Execute Pipeline Stage with id=" << qepId << " originId=" << inputBuffer.getOriginId() << " stage=" << pipelineStageId;
    if (hasWindowHandler()) {
        dbgMsg << "  withWindow_";
        auto distType = pipelineContext->getWindowDef()->getDistributionType()->getType();
        if (distType == Windowing::DistributionCharacteristic::Combining) {
            dbgMsg << "Combining";
        } else {
            dbgMsg << "Slicing";
        }
    } else {
        dbgMsg << "NoWindow";
    }
    NES_DEBUG(dbgMsg.str());

    uint32_t ret = !executablePipeline->execute(inputBuffer, pipelineContext, workerContext);

    uint64_t maxWaterMark = inputBuffer.getWatermark();
    if (hasWindowHandler() && maxWaterMark != 0) {
        NES_DEBUG("PipelineStage::execute: new max watermark=" << maxWaterMark << " originId=" << inputBuffer.getOriginId());
        pipelineContext->getWindowHandler()->updateAllMaxTs(maxWaterMark, inputBuffer.getOriginId());
        if (pipelineContext->getWindowDef()->getTriggerPolicy()->getPolicyType() == Windowing::triggerOnWatermarkChange) {
            NES_DEBUG("PipelineStage::execute: trigger window based on triggerOnWatermarkChange");
            pipelineContext->getWindowHandler()->trigger();
        }
    }

    if (hasWindowHandler() && pipelineContext->getWindowDef()->getTriggerPolicy()->getPolicyType() == Windowing::triggerOnBuffer) {
        NES_DEBUG("PipelineStage::execute: trigger window based on triggerOnBuffer");
        pipelineContext->getWindowHandler()->trigger();
    }

    return ret;
}

bool PipelineStage::setup(QueryManagerPtr queryManager, BufferManagerPtr bufferManager) {
    if (hasWindowHandler()) {
        NES_DEBUG("PipelineStage::setup windowHandler");
        return pipelineContext->getWindowHandler()->setup(queryManager, bufferManager, nextStage, pipelineStageId, qepId);
    }
    if (hasJoinHandler()) {
        NES_DEBUG("PipelineStage::setup joinHandler");
        return pipelineContext->getJoinHandler()->setup(queryManager, bufferManager, nextStage, pipelineStageId, qepId);
    }
    return true;
}

bool PipelineStage::start() {
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

bool PipelineStage::stop() {
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
    return pipelineContext->getWindowHandler() != nullptr;
}

bool PipelineStage::hasJoinHandler() {
    return pipelineContext->getJoinHandler() != nullptr;
}

bool PipelineStage::isReconfiguration() const {
    return executablePipeline->isReconfiguration();
}

PipelineStagePtr PipelineStage::create(
    uint32_t pipelineStageId,
    const QuerySubPlanId querySubPlanId,
    const ExecutablePipelinePtr executablePipeline,
    QueryExecutionContextPtr pipelineContext,
    const PipelineStagePtr nextPipelineStage) {
    return std::make_shared<PipelineStage>(pipelineStageId, querySubPlanId, executablePipeline, pipelineContext, nextPipelineStage);
}

}// namespace NES