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

    // only get the window manager and state if the pipeline has a window handler.
    uint64_t maxWaterMark = inputBuffer.getWatermark();;
    if (hasWindowHandler()) {
        auto windowHandler = pipelineContext->getWindowHandler();
        if (pipelineContext->getWindowDef()->getWindowType()->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::ProcessingTime) {
            NES_DEBUG("Execute Pipeline Stage set processing time watermark from buffer=" << inputBuffer.getWatermark());
            windowHandler->updateAllMaxTs(inputBuffer.getWatermark(), inputBuffer.getOriginId());
        } else {//Event time
            auto distType = pipelineContext->getWindowDef()->getDistributionType()->getType();
            std::string tsFieldName = "";
            if (distType == Windowing::DistributionCharacteristic::Combining) {
                NES_DEBUG("PipelineStage: process combining window");
                tsFieldName = "end";
                if (!pipelineContext->getInputSchema()->has(tsFieldName)) {
                    NES_ERROR("PipelineStage::execute: Window Operator: key field " << tsFieldName << " does not exist!");
                    NES_FATAL_ERROR("PipelineStage type error");
                }
            } else if (distType == Windowing::DistributionCharacteristic::Slicing || distType == Windowing::DistributionCharacteristic::Complete) {
                NES_DEBUG("PipelineStage: process slicing window or complete window");
                tsFieldName = pipelineContext->getWindowDef()->getWindowType()->getTimeCharacteristic()->getField()->name;
                if (!pipelineContext->getInputSchema()->has(tsFieldName)) {
                    NES_ERROR("PipelineStage::execute: Window Operator: key field " << tsFieldName << " does not exist!");
                    NES_FATAL_ERROR("PipelineStage type error");
                }
            } else {
                NES_DEBUG("PipelineStage: process complete window");
            }

            auto layout = createRowLayout(std::make_shared<Schema>(pipelineContext->getInputSchema()));
            auto fields = pipelineContext->getInputSchema()->fields;
            size_t keyIndex = pipelineContext->getInputSchema()->getIndex(tsFieldName);

            for (uint64_t recordIndex = 0; recordIndex < inputBuffer.getNumberOfTuples(); recordIndex++) {
                auto dataType = fields[keyIndex]->getDataType();
                auto physicalType = DefaultPhysicalTypeFactory().getPhysicalType(dataType);
                auto basicPhysicalType = std::dynamic_pointer_cast<BasicPhysicalType>(physicalType);
                if (basicPhysicalType->getNativeType() == BasicPhysicalType::INT_64) {
                    auto val = layout->getValueField<int64_t>(recordIndex, keyIndex)->read(inputBuffer);
                    maxWaterMark = std::max(maxWaterMark, (size_t) val);
                    NES_DEBUG("PipelineStage: maxWaterMark=" << maxWaterMark << " or " << (size_t) val << " val=" << val);

                } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::UINT_64) {
                    auto val = layout->getValueField<uint64_t>(recordIndex, keyIndex)->read(inputBuffer);
                    maxWaterMark = std::max(maxWaterMark, val);
                    NES_DEBUG("PipelineStage: maxWaterMark=" << maxWaterMark << " or "
                                                             << "  val=" << val);
                } else {
                    NES_NOT_IMPLEMENTED();
                }
            }
        }
    }

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

    //TODO just copied, has to be fixec
    if (hasJoinHandler()) {
        auto joinHandler = pipelineContext->getJoinHandler();
        if (pipelineContext->getJoinDef()->getWindowType()->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::ProcessingTime) {
            NES_DEBUG("Execute Pipeline Stage set processing time watermark from buffer=" << inputBuffer.getWatermark());
            joinHandler->updateAllMaxTs(inputBuffer.getWatermark(), inputBuffer.getOriginId());
        } else {//Event time
            auto distType = pipelineContext->getJoinDef()->getDistributionType()->getType();
            std::string tsFieldName = "";
            if (distType == Windowing::DistributionCharacteristic::Combining) {
                NES_DEBUG("PipelineStage: process combining join");
                tsFieldName = "end";
                if (!pipelineContext->getInputSchema()->has(tsFieldName)) {
                    NES_ERROR("PipelineStage::execute: join Operator: key field " << tsFieldName << " does not exist!");
                    NES_FATAL_ERROR("PipelineStage type error");
                }
            } else if (distType == Windowing::DistributionCharacteristic::Slicing || distType == Windowing::DistributionCharacteristic::Complete) {
                NES_DEBUG("PipelineStage: process slicing window or complete window");
                tsFieldName = pipelineContext->getJoinDef()->getWindowType()->getTimeCharacteristic()->getField()->name;
                if (!pipelineContext->getInputSchema()->has(tsFieldName)) {
                    NES_ERROR("PipelineStage::execute: Window Operator: key field " << tsFieldName << " does not exist!");
                    NES_FATAL_ERROR("PipelineStage type error");
                }
            } else {
                NES_DEBUG("PipelineStage: process complete join");
            }

            auto layout = createRowLayout(std::make_shared<Schema>(pipelineContext->getInputSchema()));
            auto fields = pipelineContext->getInputSchema()->fields;
            size_t keyIndex = pipelineContext->getInputSchema()->getIndex(tsFieldName);

            for (uint64_t recordIndex = 0; recordIndex < inputBuffer.getNumberOfTuples(); recordIndex++) {
                auto dataType = fields[keyIndex]->getDataType();
                auto physicalType = DefaultPhysicalTypeFactory().getPhysicalType(dataType);
                auto basicPhysicalType = std::dynamic_pointer_cast<BasicPhysicalType>(physicalType);
                if (basicPhysicalType->getNativeType() == BasicPhysicalType::INT_64) {
                    auto val = layout->getValueField<int64_t>(recordIndex, keyIndex)->read(inputBuffer);
                    maxWaterMark = std::max(maxWaterMark, (size_t) val);
                    NES_DEBUG("PipelineStage: maxWaterMark=" << maxWaterMark << " or " << (size_t) val << " val=" << val);

                } else if (basicPhysicalType->getNativeType() == BasicPhysicalType::UINT_64) {
                    auto val = layout->getValueField<uint64_t>(recordIndex, keyIndex)->read(inputBuffer);
                    maxWaterMark = std::max(maxWaterMark, val);
                    NES_DEBUG("PipelineStage: maxWaterMark=" << maxWaterMark << " or "
                                                             << "  val=" << val);
                } else {
                    NES_NOT_IMPLEMENTED();
                }
            }
        }
    }


    if (hasJoinHandler() && maxWaterMark != 0) {
        NES_DEBUG("PipelineStage::execute: new max watermark=" << maxWaterMark << " originId=" << inputBuffer.getOriginId());
        pipelineContext->getJoinHandler()->updateAllMaxTs(maxWaterMark, inputBuffer.getOriginId());
        if (pipelineContext->getJoinDef()->getTriggerPolicy()->getPolicyType() == Windowing::triggerOnWatermarkChange) {
            NES_DEBUG("PipelineStage::execute: trigger window based on triggerOnWatermarkChange");
            pipelineContext->getJoinHandler()->trigger();
        }
    }

    if (hasJoinHandler() && pipelineContext->getJoinDef()->getTriggerPolicy()->getPolicyType() == Windowing::triggerOnBuffer) {
        NES_DEBUG("PipelineStage::execute: trigger window based on triggerOnBuffer");
        pipelineContext->getJoinHandler()->trigger();
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