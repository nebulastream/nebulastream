#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalTypeFactory.hpp>
#include <QueryCompiler/ExecutablePipeline.hpp>
#include <QueryCompiler/QueryExecutionPlan.hpp>
#include <Util/Logger.hpp>
#include <Windows/WindowHandler.hpp>
#include <utility>
#include <QueryCompiler/PipelineExecutionContext.hpp>

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
    if (hasWindowHandler()) {
        NES_DEBUG("Pipelinestage ctor set origin id=" << qepId);
        this->windowHandler->setOriginId(qepId);
    }
}

bool PipelineStage::execute(TupleBuffer& inputBuffer) {

    std::stringstream dbgMsg;
    dbgMsg << "Execute Pipeline Stage with id=" << qepId << " originId=" << inputBuffer.getOriginId() << " stage=" << pipelineStageId;
    if (hasWindowHandler()) {
        dbgMsg << "  withWindow_";
        auto distType = pipelineContext->getWindowDef()->getDistributionType()->getType();
        if (distType == DistributionCharacteristic::Combining) {
            dbgMsg << "Combining";
        }
        else
        {
            dbgMsg << "Slicing";
        }
    }
    else
    {
        dbgMsg << "NoWindow";
    }
    NES_DEBUG(dbgMsg.str());
    // only get the window manager and state if the pipeline has a window handler.
    auto windowStage = hasWindowHandler() ? windowHandler->getWindowState() : nullptr;               // TODO Philipp, do we need this check?
    auto windowManager = hasWindowHandler() ? windowHandler->getWindowManager() : WindowManagerPtr();// TODO Philipp, do we need this check?

    uint64_t maxWaterMark = 0;
    if (hasWindowHandler()) {
        if (pipelineContext->getWindowDef()->getWindowType()->getTimeCharacteristic()->getType() == TimeCharacteristic::ProcessingTime) {
            NES_DEBUG("Execute Pipeline Stage set processing time watermark from buffer=" << inputBuffer.getWatermark());
            windowHandler->updateAllMaxTs(inputBuffer.getWatermark(), inputBuffer.getOriginId());
        } else {//Event time
            auto distType = pipelineContext->getWindowDef()->getDistributionType()->getType();
            std::string tsFieldName = "";
            if (distType == DistributionCharacteristic::Combining) {
                NES_DEBUG("PipelineStage: process combining window");
                tsFieldName = "end";
                if (!pipelineContext->getInputSchema()->has(tsFieldName)) {
                    NES_ERROR("PipelineStage::execute: Window Operator: key field " << tsFieldName << " does not exist!");
                    NES_FATAL_ERROR("PipelineStage type error");
                }
            } else if (distType == DistributionCharacteristic::Slicing || distType == DistributionCharacteristic::Complete) {
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

    //
    uint32_t ret = !executablePipeline->execute(inputBuffer, windowStage, windowManager, pipelineContext);

    if (maxWaterMark != 0) {
        NES_DEBUG("PipelineStage::execute: new max watermark=" << maxWaterMark << " originId=" << inputBuffer.getOriginId());
        windowHandler->updateAllMaxTs(maxWaterMark, inputBuffer.getOriginId());
    }
    return ret;
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
    } else {
        NES_DEBUG("PipelineStage::start: NO windowhandler started");
    }
    return true;
}

bool PipelineStage::stop() {
    if (hasWindowHandler()) {
        NES_DEBUG("PipelineStage::stop: windowhandler stop");
        return windowHandler->stop();
    } else {
        NES_DEBUG("PipelineStage::stop: NO windowhandler stopped");
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