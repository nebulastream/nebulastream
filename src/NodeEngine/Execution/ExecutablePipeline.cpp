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

#include <NodeEngine/Execution/ExecutablePipeline.hpp>
#include <NodeEngine/Execution/ExecutablePipelineStage.hpp>
#include <NodeEngine/Execution/DiscardingExecutablePipelineStage.hpp>
#include <NodeEngine/Execution/ExecutableQueryPlan.hpp>
#include <NodeEngine/Execution/OperatorHandler.hpp>
#include <NodeEngine/Execution/PipelineExecutionContext.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <Util/Logger.hpp>
#include <utility>

namespace NES::NodeEngine::Execution {

ExecutablePipeline::ExecutablePipeline(uint32_t pipelineStageId, QuerySubPlanId qepId,
                                       ExecutablePipelineStagePtr executablePipelineStage,
                                       PipelineExecutionContextPtr pipelineExecutionContext, uint32_t numOfProducingPipelines,
                                       ExecutablePipelinePtr nextPipeline,
                                       SchemaPtr inputSchema, SchemaPtr outputSchema, bool reconfiguration)
    : pipelineStageId(pipelineStageId), qepId(qepId), executablePipelineStage(std::move(executablePipelineStage)),
      nextPipeline(std::move(nextPipeline)), pipelineContext(std::move(pipelineExecutionContext)), inputSchema(inputSchema),
      outputSchema(outputSchema), reconfiguration(reconfiguration), isRunning(false), activeProducers(numOfProducingPipelines) {
    // nop
    NES_ASSERT(this->executablePipelineStage && this->pipelineContext, "Wrong pipeline stage argument");
}

bool ExecutablePipeline::execute(TupleBuffer& inputBuffer, WorkerContextRef workerContext) {
    NES_TRACE("Execute Pipeline Stage with id=" << qepId << " originId=" << inputBuffer.getOriginId()
                                                << " stage=" << pipelineStageId);
    NES_ASSERT2_FMT(isRunning, "Cannot execute Pipeline Stage with id=" << qepId << " originId=" << inputBuffer.getOriginId()
                                                                 << " stage=" << pipelineStageId);
    uint32_t ret = !executablePipelineStage->execute(inputBuffer, *pipelineContext.get(), workerContext);
    return ret;
}

bool ExecutablePipeline::setup(QueryManagerPtr, BufferManagerPtr) {
    return executablePipelineStage->setup(*pipelineContext.get()) == 0;
}

bool ExecutablePipeline::start() {
    auto expected = false;
    if (isRunning.compare_exchange_strong(expected, true)) {
        for (auto operatorHandler : pipelineContext->getOperatorHandlers()) {
            operatorHandler->start(pipelineContext);
        }
        executablePipelineStage->start(*pipelineContext.get());
        return true;
    }
    return false;
}

bool ExecutablePipeline::stop() {
    auto expected = true;
    if (isRunning.compare_exchange_strong(expected, false)) {
        for (auto operatorHandler : pipelineContext->getOperatorHandlers()) {
            operatorHandler->stop(pipelineContext);
        }
        return executablePipelineStage->stop(*pipelineContext.get()) == 0;
    }
    return false;
}

ExecutablePipelinePtr ExecutablePipeline::getNextPipeline() { return nextPipeline; }

uint32_t ExecutablePipeline::getPipeStageId() { return pipelineStageId; }

QuerySubPlanId ExecutablePipeline::getQepParentId() const { return qepId; }

bool ExecutablePipeline::isReconfiguration() const { return reconfiguration; }

PipelineStageArity ExecutablePipeline::getArity() { return executablePipelineStage->getArity(); }

ExecutablePipelinePtr ExecutablePipeline::create(uint32_t pipelineStageId, const QuerySubPlanId querySubPlanId,
                                                 ExecutablePipelineStagePtr executablePipelineStage,
                                                 PipelineExecutionContextPtr pipelineContext, uint32_t numOfProducingPipelines, ExecutablePipelinePtr nextPipeline,
                                                 SchemaPtr inputSchema, SchemaPtr outputSchema, bool reconfiguration) {
    NES_ASSERT2_FMT(executablePipelineStage != nullptr,
                    "Executable pipelinestage is null for " << pipelineStageId
                                                            << "within the following query sub plan: " << querySubPlanId);
    NES_ASSERT2_FMT(pipelineContext != nullptr,
                    "Pipeline context is null for " << pipelineStageId
                                                    << "within the following query sub plan: " << querySubPlanId);
    if (!reconfiguration) {
        NES_ASSERT2_FMT(inputSchema != nullptr,
                        "Input schema is null for " << pipelineStageId
                                                    << "within the following query sub plan: " << querySubPlanId);
        NES_ASSERT2_FMT(outputSchema != nullptr,
                        "Output schema is null for " << pipelineStageId
                                                     << "within the following query sub plan: " << querySubPlanId);
    }
    NES_ASSERT2_FMT(numOfProducingPipelines > 0, "invalid number of producers for " << pipelineStageId << "within the following query sub plan: " << querySubPlanId);
    return std::make_shared<ExecutablePipeline>(pipelineStageId, querySubPlanId, executablePipelineStage, pipelineContext, numOfProducingPipelines,
                                                nextPipeline, inputSchema, outputSchema, reconfiguration);
}

std::string ExecutablePipeline::getCodeAsString() {
    std::stringstream ss;
    ss << executablePipelineStage->getCodeAsString();
    return ss.str();
}

ExecutablePipeline::~ExecutablePipeline() { NES_DEBUG("~ExecutablePipeline(" + std::to_string(pipelineStageId) + ")"); }

const SchemaPtr& ExecutablePipeline::getInputSchema() const { return inputSchema; }

const SchemaPtr& ExecutablePipeline::getOutputSchema() const { return outputSchema; }

void ExecutablePipeline::reconfigure(ReconfigurationMessage& task, WorkerContext& context) {
    NES_DEBUG("Going to reconfigure pipeline belonging to query id: " << qepId << " stage id: " << pipelineStageId);
    Reconfigurable::reconfigure(task, context);
    for (auto operatorHandler : pipelineContext->getOperatorHandlers()) {
        operatorHandler->reconfigure(task, context);
    }
    if (nextPipeline != nullptr) {
        nextPipeline->reconfigure(task, context);
    }
}
void ExecutablePipeline::postReconfigurationCallback(ReconfigurationMessage& task) {
    NES_DEBUG("Going to execute postReconfigurationCallback on pipeline belonging to subplanId: " << qepId << " stage id: " << pipelineStageId);
    Reconfigurable::postReconfigurationCallback(task);
    switch (task.getType()) {
        case SoftEndOfStream: {
            auto targetQep = task.getUserData<std::weak_ptr<ExecutableQueryPlan>>();
            auto prevProducerCounter = activeProducers.fetch_sub(1);
            if (prevProducerCounter == 1) {
                NES_DEBUG("Requested reconfiguration of pipeline belonging to subplanId: " << qepId << " stage id: " << pipelineStageId << " reached prev=1");
                for (auto operatorHandler : pipelineContext->getOperatorHandlers()) {
                    operatorHandler->postReconfigurationCallback(task);
                }
                stop();
                if (nextPipeline == nullptr) {
                    auto queryManager = pipelineContext->getQueryManager();
                    NES_ASSERT2_FMT(!targetQep.expired(), "Invalid qep for reconfig of subplanId: " << qepId << " stage id: " << pipelineStageId);
                    auto newReconf = ReconfigurationMessage(qepId, SoftEndOfStream, targetQep.lock(), std::make_any<std::weak_ptr<ExecutableQueryPlan>>(targetQep));
                    queryManager->addReconfigurationMessage(qepId, newReconf, false);
                    NES_DEBUG("Going to triggering reconfig whole plan belonging to subplanId: " << qepId << " stage id: " << pipelineStageId << " got SoftEndOfStream on last pipeline");
                } else {
                    auto queryManager = pipelineContext->getQueryManager();
                    auto newReconf = ReconfigurationMessage(qepId, SoftEndOfStream, nextPipeline, std::make_any<std::weak_ptr<ExecutableQueryPlan>>(targetQep));
                    queryManager->addReconfigurationMessage(qepId, newReconf, false);
                    NES_DEBUG("Going to reconfigure next pipeline belonging to subplanId: " << qepId << " stage id: " << nextPipeline->pipelineStageId << " got SoftEndOfStream  with nextPipeline");
                }
            } else {
                NES_DEBUG("Requested reconfiguration of pipeline belonging to subplanId: " << qepId << " stage id: " << pipelineStageId << " but refCount was " << (prevProducerCounter) << " and now is " << (prevProducerCounter - 1));
            }
            break;
        }
        case HardEndOfStream: {
            NES_DEBUG("Going to reconfigure pipeline belonging to subplanId: " << qepId << " stage id: " << pipelineStageId << " got HardEndOfStream");
            if (nextPipeline != nullptr) {
                nextPipeline->postReconfigurationCallback(task);
            }
            break;
        }
        default: {
            break;
        }
    }
}

void ExecutablePipeline::pin() {
    auto oldValue = activeProducers.fetch_add(1);
    NES_DEBUG("ExecutablePipeline " << this << " has refCnt increased to " << (oldValue + 1));
}

}// namespace NES::NodeEngine::Execution