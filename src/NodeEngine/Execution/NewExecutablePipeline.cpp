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

#include <NodeEngine/Execution/ExecutablePipelineStage.hpp>
#include <NodeEngine/Execution/NewExecutablePipeline.hpp>
#include <NodeEngine/Execution/OperatorHandler.hpp>
#include <NodeEngine/Execution/PipelineExecutionContext.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Util/Logger.hpp>
#include <utility>

namespace NES::NodeEngine::Execution {
NewExecutablePipeline::NewExecutablePipeline(uint32_t pipelineId,
                                             QuerySubPlanId qepId,
                                             PipelineExecutionContextPtr pipelineExecutionContext,
                                             ExecutablePipelineStagePtr executablePipelineStage,
                                             uint32_t numOfProducingPipelines,
                                             std::vector<SuccessorExecutablePipeline> successorPipelines,
                                             bool reconfiguration)
    : pipelineStageId(pipelineId), qepId(qepId), executablePipelineStage(std::move(executablePipelineStage)),
      pipelineContext(std::move(pipelineExecutionContext)), reconfiguration(reconfiguration),
      pipelineStatus(reconfiguration ? PipelineRunning : PipelineCreated), activeProducers(numOfProducingPipelines),
      successorPipelines(successorPipelines) {
    // nop
    NES_ASSERT(this->executablePipelineStage && this->pipelineContext && numOfProducingPipelines > 0,
               "Wrong pipeline stage argument");
}

ExecutionResult NewExecutablePipeline::execute(TupleBuffer& inputBuffer, WorkerContextRef workerContext) {
    NES_TRACE("Execute Pipeline Stage with id=" << qepId << " originId=" << inputBuffer.getOriginId()
                                                << " stage=" << pipelineStageId);
    auto pipelineStatus = this->pipelineStatus.load();
    if (pipelineStatus == PipelineRunning) {
        return executablePipelineStage->execute(inputBuffer, *pipelineContext.get(), workerContext);
    } else if (pipelineStatus == PipelineStopped) {
        return ExecutionResult::Finished;
    }
    NES_ERROR("Cannot execute Pipeline Stage with id=" << qepId << " originId=" << inputBuffer.getOriginId()
                                                       << " stage=" << pipelineStageId << " as pipeline is not running anymore");
    return ExecutionResult::Error;
}

bool NewExecutablePipeline::setup(QueryManagerPtr, BufferManagerPtr) {
    return executablePipelineStage->setup(*pipelineContext.get()) == 0;
}

bool NewExecutablePipeline::start(StateManagerPtr stateManager) {
    auto expected = PipelineCreated;
    if (pipelineStatus.compare_exchange_strong(expected, PipelineRunning)) {
        for (auto operatorHandler : pipelineContext->getOperatorHandlers()) {
            operatorHandler->start(pipelineContext, stateManager);
        }
        executablePipelineStage->start(*pipelineContext.get());
        return true;
    }
    return false;
}

bool NewExecutablePipeline::stop() {
    auto expected = PipelineRunning;
    if (pipelineStatus.compare_exchange_strong(expected, PipelineStopped)) {
        for (auto operatorHandler : pipelineContext->getOperatorHandlers()) {
            operatorHandler->stop(pipelineContext);
        }
        auto ret = executablePipelineStage->stop(*pipelineContext.get()) == 0;
        return ret;
    }
    return false;
}

bool NewExecutablePipeline::isRunning() const { return pipelineStatus.load() == PipelineRunning; }

std::vector<SuccessorExecutablePipeline> NewExecutablePipeline::getSuccessors() { return successorPipelines; }

uint32_t NewExecutablePipeline::getPipeStageId() { return pipelineStageId; }

QuerySubPlanId NewExecutablePipeline::getQepParentId() const { return qepId; }

bool NewExecutablePipeline::isReconfiguration() const { return reconfiguration; }

NewExecutablePipelinePtr NewExecutablePipeline::create(uint32_t pipelineId,
                                                       QuerySubPlanId querySubPlanId,
                                                       PipelineExecutionContextPtr pipelineExecutionContext,
                                                       ExecutablePipelineStagePtr executablePipelineStage,
                                                       uint32_t numOfProducingPipelines,
                                                       std::vector<SuccessorExecutablePipeline> successorPipelines,
                                                       bool reconfiguration) {
    NES_ASSERT2_FMT(executablePipelineStage != nullptr,
                    "Executable pipelinestage is null for " << pipelineId
                                                            << "within the following query sub plan: " << querySubPlanId);
    NES_ASSERT2_FMT(pipelineExecutionContext != nullptr,
                    "Pipeline context is null for " << pipelineId << "within the following query sub plan: " << querySubPlanId);

    return std::make_shared<NewExecutablePipeline>(pipelineId,
                                                   querySubPlanId,
                                                   pipelineExecutionContext,
                                                   executablePipelineStage,
                                                   numOfProducingPipelines,
                                                   successorPipelines,
                                                   reconfiguration);
}

NewExecutablePipeline::~NewExecutablePipeline() { NES_DEBUG("~ExecutablePipeline(" + std::to_string(pipelineStageId) + ")"); }

void NewExecutablePipeline::reconfigure(ReconfigurationMessage& task, WorkerContext& context) {
    NES_ASSERT2_FMT(isRunning(), "Going to reconfigure a non-running pipeline!");
    NES_DEBUG("Going to reconfigure pipeline belonging to query id: " << qepId << " stage id: " << pipelineStageId);
    Reconfigurable::reconfigure(task, context);
    for (auto operatorHandler : pipelineContext->getOperatorHandlers()) {
        operatorHandler->reconfigure(task, context);
    }
    for (auto successorPipeline : successorPipelines) {
        if (auto pipe = std::get_if<NewExecutablePipelinePtr>(&successorPipeline)) {
            (*pipe)->reconfigure(task, context);
        }
    }
}

void NewExecutablePipeline::addSuccessor(SuccessorExecutablePipeline successorPipeline) {
    NES_ASSERT2_FMT(!isRunning(), "It is not allowed to add pipelines during execution!");
    successorPipelines.emplace_back(successorPipeline);
}

void NewExecutablePipeline::postReconfigurationCallback(ReconfigurationMessage& task) {
    NES_ASSERT2_FMT(isRunning(), "Going to reconfigure a non-running pipeline!");
    NES_DEBUG("Going to execute postReconfigurationCallback on pipeline belonging to subplanId: " << qepId << " stage id: "
                                                                                                  << pipelineStageId);
    Reconfigurable::postReconfigurationCallback(task);
    switch (task.getType()) {
        case SoftEndOfStream: {
            auto targetQep = task.getUserData<std::weak_ptr<NewExecutableQueryPlan>>();
            //we mantain a set of producers, and we will only trigger the end of stream once all producers have sent the EOS, for this we decrement the counter
            auto prevProducerCounter = activeProducers.fetch_sub(1);
            if (prevProducerCounter == 1) {//all producers sent EOS
                NES_DEBUG("Requested reconfiguration of pipeline belonging to subplanId: "
                          << qepId << " stage id: " << pipelineStageId << " reached prev=1");
                for (auto operatorHandler : pipelineContext->getOperatorHandlers()) {
                    operatorHandler->postReconfigurationCallback(task);
                }
                stop();
                //it the current pipeline is the last pipeline in the qep and thus we reconfigure the qep
                // TODO: The following code expects no broadcast operators.
                // It is not clear how to shut down a qep in that case.
                bool hasNonSinkSuccessor = false;
                if (!successorPipelines.empty()) {
                    for (auto successorPipeline : successorPipelines) {
                        if (auto pipe = std::get_if<NewExecutablePipelinePtr>(&successorPipeline)) {
                            hasNonSinkSuccessor = true;
                            auto queryManager = pipelineContext->getQueryManager();
                            auto newReconf =
                                ReconfigurationMessage(qepId,
                                                       SoftEndOfStream,
                                                       *pipe,
                                                       std::make_any<std::weak_ptr<NewExecutableQueryPlan>>(targetQep));
                            queryManager->addReconfigurationMessage(qepId, newReconf, false);
                            NES_DEBUG("Going to reconfigure next pipeline belonging to subplanId: "
                                      << qepId << " stage id: " << (*pipe)->getPipeStageId()
                                      << " got SoftEndOfStream  with nextPipeline");
                        }
                    }
                }
                if (!hasNonSinkSuccessor) {
                    //in this branch, we reoncfigure the next pipeline itself
                    auto queryManager = pipelineContext->getQueryManager();
                    NES_ASSERT2_FMT(!targetQep.expired(),
                                    "Invalid qep for reconfig of subplanId: " << qepId << " stage id: " << pipelineStageId);
                    auto newReconf = ReconfigurationMessage(qepId,
                                                            SoftEndOfStream,
                                                            targetQep.lock(),
                                                            std::make_any<std::weak_ptr<NewExecutableQueryPlan>>(targetQep));
                    queryManager->addReconfigurationMessage(qepId, newReconf, false);
                    NES_DEBUG("Going to triggering reconfig whole plan belonging to subplanId: "
                              << qepId << " stage id: " << pipelineStageId << " got SoftEndOfStream on last pipeline");
                }

                pipelineContext.reset();
            } else {
                NES_DEBUG("Requested reconfiguration of pipeline belonging to subplanId: "
                          << qepId << " stage id: " << pipelineStageId << " but refCount was " << (prevProducerCounter)
                          << " and now is " << (prevProducerCounter - 1));
            }
            break;
        }
        case HardEndOfStream: {
            NES_DEBUG("Going to reconfigure pipeline belonging to subplanId: " << qepId << " stage id: " << pipelineStageId
                                                                               << " got HardEndOfStream");
            for (auto successorPipeline : successorPipelines) {
                if (auto pipe = std::get_if<NewExecutablePipelinePtr>(&successorPipeline)) {
                    (*pipe)->postReconfigurationCallback(task);
                }
            }
            pipelineContext.reset();
            break;
        }
        default: {
            break;
        }
    }
}

void NewExecutablePipeline::incrementProducerCount() {
    auto oldValue = activeProducers.fetch_add(1);
    NES_DEBUG("ExecutablePipeline " << this << " has refCnt increased to " << (oldValue + 1));
}

}// namespace NES::NodeEngine::Execution