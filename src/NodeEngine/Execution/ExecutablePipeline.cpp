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
#include <NodeEngine/Execution/OperatorHandler.hpp>
#include <NodeEngine/Execution/PipelineExecutionContext.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Util/Logger.hpp>
#include <functional>
#include <utility>

namespace NES::NodeEngine::Execution {
ExecutablePipeline::ExecutablePipeline(uint64_t pipelineId,
                                       QuerySubPlanId qepId,
                                       PipelineExecutionContextPtr pipelineExecutionContext,
                                       ExecutablePipelineStagePtr executablePipelineStage,
                                       uint32_t numOfProducingPipelines,
                                       std::vector<SuccessorExecutablePipeline> successorPipelines,
                                       bool reconfiguration)
    : pipelineId(pipelineId), querySubPlanId(qepId), executablePipelineStage(std::move(executablePipelineStage)),
      pipelineContext(std::move(pipelineExecutionContext)), reconfiguration(reconfiguration),
      pipelineStatus(reconfiguration ? PipelineStatus::PipelineRunning : PipelineStatus::PipelineCreated),
      activeProducers(numOfProducingPipelines), successorPipelines(successorPipelines) {
    // nop
    NES_ASSERT(this->executablePipelineStage && this->pipelineContext && numOfProducingPipelines > 0,
               "Wrong pipeline stage argument");
}

const ExecutionResult ExecutablePipeline::execute(TupleBuffer& inputBuffer, WorkerContextRef workerContext) {
    NES_TRACE("Execute Pipeline Stage with id=" << querySubPlanId << " originId=" << inputBuffer.getOriginId()
                                                << " stage=" << pipelineId);
    auto pipelineStatus = this->pipelineStatus.load();
    if (pipelineStatus == PipelineStatus::PipelineRunning) {
        return executablePipelineStage->execute(inputBuffer, *pipelineContext.get(), workerContext);
    } else if (pipelineStatus == PipelineStatus::PipelineStopped) {
        return ExecutionResult::Finished;
    }
    NES_ERROR("Cannot execute Pipeline Stage with id=" << querySubPlanId << " originId=" << inputBuffer.getOriginId()
                                                       << " stage=" << pipelineId << " as pipeline is not running anymore");
    return ExecutionResult::Error;
}

bool ExecutablePipeline::setup(QueryManagerPtr, BufferManagerPtr) {
    return executablePipelineStage->setup(*pipelineContext.get()) == 0;
}

bool ExecutablePipeline::start(StateManagerPtr stateManager) {
    auto expected = PipelineStatus::PipelineCreated;
    if (pipelineStatus.compare_exchange_strong(expected, PipelineStatus::PipelineRunning)) {
        for (auto operatorHandler : pipelineContext->getOperatorHandlers()) {
            operatorHandler->start(pipelineContext, stateManager);
        }
        executablePipelineStage->start(*pipelineContext.get());
        return true;
    }
    return false;
}

bool ExecutablePipeline::stop() {
    auto expected = PipelineStatus::PipelineRunning;
    if (pipelineStatus.compare_exchange_strong(expected, PipelineStatus::PipelineStopped)) {
        for (auto operatorHandler : pipelineContext->getOperatorHandlers()) {
            operatorHandler->stop(pipelineContext);
        }
        auto ret = executablePipelineStage->stop(*pipelineContext.get()) == 0;
        return ret;
    }
    return false;
}

bool ExecutablePipeline::isRunning() const { return pipelineStatus.load() == PipelineStatus::PipelineRunning; }

const std::vector<SuccessorExecutablePipeline>& ExecutablePipeline::getSuccessors() const { return successorPipelines; }

const uint64_t ExecutablePipeline::getPipelineId() const { return pipelineId; }

const QuerySubPlanId ExecutablePipeline::getQuerySubPlanId() const { return querySubPlanId; }

bool ExecutablePipeline::isReconfiguration() const { return reconfiguration; }

ExecutablePipelinePtr ExecutablePipeline::create(uint64_t pipelineId,
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

    return std::make_shared<ExecutablePipeline>(pipelineId,
                                                querySubPlanId,
                                                pipelineExecutionContext,
                                                executablePipelineStage,
                                                numOfProducingPipelines,
                                                successorPipelines,
                                                reconfiguration);
}

ExecutablePipeline::~ExecutablePipeline() { NES_DEBUG("~ExecutablePipeline(" + std::to_string(pipelineId) + ")"); }

void ExecutablePipeline::reconfigure(ReconfigurationMessage& task, WorkerContext& context) {
    NES_ASSERT2_FMT(isRunning(), "Going to reconfigure a non-running pipeline!");
    NES_DEBUG("Going to reconfigure pipeline belonging to query id: " << querySubPlanId << " stage id: " << pipelineId);
    Reconfigurable::reconfigure(task, context);
    for (auto operatorHandler : pipelineContext->getOperatorHandlers()) {
        operatorHandler->reconfigure(task, context);
    }
    for (auto successorPipeline : successorPipelines) {
        if (auto pipe = std::get_if<ExecutablePipelinePtr>(&successorPipeline)) {
            (*pipe)->reconfigure(task, context);
        }
    }
}

void ExecutablePipeline::addSuccessor(SuccessorExecutablePipeline successorPipeline) {
    NES_ASSERT2_FMT(!isRunning(), "It is not allowed to add pipelines during execution!");
    successorPipelines.emplace_back(successorPipeline);
}

void ExecutablePipeline::postReconfigurationCallback(ReconfigurationMessage& task) {
    NES_ASSERT2_FMT(isRunning(), "Going to reconfigure a non-running pipeline!");
    NES_DEBUG("Going to execute postReconfigurationCallback on pipeline belonging to subplanId: " << querySubPlanId
                                                                                                  << " stage id: " << pipelineId);
    Reconfigurable::postReconfigurationCallback(task);
    switch (task.getType()) {
        case SoftEndOfStream: {
            auto targetQep = task.getUserData<std::weak_ptr<ExecutableQueryPlan>>();
            handleSoftStop(task, targetQep, [targetQep]() {
                return std::make_any<std::weak_ptr<ExecutableQueryPlan>>(targetQep);
            });
            break;
        }
        case HardEndOfStream: {
            NES_DEBUG("Going to reconfigure pipeline belonging to subplanId: " << querySubPlanId << " stage id: " << pipelineId
                                                                               << " got HardEndOfStream");
            for (auto successorPipeline : successorPipelines) {
                if (auto pipe = std::get_if<ExecutablePipelinePtr>(&successorPipeline)) {
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
void ExecutablePipeline::handleSoftStop(ReconfigurationMessage& task,
                                        const std::weak_ptr<ExecutableQueryPlan>& targetQep,
                                        const std::function<std::any()>& userdataSupplier) {
    //we maintain a set of producers, and we will only trigger the end of stream once all producers have sent the EOS, for this we decrement the counter
    auto taskType = task.getType();
    auto prevProducerCounter = activeProducers.fetch_sub(1);
    if (prevProducerCounter == 1) {//all producers sent EOS
        NES_DEBUG("Requested reconfiguration via " << taskType << " of pipeline belonging to subplanId: " << querySubPlanId
                                                   << " stage id: " << pipelineId << " reached prev=1");
        for (auto operatorHandler : pipelineContext->getOperatorHandlers()) {
            operatorHandler->postReconfigurationCallback(task);
        }
        stop();
        // If the current pipeline is the last pipeline in the qep and thus we reconfigure the qep
        // TODO: The following code expects no broadcast operators.
        // It is not clear how to shut down a qep in that case.
        bool hasNonSinkSuccessor = false;
        if (!successorPipelines.empty()) {
            for (auto successorPipeline : successorPipelines) {
                if (auto pipe = std::get_if<ExecutablePipelinePtr>(&successorPipeline)) {
                    hasNonSinkSuccessor = true;
                    auto queryManager = pipelineContext->getQueryManager();
                    auto newReconf = ReconfigurationMessage(querySubPlanId, task.getType(), *pipe, userdataSupplier());
                    queryManager->addReconfigurationMessage(querySubPlanId, newReconf, false);
                    NES_DEBUG("Going to reconfigure next pipeline belonging to subplanId: "
                              << querySubPlanId << " stage id: " << (*pipe)->getPipelineId() << " got " << taskType
                              << " with nextPipeline");
                }
            }
        }
        if (!hasNonSinkSuccessor) {
            //in this branch, we reconfigure the next pipeline itself
            auto queryManager = pipelineContext->getQueryManager();
            NES_ASSERT2_FMT(!targetQep.expired(),
                            "Invalid qep for reconfig of subplanId: " << querySubPlanId << " stage id: " << pipelineId);
            auto newReconf = ReconfigurationMessage(querySubPlanId, task.getType(), targetQep.lock(), userdataSupplier());
            queryManager->addReconfigurationMessage(querySubPlanId, newReconf, false);
            NES_DEBUG("Going to triggering reconfig whole plan belonging to subplanId: "
                      << querySubPlanId << " stage id: " << pipelineId << " got " << taskType << " on last pipeline");
        }

        pipelineContext.reset();
    } else {
        NES_DEBUG("Requested reconfiguration via" << taskType << " of pipeline belonging to subplanId: " << querySubPlanId
                                                  << " stage id: " << pipelineId << " but refCount was " << (prevProducerCounter)
                                                  << " and now is " << (prevProducerCounter - 1));
    }
}

void ExecutablePipeline::incrementProducerCount() {
    auto oldValue = activeProducers.fetch_add(1);
    NES_DEBUG("ExecutablePipeline " << this << " has refCnt increased to " << (oldValue + 1));
}

}// namespace NES::NodeEngine::Execution