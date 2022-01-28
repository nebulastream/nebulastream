/*
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

#include <Runtime/Execution/ExecutablePipeline.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Util/Logger.hpp>

namespace NES::Runtime::Execution {
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
      activeProducers(numOfProducingPipelines), successorPipelines(std::move(successorPipelines)) {
    // nop
    NES_ASSERT(this->executablePipelineStage && this->pipelineContext && numOfProducingPipelines > 0,
               "Wrong pipeline stage argument");
}

ExecutionResult ExecutablePipeline::execute(TupleBuffer& inputBuffer, WorkerContextRef workerContext) {
    NES_TRACE("Execute Pipeline Stage with id=" << querySubPlanId << " originId=" << inputBuffer.getOriginId()
                                                << " stage=" << pipelineId);

    switch (this->pipelineStatus.load()) {
        case PipelineStatus::PipelineRunning: {
            auto res = executablePipelineStage->execute(inputBuffer, *pipelineContext.get(), workerContext);
            return res;
        }
        case PipelineStatus::PipelineStopped: {
            return ExecutionResult::Finished;
        }
        default: {
            NES_ERROR("Cannot execute Pipeline Stage with id=" << querySubPlanId << " originId=" << inputBuffer.getOriginId()
                                                               << " stage=" << pipelineId
                                                               << " as pipeline is not running anymore");
            return ExecutionResult::Error;
        }
    }
}

bool ExecutablePipeline::setup(const QueryManagerPtr&, const BufferManagerPtr&) {
    return executablePipelineStage->setup(*pipelineContext.get()) == 0;
}

bool ExecutablePipeline::start(const StateManagerPtr& stateManager) {
    auto expected = PipelineStatus::PipelineCreated;
    uint32_t localStateVariableId = 0;
    if (pipelineStatus.compare_exchange_strong(expected, PipelineStatus::PipelineRunning)) {
        auto queryManager = pipelineContext->getQueryManager();
        auto newReconf = ReconfigurationMessage(querySubPlanId,
                                                Initialize,
                                                shared_from_this(),
                                                std::make_any<uint32_t>(activeProducers.load()));
        for (const auto& operatorHandler : pipelineContext->getOperatorHandlers()) {
            operatorHandler->start(pipelineContext, stateManager, localStateVariableId);
            localStateVariableId++;
        }
        queryManager->addReconfigurationMessage(querySubPlanId, newReconf, true);
        executablePipelineStage->start(*pipelineContext.get());
        return true;
    }
    return false;
}

bool ExecutablePipeline::stop() {
    auto expected = PipelineStatus::PipelineRunning;
    if (pipelineStatus.compare_exchange_strong(expected, PipelineStatus::PipelineStopped)) {
        for (const auto& operatorHandler : pipelineContext->getOperatorHandlers()) {
            operatorHandler->stop(pipelineContext);
        }
        return executablePipelineStage->stop(*pipelineContext.get()) == 0;
    }
    return expected == PipelineStatus::PipelineStopped;
}

bool ExecutablePipeline::isRunning() const { return pipelineStatus.load() == PipelineStatus::PipelineRunning; }

const std::vector<SuccessorExecutablePipeline>& ExecutablePipeline::getSuccessors() const { return successorPipelines; }

uint64_t ExecutablePipeline::getPipelineId() const { return pipelineId; }

QuerySubPlanId ExecutablePipeline::getQuerySubPlanId() const { return querySubPlanId; }

bool ExecutablePipeline::isReconfiguration() const { return reconfiguration; }

ExecutablePipelinePtr ExecutablePipeline::create(uint64_t pipelineId,
                                                 QuerySubPlanId querySubPlanId,
                                                 const PipelineExecutionContextPtr& pipelineExecutionContext,
                                                 const ExecutablePipelineStagePtr& executablePipelineStage,
                                                 uint32_t numOfProducingPipelines,
                                                 const std::vector<SuccessorExecutablePipeline>& successorPipelines,
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

void ExecutablePipeline::reconfigure(ReconfigurationMessage& task, WorkerContext& context) {
    NES_DEBUG("Going to reconfigure pipeline " << pipelineId << " belonging to query id: " << querySubPlanId
                                               << " stage id: " << pipelineId);
    Reconfigurable::reconfigure(task, context);
    switch (task.getType()) {
        case Initialize: {
            NES_ASSERT2_FMT(isRunning(),
                            "Going to reconfigure a non-running pipeline "
                                << pipelineId << " belonging to query id: " << querySubPlanId << " stage id: " << pipelineId);
            auto refCnt = task.getUserData<uint32_t>();
            context.setObjectRefCnt(this, refCnt);
            break;
        }
        case HardEndOfStream:
        case SoftEndOfStream: {
            if (context.decreaseObjectRefCnt(this) == 1) {
                // here we could have a tricky situation with window handlers when using folly
                // imagine the source emits a buffer b followed by eos token
                // if b triggers a window, the handler enqueues further data buffers after eos token
                // our shutdown logic needs thus to be aware of this:
                // assuming we have a window handler attached to the i-th pipeline
                // the (i+1)-th shall consider the eos token from the window handler and ignore the one from the i-th pipeline
                bool hasWindowHandler = false;
                for (const auto& operatorHandler : pipelineContext->getOperatorHandlers()) {
                    operatorHandler->reconfigure(task, context);
                    hasWindowHandler = true;
                }
                if (!hasWindowHandler) {
                    // no window handler -> no chance the above situation could have occurred
                    for (auto successorPipeline : successorPipelines) {
                        if (auto* pipe = std::get_if<ExecutablePipelinePtr>(&successorPipeline)) {
                            (*pipe)->reconfigure(task, context);
                        } else if (auto* sink = std::get_if<DataSinkPtr>(&successorPipeline)) {
                            (*sink)->reconfigure(task, context);
                        }
                    }
                }
            }

            break;
        }
        default: {
            break;
        }
    }
}

void ExecutablePipeline::postReconfigurationCallback(ReconfigurationMessage& task) {

    NES_DEBUG("Going to execute postReconfigurationCallback on pipeline belonging to subplanId: " << querySubPlanId
                                                                                                  << " stage id: " << pipelineId);
    Reconfigurable::postReconfigurationCallback(task);
    switch (task.getType()) {
        case HardEndOfStream:
        case SoftEndOfStream: {
            auto targetQep = task.getUserData<std::weak_ptr<ExecutableQueryPlan>>();
            //we mantain a set of producers, and we will only trigger the end of stream once all producers have sent the EOS, for this we decrement the counter
            auto prevProducerCounter = activeProducers.fetch_sub(1);
            if (prevProducerCounter == 1) {//all producers sent EOS
                NES_DEBUG("Requested reconfiguration of pipeline belonging to subplanId: " << querySubPlanId << " stage id: "
                                                                                           << pipelineId << " reached prev=1");
                for (const auto& operatorHandler : pipelineContext->getOperatorHandlers()) {
                    operatorHandler->postReconfigurationCallback(task);
                }
                stop();
                bool hasOnlySinksAsSuccessors = true;
                for (auto successorPipeline : successorPipelines) {
                    if (auto* pipe = std::get_if<ExecutablePipelinePtr>(&successorPipeline)) {
                        hasOnlySinksAsSuccessors &= false;
                        auto queryManager = pipelineContext->getQueryManager();
                        auto newReconf = ReconfigurationMessage(querySubPlanId,
                                                                task.getType(),
                                                                *pipe,
                                                                std::make_any<std::weak_ptr<ExecutableQueryPlan>>(targetQep));
                        queryManager->addReconfigurationMessage(querySubPlanId, newReconf, false);
                        NES_DEBUG("Going to reconfigure next pipeline belonging to subplanId: "
                                  << querySubPlanId << " stage id: " << (*pipe)->getPipelineId()
                                  << " got EndOfStream  with nextPipeline");
                    } else if (auto* sink = std::get_if<DataSinkPtr>(&successorPipeline)) {
                        hasOnlySinksAsSuccessors &= true;
                        auto queryManager = pipelineContext->getQueryManager();
                        auto newReconf = ReconfigurationMessage(querySubPlanId, task.getType(), *sink);
                        queryManager->addReconfigurationMessage(querySubPlanId, newReconf, false);
                        NES_DEBUG("Going to reconfigure next pipeline belonging to subplanId: "
                                  << querySubPlanId << " sink id: " << (*sink)->toString()
                                  << " got EndOfStream  with nextPipeline");
                    }
                }

                if (hasOnlySinksAsSuccessors) {
                    auto queryManager = pipelineContext->getQueryManager();
                    NES_ASSERT2_FMT(!targetQep.expired(),
                                    "Invalid qep for reconfig of subplanId: " << querySubPlanId << " stage id: " << pipelineId);
                    auto newReconf = ReconfigurationMessage(querySubPlanId, task.getType(), targetQep.lock());
                    queryManager->addReconfigurationMessage(querySubPlanId, newReconf, false);
                    NES_DEBUG("Going to triggering reconfig whole plan belonging to subplanId: "
                              << querySubPlanId << " stage id: " << pipelineId << " got EndOfStream on last pipeline");
                }

            } else {
                NES_DEBUG("Requested reconfiguration of pipeline belonging to subplanId: "
                          << querySubPlanId << " stage id: " << pipelineId << " but refCount was " << (prevProducerCounter)
                          << " and now is " << (prevProducerCounter - 1));
            }
            break;
        }
        default: {
            break;
        }
    }
}

void ExecutablePipeline::incrementProducerCount() {
    auto oldValue = activeProducers.fetch_add(1);
    NES_DEBUG("ExecutablePipeline " << this << " has refCnt increased to " << (oldValue + 1));
}

}// namespace NES::Runtime::Execution