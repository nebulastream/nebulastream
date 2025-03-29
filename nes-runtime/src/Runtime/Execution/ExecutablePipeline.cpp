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

#include <atomic>
#include <chrono>
#include <Runtime/Events.hpp>
#include <Runtime/Execution/ExecutablePipeline.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

using namespace std::chrono_literals;
namespace NES
{
ExecutablePipeline::ExecutablePipeline(
    PipelineId pipelineId,
    QueryId queryId,
    QueryManagerPtr queryManager,
    PipelineExecutionContextPtr pipelineExecutionContext,
    ExecutablePipelineStagePtr executablePipelineStage,
    uint32_t numOfProducingPipelines,
    std::vector<SuccessorExecutablePipeline> successorPipelines,
    bool reconfiguration)
    : pipelineId(pipelineId)
    , queryId(queryId)
    , queryManager(queryManager)
    , executablePipelineStage(std::move(executablePipelineStage))
    , pipelineContext(std::move(pipelineExecutionContext))
    , reconfiguration(reconfiguration)
    , pipelineStatus(reconfiguration ? PipelineStatus::PipelineRunning : PipelineStatus::PipelineCreated)
    , activeProducers(numOfProducingPipelines)
    , successorPipelines(std::move(successorPipelines))
{
    /// nop
    NES_ASSERT(this->executablePipelineStage && this->pipelineContext && numOfProducingPipelines > 0, "Wrong pipeline stage argument");
}

ExecutionResult ExecutablePipeline::execute(Memory::TupleBuffer& inputBuffer, WorkerContextRef workerContext)
{
    NES_TRACE("Execute Pipeline Stage with id={} originId={} stage={}", queryId, inputBuffer.getOriginId(), pipelineId);

    switch (this->pipelineStatus.load())
    {
        case PipelineStatus::PipelineRunning: {
            auto res = executablePipelineStage->execute(inputBuffer, *pipelineContext.get(), workerContext);
            return res;
        }
        case PipelineStatus::PipelineStopped: {
            return ExecutionResult::Finished;
        }
        default: {
            NES_ERROR(
                "Cannot execute Pipeline Stage with id={} originId={} stage={} as pipeline is not running anymore.",
                queryId,
                inputBuffer.getOriginId(),
                pipelineId);
            return ExecutionResult::Error;
        }
    }
}

bool ExecutablePipeline::setup(const QueryManagerPtr&, const Memory::BufferManagerPtr&)
{
    /// Compile the ExecutablePipelineStage
    return executablePipelineStage->setup(*pipelineContext.get()) == 0;
}

bool ExecutablePipeline::start()
{
    auto expected = PipelineStatus::PipelineCreated;
    uint32_t localStateVariableId = 0;
    if (pipelineStatus.compare_exchange_strong(expected, PipelineStatus::PipelineRunning))
    {
        auto newReconf = ReconfigurationMessage(
            queryId, ReconfigurationType::Initialize, shared_from_this(), std::make_any<uint32_t>(activeProducers.load()));
        for (const auto& operatorHandler : pipelineContext->getOperatorHandlers())
        {
            operatorHandler->start(pipelineContext, localStateVariableId);
            localStateVariableId++;
        }
        queryManager->addReconfigurationMessage(queryId, newReconf, true);
        return true;
    }
    return false;
}

bool ExecutablePipeline::stop(QueryTerminationType)
{
    auto expected = PipelineStatus::PipelineRunning;
    if (pipelineStatus.compare_exchange_strong(expected, PipelineStatus::PipelineStopped))
    {
        return executablePipelineStage->stop(*pipelineContext.get()) == 0;
    }
    return expected == PipelineStatus::PipelineStopped;
}

bool ExecutablePipeline::fail()
{
    auto expected = PipelineStatus::PipelineRunning;
    if (pipelineStatus.compare_exchange_strong(expected, PipelineStatus::PipelineFailed))
    {
        return executablePipelineStage->stop(*pipelineContext.get()) == 0;
    }
    return expected == PipelineStatus::PipelineFailed;
}

bool ExecutablePipeline::isRunning() const
{
    return pipelineStatus.load() == PipelineStatus::PipelineRunning;
}

const std::vector<SuccessorExecutablePipeline>& ExecutablePipeline::getSuccessors() const
{
    return successorPipelines;
}

PipelineId ExecutablePipeline::getPipelineId() const
{
    return pipelineId;
}

QueryId ExecutablePipeline::getQueryId() const
{
    return queryId;
}

bool ExecutablePipeline::isReconfiguration() const
{
    return reconfiguration;
}

std::shared_ptr<ExecutablePipeline> ExecutablePipeline::create(
    PipelineId pipelineId,
    QueryId queryId,
    const QueryManagerPtr& queryManager,
    const PipelineExecutionContextPtr& pipelineExecutionContext,
    const ExecutablePipelineStagePtr& executablePipelineStage,
    uint32_t numOfProducingPipelines,
    const std::vector<SuccessorExecutablePipeline>& successorPipelines,
    bool reconfiguration)
{
    NES_ASSERT2_FMT(
        executablePipelineStage != nullptr,
        "Executable pipelinestage is null for " << pipelineId << "within the following query sub plan: " << queryId);
    NES_ASSERT2_FMT(
        pipelineExecutionContext != nullptr,
        "Pipeline context is null for " << pipelineId << "within the following query sub plan: " << queryId);

    return std::make_shared<ExecutablePipeline>(
        pipelineId,
        queryId,
        queryManager,
        pipelineExecutionContext,
        executablePipelineStage,
        numOfProducingPipelines,
        successorPipelines,
        reconfiguration);
}

void ExecutablePipeline::reconfigure(ReconfigurationMessage& task, WorkerContext& context)
{
    NES_DEBUG("Going to reconfigure pipeline {} belonging to query id: {} stage id: {}", pipelineId, queryId, pipelineId);
    Reconfigurable::reconfigure(task, context);
    switch (task.getType())
    {
        case ReconfigurationType::Initialize: {
            NES_ASSERT2_FMT(
                isRunning(),
                "Going to reconfigure a non-running pipeline " << pipelineId << " belonging to query id: " << queryId
                                                               << " stage id: " << pipelineId);
            auto refCnt = task.getUserData<uint32_t>();
            context.setObjectRefCnt(this, refCnt);
            break;
        }
        case ReconfigurationType::FailEndOfStream:
        case ReconfigurationType::HardEndOfStream:
        case ReconfigurationType::SoftEndOfStream: {
            if (context.decreaseObjectRefCnt(this) == 1)
            {
                for (const auto& operatorHandler : pipelineContext->getOperatorHandlers())
                {
                    operatorHandler->reconfigure(task, context);
                }
            }
            break;
        }
        default: {
            break;
        }
    }
}

void ExecutablePipeline::postReconfigurationCallback(ReconfigurationMessage& task)
{
    NES_DEBUG("Going to execute postReconfigurationCallback on pipeline belonging to subplanId: {} stage id: {}", queryId, pipelineId);
    Reconfigurable::postReconfigurationCallback(task);
    switch (task.getType())
    {
        case ReconfigurationType::FailEndOfStream: {
            auto prevProducerCounter = activeProducers.fetch_sub(1);
            if (prevProducerCounter == 1)
            { ///all producers sent EOS
                for (const auto& operatorHandler : pipelineContext->getOperatorHandlers())
                {
                    operatorHandler->postReconfigurationCallback(task);
                }
                /// mark the pipeline as failed
                bool markedAsFailed = fail();
                if (!markedAsFailed)
                {
                    NES_ERROR(
                        "Requested reconfiguration of pipeline belonging to subplanId: {} stage id: {} but pipeline was not "
                        "marked as failed",
                        queryId,
                        pipelineId);
                }
                /// tell the query manager about it
                queryManager->notifyPipelineCompletion(queryId, shared_from_this<ExecutablePipeline>(), QueryTerminationType::Failure);
                for (const auto& successorPipeline : successorPipelines)
                {
                    if (auto* pipe = std::get_if<std::shared_ptr<ExecutablePipeline>>(&successorPipeline))
                    {
                        auto newReconf = ReconfigurationMessage(queryId, task.getType(), *pipe);
                        queryManager->addReconfigurationMessage(queryId, newReconf, false);
                        NES_DEBUG(
                            "Going to reconfigure next pipeline belonging to subplanId: {} stage id: {} got "
                            "FailEndOfStream with nextPipeline",
                            queryId,
                            (*pipe)->getPipelineId());
                    }
                }
            }
        }
        case ReconfigurationType::HardEndOfStream:
        case ReconfigurationType::SoftEndOfStream: {
            ///we mantain a set of producers, and we will only trigger the end of stream once all producers have sent the EOS, for this we decrement the counter
            auto prevProducerCounter = activeProducers.fetch_sub(1);
            if (prevProducerCounter == 1)
            { ///all producers sent EOS
                NES_DEBUG("Reconfiguration of pipeline belonging to subplanId:{} stage id:{} reached prev=1", queryId, pipelineId);
                auto terminationType = task.getType() == Runtime::ReconfigurationType::SoftEndOfStream ? QueryTerminationType::Graceful
                                                                                                       : QueryTerminationType::HardStop;

                /// do not change the order here
                /// first, stop and drain handlers, if necessary
                ///todo #4282: drain without stopping for VersionDrainEvent
                for (const auto& operatorHandler : pipelineContext->getOperatorHandlers())
                {
                    operatorHandler->stop(terminationType, pipelineContext);
                }
                for (const auto& operatorHandler : pipelineContext->getOperatorHandlers())
                {
                    operatorHandler->postReconfigurationCallback(task);
                }
                /// second, stop pipeline, if not stopped yet
                bool stopped = stop(terminationType);
                if (!stopped)
                {
                    NES_ERROR(
                        "Requested reconfiguration of pipeline belonging to subplanId: {} stage id: {} but pipeline was not "
                        "stopped",
                        queryId,
                        pipelineId);
                }
                /// finally, notify query manager
                queryManager->notifyPipelineCompletion(queryId, shared_from_this<ExecutablePipeline>(), terminationType);

                for (const auto& successorPipeline : successorPipelines)
                {
                    if (auto* pipe = std::get_if<std::shared_ptr<ExecutablePipeline>>(&successorPipeline))
                    {
                        auto newReconf = ReconfigurationMessage(queryId, task.getType(), *pipe);
                        queryManager->addReconfigurationMessage(queryId, newReconf, false);
                        NES_DEBUG(
                            "Going to reconfigure next pipeline belonging to subplanId: {} stage id: {} got EndOfStream  "
                            "with nextPipeline",
                            queryId,
                            (*pipe)->getPipelineId());
                    }
                }
            }
            else
            {
                NES_DEBUG(
                    "Requested reconfiguration of pipeline belonging to subplanId: {} stage id: {} but refCount was {} "
                    "and now is {}",
                    queryId,
                    pipelineId,
                    (prevProducerCounter),
                    (prevProducerCounter - 1));
            }
            break;
        }
        default: {
            break;
        }
    }
}

}
