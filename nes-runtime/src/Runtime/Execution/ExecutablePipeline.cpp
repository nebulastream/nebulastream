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

#include <Reconfiguration/Metadata/DrainQueryMetadata.hpp>
#include <Reconfiguration/Metadata/UpdateAndDrainQueryMetadata.hpp>
#include <Runtime/Events.hpp>
#include <Runtime/Execution/ExecutablePipeline.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Util/Logger/Logger.hpp>
#include <atomic>
#include <chrono>

using namespace std::chrono_literals;
namespace NES::Runtime::Execution {
ExecutablePipeline::ExecutablePipeline(PipelineId pipelineId,
                                       SharedQueryId sharedQueryId,
                                       DecomposedQueryId decomposedQueryId,
                                       DecomposedQueryPlanVersion decomposedQueryVersion,
                                       QueryManagerPtr queryManager,
                                       PipelineExecutionContextPtr pipelineExecutionContext,
                                       ExecutablePipelineStagePtr executablePipelineStage,
                                       uint32_t numOfProducingPipelines,
                                       std::vector<SuccessorExecutablePipeline> successorPipelines,
                                       bool reconfiguration,
                                       bool isMigrationPipeline)
    : pipelineId(pipelineId), sharedQueryId(sharedQueryId), decomposedQueryId(decomposedQueryId),
      decomposedQueryVersion(decomposedQueryVersion), queryManager(queryManager),
      executablePipelineStage(std::move(executablePipelineStage)), pipelineContext(std::move(pipelineExecutionContext)),
      reconfiguration(reconfiguration), isMigrationPipeline(isMigrationPipeline),
      pipelineStatus(reconfiguration ? PipelineStatus::PipelineRunning : PipelineStatus::PipelineCreated),
      activeProducers(numOfProducingPipelines), successorPipelines(std::move(successorPipelines)) {
    // nop
    NES_ASSERT(this->executablePipelineStage && this->pipelineContext && numOfProducingPipelines > 0,
               "Wrong pipeline stage argument");
}

ExecutionResult ExecutablePipeline::execute(TupleBuffer& inputBuffer, WorkerContextRef workerContext) {
    NES_TRACE("Execute Pipeline Stage with id={} originId={} stage={}", decomposedQueryId, inputBuffer.getOriginId(), pipelineId);
    auto* task = inputBuffer.getBuffer<ReconfigurationMessage>();
    switch (this->pipelineStatus.load()) {
        case PipelineStatus::PipelineRunning: {
            isExecuted.fetch_add(1);
            auto res = executablePipelineStage->execute(inputBuffer, *pipelineContext.get(), workerContext);
            isExecuted.fetch_sub(1);
            return res;
        }
        case PipelineStatus::PipelineStopped: {
            return ExecutionResult::Finished;
        }
        default: {
            NES_ERROR("Cannot execute Pipeline Stage with id={} originId={} stage={} as pipeline is not running anymore.",
                      decomposedQueryId,
                      inputBuffer.getOriginId(),
                      pipelineId);
            return ExecutionResult::Error;
        }
    }
}

bool ExecutablePipeline::setup(const QueryManagerPtr&, const BufferManagerPtr&) {
    return executablePipelineStage->setup(*pipelineContext.get()) == 0;
}

bool ExecutablePipeline::start() {
    auto expected = PipelineStatus::PipelineCreated;
    uint32_t localStateVariableId = 0;
    if (pipelineStatus.compare_exchange_strong(expected, PipelineStatus::PipelineRunning)) {
        auto newReconf = ReconfigurationMessage(sharedQueryId,
                                                decomposedQueryId,
                                                decomposedQueryVersion,
                                                ReconfigurationType::Initialize,
                                                inherited0::shared_from_this(),
                                                std::make_any<uint32_t>(activeProducers.load()));
        for (const auto& operatorHandler : pipelineContext->getOperatorHandlers()) {
            operatorHandler->start(pipelineContext, localStateVariableId);
            localStateVariableId++;
        }
        queryManager->addReconfigurationMessage(sharedQueryId, decomposedQueryId, decomposedQueryVersion, newReconf, true);
        executablePipelineStage->start(*pipelineContext.get());
        return true;
    }
    return false;
}

bool ExecutablePipeline::shouldRecreate() {
    if (pipelineStatus == PipelineStatus::PipelineRunning) {
        auto newReconf = ReconfigurationMessage(sharedQueryId,
                                                decomposedQueryId,
                                                decomposedQueryVersion,
                                                ReconfigurationType::Initialize,
                                                inherited0::shared_from_this(),
                                                std::make_any<uint32_t>(activeProducers.load()));
        for (const auto& operatorHandler : pipelineContext->getOperatorHandlers()) {
            if (operatorHandler->shouldRecreate()) {
                return true;
            }
        }
    }
    return false;
}

bool ExecutablePipeline::recreate() {
    if (pipelineStatus == PipelineStatus::PipelineRunning) {
        auto newReconf = ReconfigurationMessage(sharedQueryId,
                                                decomposedQueryId,
                                                decomposedQueryVersion,
                                                ReconfigurationType::Initialize,
                                                inherited0::shared_from_this(),
                                                std::make_any<uint32_t>(activeProducers.load()));
        for (const auto& operatorHandler : pipelineContext->getOperatorHandlers()) {
            operatorHandler->recreate();
        }
        return true;
    }
    return false;
}
bool ExecutablePipeline::stop(QueryTerminationType) {
    auto expected = PipelineStatus::PipelineRunning;
    if (pipelineStatus.compare_exchange_strong(expected, PipelineStatus::PipelineStopped)) {
        while (isExecuted.load() != 0) {
        }
        return executablePipelineStage->stop(*pipelineContext.get()) == 0;
    }
    return expected == PipelineStatus::PipelineStopped;
}

bool ExecutablePipeline::fail() {
    auto expected = PipelineStatus::PipelineRunning;
    if (pipelineStatus.compare_exchange_strong(expected, PipelineStatus::PipelineFailed)) {
        return executablePipelineStage->stop(*pipelineContext.get()) == 0;
    }
    return expected == PipelineStatus::PipelineFailed;
}

bool ExecutablePipeline::isRunning() const { return pipelineStatus.load() == PipelineStatus::PipelineRunning; }

const std::vector<SuccessorExecutablePipeline>& ExecutablePipeline::getSuccessors() const { return successorPipelines; }

void ExecutablePipeline::onEvent(Runtime::BaseEvent& event) {
    NES_DEBUG("ExecutablePipeline::onEvent(event) called. pipelineId:  {}", this->pipelineId);
    if (event.getEventType() == EventType::kStartSourceEvent) {
        NES_DEBUG("ExecutablePipeline: Propagate startSourceEvent further upstream to predecessors, without workerContext.");

        for (auto predecessor : this->pipelineContext->getPredecessors()) {
            if (const auto* sourcePredecessor = std::get_if<std::weak_ptr<NES::DataSource>>(&predecessor)) {
                NES_DEBUG(
                    "ExecutablePipeline: Found Source in predecessor. Start it with startSourceEvent, without workerContext.");
                sourcePredecessor->lock()->onEvent(event);
            } else if (const auto* pipelinePredecessor =
                           std::get_if<std::weak_ptr<NES::Runtime::Execution::ExecutablePipeline>>(&predecessor)) {
                NES_DEBUG("ExecutablePipeline: Found Pipeline in Predecessors. Propagate startSourceEvent to it, without "
                          "workerContext.");
                pipelinePredecessor->lock()->onEvent(event);
            }
        }
    }
}

void ExecutablePipeline::onEvent(Runtime::BaseEvent& event, WorkerContextRef workerContext) {
    NES_DEBUG("ExecutablePipeline::onEvent(event, wrkContext) called. pipelineId:  {}", this->pipelineId);
    if (event.getEventType() == EventType::kStartSourceEvent) {
        NES_DEBUG("ExecutablePipeline: Propagate startSourceEvent further upstream to predecessors, with workerContext.");

        for (auto predecessor : this->pipelineContext->getPredecessors()) {
            if (const auto* sourcePredecessor = std::get_if<std::weak_ptr<NES::DataSource>>(&predecessor)) {
                NES_DEBUG("ExecutablePipeline: Found Source in predecessor. Start it with startSourceEvent, with workerContext.");
                sourcePredecessor->lock()->onEvent(event, workerContext);
            } else if (const auto* pipelinePredecessor =
                           std::get_if<std::weak_ptr<NES::Runtime::Execution::ExecutablePipeline>>(&predecessor)) {
                NES_DEBUG(
                    "ExecutablePipeline: Found Pipeline in Predecessors. Propagate startSourceEvent to it, with workerContext.");
                pipelinePredecessor->lock()->onEvent(event, workerContext);
            }
        }
    }
}

PipelineId ExecutablePipeline::getPipelineId() const { return pipelineId; }

SharedQueryId ExecutablePipeline::getSharedQueryId() const { return sharedQueryId; }

DecomposedQueryId ExecutablePipeline::getDecomposedQueryId() const { return decomposedQueryId; }

DecomposedQueryPlanVersion ExecutablePipeline::getDecomposedQueryVersion() const { return decomposedQueryVersion; }

bool ExecutablePipeline::isReconfiguration() const { return reconfiguration; }

ExecutablePipelinePtr ExecutablePipeline::create(PipelineId pipelineId,
                                                 SharedQueryId sharedQueryId,
                                                 DecomposedQueryId decomposedQueryId,
                                                 DecomposedQueryPlanVersion decomposedQueryVersion,
                                                 const QueryManagerPtr& queryManager,
                                                 const PipelineExecutionContextPtr& pipelineExecutionContext,
                                                 const ExecutablePipelineStagePtr& executablePipelineStage,
                                                 uint32_t numOfProducingPipelines,
                                                 const std::vector<SuccessorExecutablePipeline>& successorPipelines,
                                                 bool reconfiguration,
                                                 bool isMigrationPipeline) {
    NES_ASSERT2_FMT(executablePipelineStage != nullptr,
                    "Executable pipelinestage is null for " << pipelineId << "within the following query sub plan: "
                                                            << decomposedQueryId << "." << decomposedQueryVersion);
    NES_ASSERT2_FMT(pipelineExecutionContext != nullptr,
                    "Pipeline context is null for " << pipelineId << "within the following query sub plan: " << decomposedQueryId
                                                    << "." << decomposedQueryVersion);

    return std::make_shared<ExecutablePipeline>(pipelineId,
                                                sharedQueryId,
                                                decomposedQueryId,
                                                decomposedQueryVersion,
                                                queryManager,
                                                pipelineExecutionContext,
                                                executablePipelineStage,
                                                numOfProducingPipelines,
                                                successorPipelines,
                                                reconfiguration,
                                                isMigrationPipeline);
}

void ExecutablePipeline::reconfigure(ReconfigurationMessage& task, WorkerContext& context) {
    NES_DEBUG("Going to reconfigure pipeline {} belonging to query id: {} stage id: {}",
              pipelineId,
              decomposedQueryId,
              pipelineId);
    Reconfigurable::reconfigure(task, context);
    switch (task.getType()) {
        case ReconfigurationType::Initialize: {
            NES_ASSERT2_FMT(isRunning(),
                            "Going to reconfigure a non-running pipeline "
                                << pipelineId << " belonging to query id: " << decomposedQueryId << " stage id: " << pipelineId);
            auto refCnt = task.getUserData<uint32_t>();
            context.setObjectRefCnt(this, refCnt);
            break;
        }
        case ReconfigurationType::ReconfigurationMarker: {
            auto marker = task.getUserData<ReconfigurationMarkerPtr>();
            auto event = marker->getReconfigurationEvent(DecomposedQueryIdWithVersion(decomposedQueryId, decomposedQueryVersion));
//            if (!event) {
//                NES_ERROR("No event found for plan {} version {}", decomposedQueryId, decomposedQueryVersion);
//                for (auto& [i, e] : marker->getAllReconfigurationMarkerEvents()) {
//                    NES_ERROR("Marker contains event for for plan {} version {} of type {}", i.id, i.version, magic_enum::enum_name(e->reconfigurationMetadata->reconfigurationMetadataType));
//                }
//            }
            NES_ASSERT2_FMT(event, "Markers should only be propagated to a pipeline if the plan is to be reconfigured");

            // if this is pipeline for migration and reconfiguration is of type drain, then migration should be started
            if (event.value()->reconfigurationMetadata->instanceOf<DrainQueryMetadata>() && isMigrationPipeline) {
                NES_ERROR("serializing");
                if (!serialized) {
                    for (auto& operatorHandler : pipelineContext->getOperatorHandlers()) {
                        // NES_ERROR("sync context wait {}", context.getId());
                        preSerBarrier->wait();
                        // NES_ERROR("sync contexts synced {}", context.getId());
                        std::call_once(serializeOnceFlag, [&context, &operatorHandler, this]() {
                            NES_ERROR("context {} took serialization", context.getId());
                            operatorHandler->serializeOperatorHandlerForMigration();
                            serialized = true;
                            conditionSerialized.notify_all();
                        });
                        std::unique_lock<std::mutex> lock(serializeMtx);
                        conditionSerialized.wait(lock, [this] {
                            return serialized.load(std::memory_order_acquire);
                        });
                        lock.unlock();
                        // NES_ERROR("context {} continued to send data", context.getId());
                        sendBuffers(operatorHandler, context);
                    }
                }
            }

            if (!(event.value()->reconfigurationMetadata->instanceOf<DrainQueryMetadata>())
                && !(event.value()->reconfigurationMetadata->instanceOf<UpdateAndDrainQueryMetadata>())) {
                NES_WARNING("Non drain reconfigurations not yes supported");
                NES_NOT_IMPLEMENTED();
            }

            //todo #5119: What has to be done when the reconfiguration event is not of type drain?
        }
        case ReconfigurationType::FailEndOfStream:
        case ReconfigurationType::HardEndOfStream:
        case ReconfigurationType::SoftEndOfStream: {
            if (context.decreaseObjectRefCnt(this) == 1) {
                for (const auto& operatorHandler : pipelineContext->getOperatorHandlers()) {
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

void ExecutablePipeline::sendBuffers(OperatorHandlerPtr operatorHandler, WorkerContext& context) {
    auto dataToMigrate = operatorHandler->getSerializedPortion(context.getId().getRawValue() % 4);
    auto startTime = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    NES_ERROR("context {} started sending {} buffers from pipeline at {}", context.getId(), dataToMigrate.size(), startTime);
    for (auto& buffer : dataToMigrate) {
        pipelineContext->migrateBuffer(buffer, context);
    }
    auto endTime = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    NES_ERROR("context {} finished sending {} buffers from pipeline at {}", context.getId(), dataToMigrate.size(), endTime);
}

void ExecutablePipeline::propagateReconfiguration(ReconfigurationMessage& task) {
    auto terminationType = reconfigurationTypeToTerminationType(task.getType());

    // do not change the order here
    // first, stop and drain handlers, if necessary
    //todo #4282: drain without stopping for VersionDrainEvent
    for (const auto& operatorHandler : pipelineContext->getOperatorHandlers()) {
        operatorHandler->stop(terminationType, pipelineContext);
    }
    for (const auto& operatorHandler : pipelineContext->getOperatorHandlers()) {
        operatorHandler->postReconfigurationCallback(task);
    }
    // second, stop pipeline, if not stopped yet
    stop(terminationType);
    // finally, notify query manager
    queryManager->notifyPipelineCompletion(decomposedQueryId,
                                           inherited0::shared_from_this<ExecutablePipeline>(),
                                           terminationType);
    //extract marker from user data
    auto marker = task.getUserData<ReconfigurationMarkerPtr>();

    for (const auto& successorPipeline : successorPipelines) {
        if (auto* pipe = std::get_if<ExecutablePipelinePtr>(&successorPipeline)) {
            auto newReconf =
                ReconfigurationMessage(sharedQueryId, decomposedQueryId, decomposedQueryVersion, task.getType(), *pipe, marker);
            queryManager->addReconfigurationMessage(sharedQueryId, decomposedQueryId, decomposedQueryVersion, newReconf, false);
            NES_DEBUG("Going to reconfigure next pipeline belonging to subplanId: {} stage id: {} got EndOfStream  "
                      "with nextPipeline",
                      decomposedQueryId,
                      (*pipe)->getPipelineId());
        } else if (auto* sink = std::get_if<DataSinkPtr>(&successorPipeline)) {
            auto newReconf =
                ReconfigurationMessage(sharedQueryId, decomposedQueryId, decomposedQueryVersion, task.getType(), *sink, marker);
            queryManager->addReconfigurationMessage(sharedQueryId, decomposedQueryId, decomposedQueryVersion, newReconf, false);
            NES_DEBUG("Going to reconfigure next sink belonging to subplanId: {} sink id: {} got EndOfStream  with "
                      "nextPipeline",
                      decomposedQueryId,
                      (*sink)->toString());
        }
    }
}

void ExecutablePipeline::propagateEndOfStream(ReconfigurationMessage& task) {
    auto terminationType = reconfigurationTypeToTerminationType(task.getType());

    // do not change the order here
    // first, stop and drain handlers, if necessary
    //todo #4282: drain without stopping for VersionDrainEvent
    for (const auto& operatorHandler : pipelineContext->getOperatorHandlers()) {
        operatorHandler->stop(terminationType, pipelineContext);
    }
    for (const auto& operatorHandler : pipelineContext->getOperatorHandlers()) {
        operatorHandler->postReconfigurationCallback(task);
    }
    // second, stop pipeline, if not stopped yet
    stop(terminationType);
    // finally, notify query manager
    queryManager->notifyPipelineCompletion(decomposedQueryId,
                                           inherited0::shared_from_this<ExecutablePipeline>(),
                                           terminationType);

    for (const auto& successorPipeline : successorPipelines) {
        if (auto* pipe = std::get_if<ExecutablePipelinePtr>(&successorPipeline)) {
            auto newReconf =
                ReconfigurationMessage(sharedQueryId, decomposedQueryId, decomposedQueryVersion, task.getType(), *pipe);
            queryManager->addReconfigurationMessage(sharedQueryId, decomposedQueryId, decomposedQueryVersion, newReconf, false);
            NES_DEBUG("Going to reconfigure next pipeline belonging to subplanId: {} stage id: {} got EndOfStream  "
                      "with nextPipeline",
                      decomposedQueryId,
                      (*pipe)->getPipelineId());
        } else if (auto* sink = std::get_if<DataSinkPtr>(&successorPipeline)) {
            auto newReconf =
                ReconfigurationMessage(sharedQueryId, decomposedQueryId, decomposedQueryVersion, task.getType(), *sink);
            queryManager->addReconfigurationMessage(sharedQueryId, decomposedQueryId, decomposedQueryVersion, newReconf, false);
            NES_DEBUG("Going to reconfigure next sink belonging to subplanId: {} sink id: {} got EndOfStream  with "
                      "nextPipeline",
                      decomposedQueryId,
                      (*sink)->toString());
        }
    }
}

void ExecutablePipeline::postReconfigurationCallback(ReconfigurationMessage& task) {
    NES_DEBUG("Going to execute postReconfigurationCallback on pipeline belonging to subplanId: {} stage id: {}",
              decomposedQueryId,
              pipelineId);
    Reconfigurable::postReconfigurationCallback(task);
    switch (task.getType()) {
        case ReconfigurationType::ReconfigurationMarker: {
            //todo #5119: handle non shutdown cases here?
            auto prevProducerCounter = activeProducers.fetch_sub(1);
            if (prevProducerCounter == 1) {//all producers sent EOS
                NES_DEBUG("Reconfiguration of pipeline belonging to subplanId:{} stage id:{} reached prev=1",
                          decomposedQueryId,
                          pipelineId);
                propagateReconfiguration(task);
            } else {
                NES_DEBUG("Requested reconfiguration of pipeline belonging to subplanId: {} stage id: {} but refCount was {} "
                          "and now is {}",
                          decomposedQueryId,
                          pipelineId,
                          (prevProducerCounter),
                          (prevProducerCounter - 1));
            }
            break;
        }
        case ReconfigurationType::FailEndOfStream: {
            auto prevProducerCounter = activeProducers.fetch_sub(1);
            if (prevProducerCounter == 1) {//all producers sent EOS
                for (const auto& operatorHandler : pipelineContext->getOperatorHandlers()) {
                    operatorHandler->postReconfigurationCallback(task);
                }
                // mark the pipeline as failed
                fail();
                // tell the query manager about it
                queryManager->notifyPipelineCompletion(decomposedQueryId,
                                                       inherited0::shared_from_this<ExecutablePipeline>(),
                                                       Runtime::QueryTerminationType::Failure);
                for (const auto& successorPipeline : successorPipelines) {
                    if (auto* pipe = std::get_if<ExecutablePipelinePtr>(&successorPipeline)) {
                        auto newReconf = ReconfigurationMessage(sharedQueryId,
                                                                decomposedQueryId,
                                                                decomposedQueryVersion,
                                                                task.getType(),
                                                                *pipe);
                        queryManager->addReconfigurationMessage(sharedQueryId,
                                                                decomposedQueryId,
                                                                decomposedQueryVersion,
                                                                newReconf,
                                                                false);
                        NES_DEBUG("Going to reconfigure next pipeline belonging to subplanId: {} stage id: {} got "
                                  "FailEndOfStream with nextPipeline",
                                  decomposedQueryId,
                                  (*pipe)->getPipelineId());
                    } else if (auto* sink = std::get_if<DataSinkPtr>(&successorPipeline)) {
                        auto newReconf = ReconfigurationMessage(sharedQueryId,
                                                                decomposedQueryId,
                                                                decomposedQueryVersion,
                                                                task.getType(),
                                                                *sink);
                        queryManager->addReconfigurationMessage(sharedQueryId,
                                                                decomposedQueryId,
                                                                decomposedQueryVersion,
                                                                newReconf,
                                                                false);
                        NES_DEBUG("Going to reconfigure next sink belonging to subplanId: {} sink id:{} got FailEndOfStream  "
                                  "with nextPipeline",
                                  decomposedQueryId,
                                  (*sink)->toString());
                    }
                }
            }
        }
        case ReconfigurationType::HardEndOfStream:
        case ReconfigurationType::SoftEndOfStream: {
            //we mantain a set of producers, and we will only trigger the end of stream once all producers have sent the EOS, for this we decrement the counter
            auto prevProducerCounter = activeProducers.fetch_sub(1);
            if (prevProducerCounter == 1) {//all producers sent EOS
                NES_DEBUG("Reconfiguration of pipeline belonging to subplanId:{} stage id:{} reached prev=1",
                          decomposedQueryId,
                          pipelineId);
                propagateEndOfStream(task);
            } else {
                NES_DEBUG("Requested reconfiguration of pipeline belonging to subplanId: {} stage id: {} but refCount was {} "
                          "and now is {}",
                          decomposedQueryId,
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

void ExecutablePipeline::stopQueryFromSharedJoin(QueryId queryId) {
    NES_INFO("stopping query from shared join with id {}", queryId.getRawValue())
    for (const auto& operatorHandler : pipelineContext->getOperatorHandlers()) {
        if (operatorHandler->instanceOf<OperatorHandlerSlices>()) {
            auto opHandler = operatorHandler->as<OperatorHandlerSlices>();
            opHandler->removeQueryFromSharedJoin(queryId);
            NES_INFO("An op handler of type OperatorHandlerSlices (SJOH)")
        } else {
            NES_INFO("An op handler of a different type than OperatorHandlerSlices (SJOH)")
        }
    }
}

void ExecutablePipeline::addQueryToSharedJoin(NES::QueryId queryId,
                                              uint64_t deploymentTime,
                                              SharedJoinApproach sharedJoinApproach) {
    NES_INFO("stopping query from shared join with id {}", queryId.getRawValue())
    for (const auto& operatorHandler : pipelineContext->getOperatorHandlers()) {
        if (operatorHandler->instanceOf<OperatorHandlerSlices>()) {
            auto opHandler = operatorHandler->as<OperatorHandlerSlices>();
            switch (sharedJoinApproach) {
                case SharedJoinApproach::APPROACH_PROBING:
                    opHandler->addQueryToSharedJoinApproachProbing(queryId, deploymentTime);
                    break;
                case SharedJoinApproach::APPROACH_DELETING:
                    opHandler->addQueryToSharedJoinApproachDeleting(queryId, deploymentTime);
                    break;
                default: NES_ERROR("not implemented") break;
            }
            NES_INFO("An op handler of type OperatorHandlerSlices (SJOH)")
        } else {
            NES_INFO("An op handler of a different type than OperatorHandlerSlices (SJOH)")
        }
    }
}

}// namespace NES::Runtime::Execution
