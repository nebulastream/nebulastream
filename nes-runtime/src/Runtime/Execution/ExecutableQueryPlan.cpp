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

#include <Network/NetworkSink.hpp>
#include <Runtime/Execution/ExecutablePipeline.hpp>
#include <Runtime/Execution/ExecutableQueryPlan.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryManager.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <iomanip>
#include <sstream>

namespace NES::Runtime::Execution {

ExecutableQueryPlan::ExecutableQueryPlan(SharedQueryId sharedQueryId,
                                         DecomposedQueryId decomposedQueryId,
                                         DecomposedQueryPlanVersion decomposedQueryVersion,
                                         std::vector<DataSourcePtr>&& sources,
                                         std::vector<DataSinkPtr>&& sinks,
                                         std::vector<ExecutablePipelinePtr>&& pipelines,
                                         QueryManagerPtr&& queryManager,
                                         BufferManagerPtr&& bufferManager)
    : sharedQueryId(sharedQueryId), decomposedQueryId(decomposedQueryId), decomposedQueryVersion(decomposedQueryVersion),
      sources(std::move(sources)), sinks(std::move(sinks)), pipelines(std::move(pipelines)),
      queryManager(std::move(queryManager)), bufferManager(std::move(bufferManager)),
      qepStatus(Execution::ExecutableQueryPlanStatus::Created),
      qepTerminationStatusFuture(qepTerminationStatusPromise.get_future()), drainedSourceCount(sources.size()),
      successor(nullptr), drainMarker(nullptr) {
    // the +1 is the termination token for the query plan itself
    numOfTerminationTokens.store(1 + this->sources.size() + this->pipelines.size() + this->sinks.size());
}

ExecutableQueryPlanPtr ExecutableQueryPlan::create(SharedQueryId sharedQueryId,
                                                   DecomposedQueryId decomposedQueryId,
                                                   DecomposedQueryPlanVersion decomposedQueryVersion,
                                                   std::vector<DataSourcePtr> sources,
                                                   std::vector<DataSinkPtr> sinks,
                                                   std::vector<ExecutablePipelinePtr> pipelines,
                                                   QueryManagerPtr queryManager,
                                                   BufferManagerPtr bufferManager) {
    return std::make_shared<ExecutableQueryPlan>(sharedQueryId,
                                                 decomposedQueryId,
                                                 decomposedQueryVersion,
                                                 std::move(sources),
                                                 std::move(sinks),
                                                 std::move(pipelines),
                                                 std::move(queryManager),
                                                 std::move(bufferManager));
}

SharedQueryId ExecutableQueryPlan::getSharedQueryId() const { return sharedQueryId; }

const std::vector<ExecutablePipelinePtr>& ExecutableQueryPlan::getPipelines() const { return pipelines; }

DecomposedQueryId ExecutableQueryPlan::getDecomposedQueryId() const { return decomposedQueryId; }

DecomposedQueryPlanVersion ExecutableQueryPlan::getDecomposedQueryVersion() const { return decomposedQueryVersion; }

void ExecutableQueryPlan::updateDecomposedQueryVersion(DecomposedQueryPlanVersion newVersion) {
    decomposedQueryVersion = newVersion;
};

ExecutableQueryPlan::~ExecutableQueryPlan() {
    NES_DEBUG("destroy qep {} {}", sharedQueryId, decomposedQueryId);
    NES_ASSERT(qepStatus.load() == Execution::ExecutableQueryPlanStatus::Created
                   || qepStatus.load() == Execution::ExecutableQueryPlanStatus::Stopped
                   || qepStatus.load() == Execution::ExecutableQueryPlanStatus::ErrorState,
               "QueryPlan is created but not executing " << sharedQueryId);
    destroy();
}

std::shared_future<ExecutableQueryPlanResult> ExecutableQueryPlan::getTerminationFuture() {
    return qepTerminationStatusFuture.share();
}

bool ExecutableQueryPlan::fail() {
    bool ret = true;
    auto expected = Execution::ExecutableQueryPlanStatus::Running;
    if (qepStatus.compare_exchange_strong(expected, Execution::ExecutableQueryPlanStatus::ErrorState)) {
        NES_DEBUG("QueryExecutionPlan: fail query={} decomposedlplan={}", sharedQueryId, decomposedQueryId);
        for (auto& stage : pipelines) {
            if (!stage->fail()) {
                std::stringstream s;
                s << stage;
                std::string stageAsString = s.str();
                NES_ERROR("QueryExecutionPlan: fail failed for stage {}", stageAsString);
                ret = false;
            }
        }
        qepTerminationStatusPromise.set_value(ExecutableQueryPlanResult::Fail);
    }

    if (!ret) {
        qepStatus.store(Execution::ExecutableQueryPlanStatus::ErrorState);
    }
    return ret;
}

bool ExecutableQueryPlan::shouldDelayStart() {
    auto shouldRecreate = false;

    for (auto& pipeline : pipelines) {
        shouldRecreate |= pipeline->shouldRecreate();
    }

    return shouldRecreate;
}

ExecutableQueryPlanStatus ExecutableQueryPlan::getStatus() const { return qepStatus.load(); }

bool ExecutableQueryPlan::setup() {
    NES_DEBUG("QueryExecutionPlan: setup sharedQueryId={} decomposedQueryId={}", sharedQueryId, decomposedQueryId);
    auto expected = Execution::ExecutableQueryPlanStatus::Created;
    if (qepStatus.compare_exchange_strong(expected, Execution::ExecutableQueryPlanStatus::Deployed)) {
        for (auto& stage : pipelines) {
            if (!stage->setup(queryManager, bufferManager)) {
                NES_ERROR("QueryExecutionPlan: setup failed! {} {}", sharedQueryId, decomposedQueryId);
                this->stop();
                return false;
            }
        }
    } else {
        NES_THROW_RUNTIME_ERROR("QEP expected to be Created but was not");
    }
    return true;
}

bool ExecutableQueryPlan::start() {
    NES_DEBUG("QueryExecutionPlan: start query={} decomposedplan={}", sharedQueryId, decomposedQueryId);
    auto expected = Execution::ExecutableQueryPlanStatus::Deployed;
    if (qepStatus.compare_exchange_strong(expected, Execution::ExecutableQueryPlanStatus::Running)) {
        for (auto& stage : pipelines) {
            NES_DEBUG("ExecutableQueryPlan::start qep={} pipe={}", stage->getDecomposedQueryId(), stage->getPipelineId());
            if (!stage->start()) {
                NES_ERROR("QueryExecutionPlan: start failed! query={} decomposedplan={}", sharedQueryId, decomposedQueryId);
                this->stop();
                return false;
            }
        }
    } else {
        NES_THROW_RUNTIME_ERROR("QEP expected to be Deployed but was not");
    }
    return true;
}

bool ExecutableQueryPlan::checkRecreation() {
    NES_DEBUG("QueryExecutionPlan: check recreations for query={} decomposedplan={}", sharedQueryId, decomposedQueryId);
    auto expected = Execution::ExecutableQueryPlanStatus::Running;
    if (qepStatus == expected) {
        for (auto& stage : pipelines) {
            NES_DEBUG("ExecutableQueryPlan::recreate qep={} pipe={}", stage->getDecomposedQueryId(), stage->getPipelineId());
            if (!stage->recreate()) {
                NES_ERROR("QueryExecutionPlan: start failed! query={} decomposedplan={}", sharedQueryId, decomposedQueryId);
                this->stop();
                return false;
            }
        }
    } else {
        NES_THROW_RUNTIME_ERROR("QEP expected to be Deployed but was not");
    }
    return true;
}

QueryManagerPtr ExecutableQueryPlan::getQueryManager() { return queryManager; }

BufferManagerPtr ExecutableQueryPlan::getBufferManager() { return bufferManager; }

const std::vector<DataSourcePtr>& ExecutableQueryPlan::getSources() const { return sources; }

const std::vector<DataSinkPtr>& ExecutableQueryPlan::getSinks() const { return sinks; }

bool ExecutableQueryPlan::stop() {
    bool allStagesStopped = true;
    auto expected = Execution::ExecutableQueryPlanStatus::Running;
    NES_WARNING("QueryExecutionPlan: stop sharedQueryId={} decomposedQueryId={}", sharedQueryId, decomposedQueryId);
    if (qepStatus.compare_exchange_strong(expected, Execution::ExecutableQueryPlanStatus::Stopped)) {
        NES_DEBUG("QueryExecutionPlan: stop {}-{} is marked as stopped now", sharedQueryId, decomposedQueryId);
        for (auto& stage : pipelines) {
            if (!stage->stop(QueryTerminationType::HardStop)) {
                std::stringstream s;
                s << stage;
                std::string stageAsString = s.str();
                NES_ERROR("QueryExecutionPlan: stop failed for stage {}", stageAsString);
                allStagesStopped = false;
            }
        }

        bufferManager.reset();

        if (allStagesStopped) {
            qepTerminationStatusPromise.set_value(ExecutableQueryPlanResult::Ok);
            return true;// correct stop
        }

        qepStatus.store(Execution::ExecutableQueryPlanStatus::ErrorState);
        qepTerminationStatusPromise.set_value(ExecutableQueryPlanResult::Fail);

        return false;// one stage failed to stop
    }

    if (expected == Execution::ExecutableQueryPlanStatus::Stopped) {
        return true;// we have tried to stop the same QEP twice.
    }
    NES_ERROR("Something is wrong with query {} as it was not possible to stop", decomposedQueryId);
    // if we get there it mean the CAS failed and expected is the current value
    while (!qepStatus.compare_exchange_strong(expected, Execution::ExecutableQueryPlanStatus::ErrorState)) {
        // try to install ErrorState
    }

    qepTerminationStatusPromise.set_value(ExecutableQueryPlanResult::Fail);

    bufferManager.reset();
    return false;
}

void ExecutableQueryPlan::postReconfigurationCallback(ReconfigurationMessage& task) {
    Reconfigurable::postReconfigurationCallback(task);
    switch (task.getType()) {
        case ReconfigurationType::FailEndOfStream: {
            NES_DEBUG("Executing FailEndOfStream on qep sharedQueryId={} decomposedQueryId={}", sharedQueryId, decomposedQueryId);
            NES_ASSERT2_FMT((numOfTerminationTokens.fetch_sub(1) == 1),
                            "Invalid FailEndOfStream on qep sharedQueryId=" << sharedQueryId
                                                                            << " decomposedQueryId=" << decomposedQueryId);
            NES_ASSERT2_FMT(fail(), "Cannot fail sharedQueryId=" << sharedQueryId << " decomposedQueryId=" << decomposedQueryId);
            NES_DEBUG("QueryExecutionPlan: query plan {} decomposedplan {} is marked as failed now",
                      sharedQueryId,
                      decomposedQueryId);
            queryManager->notifyQueryStatusChange(shared_from_base<ExecutableQueryPlan>(),
                                                  Execution::ExecutableQueryPlanStatus::ErrorState);
            break;
        }
        case ReconfigurationType::HardEndOfStream: {
            NES_DEBUG("Executing HardEndOfStream on qep sharedQueryId={} decomposedQueryId={}", sharedQueryId, decomposedQueryId);
            NES_ASSERT2_FMT((numOfTerminationTokens.fetch_sub(1) == 1),
                            "Invalid HardEndOfStream on qep sharedQueryId=" << sharedQueryId
                                                                            << " decomposedQueryId=" << decomposedQueryId);
            NES_ASSERT2_FMT(stop(),
                            "Cannot hard stop qep sharedQueryId=" << sharedQueryId << " decomposedQueryId=" << decomposedQueryId);
            queryManager->notifyQueryStatusChange(shared_from_base<ExecutableQueryPlan>(),
                                                  Execution::ExecutableQueryPlanStatus::Stopped);
            break;
        }
        //if a reconfiguration marker was passed into the function it means that the plan has to be shut down
        case ReconfigurationType::ReconfigurationMarker:
        case ReconfigurationType::SoftEndOfStream: {
            NES_WARNING("QueryExecutionPlan: soft stop request received for query plan {} decomposed plan {} left tokens = {}",
                        sharedQueryId,
                        decomposedQueryId,
                        numOfTerminationTokens);
            if (numOfTerminationTokens.fetch_sub(1) == 1) {
                auto expected = Execution::ExecutableQueryPlanStatus::Running;
                if (qepStatus.compare_exchange_strong(expected, Execution::ExecutableQueryPlanStatus::Finished)) {
                    // if CAS fails - it means the query was already stopped or failed
                    NES_DEBUG("QueryExecutionPlan: query plan {} decomposedplan {} is marked as (soft) stopped now",
                              sharedQueryId,
                              decomposedQueryId);
                    qepTerminationStatusPromise.set_value(ExecutableQueryPlanResult::Ok);
                    if (task.getType() != ReconfigurationType::ReconfigurationMarker) {
                        queryManager->notifyQueryStatusChange(shared_from_base<ExecutableQueryPlan>(),
                                                              Execution::ExecutableQueryPlanStatus::Finished);
                    }
                    return;
                }
            } else {
                NES_DEBUG("QueryExecutionPlan: query plan {} decomposed query plan {} was already marked as stopped",
                          sharedQueryId,
                          decomposedQueryId);
            }
            break;
        }
        default: {
            break;
        }
    }
}

void ExecutableQueryPlan::destroy() {
    // sanity checks: ensure we can destroy stopped instances
    auto expected = 0u;
    if (qepStatus == ExecutableQueryPlanStatus::Created) {
        return;// there is nothing to destroy;
    }
    NES_ASSERT2_FMT(numOfTerminationTokens.compare_exchange_strong(expected, uint32_t(-1)),
                    "Destroying still active query plan, tokens left=" << expected);
    for (const auto& source : sources) {
        NES_ASSERT(!source->isRunning(), "Source " << source->getOperatorId() << " is still running");
    }
    for (const auto& pipeline : pipelines) {
        NES_ASSERT(!pipeline->isRunning(), "Pipeline " << pipeline->getPipelineId() << " is still running");
    }
    sources.clear();
    pipelines.clear();
    sinks.clear();
    bufferManager.reset();
}

void ExecutableQueryPlan::onEvent(BaseEvent&) {
    // nop :: left on purpose -> fill this in when you want to support events
}

void ExecutableQueryPlan::notifySourceCompletion(DataSourcePtr source, QueryTerminationType terminationType) {
    NES_DEBUG("QEP {} Source {} is notifying completion: {}", decomposedQueryId, source->getOperatorId(), terminationType);
    NES_ASSERT2_FMT(qepStatus.load() == ExecutableQueryPlanStatus::Running,
                    "Cannot complete source in the state " << magic_enum::enum_name(qepStatus.load())
                                                           << " on non running decomposed query plan id=" << decomposedQueryId);
    auto it = std::find(sources.begin(), sources.end(), source);
    NES_ASSERT2_FMT(it != sources.end(),
                    "Cannot find source " << source->getOperatorId() << " in decomposed query plan " << decomposedQueryId);
    uint32_t tokensLeft = numOfTerminationTokens.fetch_sub(1);
    NES_ASSERT2_FMT(tokensLeft >= 1, "Source was last termination token for " << decomposedQueryId << " = " << terminationType);
    NES_DEBUG("QEP {} Source {} is terminated; tokens left = {}", decomposedQueryId, source->getOperatorId(), (tokensLeft - 1));
    // the following check is necessary because a data sources first emits an EoS marker and then calls this method.
    // However, it might happen that the marker is so fast that one possible execution is as follows:
    // 1) Data Source emits EoS marker
    // 2) Next pipeline/sink picks the marker and finishes by decreasing `numOfTerminationTokens`
    // 3) DataSource invokes `notifySourceCompletion`
    // as a result, the data source might be the last one to decrease the `numOfTerminationTokens` thus it has to
    // trigger the QEP reconfiguration
    auto reconfigurationType = terminationTypeToReconfigurationType(terminationType);
    if (tokensLeft == 2) {// this is the second last token to be removed (last one is the qep itself)
        auto reconfMessageQEP = ReconfigurationMessage(getSharedQueryId(),
                                                       getDecomposedQueryId(),
                                                       getDecomposedQueryVersion(),
                                                       reconfigurationType,
                                                       Reconfigurable::shared_from_this<ExecutableQueryPlan>());
        queryManager->addReconfigurationMessage(getSharedQueryId(),
                                                getDecomposedQueryId(),
                                                getDecomposedQueryVersion(),
                                                reconfMessageQEP,
                                                false);
    }
}

void ExecutableQueryPlan::notifyPipelineCompletion(ExecutablePipelinePtr pipeline, QueryTerminationType terminationType) {
    NES_ASSERT2_FMT(qepStatus.load() == ExecutableQueryPlanStatus::Running,
                    "Cannot complete pipeline on non running query plan id=" << decomposedQueryId);
    auto it = std::find(pipelines.begin(), pipelines.end(), pipeline);
    NES_ASSERT2_FMT(it != pipelines.end(),
                    "Cannot find pipeline " << pipeline->getPipelineId() << " in decomposed query plan " << decomposedQueryId);
    uint32_t tokensLeft = numOfTerminationTokens.fetch_sub(1);
    NES_ASSERT2_FMT(tokensLeft >= 1, "Pipeline was last termination token for " << decomposedQueryId);
    NES_DEBUG("QEP {} Pipeline {} is terminated; tokens left = {}",
              decomposedQueryId,
              pipeline->getPipelineId(),
              (tokensLeft - 1));
    // the same applies here for the pipeline
    auto reconfigurationType = terminationTypeToReconfigurationType(terminationType);
    if (tokensLeft == 2) {// this is the second last token to be removed (last one is the qep itself)
        auto reconfMessageQEP = ReconfigurationMessage(getSharedQueryId(),
                                                       getDecomposedQueryId(),
                                                       getDecomposedQueryVersion(),
                                                       reconfigurationType,
                                                       Reconfigurable::shared_from_this<ExecutableQueryPlan>());
        queryManager->addReconfigurationMessage(getSharedQueryId(),
                                                getDecomposedQueryId(),
                                                getDecomposedQueryVersion(),
                                                reconfMessageQEP,
                                                false);
    }
}

void ExecutableQueryPlan::notifySinkCompletion(DataSinkPtr sink, QueryTerminationType terminationType) {
    NES_ASSERT2_FMT(qepStatus.load() == ExecutableQueryPlanStatus::Running,
                    "Cannot complete sink on non running query plan id=" << decomposedQueryId);
    auto it = std::find(sinks.begin(), sinks.end(), sink);
    NES_ASSERT2_FMT(it != sinks.end(),
                    "Cannot find sink " << sink->toString() << " in decomposed query plan " << decomposedQueryId);
    uint32_t tokensLeft = numOfTerminationTokens.fetch_sub(1);
    NES_ASSERT2_FMT(tokensLeft >= 1, "Sink was last termination token for " << decomposedQueryId);
    NES_DEBUG("QEP {} Sink {} is terminated; tokens left = {}", decomposedQueryId, sink->toString(), (tokensLeft - 1));
    // sinks also require to spawn a reconfig task for the qep
    auto reconfigurationType = terminationTypeToReconfigurationType(terminationType);
    if (tokensLeft == 2) {// this is the second last token to be removed (last one is the qep itself)
        auto reconfMessageQEP = ReconfigurationMessage(getSharedQueryId(),
                                                       getDecomposedQueryId(),
                                                       getDecomposedQueryVersion(),
                                                       reconfigurationType,
                                                       Reconfigurable::shared_from_this<ExecutableQueryPlan>());
        queryManager->addReconfigurationMessage(getSharedQueryId(),
                                                getDecomposedQueryId(),
                                                getDecomposedQueryVersion(),
                                                reconfMessageQEP,
                                                false);
    }
}

void ExecutableQueryPlan::increaseSourceDrainCounter(const ReconfigurationMarkerPtr& marker) {
    drainMarker = marker;
    if (handleSourceDrainHelper()) {
        transferSourcesToSuccessor(marker);
    }
}

std::vector<OperatorId> ExecutableQueryPlan::getSourcesToReuse() const { return sourcesToReuse; }

uint64_t ExecutableQueryPlan::getSourcesToReuseCount() const { return sourcesToReuse.size(); }

void ExecutableQueryPlan::setSourcesToReuse(std::vector<OperatorId> sourcesToReuse) {
    this->sourcesToReuse = std::move(sourcesToReuse);
}

bool ExecutableQueryPlan::addSuccessorPlan(ExecutableQueryPlanPtr successor) {
    this->successor = successor;
    if (allSourcesDrained()) {
        if (*drainMarker.rlock() == nullptr) {
            auto lockedSuccessor = this->successor.rlock();
            auto expectedDrainCount = sources.size() - (*lockedSuccessor)->getSourcesToReuseCount();
            auto toReuse = (*lockedSuccessor)->getSourcesToReuseCount();
            NES_ERROR("expected drain count {} (sources - {} sources to reuse {}", expectedDrainCount, sources.size(), toReuse)
            NES_ERROR("Reusing all sources of a plan is currently not supported")
            NES_NOT_IMPLEMENTED();
        }
        transferSourcesToSuccessor(drainMarker.copy());
        return true;
    }
    return false;
}

void ExecutableQueryPlan::transferSourcesToSuccessor(const ReconfigurationMarkerPtr& marker) {
    // get the list of sources to be reused from the successor
    auto lockedSuccessor = successor.wlock();
    if (!lockedSuccessor) {
        NES_DEBUG("Cannot transfer sources to successor because no successor was registered");
        return;
    }
    NES_ERROR("Transfer sources to successor for plan with id {} of query {} from version {} to {}", decomposedQueryId, sharedQueryId, decomposedQueryVersion, (*lockedSuccessor)->decomposedQueryVersion);
    auto sourcesToReuseBySuccessorPlan = (*lockedSuccessor)->getSourcesToReuse();

    auto sources = getSources();

    NES_ERROR("Sources in old plan")
    for (auto source : sources) {
        NES_ERROR("{}", source->toString())
    }

    NES_ERROR("Sources in new plan")
    for (auto source : (*lockedSuccessor)->getSources()) {
        NES_ERROR("{}", source->toString())
    }

    NES_ERROR("Id's of sources to reuse in new plan")
    for (auto id : sourcesToReuseBySuccessorPlan) {
        NES_ERROR("{}", id)
    }

    for (auto successorSource : (*lockedSuccessor)->getSources()) {
        auto sourceId = successorSource->getOperatorId();

        if (std::find(sourcesToReuseBySuccessorPlan.cbegin(), sourcesToReuseBySuccessorPlan.cend(), sourceId)
            != sourcesToReuseBySuccessorPlan.cend()) {
            auto predicate = [successorSource](auto src) {
                return DataSource::compareId(successorSource, src);
            };

            auto currentSrc = std::find_if(sources.begin(), sources.end(), predicate);
            NES_ASSERT(currentSrc != sources.end(), "Source to reuse does not exist in old plan");

            //update successor of existing source
            (*currentSrc)->updateSuccessors(marker, sourceId, {*lockedSuccessor});
        }
    }

    //delete successor to prevent executing the sources transfer more than once
    *lockedSuccessor = nullptr;
}

bool ExecutableQueryPlan::handleSourceDrainHelper() {
    drainedSourceCount++;
    return allSourcesDrained();
}

bool ExecutableQueryPlan::allSourcesDrained() {
    auto lockedSuccessor = successor.rlock();
    if (*lockedSuccessor == nullptr) {
        return false;
    }
    auto expectedDrainCount = sources.size() - (*lockedSuccessor)->getSourcesToReuseCount();
    NES_ASSERT(expectedDrainCount <= sources.size(), "Expected source drain count must not be higher than the amount of sources");
    return drainedSourceCount == expectedDrainCount;
}
}// namespace NES::Runtime::Execution
