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
#include <Runtime/Execution/ExecutableQueryPlan.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Sources/DataSource.hpp>
#include <Util/Logger/Logger.hpp>
#include <iostream>
#include <utility>

namespace NES::Runtime::Execution {

ExecutableQueryPlan::ExecutableQueryPlan(QueryId queryId,
                                         QuerySubPlanId querySubPlanId,
                                         std::vector<DataSourcePtr>&& sources,
                                         std::vector<DataSinkPtr>&& sinks,
                                         std::vector<ExecutablePipelinePtr>&& pipelines,
                                         QueryManagerPtr&& queryManager,
                                         BufferManagerPtr&& bufferManager)
    : queryId(queryId), querySubPlanId(querySubPlanId), sources(std::move(sources)), sinks(std::move(sinks)),
      pipelines(std::move(pipelines)), queryManager(std::move(queryManager)), bufferManager(std::move(bufferManager)),
      qepStatus(Execution::ExecutableQueryPlanStatus::Created), numOfActiveSources(this->sources.size()),
      qepTerminationStatusFuture(qepTerminationStatusPromise.get_future()) {
    // nop
}

ExecutableQueryPlanPtr ExecutableQueryPlan::create(QueryId queryId,
                                                   QuerySubPlanId querySubPlanId,
                                                   std::vector<DataSourcePtr> sources,
                                                   std::vector<DataSinkPtr> sinks,
                                                   std::vector<ExecutablePipelinePtr> pipelines,
                                                   QueryManagerPtr queryManager,
                                                   BufferManagerPtr bufferManager) {
    return std::make_shared<ExecutableQueryPlan>(queryId,
                                                 querySubPlanId,
                                                 std::move(sources),
                                                 std::move(sinks),
                                                 std::move(pipelines),
                                                 std::move(queryManager),
                                                 std::move(bufferManager));
}

void ExecutableQueryPlan::incrementProducerCount() { numOfActiveSources++; }

QueryId ExecutableQueryPlan::getQueryId() const { return queryId; }

const std::vector<ExecutablePipelinePtr>& ExecutableQueryPlan::getPipelines() const { return pipelines; }

QuerySubPlanId ExecutableQueryPlan::getQuerySubPlanId() const { return querySubPlanId; }

ExecutableQueryPlan::~ExecutableQueryPlan() {
    NES_DEBUG("destroy qep " << queryId << " " << querySubPlanId);
    NES_ASSERT(qepStatus.load() == Execution::ExecutableQueryPlanStatus::Created
                   || qepStatus.load() == Execution::ExecutableQueryPlanStatus::Stopped
                   || qepStatus.load() == Execution::ExecutableQueryPlanStatus::ErrorState,
               "QueryPlan is created but not executing " << queryId);
    destroy();
}

std::shared_future<ExecutableQueryPlanResult> ExecutableQueryPlan::getTerminationFuture() {
    return qepTerminationStatusFuture.share();
}

bool ExecutableQueryPlan::fail() {
    bool ret = true;
    auto expected = Execution::ExecutableQueryPlanStatus::Running;
    if (qepStatus.compare_exchange_strong(expected, Execution::ExecutableQueryPlanStatus::ErrorState)) {
        NES_DEBUG("QueryExecutionPlan: fail " << queryId << " " << querySubPlanId);
        for (auto& stage : pipelines) {
            if (!stage->stop()) {
                NES_ERROR("QueryExecutionPlan: fail failed for stage " << stage);
                ret = false;
            }
        }
    }

    if (!ret) {
        qepStatus.store(Execution::ExecutableQueryPlanStatus::ErrorState);
    }
    return ret;
}

ExecutableQueryPlanStatus ExecutableQueryPlan::getStatus() { return qepStatus.load(); }

bool ExecutableQueryPlan::setup() {
    NES_DEBUG("QueryExecutionPlan: setup " << queryId << " " << querySubPlanId);
    auto expected = Execution::ExecutableQueryPlanStatus::Created;
    if (qepStatus.compare_exchange_strong(expected, Execution::ExecutableQueryPlanStatus::Deployed)) {
        for (auto& stage : pipelines) {
            if (!stage->setup(queryManager, bufferManager)) {
                NES_ERROR("QueryExecutionPlan: setup failed!" << queryId << " " << querySubPlanId);
                this->stop();
                return false;
            }
        }
    } else {
        NES_THROW_RUNTIME_ERROR("QEP expected to be Created but was not");
    }
    return true;
}

bool ExecutableQueryPlan::start(const StateManagerPtr& stateManager) {
    NES_DEBUG("QueryExecutionPlan: start " << queryId << " " << querySubPlanId);
    auto expected = Execution::ExecutableQueryPlanStatus::Deployed;
    if (qepStatus.compare_exchange_strong(expected, Execution::ExecutableQueryPlanStatus::Running)) {
        for (auto& stage : pipelines) {
            NES_DEBUG("ExecutableQueryPlan::start qep=" << stage->getQuerySubPlanId() << " pipe=" << stage->getPipelineId());
            if (!stage->start(stateManager)) {
                NES_ERROR("QueryExecutionPlan: start failed! " << queryId << " " << querySubPlanId);
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

void ExecutableQueryPlan::reconfigure(ReconfigurationMessage& task, WorkerContext& context) {
    Reconfigurable::reconfigure(task, context);
}

bool ExecutableQueryPlan::stop() {
    bool allStagesStopped = true;
    auto expected = Execution::ExecutableQueryPlanStatus::Running;
    NES_DEBUG("QueryExecutionPlan: stop queryId=" << queryId << " querySubPlanId=" << querySubPlanId);
    if (numOfActiveSources.fetch_sub(1) != 1) {
        NES_WARNING("QueryExecutionPlan: cannot stop queryId=" << queryId << " querySubPlanId=" << querySubPlanId);
        return true;
    }
    if (qepStatus.compare_exchange_strong(expected, Execution::ExecutableQueryPlanStatus::Stopped)) {
        NES_DEBUG("QueryExecutionPlan: stop " << queryId << "-" << querySubPlanId << " is marked as stopped now");
        for (auto& stage : pipelines) {
            if (!stage->stop()) {
                NES_ERROR("QueryExecutionPlan: stop failed for stage " << stage);
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
        return true;// we have tried to stop the same QEP twice..
    }
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
    //soft eos means we drain the state and hard means we truncate it
    switch (task.getType()) {
        case HardEndOfStream: {
            NES_DEBUG("Executing HardEndOfStream on qep queryId=" << queryId << " querySubPlanId=" << querySubPlanId);
            break;
        }
        case SoftEndOfStream: {
            NES_DEBUG("QueryExecutionPlan: soft stop request received for query plan " << queryId << " sub plan "
                                                                                       << querySubPlanId);
            auto expected = Execution::ExecutableQueryPlanStatus::Running;
            if (qepStatus.compare_exchange_strong(expected, Execution::ExecutableQueryPlanStatus::Stopped)) {
                NES_DEBUG("QueryExecutionPlan: query plan " << queryId << " subplan " << querySubPlanId
                                                            << " is marked as stopped now");
                qepTerminationStatusPromise.set_value(ExecutableQueryPlanResult::Ok);
                queryManager->notifyQueryStatusChange(shared_from_base<ExecutableQueryPlan>(), Execution::ExecutableQueryPlanStatus::Stopped);
                return;
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
    for (const auto& source : sources) {
        NES_ASSERT(source->isRunning(), "Source " << source->getOperatorId() << " is still running");
    }
    for (const auto& pipeline : pipelines) {
        NES_ASSERT(pipeline->isRunning(), "Pipeline " << pipeline->getPipelineId() << " is still running");
    }
    sources.clear();
    pipelines.clear();
    sinks.clear();
    bufferManager.reset();
}

void ExecutableQueryPlan::onEvent(BaseEvent&) {
    // nop :: left on purpose -> fill this in when you want to support events
}

}// namespace NES::Runtime::Execution
