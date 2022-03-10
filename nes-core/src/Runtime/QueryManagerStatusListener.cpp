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
#include <Network/NetworkSource.hpp>
#include <Runtime/AsyncTaskExecutor.hpp>
#include <Runtime/Execution/ExecutablePipeline.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/ExecutableQueryPlan.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/HardwareManager.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/ThreadPool.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <iostream>
#include <memory>
#include <stack>
#include <utility>

namespace NES::Runtime {
void AbstractQueryManager::notifyQueryStatusChange(const Execution::ExecutableQueryPlanPtr& qep,
                                                   Execution::ExecutableQueryPlanStatus status) {
    NES_ASSERT(qep, "Invalid query plan object");
    if (status == Execution::ExecutableQueryPlanStatus::Stopped) {
        // if we got here it means the query gracefully stopped
        // we need to go to each source and terminate their threads
        // alternatively, we need to detach source threads
        for (const auto& source : qep->getSources()) {
            if (!std::dynamic_pointer_cast<Network::NetworkSource>(source)) {
                NES_ASSERT2_FMT(source->stop(Runtime::QueryTerminationType::Graceful),
                                "Cannot cleanup source " << source->getOperatorId());// just a clean-up op
            }
        }
        addReconfigurationMessage(
            qep->getQueryId(),
            qep->getQuerySubPlanId(),
            ReconfigurationMessage(qep->getQueryId(), qep->getQuerySubPlanId(), Destroy, inherited1::shared_from_this()),
            false);
    }
}
void AbstractQueryManager::notifyOperatorFailure(DataSourcePtr failedSource, const std::string) {
    std::unique_lock lock(queryMutex);
    auto qepToFail = sourceToQEPMapping[failedSource->getOperatorId()];
    lock.unlock();
    // we cant fail a query from a source because failing a query eventually calls stop on the failed query
    // this means we are going to call join on the source thread
    // however, notifyOperatorFailure may be called from the source thread itself, thus, resulting in a deadlock
    auto future = asyncTaskExecutor
                      ->runAsync(
                          [this](auto qepToFail) {
                              return failQuery(qepToFail);
                          },
                          std::move(qepToFail))
                      .thenAsync([](bool stopped) {
                          // TODO make rpc call to coordinator to fail the qep globally
                          NES_ASSERT(stopped, "Cannot stop query");
                          return true;
                      });
}

void AbstractQueryManager::notifyTaskFailure(Execution::SuccessorExecutablePipeline, const std::string& errorMessage) {
    NES_ASSERT(false, errorMessage);
}

void AbstractQueryManager::notifySourceCompletion(DataSourcePtr source, QueryTerminationType terminationType) {
    std::unique_lock lock(queryMutex);
    auto& qep = sourceToQEPMapping[source->getOperatorId()];
    NES_ASSERT2_FMT(qep, "invalid query plan for source " << source->getOperatorId());
    qep->notifySourceCompletion(source, terminationType);
}

void AbstractQueryManager::notifyPipelineCompletion(QuerySubPlanId subPlanId, Execution::ExecutablePipelinePtr pipeline, QueryTerminationType terminationType) {
    std::unique_lock lock(queryMutex);
    auto& qep = runningQEPs[subPlanId];
    NES_ASSERT2_FMT(qep, "invalid query plan for pipeline " << pipeline->getPipelineId());
    qep->notifyPipelineCompletion(pipeline, terminationType);
}

void AbstractQueryManager::notifySinkCompletion(QuerySubPlanId subPlanId, DataSinkPtr sink, QueryTerminationType terminationType) {
    std::unique_lock lock(queryMutex);
    auto& qep = runningQEPs[subPlanId];
    NES_ASSERT2_FMT(qep, "invalid query plan for sink " << sink->getOperatorId());
    qep->notifySinkCompletion(sink, terminationType);
}
}// namespace NES::Runtime