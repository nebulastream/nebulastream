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
#include <Util/Logger//Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <iostream>
#include <memory>
#include <stack>
#include <utility>

namespace NES::Runtime {
void AbstractQueryManager::notifyQueryStatusChange(const Execution::ExecutableQueryPlanPtr& qep,
                                                   Execution::ExecutableQueryPlanStatus status) {
    NES_ASSERT(qep, "Invalid query plan object");
    if (status == Execution::ExecutableQueryPlanStatus::Finished) {
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

        queryStatusListener->notifyQueryStatusChange(qep->getQueryId(),
                                                     qep->getQuerySubPlanId(),
                                                     Execution::ExecutableQueryPlanStatus::Finished);

    } else if (status == Execution::ExecutableQueryPlanStatus::ErrorState) {
        addReconfigurationMessage(
            qep->getQueryId(),
            qep->getQuerySubPlanId(),
            ReconfigurationMessage(qep->getQueryId(), qep->getQuerySubPlanId(), Destroy, inherited1::shared_from_this()),
            false);

        queryStatusListener->notifyQueryStatusChange(qep->getQueryId(),
                                                     qep->getQuerySubPlanId(),
                                                     Execution::ExecutableQueryPlanStatus::ErrorState);
    }
}
void AbstractQueryManager::notifySourceFailure(DataSourcePtr failedSource, const std::string reason) {
    std::unique_lock lock(queryMutex);

    bool retFinal = true;
    for (auto qep : sourceToQEPMapping[failedSource->getOperatorId()]) {
        auto qepToFail = qep;
        // we cant fail a query from a source because failing a query eventually calls stop on the failed query
        // this means we are going to call join on the source thread
        // however, notifySourceFailure may be called from the source thread itself, thus, resulting in a deadlock
        auto future =
            asyncTaskExecutor
                ->runAsync(
                    [this](Execution::ExecutableQueryPlanPtr qepToFail) -> Execution::ExecutableQueryPlanPtr {
                        if (failQuery(qepToFail)) {
                            return qepToFail;
                        }
                        return nullptr;
                    },
                    std::move(qepToFail))
                .thenAsync([this, reason](Execution::ExecutableQueryPlanPtr failedQep) {
                    if (failedQep && failedQep->getStatus() == Execution::ExecutableQueryPlanStatus::ErrorState) {
                        queryStatusListener->notifyQueryFailure(failedQep->getQueryId(), failedQep->getQuerySubPlanId(), reason);
                    }
                    return true;
                });
    }
}

void AbstractQueryManager::notifyTaskFailure(Execution::SuccessorExecutablePipeline, const std::string& errorMessage) {
    NES_ASSERT(false, errorMessage);
}

void AbstractQueryManager::notifySourceCompletion(DataSourcePtr source, QueryTerminationType terminationType) {
    std::unique_lock lock(queryMutex);
    //THIS is now shutting down all
    for (auto entry : sourceToQEPMapping[source->getOperatorId()]) {
        std::cout << " NOFITFY operator id=" << source->getOperatorId() << "plan id=" << entry->getQueryId()
                  << " subplan=" << entry->getQuerySubPlanId() << std::endl;

        entry->notifySourceCompletion(source, terminationType);
        if (terminationType == QueryTerminationType::Graceful) {
            queryStatusListener->notifySourceTermination(entry->getQueryId(),
                                                         entry->getQuerySubPlanId(),
                                                         source->getOperatorId(),
                                                         terminationType);
        }
    }
}

void AbstractQueryManager::notifyPipelineCompletion(QuerySubPlanId subPlanId,
                                                    Execution::ExecutablePipelinePtr pipeline,
                                                    QueryTerminationType terminationType) {
    std::unique_lock lock(queryMutex);
    auto& qep = runningQEPs[subPlanId];
    NES_ASSERT2_FMT(qep, "invalid query plan for pipeline " << pipeline->getPipelineId());
    qep->notifyPipelineCompletion(pipeline, terminationType);
}

void AbstractQueryManager::notifySinkCompletion(QuerySubPlanId subPlanId,
                                                DataSinkPtr sink,
                                                QueryTerminationType terminationType) {
    std::unique_lock lock(queryMutex);
    auto& qep = runningQEPs[subPlanId];
    NES_ASSERT2_FMT(qep, "invalid query plan " << subPlanId << " for sink " << sink->toString());
    qep->notifySinkCompletion(sink, terminationType);
}
}// namespace NES::Runtime