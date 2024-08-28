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

#include <iostream>
#include <memory>
#include <variant>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AsyncTaskExecutor.hpp>
#include <Runtime/Execution/ExecutablePipeline.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/ExecutableQueryPlan.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/ThreadPool.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Runtime
{
void QueryManager::notifyQueryStatusChange(const Execution::ExecutableQueryPlanPtr& qep, Execution::ExecutableQueryPlanStatus status)
{
    NES_ASSERT(qep, "Invalid query plan object");
    if (status == Execution::ExecutableQueryPlanStatus::Finished)
    {
        for (const auto& source : qep->getSources())
        {
            NES_ASSERT2_FMT(source->stop(),
                            "Cannot cleanup source " << source->getSourceId()); /// just a clean-up op
        }
        addReconfigurationMessage(
            qep->getQueryId(),
            ReconfigurationMessage(qep->getQueryId(), ReconfigurationType::Destroy, inherited1::shared_from_this()),
            false);

        queryStatusListener->notifyQueryStatusChange(qep->getQueryId(), Execution::ExecutableQueryPlanStatus::Finished);
    }
    else if (status == Execution::ExecutableQueryPlanStatus::ErrorState)
    {
        addReconfigurationMessage(
            qep->getQueryId(),
            ReconfigurationMessage(qep->getQueryId(), ReconfigurationType::Destroy, inherited1::shared_from_this()),
            false);

        queryStatusListener->notifyQueryStatusChange(qep->getQueryId(), Execution::ExecutableQueryPlanStatus::ErrorState);
    }
}
void QueryManager::notifySourceFailure(const OriginId failedSourceOriginId, const std::string reason)
{
    NES_DEBUG("notifySourceFailure called for query id= {}  reason= {}", failedSourceOriginId, reason);
    std::vector<Execution::ExecutableQueryPlanPtr> plansToFail;
    {
        std::unique_lock lock(queryMutex);
        plansToFail = sourceToQEPMapping[failedSourceOriginId];
    }
    /// we cant fail a query from a source because failing a query eventually calls stop on the failed query
    /// this means we are going to call join on the source thread
    /// however, notifySourceFailure may be called from the source thread itself, thus, resulting in a deadlock
    for (auto qepToFail : plansToFail)
    {
        auto future = asyncTaskExecutor->runAsync(
            [this, reason, qepToFail = std::move(qepToFail)]() -> Execution::ExecutableQueryPlanPtr
            {
                NES_DEBUG("Going to fail query id={}", qepToFail->getQueryId());
                if (failQuery(qepToFail))
                {
                    NES_DEBUG("Failed query id= {}", qepToFail->getQueryId());
                    queryStatusListener->notifyQueryFailure(qepToFail->getQueryId(), reason);
                    return qepToFail;
                }
                return nullptr;
            });
    }
}

void QueryManager::notifyTaskFailure(Execution::SuccessorExecutablePipeline pipelineOrSink, const std::string& errorMessage)
{
    QueryId queryId = INVALID_QUERY_ID;
    Execution::ExecutableQueryPlanPtr qepToFail;
    if (auto* pipe = std::get_if<Execution::ExecutablePipelinePtr>(&pipelineOrSink))
    {
        queryId = (*pipe)->getQueryId();
    }
    else if (auto* sink = std::get_if<DataSinkPtr>(&pipelineOrSink))
    {
        queryId = (*sink)->getQueryId();
    }
    {
        std::unique_lock lock(queryMutex);
        if (auto it = runningQEPs.find(queryId); it != runningQEPs.end())
        {
            qepToFail = it->second;
        }
        else
        {
            NES_WARNING("Cannot fail non existing query: {}", queryId);
            return;
        }
    }
    auto future = asyncTaskExecutor->runAsync(
        [this, errorMessage](Execution::ExecutableQueryPlanPtr qepToFail) -> Execution::ExecutableQueryPlanPtr
        {
            NES_DEBUG("Going to fail query id= {}", qepToFail->getQueryId());
            if (failQuery(qepToFail))
            {
                NES_DEBUG("Failed query id= {}", qepToFail->getQueryId());
                queryStatusListener->notifyQueryFailure(qepToFail->getQueryId(), errorMessage);
                return qepToFail;
            }
            return nullptr;
        },
        std::move(qepToFail));
}

void QueryManager::notifySourceCompletion(OriginId sourceId, QueryTerminationType terminationType)
{
    std::unique_lock lock(queryMutex);
    ///THIS is now shutting down all
    for (auto& entry : sourceToQEPMapping[sourceId])
    {
        NES_TRACE("notifySourceCompletion operator id={} plan id={}", sourceId, entry->getQueryId());
        entry->notifySourceCompletion(sourceId, terminationType);
        if (terminationType == QueryTerminationType::Graceful)
        {
            queryStatusListener->notifySourceTermination(entry->getQueryId(), sourceId, QueryTerminationType::Graceful);
        }
    }
}

void QueryManager::notifyPipelineCompletion(
    QueryId queryId, Execution::ExecutablePipelinePtr pipeline, QueryTerminationType terminationType)
{
    std::unique_lock lock(queryMutex);
    auto& qep = runningQEPs[queryId];
    NES_ASSERT2_FMT(qep, "invalid query plan for pipeline " << pipeline->getPipelineId());
    qep->notifyPipelineCompletion(pipeline, terminationType);
}

void QueryManager::notifySinkCompletion(QueryId queryId, DataSinkPtr sink, QueryTerminationType terminationType)
{
    std::unique_lock lock(queryMutex);
    auto& qep = runningQEPs[queryId];
    NES_ASSERT2_FMT(qep, "invalid query id " << queryId << " for sink " << sink->toString());
    qep->notifySinkCompletion(sink, terminationType);
}
} /// namespace NES::Runtime
