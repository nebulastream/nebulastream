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
#include <stack>
#include <utility>
#include <Runtime/AsyncTaskExecutor.hpp>
#include <Runtime/Execution/ExecutablePipeline.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/ExecutableQueryPlan.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/ThreadPool.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sinks/Sink.hpp>


namespace NES::Runtime
{
bool QueryManager::registerQuery(const Execution::ExecutableQueryPlanPtr& qep)
{
    NES_DEBUG("QueryManager::registerExecutableQueryPlan: query {}", qep->getQueryId());

    NES_ASSERT2_FMT(
        queryManagerStatus.load() == QueryManagerStatus::Running,
        "QueryManager::registerQuery: cannot accept new query id " << qep->getQueryId());
    {
        std::scoped_lock lock(queryMutex);
        /// test if elements already exist
        NES_DEBUG("QueryManager: resolving sources for query  {}  with sources= {}", qep->getQueryId(), qep->getSources().size());

        /// Todo #241: Use the new unique source identifier to identify sources.
        for (const auto& source : qep->getSources())
        {
            /// source already exists, add qep to source set if not there
            NES_DEBUG("QueryManager: Source {}  not found. Creating new element with with qep ", source->getSourceId());

            ///bookkeep which qep is now reading from this source
            sourceToQEPMapping[source->getSourceId()].emplace_back(qep);
        }
        notifyQueryStatusChange(qep, Execution::QueryStatus::Registered);
    }

    NES_DEBUG("queryToStatisticsMap add for= {}", qep->getQueryId());

    ///TODO: This assumes 1) that there is only one pipeline per query and 2) that the subqueryplan id is unique => both can become a problem
    queryToStatisticsMap.insert(qep->getQueryId(), std::make_shared<QueryStatistics>(qep->getQueryId()));

    NES_ASSERT2_FMT(
        queryManagerStatus.load() == QueryManagerStatus::Running,
        "QueryManager::startQuery: cannot accept new query id " << qep->getQueryId());
    NES_ASSERT(qep->getStatus() == Execution::QueryStatus::Registered, "Invalid status for starting the QEP " << qep->getQueryId());

    /// 1. start the qep and handlers, if any
    if (!qep->setup() || !qep->start())
    {
        NES_FATAL_ERROR("QueryManager: query execution plan could not started");
        return false;
    }

    std::vector<AsyncTaskExecutor::AsyncTaskFuture<bool>> startFutures;

    for (auto& future : startFutures)
    {
        NES_ASSERT(future.wait(), "Cannot start query");
    }

    /// 4. start data sinks
    for (const auto& sink : qep->getSinks())
    {
        NES_DEBUG("QueryManager: start sink  {}", *sink);
        sink->open();
    }

    {
        std::unique_lock lock(queryMutex);
        runningQEPs.emplace(qep->getQueryId(), qep);
    }

    return true;
}

bool QueryManager::startQuery(const Execution::ExecutableQueryPlanPtr& qep)
{
    NES_DEBUG("QueryManager::startQuery: query id {}", qep->getQueryId());
    NES_ASSERT2_FMT(
        queryManagerStatus.load() == QueryManagerStatus::Running,
        "QueryManager::startQuery: cannot accept new query id " << qep->getQueryId());

    /// 5. start data sources
    for (const auto& source : qep->getSources())
    {
        source->start();
        NES_DEBUG("QueryManager: source  {}  started successfully", *source);
    }

    /// register start timestamp of query in statistics
    if (queryToStatisticsMap.contains(qep->getQueryId()))
    {
        auto statistics = queryToStatisticsMap.find(qep->getQueryId());
        if (statistics->getTimestampQueryStart() == 0)
        {
            auto now = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch())
                           .count();
            statistics->setTimestampQueryStart(now, true);
        }
        else
        {
            NES_DEBUG("Start timestamp already exists, this is expected in case of query reconfiguration");
        }
    }
    else
    {
        NES_FATAL_ERROR("queryToStatisticsMap not set, this should only happen for testing");
        NES_THROW_RUNTIME_ERROR("got buffer for not registered qep");
    }
    notifyQueryStatusChange(qep, Execution::QueryStatus::Running);
    return true;
}

bool QueryManager::unregisterQuery(const Execution::ExecutableQueryPlanPtr& qep)
{
    NES_DEBUG("QueryManager::unregisterAndUndeployQuery: query {}", qep->getQueryId());
    std::unique_lock lock(queryMutex);
    auto qepStatus = qep->getStatus();
    NES_ASSERT2_FMT(
        qepStatus == Execution::QueryStatus::Finished || qepStatus == Execution::QueryStatus::Failed
            || qepStatus == Execution::QueryStatus::Stopped,
        "Cannot unregister query " << qep->getQueryId() << " as it is not stopped/failed");
    for (auto const& source : qep->getSources())
    {
        sourceToQEPMapping.erase(source->getSourceId());
    }
    notifyQueryStatusChange(qep, Execution::QueryStatus::Unregistered);
    return true;
}

bool QueryManager::failQuery(const Execution::ExecutableQueryPlanPtr& qep)
{
    bool ret = true;
    NES_DEBUG("QueryManager::failQuery: query= {}", qep->getQueryId());
    switch (qep->getStatus())
    {
        case Execution::QueryStatus::Failed:
        case Execution::QueryStatus::Finished:
        case Execution::QueryStatus::Stopped: {
            return true;
        }
        case Execution::QueryStatus::Invalid: {
            NES_ASSERT2_FMT(false, "not supported yet " << qep->getQueryId());
        }
        default: {
            break;
        }
    }
    for (const auto& source : qep->getSources())
    {
        source->stop();
    }

    auto terminationFuture = qep->getTerminationFuture();
    auto terminationStatus = terminationFuture.wait_for(std::chrono::minutes(10));
    switch (terminationStatus)
    {
        case std::future_status::ready: {
            if (terminationFuture.get() != Execution::ExecutableQueryPlan::Result::FAILED)
            {
                NES_FATAL_ERROR("QueryManager: QEP {} could not be failed", qep->getQueryId());
                ret = false;
            }
            break;
        }
        case std::future_status::timeout:
        case std::future_status::deferred: {
            /// TODO we need to fail the query now as it could not be stopped?
            NES_ASSERT2_FMT(false, "Cannot stop query within deadline getQueryId()=" << qep->getQueryId());
            break;
        }
    }
    if (ret)
    {
        addReconfigurationMessage(
            qep->getQueryId(),
            ReconfigurationMessage(qep->getQueryId(), ReconfigurationType::Destroy, inherited1::shared_from_this()),
            true);
    }
    NES_DEBUG("QueryManager::failQuery: query {} was {}", qep->getQueryId(), (ret ? "successful" : " not successful"));
    notifyQueryStatusChange(qep, Execution::QueryStatus::Failed);
    return true;
}

bool QueryManager::stopQuery(const Execution::ExecutableQueryPlanPtr& qep, Runtime::QueryTerminationType type)
{
    NES_DEBUG("QueryManager::stopQuery: query sub-plan id  {}  type= {}", qep->getQueryId(), type);
    NES_ASSERT2_FMT(type != QueryTerminationType::Failure, "Requested stop with failure for query " << qep->getQueryId());
    bool ret = true;
    switch (qep->getStatus())
    {
        case Execution::QueryStatus::Failed:
        case Execution::QueryStatus::Finished:
        case Execution::QueryStatus::Stopped: {
            NES_DEBUG("QueryManager::stopQuery: query sub-plan id  {}  is already stopped", qep->getQueryId());
            return true;
        }
        case Execution::QueryStatus::Invalid: {
            NES_ASSERT2_FMT(false, "not supported yet " << qep->getQueryId());
            break;
        }
        default: {
            break;
        }
    }

    for (const auto& source : qep->getSources())
    {
        source->stop();
    }

    if (type == QueryTerminationType::HardStop || type == QueryTerminationType::Failure)
    {
        for (auto& stage : qep->getPipelines())
        {
            NES_ASSERT2_FMT(stage->stop(type), "Cannot hard stop pipeline " << stage->getPipelineId());
        }
    }

    /// TODO evaluate if we need to have this a wait instead of a get
    /// TODO for instance we could wait N seconds and if the stopped is not successful by then
    /// TODO we need to trigger a hard local kill of a QEP
    auto terminationFuture = qep->getTerminationFuture();
    auto terminationStatus = terminationFuture.wait_for(std::chrono::minutes(10));
    switch (terminationStatus)
    {
        case std::future_status::ready: {
            if (terminationFuture.get() != Execution::ExecutableQueryPlan::Result::OK)
            {
                NES_FATAL_ERROR("QueryManager: QEP {} could not be stopped", qep->getQueryId());
                ret = false;
            }
            break;
        }
        case std::future_status::timeout:
        case std::future_status::deferred: {
            /// TODO we need to fail the query now as it could not be stopped?
            NES_ASSERT2_FMT(false, "Cannot stop query within deadline " << qep->getQueryId());
        }
    }
    if (ret)
    {
        addReconfigurationMessage(
            qep->getQueryId(),
            ReconfigurationMessage(qep->getQueryId(), ReconfigurationType::Destroy, inherited1::shared_from_this()),
            true);
    }
    NES_DEBUG("QueryManager::stopQuery: query {} was {}", qep->getQueryId(), (ret ? "successful" : " not successful"));
    notifyQueryStatusChange(qep, Execution::QueryStatus::Stopped);
    return ret;
}

bool QueryManager::addSoftEndOfStream(OriginId sourceId, const std::vector<Execution::SuccessorExecutablePipeline>& pipelineSuccessors)
{
    for (auto successor : pipelineSuccessors)
    {
        /// create reconfiguration message. If the successor is an executable pipeline we send a reconfiguration message to the pipeline.
        /// If successor is a data sink we send the reconfiguration message to the query plan.
        if (auto* executablePipeline = std::get_if<Execution::ExecutablePipelinePtr>(&successor))
        {
            auto reconfMessage = ReconfigurationMessage(
                executablePipeline->get()->getQueryId(), ReconfigurationType::SoftEndOfStream, (*executablePipeline));
            addReconfigurationMessage(executablePipeline->get()->getQueryId(), reconfMessage, false);
            NES_DEBUG(
                "soft end-of-stream Exec Pipeline opId={} reconfType={} "
                "threadPool->getNumberOfThreads()={} qep {}",
                sourceId,
                magic_enum::enum_name(ReconfigurationType::SoftEndOfStream),
                threadPool->getNumberOfThreads(),
                executablePipeline->get()->getQueryId());
        }
    }
    return true;
}

bool QueryManager::addHardEndOfStream(OriginId sourceId, const std::vector<Execution::SuccessorExecutablePipeline>& pipelineSuccessors)
{
    for (auto successor : pipelineSuccessors)
    {
        /// create reconfiguration message. If the successor is an executable pipeline we send a reconfiguration message to the pipeline.
        /// If successor is a data sink we send the reconfiguration message to the query plan.
        if (auto* executablePipeline = std::get_if<Execution::ExecutablePipelinePtr>(&successor))
        {
            auto reconfMessage = ReconfigurationMessage(
                executablePipeline->get()->getQueryId(), ReconfigurationType::HardEndOfStream, (*executablePipeline));
            addReconfigurationMessage(executablePipeline->get()->getQueryId(), reconfMessage, false);
            NES_DEBUG(
                "hard end-of-stream Exec Op opId={} reconfType={} queryId={} querySubPlanId={} "
                "threadPool->getNumberOfThreads()={}",
                sourceId,
                magic_enum::enum_name(ReconfigurationType::HardEndOfStream),
                executablePipeline->get()->getQueryId(),
                executablePipeline->get()->getQueryId(),
                threadPool->getNumberOfThreads());
        }
    }

    return true;
}

bool QueryManager::addFailureEndOfStream(OriginId sourceId, const std::vector<Execution::SuccessorExecutablePipeline>& pipelineSuccessors)
{
    for (auto successor : pipelineSuccessors)
    {
        /// create reconfiguration message. If the successor is an executable pipeline we send a reconfiguration message to the pipeline.
        /// If successor is a data sink we send the reconfiguration message to the query plan.
        if (auto* executablePipeline = std::get_if<Execution::ExecutablePipelinePtr>(&successor))
        {
            auto reconfMessage = ReconfigurationMessage(
                executablePipeline->get()->getQueryId(), ReconfigurationType::FailEndOfStream, (*executablePipeline));
            addReconfigurationMessage(executablePipeline->get()->getQueryId(), reconfMessage, false);

            NES_DEBUG(
                "failure end-of-stream Exec Op opId={} reconfType={} queryExecutionPlanId={} "
                "threadPool->getNumberOfThreads()={} qep {}",
                sourceId,
                magic_enum::enum_name(ReconfigurationType::FailEndOfStream),
                executablePipeline->get()->getQueryId(),
                threadPool->getNumberOfThreads(),
                executablePipeline->get()->getQueryId());
        }
    }
    return true;
}

bool QueryManager::addEndOfStream(
    OriginId sourceId,
    const std::vector<Execution::SuccessorExecutablePipeline>& pipelineSuccessors,
    Runtime::QueryTerminationType terminationType)
{
    std::unique_lock queryLock(queryMutex);
    NES_DEBUG("QueryManager: QueryManager::addEndOfStream for source operator {} terminationType={}", sourceId, terminationType);
    NES_ASSERT2_FMT(threadPool->isRunning(), "thread pool no longer running");
    NES_ASSERT2_FMT(sourceToQEPMapping.contains(sourceId), "invalid source " << sourceId);

    bool success = false;
    switch (terminationType)
    {
        case Runtime::QueryTerminationType::Graceful: {
            success = addSoftEndOfStream(sourceId, pipelineSuccessors);
            break;
        }
        case Runtime::QueryTerminationType::HardStop: {
            success = addHardEndOfStream(sourceId, pipelineSuccessors);
            break;
        }
        case Runtime::QueryTerminationType::Failure: {
            success = addFailureEndOfStream(sourceId, pipelineSuccessors);
            break;
        }
        default: {
            NES_ASSERT2_FMT(false, "Invalid termination type");
        }
    }

    return success;
}

}
