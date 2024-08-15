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

#include <chrono>
#include <iomanip>
#include <sstream>
#include <Runtime/Execution/ExecutablePipeline.hpp>
#include <Runtime/Execution/ExecutableQueryPlan.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryManager.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Runtime::Execution
{

ExecutableQueryPlan::ExecutableQueryPlan(
    QueryId queryId,
    std::vector<DataSourcePtr>&& sources,
    std::vector<DataSinkPtr>&& sinks,
    std::vector<ExecutablePipelinePtr>&& pipelines,
    QueryManagerPtr&& queryManager,
    Memory::BufferManagerPtr&& bufferManager)
    : queryId(queryId)
    , sources(std::move(sources))
    , sinks(std::move(sinks))
    , pipelines(std::move(pipelines))
    , queryManager(std::move(queryManager))
    , bufferManager(std::move(bufferManager))
    , queryStatus(QueryStatus::Registered)
    , qepTerminationStatusFuture(qepTerminationStatusPromise.get_future())
{
    /// the +1 is the termination token for the query plan itself
    numOfTerminationTokens.store(1 + this->sources.size() + this->pipelines.size() + this->sinks.size());
}

ExecutableQueryPlanPtr ExecutableQueryPlan::create(
    QueryId queryId,
    std::vector<DataSourcePtr> sources,
    std::vector<DataSinkPtr> sinks,
    std::vector<ExecutablePipelinePtr> pipelines,
    QueryManagerPtr queryManager,
    Memory::BufferManagerPtr bufferManager)
{
    return std::make_shared<ExecutableQueryPlan>(
        queryId, std::move(sources), std::move(sinks), std::move(pipelines), std::move(queryManager), std::move(bufferManager));
}

QueryId ExecutableQueryPlan::getQueryId() const
{
    return queryId;
}

const std::vector<ExecutablePipelinePtr>& ExecutableQueryPlan::getPipelines() const
{
    return pipelines;
}

ExecutableQueryPlan::~ExecutableQueryPlan()
{
    NES_DEBUG("destroy qep {}", queryId);
    NES_ASSERT(
        queryStatus.load() == QueryStatus::Registered || queryStatus.load() == QueryStatus::Stopped
            || queryStatus.load() == QueryStatus::Failed,
        "QueryPlan is created but not executing " << queryId);
    destroy();
}

std::shared_future<ExecutableQueryPlanResult> ExecutableQueryPlan::getTerminationFuture()
{
    return qepTerminationStatusFuture.share();
}

bool ExecutableQueryPlan::fail()
{
    bool ret = true;
    auto expected = QueryStatus::Running;
    if (queryStatus.compare_exchange_strong(expected, QueryStatus::Failed))
    {
        NES_DEBUG("QueryExecutionPlan: fail query={}", queryId);
        for (auto& stage : pipelines)
        {
            if (!stage->fail())
            {
                std::stringstream s;
                s << stage;
                std::string stageAsString = s.str();
                NES_ERROR("QueryExecutionPlan: fail failed for stage {}", stageAsString);
                ret = false;
            }
        }
        qepTerminationStatusPromise.set_value(ExecutableQueryPlanResult::Fail);
    }

    if (!ret)
    {
        queryStatus.store(QueryStatus::Failed);
    }
    return ret;
}

QueryStatus ExecutableQueryPlan::getStatus() const
{
    return queryStatus.load();
}

QueryManagerPtr ExecutableQueryPlan::getQueryManager() const
{
    return queryManager;
}


bool ExecutableQueryPlan::setup()
{
    NES_DEBUG("QueryExecutionPlan: setup QueryId={}", queryId);
    auto expected = Execution::QueryStatus::Registered;
    if (queryStatus.compare_exchange_strong(expected, Execution::QueryStatus::Deployed))
    {
        for (auto& stage : pipelines)
        {
            if (!stage->setup(queryManager, bufferManager))
            {
                NES_ERROR("QueryExecutionPlan: setup failed! {}", queryId);
                this->stop();
                return false;
            }
        }
    }
    else
    {
        NES_THROW_RUNTIME_ERROR("QEP expected to be Created but was not");
    }
    return true;
}

bool ExecutableQueryPlan::start()
{
    NES_DEBUG("QueryExecutionPlan: start query={}", queryId);
    auto expected = Execution::QueryStatus::Deployed;
    if (queryStatus.compare_exchange_strong(expected, Execution::QueryStatus::Running))
    {
        for (auto& stage : pipelines)
        {
            NES_DEBUG("ExecutableQueryPlan::start qep={} pipe={}", stage->getQueryId(), stage->getPipelineId());
            if (!stage->start())
            {
                NES_ERROR("QueryExecutionPlan: start failed! query={}", queryId);
                this->stop();
                return false;
            }
        }
    }
    else
    {
        NES_THROW_RUNTIME_ERROR("QEP expected to be Deployed but was not");
    }
    return true;
}

Memory::BufferManagerPtr ExecutableQueryPlan::getBufferManager() const
{
    return bufferManager;
}

const std::vector<DataSourcePtr>& ExecutableQueryPlan::getSources() const
{
    return sources;
}

const std::vector<DataSinkPtr>& ExecutableQueryPlan::getSinks() const
{
    return sinks;
}

bool ExecutableQueryPlan::stop()
{
    bool allStagesStopped = true;
    auto expected = QueryStatus::Running;
    NES_DEBUG("QueryExecutionPlan: stop QueryId={}", queryId);
    if (queryStatus.compare_exchange_strong(expected, QueryStatus::Stopped))
    {
        NES_DEBUG("QueryExecutionPlan: stop {} is marked as stopped now", queryId);
        for (auto& stage : pipelines)
        {
            if (!stage->stop(QueryTerminationType::HardStop))
            {
                std::stringstream s;
                s << stage;
                std::string stageAsString = s.str();
                NES_ERROR("QueryExecutionPlan: stop failed for stage {}", stageAsString);
                allStagesStopped = false;
            }
        }

        bufferManager.reset();

        if (allStagesStopped)
        {
            qepTerminationStatusPromise.set_value(ExecutableQueryPlanResult::Ok);
            return true; /// correct stop
        }

        queryStatus.store(QueryStatus::Failed);
        qepTerminationStatusPromise.set_value(ExecutableQueryPlanResult::Fail);

        return false; /// one stage failed to stop
    }

    if (expected == QueryStatus::Stopped)
    {
        return true; /// we have tried to stop the same QEP twice.
    }
    NES_ERROR("Something is wrong with query {} as it was not possible to stop", queryId);
    /// if we get there it mean the CAS failed and expected is the current value
    while (!queryStatus.compare_exchange_strong(expected, QueryStatus::Failed))
    {
        /// try to install ErrorState
    }

    qepTerminationStatusPromise.set_value(ExecutableQueryPlanResult::Fail);

    bufferManager.reset();
    return false;
}

void ExecutableQueryPlan::postReconfigurationCallback(ReconfigurationMessage& task)
{
    Reconfigurable::postReconfigurationCallback(task);
    switch (task.getType())
    {
        case ReconfigurationType::FailEndOfStream: {
            NES_DEBUG("Executing FailEndOfStream on qep QueryId={}", queryId);
            NES_ASSERT2_FMT((numOfTerminationTokens.fetch_sub(1) == 1), "Invalid FailEndOfStream on qep QueryId=" << queryId);
            NES_ASSERT2_FMT(fail(), "Cannot fail QueryId=" << queryId);
            NES_DEBUG("QueryExecutionPlan: query plan {} is marked as failed now", queryId);
            queryManager->notifyQueryStatusChange(shared_from_base<ExecutableQueryPlan>(), Execution::QueryStatus::Failed);
            break;
        }
        case ReconfigurationType::HardEndOfStream: {
            NES_DEBUG("Executing HardEndOfStream on qep QueryId={}", queryId);
            NES_ASSERT2_FMT((numOfTerminationTokens.fetch_sub(1) == 1), "Invalid HardEndOfStream on qep QueryId=" << queryId);
            NES_ASSERT2_FMT(stop(), "Cannot hard stop qep QueryId=" << queryId);
            queryManager->notifyQueryStatusChange(shared_from_base<ExecutableQueryPlan>(), Execution::QueryStatus::Stopped);
            break;
        }
        case ReconfigurationType::SoftEndOfStream: {
            NES_DEBUG("QueryExecutionPlan: soft stop request received for query plan {} left tokens = {}", queryId, numOfTerminationTokens);
            if (numOfTerminationTokens.fetch_sub(1) == 1)
            {
                auto expected = Execution::QueryStatus::Running;
                if (queryStatus.compare_exchange_strong(expected, Execution::QueryStatus::Finished))
                {
                    /// if CAS fails - it means the query was already stopped or failed
                    NES_DEBUG("QueryExecutionPlan: query plan {} is marked as (soft) stopped now", queryId);
                    qepTerminationStatusPromise.set_value(ExecutableQueryPlanResult::Ok);
                    queryManager->notifyQueryStatusChange(shared_from_base<ExecutableQueryPlan>(), Execution::QueryStatus::Finished);
                    return;
                }
            }
            else
            {
                NES_DEBUG("QueryExecutionPlan: query plan {} was already marked as stopped now", queryId);
            }
            break;
        }
        default: {
            break;
        }
    }
}

void ExecutableQueryPlan::destroy()
{
    /// sanity checks: ensure we can destroy stopped instances
    auto expected = 0u;
    if (queryStatus == QueryStatus::Registered)
    {
        return; /// there is nothing to destroy;
    }
    NES_ASSERT2_FMT(
        numOfTerminationTokens.compare_exchange_strong(expected, uint32_t(-1)),
        "Destroying still active query plan, tokens left=" << expected);
    for (const auto& source : sources)
    {
        NES_ASSERT(!source->isRunning(), "Source " << source->getOperatorId() << " is still running");
    }
    for (const auto& pipeline : pipelines)
    {
        NES_ASSERT(!pipeline->isRunning(), "Pipeline " << pipeline->getPipelineId() << " is still running");
    }
    sources.clear();
    pipelines.clear();
    sinks.clear();
    bufferManager.reset();
}

void ExecutableQueryPlan::onEvent(BaseEvent&)
{
    /// nop :: left on purpose -> fill this in when you want to support events
}
void ExecutableQueryPlan::notifySourceCompletion(DataSourcePtr source, QueryTerminationType terminationType)
{
    NES_DEBUG("QEP {} Source {} is notifying completion: {}", queryId, source->getOperatorId(), terminationType);
    NES_ASSERT2_FMT(queryStatus.load() == QueryStatus::Running, "Cannot complete source on non running query plan id=" << queryId);
    auto it = std::find(sources.begin(), sources.end(), source);
    NES_ASSERT2_FMT(it != sources.end(), "Cannot find source " << source->getOperatorId() << " in query plan " << queryId);
    uint32_t tokensLeft = numOfTerminationTokens.fetch_sub(1);
    NES_ASSERT2_FMT(tokensLeft >= 1, "Source was last termination token for " << queryId << " = " << terminationType);
    NES_DEBUG("QEP {} Source {} is terminated; tokens left = {}", queryId, source->getOperatorId(), (tokensLeft - 1));
    /// the following check is necessary because a data sources first emits an EoS marker and then calls this method.
    /// However, it might happen that the marker is so fast that one possible execution is as follows:
    /// 1) Data Source emits EoS marker
    /// 2) Next pipeline/sink picks the marker and finishes by decreasing `numOfTerminationTokens`
    /// 3) DataSource invokes `notifySourceCompletion`
    /// as a result, the data source might be the last one to decrease the `numOfTerminationTokens` thus it has to
    /// trigger the QEP reconfiguration
    if (tokensLeft == 2)
    { /// this is the second last token to be removed (last one is the qep itself)
        auto reconfMessageQEP = ReconfigurationMessage(
            getQueryId(),
            terminationType == QueryTerminationType::Graceful
                ? ReconfigurationType::SoftEndOfStream
                : (terminationType == QueryTerminationType::HardStop ? ReconfigurationType::HardEndOfStream
                                                                     : ReconfigurationType::FailEndOfStream),
            Reconfigurable::shared_from_this<ExecutableQueryPlan>());
        queryManager->addReconfigurationMessage(getQueryId(), reconfMessageQEP, false);
    }
}

void ExecutableQueryPlan::notifyPipelineCompletion(ExecutablePipelinePtr pipeline, QueryTerminationType terminationType)
{
    NES_ASSERT2_FMT(queryStatus.load() == QueryStatus::Running, "Cannot complete pipeline on non running query plan id=" << queryId);
    auto it = std::find(pipelines.begin(), pipelines.end(), pipeline);
    NES_ASSERT2_FMT(it != pipelines.end(), "Cannot find pipeline " << pipeline->getPipelineId() << " in query " << queryId);
    uint32_t tokensLeft = numOfTerminationTokens.fetch_sub(1);
    NES_ASSERT2_FMT(tokensLeft >= 1, "Pipeline was last termination token for " << queryId);
    NES_DEBUG("QEP {} Pipeline {} is terminated; tokens left = {}", queryId, pipeline->getPipelineId(), (tokensLeft - 1));
    /// the same applies here for the pipeline
    if (tokensLeft == 2)
    { /// this is the second last token to be removed (last one is the qep itself)
        auto reconfMessageQEP = ReconfigurationMessage(
            getQueryId(),
            terminationType == QueryTerminationType::Graceful
                ? ReconfigurationType::SoftEndOfStream
                : (terminationType == QueryTerminationType::HardStop ? ReconfigurationType::HardEndOfStream
                                                                     : ReconfigurationType::FailEndOfStream),
            Reconfigurable::shared_from_this<ExecutableQueryPlan>());
        queryManager->addReconfigurationMessage(getQueryId(), reconfMessageQEP, false);
    }
}

void ExecutableQueryPlan::notifySinkCompletion(DataSinkPtr sink, QueryTerminationType terminationType)
{
    NES_ASSERT2_FMT(queryStatus.load() == QueryStatus::Running, "Cannot complete sink on non running query plan id=" << queryId);
    auto it = std::find(sinks.begin(), sinks.end(), sink);
    NES_ASSERT2_FMT(it != sinks.end(), "Cannot find sink " << sink->toString() << " in sub query plan " << queryId);
    uint32_t tokensLeft = numOfTerminationTokens.fetch_sub(1);
    NES_ASSERT2_FMT(tokensLeft >= 1, "Sink was last termination token for " << queryId);
    NES_DEBUG("QEP {} Sink {} is terminated; tokens left = {}", queryId, sink->toString(), (tokensLeft - 1));
    /// sinks also require to spawn a reconfig task for the qep
    if (tokensLeft == 2)
    { /// this is the second last token to be removed (last one is the qep itself)
        auto reconfMessageQEP = ReconfigurationMessage(
            getQueryId(),
            terminationType == QueryTerminationType::Graceful
                ? ReconfigurationType::SoftEndOfStream
                : (terminationType == QueryTerminationType::HardStop ? ReconfigurationType::HardEndOfStream
                                                                     : ReconfigurationType::FailEndOfStream),
            Reconfigurable::shared_from_this<ExecutableQueryPlan>());
        queryManager->addReconfigurationMessage(getQueryId(), reconfMessageQEP, false);
    }
}

} /// namespace NES::Runtime::Execution
