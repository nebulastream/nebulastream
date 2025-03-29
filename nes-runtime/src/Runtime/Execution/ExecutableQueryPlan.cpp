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

#include <iomanip>
#include <sstream>
#include <Runtime/Execution/ExecutablePipeline.hpp>
#include <Runtime/Execution/ExecutableQueryPlan.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryManager.hpp>
#include <Sinks/Sink.hpp>
#include <Sources/SourceHandle.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES
{

ExecutableQueryPlan::ExecutableQueryPlan(
    QueryId queryId,
    std::vector<std::unique_ptr<Sources::SourceHandle>>&& sources,
    std::unordered_set<std::shared_ptr<NES::Sinks::Sink>>&& sinks,
    std::vector<std::shared_ptr<ExecutablePipeline>>&& pipelines,
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
    /// not tracking sinks. Sinks don't have a successor and therefore don't need to notify their successors. We rely on the sink destructor.
    /// Todo #34: improved by @ls-1801 in the QueryManager refactoring.
    numOfTerminationTokens.store(1 + this->sources.size() + this->pipelines.size());
}

std::shared_ptr<ExecutableQueryPlan> ExecutableQueryPlan::create(
    QueryId queryId,
    std::vector<std::unique_ptr<Sources::SourceHandle>>&& sources,
    std::unordered_set<std::shared_ptr<NES::Sinks::Sink>>&& sinks,
    std::vector<std::shared_ptr<ExecutablePipeline>> pipelines,
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

const std::vector<std::shared_ptr<ExecutablePipeline>>& ExecutableQueryPlan::getPipelines() const
{
    return pipelines;
}

ExecutableQueryPlan::~ExecutableQueryPlan()
{
    NES_DEBUG("destroy qep {}", queryId);
    NES_ASSERT(QueryStatusUtil::isRegisteredButNotRunning(queryStatus.load()), "QueryPlan is created but not executing " << queryId);
    destroy();
}

std::shared_future<ExecutableQueryPlan::Result> ExecutableQueryPlan::getTerminationFuture()
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
        qepTerminationStatusPromise.set_value(Result::FAILED);
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
    auto expected = QueryStatus::Registered;
    if (queryStatus.compare_exchange_strong(expected, QueryStatus::Deployed))
    {
        for (auto& stage : pipelines)
        {
            /// Compile ExecutablePipeline
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
    auto expected = QueryStatus::Deployed;
    if (queryStatus.compare_exchange_strong(expected, QueryStatus::Running))
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

const std::vector<std::unique_ptr<Sources::SourceHandle>>& ExecutableQueryPlan::getSources() const
{
    return sources;
}

const std::unordered_set<std::shared_ptr<NES::Sinks::Sink>>& ExecutableQueryPlan::getSinks() const
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
            qepTerminationStatusPromise.set_value(Result::OK);
            return true; /// correct stop
        }

        queryStatus.store(QueryStatus::Failed);
        qepTerminationStatusPromise.set_value(Result::FAILED);

        return false; /// one stage failed to stop
    }

    if (expected == QueryStatus::Stopped)
    {
        return true; /// we have tried to stop the same QEP twice.
    }
    NES_ERROR("Something is wrong with query {} as it was not possible to stop", queryId);
    /// if we get there it mean the CAS failed and expected is the current value
    queryStatus.wait(QueryStatus::Failed);

    qepTerminationStatusPromise.set_value(Result::FAILED);

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
            queryManager->notifyQueryStatusChange(shared_from_base<ExecutableQueryPlan>(), QueryStatus::Failed);
            break;
        }
        case ReconfigurationType::HardEndOfStream: {
            NES_DEBUG("Executing HardEndOfStream on qep QueryId={}", queryId);
            NES_ASSERT2_FMT((numOfTerminationTokens.fetch_sub(1) == 1), "Invalid HardEndOfStream on qep QueryId=" << queryId);
            NES_ASSERT2_FMT(stop(), "Cannot hard stop qep QueryId=" << queryId);
            queryManager->notifyQueryStatusChange(shared_from_base<ExecutableQueryPlan>(), QueryStatus::Stopped);
            break;
        }
        case ReconfigurationType::SoftEndOfStream: {
            NES_DEBUG("QueryExecutionPlan: soft stop request received for query plan {} left tokens = {}", queryId, numOfTerminationTokens);
            if (numOfTerminationTokens.fetch_sub(1) == 1)
            {
                auto expected = QueryStatus::Running;
                if (queryStatus.compare_exchange_strong(expected, QueryStatus::Finished))
                {
                    /// if CAS fails - it means the query was already stopped or failed
                    NES_DEBUG("QueryExecutionPlan: query plan {} is marked as (soft) stopped now", queryId);
                    qepTerminationStatusPromise.set_value(Result::OK);
                    queryManager->notifyQueryStatusChange(shared_from_base<ExecutableQueryPlan>(), QueryStatus::Finished);
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
    for (const auto& pipeline : pipelines)
    {
        NES_ASSERT(!pipeline->isRunning(), "Pipeline " << pipeline->getPipelineId() << " is still running");
    }
    pipelines.clear();
    sinks.clear();
    bufferManager.reset();
}

void ExecutableQueryPlan::notifySourceCompletion(OriginId, QueryTerminationType terminationType)
{
    NES_DEBUG("QEP {} Source {} is notifying completion: {}", queryId, 0, terminationType);
    /// TODO #306: assert is commented out. We need to fix the source termination logic
    /// NES_ASSERT2_FMT(queryStatus.load() == QueryStatus::Running, "Cannot complete source on non running query plan id=" << queryId);
    /// TODO #306: assert is commented out. We need to fix the source termination logic
    /// NES_ASSERT2_FMT(it != sources.end(), "Cannot find source " << sourceId << " in query plan " << queryId);
    uint32_t tokensLeft = numOfTerminationTokens.fetch_sub(1);
    /// TODO #306: assert is commented out. We need to fix the source termination logic
    /// NES_ASSERT2_FMT(tokensLeft >= 1, "Source was last termination token for " << queryId << " = " << terminationType);
    NES_DEBUG("QEP {} Source {} is terminated; tokens left = {}", queryId, 0, (tokensLeft - 1));
    /// the following check is necessary because a data sources first emits an EoS marker and then calls this method.
    /// However, it might happen that the marker is so fast that one possible execution is as follows:
    /// 1) Data Source emits EoS marker
    /// 2) Next pipeline/sink picks the marker and finishes by decreasing `numOfTerminationTokens`
    /// 3) SourceThread invokes `notifySourceCompletion`
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

void ExecutableQueryPlan::notifyPipelineCompletion(std::shared_ptr<ExecutablePipeline> pipeline, QueryTerminationType terminationType)
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

}
