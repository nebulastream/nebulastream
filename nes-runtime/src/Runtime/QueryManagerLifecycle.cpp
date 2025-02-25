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
#include <Reconfiguration/Metadata/UpdateAndDrainQueryMetadata.hpp>
#include <Runtime/AsyncTaskExecutor.hpp>
#include <Runtime/Execution/ExecutablePipeline.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/ExecutableQueryPlan.hpp>
#include <Runtime/Execution/ExecutableQueryPlanStatus.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/HardwareManager.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/ThreadPool.hpp>
#include <Runtime/WorkerContext.hpp>
#include <iostream>
#include <memory>
#include <stack>
#include <utility>

namespace NES::Runtime {
bool AbstractQueryManager::registerExecutableQueryPlan(const Execution::ExecutableQueryPlanPtr& executableQueryPlan, bool replayData) {
    const auto& sharedQueryId = executableQueryPlan->getSharedQueryId();
    const auto& decomposedQueryId = executableQueryPlan->getDecomposedQueryId();
    const auto& decomposedQueryVersion = executableQueryPlan->getDecomposedQueryVersion();

    if (replayData) {
        for (auto source : executableQueryPlan->getSources()) {
            source->setReplayData();
        }
        for (auto sink : executableQueryPlan->getSinks()) {
            sink->setReplayData();
        }
    }

    auto decomposedQueryIdWithVersion = DecomposedQueryIdWithVersion(decomposedQueryId, decomposedQueryVersion);

    NES_DEBUG("AbstractQueryManager::registerExecutableQueryPlan: shared query id {} and decomposed query id {}.{}",
              sharedQueryId,
              decomposedQueryId,
              decomposedQueryVersion);

    NES_ASSERT2_FMT(queryManagerStatus.load() == QueryManagerStatus::Running,
                    "AbstractQueryManager::registerDecomposedQuery: cannot accept new shared query id "
                        << sharedQueryId << " and decomposed query id " << decomposedQueryId << "." << decomposedQueryVersion);
    {
        std::scoped_lock lock(queryMutex);
        // test if elements already exist
        NES_DEBUG("AbstractQueryManager: resolving sources for shared query {} and decomposed query {}.{} with sources= {}",
                  sharedQueryId,
                  decomposedQueryId,
                  decomposedQueryVersion,
                  executableQueryPlan->getSources().size());

        std::vector<OperatorId> sourceIds;
        auto sourcesToReuse = executableQueryPlan->getSourcesToReuse();
        for (const auto& source : executableQueryPlan->getSources()) {
            OperatorId sourceOperatorId = source->getOperatorId();

            //check if a running source of an existing plan is to be reused. If not, start a new one
            if (std::find(sourcesToReuse.cbegin(), sourcesToReuse.cend(), sourceOperatorId) == sourcesToReuse.cend()) {
                // source already exists, add qep to source set if not there
                sourceIds.emplace_back(sourceOperatorId);
                NES_DEBUG("AbstractQueryManager: Source  {}  not found. Creating new element with qep ", sourceOperatorId);

                //If source sharing is active and this is the first query for this source, init the map
                if (sourceToQEPMapping.find(sourceOperatorId) == sourceToQEPMapping.end()) {
                    sourceToQEPMapping[sourceOperatorId] = std::vector<Execution::ExecutableQueryPlanPtr>();
                }

                //bookkeep which qep is now reading from this source
                sourceToQEPMapping[sourceOperatorId].push_back(executableQueryPlan);
            }
        }
        decomposeQueryToSourceIdMapping[decomposedQueryIdWithVersion] = sourceIds;
    }

    NES_DEBUG("queryToStatisticsMap add for= {}  pair queryId= {}  subplanId= {}",
              decomposedQueryId,
              sharedQueryId,
              decomposedQueryId);

    //TODO: This assumes 1) that there is only one pipeline per query and 2) that the subqueryplan id is unique => both can become a problem
    queryToStatisticsMap.insert(decomposedQueryId, std::make_shared<QueryStatistics>(sharedQueryId, decomposedQueryId));

    NES_ASSERT2_FMT(queryManagerStatus.load() == QueryManagerStatus::Running,
                    "AbstractQueryManager::startDecomposedQuery: cannot accept new query id " << decomposedQueryId << " "
                                                                                              << sharedQueryId);
    NES_ASSERT(executableQueryPlan->getStatus() == Execution::ExecutableQueryPlanStatus::Created,
               "Invalid status for starting the QEP " << decomposedQueryId);

    // 1. start the qep and handlers, if any
    if (!executableQueryPlan->setup() || !executableQueryPlan->start()) {
        NES_FATAL_ERROR("AbstractQueryManager: query execution plan could not started");
        return false;
    }

    if (!executableQueryPlan->shouldDelayStart()) {
        std::vector<AsyncTaskExecutor::AsyncTaskFuture<bool>> startFutures;

        std::vector<Network::NetworkSourcePtr> netSources;
        std::vector<Network::NetworkSinkPtr> netSinks;

        // 2a. sort net sinks and sources
        for (const auto& sink : executableQueryPlan->getSinks()) {
            if (auto netSink = std::dynamic_pointer_cast<Network::NetworkSink>(sink); netSink) {
                netSinks.emplace_back(netSink);
            }
        }
        std::sort(netSinks.begin(), netSinks.end(), [](const Network::NetworkSinkPtr& lhs, const Network::NetworkSinkPtr& rhs) {
            return *lhs < *rhs;
        });
        for (const auto& sink : executableQueryPlan->getSources()) {
            if (auto netSource = std::dynamic_pointer_cast<Network::NetworkSource>(sink); netSource) {
                netSources.emplace_back(netSource);
            }
        }
        std::sort(netSources.begin(),
                  netSources.end(),
                  [](const Network::NetworkSourcePtr& lhs, const Network::NetworkSourcePtr& rhs) {
                      return *lhs < *rhs;
                  });

        // 2b. pre-start net sinks
        for (const auto& netSink : netSinks) {
            netSink->preSetup();
        }

        // 2b. start net sinks
        for (const auto& sink : netSinks) {
            startFutures.emplace_back(asyncTaskExecutor->runAsync([sink]() {
                NES_DEBUG("AbstractQueryManager: start network sink  {}", sink->toString());
                sink->setup();
                return true;
            }));
        }

        auto sourcesToReuse = executableQueryPlan->getSourcesToReuse();
        bool registeredAsSuccessor = false;

        // 3a. pre-start net sources
        for (const auto& source : netSources) {

            //check if source is an existing source to be reused
            if (std::find(sourcesToReuse.begin(), sourcesToReuse.end(), source->getUniqueId()) != sourcesToReuse.end()) {
                if (!registeredAsSuccessor) {
                    auto predecessorPlans = sourceToQEPMapping[source->getOperatorId()];

                    if (predecessorPlans.size() > 1) {
                        NES_FATAL_ERROR(
                            "AbstractQueryManager: source {} is used by multiple plans, reusing this source is not supported",
                            source->getOperatorId());
                    }

                    auto predecessorPlan = predecessorPlans.front();

                    predecessorPlan->addSuccessorPlan(executableQueryPlan);
                    registeredAsSuccessor = true;
                }

                //do not bind
                continue;
            }

            std::stringstream s;
            s << source;
            std::string sourceString = s.str();
            if (!source->bind()) {
                NES_WARNING("AbstractQueryManager: network source {} could not started as it is already running", sourceString);
            } else {
                NES_DEBUG("AbstractQueryManager: network source  {}  started successfully", sourceString);
            }
        }
		//todo: before starting any sources, check that incremental property is set
        // 3b. start net sources
        for (const auto& source : netSources) {
            std::stringstream s;
            s << source;
            std::string sourceString = s.str();
            if (!source->start()) {
                NES_WARNING("AbstractQueryManager: network source {} could not started as it is already running", sourceString);
            } else {
                NES_DEBUG("AbstractQueryManager: network source  {}  started successfully", sourceString);
            }
        }

        // 4. start data sinks
        for (const auto& sink : executableQueryPlan->getSinks()) {
            if (std::dynamic_pointer_cast<Network::NetworkSink>(sink)) {
                continue;
            }
            NES_DEBUG("AbstractQueryManager: start sink  {}", sink->toString());
            sink->setup();
        }
    }

    {
        std::unique_lock lock(queryMutex);
        runningQEPs.emplace(decomposedQueryIdWithVersion, executableQueryPlan);
    }

    return true;
}

bool MultiQueueQueryManager::registerExecutableQueryPlan(const Execution::ExecutableQueryPlanPtr& qep, bool replay) {
    auto ret = AbstractQueryManager::registerExecutableQueryPlan(qep, replay);
    std::scoped_lock lock(queryMutex);
    NES_ASSERT2_FMT(queryToStatisticsMap.size() <= numberOfQueues,
                    "AbstractQueryManager::registerDecomposedQuery: not enough queues are free for numberOfQueues="
                        << numberOfQueues << " query cnt=" << queryToStatisticsMap.size());
    //currently we assume all queues have same number of threads, so we can do this.
    queryToTaskQueueIdMap[qep->getDecomposedQueryId()] = currentTaskQueueId++;
    NES_DEBUG("queryToTaskQueueIdMap add for= {}  queue= {}", qep->getSharedQueryId(), currentTaskQueueId - 1);
    return ret;
}

bool AbstractQueryManager::startExecutableQueryPlan(const Execution::ExecutableQueryPlanPtr& qep, uint64_t reconnectCount) {
    NES_DEBUG("AbstractQueryManager::startDecomposedQuery: shared query id  {}  and decomposed query plan id {}",
              qep->getSharedQueryId(),
              qep->getDecomposedQueryId());
    NES_ASSERT2_FMT(queryManagerStatus.load() == QueryManagerStatus::Running,
                    "AbstractQueryManager::startDecomposedQuery: cannot accept new shared query id "
                        << qep->getSharedQueryId() << " and decomposed query id " << qep->getDecomposedQueryId());
    //    NES_ASSERT(qep->getStatus() == Execution::ExecutableQueryPlanStatus::Running,
    //               "Invalid status for starting the QEP " << qep->getQuerySubPlanId());
    if (qep->shouldDelayStart()) {
        std::thread thread([qep = qep,
                            queryToStatisticsMap = queryToStatisticsMap,
                            asyncTaskExecutor = asyncTaskExecutor,
                            sourceToQEPMapping = sourceToQEPMapping,
                            reconnectCount = reconnectCount]() mutable {
            // Recreate
            if (!qep->checkRecreation()) {
                NES_FATAL_ERROR("AbstractQueryManager: query execution plan could not started");
            }

            std::vector<AsyncTaskExecutor::AsyncTaskFuture<bool>> startFutures;

            std::vector<Network::NetworkSourcePtr> netSources;
            std::vector<Network::NetworkSinkPtr> netSinks;

            // 2a. sort net sinks and sources
            for (const auto& sink : qep->getSinks()) {
                if (auto netSink = std::dynamic_pointer_cast<Network::NetworkSink>(sink); netSink) {
                    netSinks.emplace_back(netSink);
                }
            }
            std::sort(netSinks.begin(), netSinks.end(), [](const Network::NetworkSinkPtr& lhs, const Network::NetworkSinkPtr& rhs) {
                return *lhs < *rhs;
            });
            for (const auto& sink : qep->getSources()) {
                if (auto netSource = std::dynamic_pointer_cast<Network::NetworkSource>(sink); netSource) {
                    netSources.emplace_back(netSource);
                }
            }
            std::sort(netSources.begin(),
                      netSources.end(),
                      [](const Network::NetworkSourcePtr& lhs, const Network::NetworkSourcePtr& rhs) {
                          return *lhs < *rhs;
                      });

            // 2b. pre-start net sinks
            for (const auto& netSink : netSinks) {
                netSink->preSetup();
            }

            // 2b. start net sinks
            for (const auto& sink : netSinks) {
                startFutures.emplace_back(asyncTaskExecutor->runAsync([sink]() {
                    NES_DEBUG("AbstractQueryManager: start network sink  {}", sink->toString());
                    sink->setup();
                    return true;
                }));
            }

            auto sourcesToReuse = qep->getSourcesToReuse();
            bool registeredAsSuccessor = false;

            // 3a. pre-start net sources
            for (const auto& source : netSources) {

                //check if source is an existing source to be reused
                if (std::find(sourcesToReuse.begin(), sourcesToReuse.end(), source->getUniqueId()) != sourcesToReuse.end()) {
                    if (!registeredAsSuccessor) {
                        auto predecessorPlans = sourceToQEPMapping[source->getOperatorId()];

                        if (predecessorPlans.size() > 1) {
                            NES_FATAL_ERROR(
                                "AbstractQueryManager: source {} is used by multiple plans, reusing this source is not supported",
                                source->getOperatorId());
                        }

                        auto predecessorPlan = predecessorPlans.front();

                        predecessorPlan->addSuccessorPlan(qep);
                        registeredAsSuccessor = true;
                    }

                    //do not bind
                    continue;
                }

                std::stringstream s;
                s << source;
                std::string sourceString = s.str();
                if (!source->bind()) {
                    NES_WARNING("AbstractQueryManager: network source {} could not started as it is already running", sourceString);
                } else {
                    NES_DEBUG("AbstractQueryManager: network source  {}  started successfully", sourceString);
                }
            }

            // 3b. start net sources
            for (const auto& source : netSources) {
                std::stringstream s;
                s << source;
                std::string sourceString = s.str();
                if (!source->start()) {
                    NES_WARNING("AbstractQueryManager: network source {} could not started as it is already running", sourceString);
                } else {
                    NES_DEBUG("AbstractQueryManager: network source  {}  started successfully", sourceString);
                }
            }

            for (auto& future : startFutures) {
                NES_ASSERT(future.wait(), "Cannot start query");
            }

            // 4. start data sinks
            for (const auto& sink : qep->getSinks()) {
                if (std::dynamic_pointer_cast<Network::NetworkSink>(sink)) {
                    continue;
                }
                NES_DEBUG("AbstractQueryManager: start sink  {}", sink->toString());
                sink->setup();
            }

			for (const auto& sink : qep->getSinks()) {
        		//cast to network sink
        		auto networkSink = std::dynamic_pointer_cast<Network::NetworkSink>(sink);
       		 	if (networkSink) {
                    networkSink->setReconnectCount(reconnectCount);
                }
        	}

            // 5. start data sources
            for (const auto& source : qep->getSources()) {
                if (std::dynamic_pointer_cast<Network::NetworkSource>(source)) {
                    continue;
                }
                std::stringstream s;
                s << source;
                std::string sourceString = s.str();
                NES_DEBUG("AbstractQueryManager: start source  {}  str= {}", sourceString, source->toString());
                if (!source->start()) {
                    NES_WARNING("AbstractQueryManager: source {} could not started as it is already running", sourceString);
                } else {
                    NES_DEBUG("AbstractQueryManager: source  {}  started successfully", sourceString);
                }
            }

            // register start timestamp of query in statistics
            if (queryToStatisticsMap.contains(qep->getDecomposedQueryId())) {
                auto statistics = queryToStatisticsMap.find(qep->getDecomposedQueryId());
                if (statistics->getTimestampQueryStart() == 0) {
                    auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                                   std::chrono::high_resolution_clock::now().time_since_epoch())
                                   .count();
                    statistics->setTimestampQueryStart(now, true);
                } else {
                    NES_DEBUG("Start timestamp already exists, this is expected in case of query reconfiguration");
                }
            } else {
                NES_FATAL_ERROR("queryToStatisticsMap not set, this should only happen for testing");
                NES_THROW_RUNTIME_ERROR("got buffer for not registered qep");
            }
        });

        thread.detach();
    } else {

        for (const auto& sink : qep->getSinks()) {
            //cast to network sink
            auto networkSink = std::dynamic_pointer_cast<Network::NetworkSink>(sink);
            if (networkSink) {
                networkSink->setReconnectCount(reconnectCount);
            }
        }

        // 5. start data sources
        for (const auto& source : qep->getSources()) {
            if (std::dynamic_pointer_cast<Network::NetworkSource>(source)) {
                continue;
            }
            std::stringstream s;
            s << source;
            std::string sourceString = s.str();
            NES_DEBUG("AbstractQueryManager: start source  {}  str= {}", sourceString, source->toString());
            if (!source->start()) {
                NES_WARNING("AbstractQueryManager: source {} could not started as it is already running", sourceString);
            } else {
                NES_DEBUG("AbstractQueryManager: source  {}  started successfully", sourceString);
            }
        }

        // register start timestamp of query in statistics
        if (queryToStatisticsMap.contains(qep->getDecomposedQueryId())) {
            auto statistics = queryToStatisticsMap.find(qep->getDecomposedQueryId());
            if (statistics->getTimestampQueryStart() == 0) {
                auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                               std::chrono::high_resolution_clock::now().time_since_epoch())
                               .count();
                statistics->setTimestampQueryStart(now, true);
            } else {
                NES_DEBUG("Start timestamp already exists, this is expected in case of query reconfiguration");
            }
        } else {
            NES_FATAL_ERROR("queryToStatisticsMap not set, this should only happen for testing");
            NES_THROW_RUNTIME_ERROR("got buffer for not registered qep");
        }
    }
    return true;
}

bool AbstractQueryManager::unregisterExecutableQueryPlan(const Execution::ExecutableQueryPlanPtr& executableQueryPlan) {
    const auto& sharedQueryId = executableQueryPlan->getSharedQueryId();
    const auto& decomposedQueryId = executableQueryPlan->getDecomposedQueryId();
    const auto& decomposedQueryVersion = executableQueryPlan->getDecomposedQueryVersion();

    auto decomposedQueryIdWithVersion = DecomposedQueryIdWithVersion(decomposedQueryId, decomposedQueryVersion);
    NES_DEBUG("AbstractQueryManager::deregisterAndUndeployQuery: shared query id {} and decomposed query id {}.{}",
              sharedQueryId,
              decomposedQueryId,
              decomposedQueryVersion);
    std::unique_lock lock(queryMutex);
    auto qepStatus = executableQueryPlan->getStatus();
    NES_ASSERT2_FMT(qepStatus == Execution::ExecutableQueryPlanStatus::Finished
                        || qepStatus == Execution::ExecutableQueryPlanStatus::ErrorState
                        || qepStatus == Execution::ExecutableQueryPlanStatus::Stopped,
                    "Cannot unregisterExecutableQueryPlan query " << decomposedQueryId << "." << decomposedQueryVersion
                                                                  << " as it is not stopped/failed");

    auto registeredSourceIds = decomposeQueryToSourceIdMapping[decomposedQueryIdWithVersion];
    for (auto const& sourceId : registeredSourceIds) {
        std::erase_if(sourceToQEPMapping.at(sourceId),
                      [decomposedQueryId, decomposedQueryVersion](Execution::ExecutableQueryPlanPtr plan) {
                          return plan->getDecomposedQueryId() == decomposedQueryId
                              && plan->getDecomposedQueryVersion() == decomposedQueryVersion;
                      });
    }
    decomposeQueryToSourceIdMapping.erase(decomposedQueryIdWithVersion);
    runningQEPs.erase(decomposedQueryIdWithVersion);
    return true;
}

bool AbstractQueryManager::canTriggerEndOfStream(DataSourcePtr source, Runtime::QueryTerminationType terminationType) {
    std::unique_lock lock(queryMutex);
    NES_ASSERT2_FMT(sourceToQEPMapping.contains(source->getOperatorId()),
                    "source=" << source->getOperatorId() << " wants to terminate but is not part of the mapping process");

    bool overallResult = true;
    //we have to check for each query on this source if we can terminate and return a common answer
    for (auto qep : sourceToQEPMapping[source->getOperatorId()]) {
        bool ret = queryStatusListener->canTriggerEndOfStream(qep->getSharedQueryId(),
                                                              qep->getDecomposedQueryId(),
                                                              source->getOperatorId(),
                                                              terminationType);
        if (!ret) {
            NES_ERROR("Query cannot trigger EOS for query manager for shared query id {}", qep->getSharedQueryId());
            NES_THROW_RUNTIME_ERROR("cannot trigger EOS in canTriggerEndOfStream()");
        }
        overallResult &= ret;
    }
    return overallResult;
}

bool AbstractQueryManager::failExecutableQueryPlan(const Execution::ExecutableQueryPlanPtr& qep) {
    bool ret = true;
    NES_DEBUG("AbstractQueryManager::failExecutableQueryPlan: shared query id {}", qep->getSharedQueryId());
    switch (qep->getStatus()) {
        case Execution::ExecutableQueryPlanStatus::ErrorState:
        case Execution::ExecutableQueryPlanStatus::Finished:
        case Execution::ExecutableQueryPlanStatus::Stopped: {
            return true;
        }
        case Execution::ExecutableQueryPlanStatus::Invalid: {
            NES_ASSERT2_FMT(false, "not supported yet " << qep->getDecomposedQueryId());
            break;
        }
        default: {
            break;
        }
    }
    for (const auto& source : qep->getSources()) {
        NES_ASSERT2_FMT(source->fail(),
                        "Cannot fail source " << source->getOperatorId()
                                              << " belonging to query plan=" << qep->getDecomposedQueryId());
    }

    auto terminationFuture = qep->getTerminationFuture();
    auto terminationStatus = terminationFuture.wait_for(std::chrono::minutes(10));
    switch (terminationStatus) {
        case std::future_status::ready: {
            if (terminationFuture.get() != Execution::ExecutableQueryPlanResult::Fail) {
                NES_FATAL_ERROR("AbstractQueryManager: QEP {} could not be failed", qep->getDecomposedQueryId());
                ret = false;
            }
            break;
        }
        case std::future_status::timeout:
        case std::future_status::deferred: {
            // TODO we need to fail the query now as it could not be stopped?
            NES_ASSERT2_FMT(false, "Cannot stop query within deadline getDecomposedQueryId()=" << qep->getDecomposedQueryId());
            break;
        }
    }
    if (ret) {
        addReconfigurationMessage(qep->getSharedQueryId(),
                                  qep->getDecomposedQueryId(),
                                  qep->getDecomposedQueryVersion(),
                                  ReconfigurationMessage(qep->getSharedQueryId(),
                                                         qep->getDecomposedQueryId(),
                                                         qep->getDecomposedQueryVersion(),
                                                         ReconfigurationType::Destroy,
                                                         inherited1::shared_from_this()),
                                  true);
    }
    NES_DEBUG("AbstractQueryManager::failExecutableQueryPlan: query {}.{} was {}",
              qep->getDecomposedQueryId(),
              qep->getDecomposedQueryVersion(),
              (ret ? "successful" : " not successful"));
    return true;
}

bool AbstractQueryManager::stopExecutableQueryPlan(const Execution::ExecutableQueryPlanPtr& qep,
                                                   Runtime::QueryTerminationType type) {
    const auto& sharedQueryId = qep->getSharedQueryId();
    const auto& decomposedQueryId = qep->getDecomposedQueryId();
    const auto& decomposedQueryVersion = qep->getDecomposedQueryVersion();
    NES_DEBUG("AbstractQueryManager::stopDecomposedQueryPlan: share query id {} and decomposed query plan id  {}.{}  type= {}",
                sharedQueryId,
                decomposedQueryId,
                decomposedQueryVersion,
                type);
    NES_ASSERT2_FMT(type != QueryTerminationType::Failure,
                    "Requested stop with failure for query " << decomposedQueryId << "." << decomposedQueryVersion);
    bool ret = true;
    switch (qep->getStatus()) {
        case Execution::ExecutableQueryPlanStatus::ErrorState:
        case Execution::ExecutableQueryPlanStatus::Finished:
        case Execution::ExecutableQueryPlanStatus::Stopped: {
            NES_DEBUG("AbstractQueryManager::stopDecomposedQueryPlan: query decomposed query plan id  {}.{}  is already stopped",
                      decomposedQueryId,
                      decomposedQueryVersion);
            return true;
        }
        case Execution::ExecutableQueryPlanStatus::Invalid: {
            NES_ASSERT2_FMT(false, "not supported yet " << decomposedQueryId << "." << decomposedQueryVersion);
            break;
        }
        default: {
            break;
        }
    }

    NES_DEBUG("stopping sources")
    for (const auto& source : qep->getSources()) {
        if (type == QueryTerminationType::Graceful) {
            // graceful shutdown :: only leaf sources
            if (!std::dynamic_pointer_cast<Network::NetworkSource>(source)) {
                NES_ASSERT2_FMT(source->performSoftStop(),
                                "Cannot terminate source data gathering loop " << source->getOperatorId());
            }
        } else if (type == QueryTerminationType::HardStop) {
            NES_ASSERT2_FMT(source->stop(QueryTerminationType::HardStop), "Cannot terminate source " << source->getOperatorId());
        }
    }

    NES_DEBUG("stopping pipelines")
    if (type == QueryTerminationType::HardStop || type == QueryTerminationType::Failure) {
        for (auto& stage : qep->getPipelines()) {
            NES_ASSERT2_FMT(stage->stop(type), "Cannot hard stop pipeline " << stage->getPipelineId());
        }
    }

    NES_DEBUG("wait for termination future")
    // TODO evaluate if we need to have this a wait instead of a get
    // TODO for instance we could wait N seconds and if the stopped is not successful by then
    // TODO we need to trigger a hard local kill of a QEP
    auto terminationFuture = qep->getTerminationFuture();
    auto terminationStatus = terminationFuture.wait_for(std::chrono::minutes(10));
    NES_WARNING("Received termination status {} for shared query id {} and decomposed query id {}",
                magic_enum::enum_name(terminationStatus),
                sharedQueryId,
                decomposedQueryId);
    switch (terminationStatus) {
        case std::future_status::ready: {
            if (terminationFuture.get() != Execution::ExecutableQueryPlanResult::Ok) {
                NES_FATAL_ERROR("AbstractQueryManager: QEP {} could not be stopped", decomposedQueryId);
                ret = false;
            }
            break;
        }
        case std::future_status::timeout:
        case std::future_status::deferred: {
            // TODO we need to fail the query now as it could not be stopped?
            NES_ASSERT2_FMT(false, "Cannot stop query within deadline " << decomposedQueryId);
            break;
        }
    }
    NES_DEBUG("ret = {}", ret);
    if (ret) {
        addReconfigurationMessage(sharedQueryId,
                                  decomposedQueryId,
                                  decomposedQueryVersion,
                                  ReconfigurationMessage(sharedQueryId,
                                                         decomposedQueryId,
                                                         decomposedQueryVersion,
                                                         ReconfigurationType::Destroy,
                                                         inherited1::shared_from_this()),
                                  true);
    }
    NES_DEBUG("AbstractQueryManager::stopDecomposedQueryPlan: shared query id {} and decomposed query id {}.{} was {}",
              sharedQueryId,
              decomposedQueryId,
              decomposedQueryVersion,
              (ret ? "successful" : " not successful"));
    return ret;
}

bool AbstractQueryManager::addSoftEndOfStream(DataSourcePtr source) {
    auto sourceId = source->getOperatorId();
    auto pipelineSuccessors = source->getExecutableSuccessors();

    // send EOS to `source` itself, iff a network source
    if (auto netSource = std::dynamic_pointer_cast<Network::NetworkSource>(source); netSource != nullptr) {
        //add soft eaos for network source
        auto reconfMessage = ReconfigurationMessage(INVALID_SHARED_QUERY_ID,
                                                    INVALID_DECOMPOSED_QUERY_PLAN_ID,
                                                    INVALID_DECOMPOSED_QUERY_PLAN_VERSION,
                                                    ReconfigurationType::SoftEndOfStream,
                                                    netSource);
        addReconfigurationMessage(INVALID_SHARED_QUERY_ID,
                                  INVALID_DECOMPOSED_QUERY_PLAN_ID,
                                  INVALID_DECOMPOSED_QUERY_PLAN_VERSION,
                                  reconfMessage,
                                  false);
    }

    for (auto successor : pipelineSuccessors) {
        // create reconfiguration message. If the successor is an executable pipeline we send a reconfiguration message to the pipeline.
        // If successor is a data sink we send the reconfiguration message to the query plan.
        if (auto* executablePipeline = std::get_if<Execution::ExecutablePipelinePtr>(&successor)) {
            auto reconfMessage = ReconfigurationMessage(executablePipeline->get()->getSharedQueryId(),
                                                        executablePipeline->get()->getDecomposedQueryId(),
                                                        executablePipeline->get()->getDecomposedQueryVersion(),
                                                        ReconfigurationType::SoftEndOfStream,
                                                        (*executablePipeline));
            addReconfigurationMessage(executablePipeline->get()->getSharedQueryId(),
                                      executablePipeline->get()->getDecomposedQueryId(),
                                      executablePipeline->get()->getDecomposedQueryVersion(),
                                      reconfMessage,
                                      false);
            NES_DEBUG("soft end-of-stream Exec Pipeline opId={} reconfType={} queryExecutionPlanId={}.{} "
                      "threadPool->getNumberOfThreads()={} qep {}",
                      sourceId,
                      magic_enum::enum_name(ReconfigurationType::SoftEndOfStream),
                      executablePipeline->get()->getDecomposedQueryId(),
                      executablePipeline->get()->getDecomposedQueryVersion(),
                      threadPool->getNumberOfThreads(),
                      executablePipeline->get()->getSharedQueryId());
        } else if (auto* sink = std::get_if<DataSinkPtr>(&successor)) {
            auto reconfMessageSink = ReconfigurationMessage(sink->get()->getSharedQueryId(),
                                                            sink->get()->getParentPlanId(),
                                                            sink->get()->getParentPlanVersion(),
                                                            ReconfigurationType::SoftEndOfStream,
                                                            (*sink));
            addReconfigurationMessage(sink->get()->getSharedQueryId(),
                                      sink->get()->getParentPlanId(),
                                      sink->get()->getParentPlanVersion(),
                                      reconfMessageSink,
                                      false);
            NES_DEBUG(
                "soft end-of-stream Sink opId={} reconfType={} queryExecutionPlanId={} threadPool->getNumberOfThreads()={} qep{}",
                sourceId,
                magic_enum::enum_name(ReconfigurationType::SoftEndOfStream),
                sink->get()->getParentPlanId(),
                threadPool->getNumberOfThreads(),
                sink->get()->getSharedQueryId());
        }
    }
    return true;
}

bool AbstractQueryManager::addHardEndOfStream(DataSourcePtr source) {
    auto sourceId = source->getOperatorId();
    auto pipelineSuccessors = source->getExecutableSuccessors();

    for (auto successor : pipelineSuccessors) {
        // create reconfiguration message. If the successor is an executable pipeline we send a reconfiguration message to the pipeline.
        // If successor is a data sink we send the reconfiguration message to the query plan.
        if (auto* executablePipeline = std::get_if<Execution::ExecutablePipelinePtr>(&successor)) {
            auto reconfMessage = ReconfigurationMessage(executablePipeline->get()->getSharedQueryId(),
                                                        executablePipeline->get()->getDecomposedQueryId(),
                                                        executablePipeline->get()->getDecomposedQueryVersion(),
                                                        ReconfigurationType::HardEndOfStream,
                                                        (*executablePipeline));
            addReconfigurationMessage(executablePipeline->get()->getSharedQueryId(),
                                      executablePipeline->get()->getDecomposedQueryId(),
                                      executablePipeline->get()->getDecomposedQueryVersion(),
                                      reconfMessage,
                                      false);
            NES_DEBUG("hard end-of-stream Exec Op opId={} reconfType={} queryId={} querySubPlanId={} "
                      "threadPool->getNumberOfThreads()={}",
                      sourceId,
                      magic_enum::enum_name(ReconfigurationType::HardEndOfStream),
                      executablePipeline->get()->getSharedQueryId(),
                      executablePipeline->get()->getDecomposedQueryId(),
                      threadPool->getNumberOfThreads());
        } else if (auto* sink = std::get_if<DataSinkPtr>(&successor)) {
            auto reconfMessageSink = ReconfigurationMessage(sink->get()->getSharedQueryId(),
                                                            sink->get()->getParentPlanId(),
                                                            sink->get()->getParentPlanVersion(),
                                                            ReconfigurationType::HardEndOfStream,
                                                            (*sink));
            addReconfigurationMessage(sink->get()->getSharedQueryId(),
                                      sink->get()->getParentPlanId(),
                                      sink->get()->getParentPlanVersion(),
                                      reconfMessageSink,
                                      false);
            NES_DEBUG("hard end-of-stream Sink opId={} reconfType={} queryId={} querySubPlanId={}.{} "
                      "threadPool->getNumberOfThreads()={}",
                      sourceId,
                      magic_enum::enum_name(ReconfigurationType::HardEndOfStream),
                      sink->get()->getSharedQueryId(),
                      sink->get()->getParentPlanId(),
                      sink->get()->getParentPlanVersion(),
                      threadPool->getNumberOfThreads());
        }
    }

    return true;
}

bool AbstractQueryManager::addFailureEndOfStream(DataSourcePtr source) {
    auto sourceId = source->getOperatorId();
    auto pipelineSuccessors = source->getExecutableSuccessors();

    for (auto successor : pipelineSuccessors) {
        // create reconfiguration message. If the successor is an executable pipeline we send a reconfiguration message to the pipeline.
        // If successor is a data sink we send the reconfiguration message to the query plan.
        if (auto* executablePipeline = std::get_if<Execution::ExecutablePipelinePtr>(&successor)) {
            auto reconfMessage = ReconfigurationMessage(executablePipeline->get()->getSharedQueryId(),
                                                        executablePipeline->get()->getDecomposedQueryId(),
                                                        executablePipeline->get()->getDecomposedQueryVersion(),
                                                        ReconfigurationType::FailEndOfStream,
                                                        (*executablePipeline));
            addReconfigurationMessage(executablePipeline->get()->getSharedQueryId(),
                                      executablePipeline->get()->getDecomposedQueryId(),
                                      executablePipeline->get()->getDecomposedQueryVersion(),
                                      reconfMessage,
                                      false);

            NES_DEBUG("failure end-of-stream Exec Op opId={} reconfType={} queryExecutionPlanId={} "
                      "threadPool->getNumberOfThreads()={} qep {}",
                      sourceId,
                      magic_enum::enum_name(ReconfigurationType::FailEndOfStream),
                      executablePipeline->get()->getDecomposedQueryId(),
                      threadPool->getNumberOfThreads(),
                      executablePipeline->get()->getSharedQueryId());

        } else if (auto* sink = std::get_if<DataSinkPtr>(&successor)) {
            auto reconfMessageSink = ReconfigurationMessage(sink->get()->getSharedQueryId(),
                                                            sink->get()->getParentPlanId(),
                                                            sink->get()->getParentPlanVersion(),
                                                            ReconfigurationType::FailEndOfStream,
                                                            (*sink));
            addReconfigurationMessage(sink->get()->getSharedQueryId(),
                                      sink->get()->getParentPlanId(),
                                      sink->get()->getParentPlanVersion(),
                                      reconfMessageSink,
                                      false);
            NES_DEBUG("failure end-of-stream Sink opId={} reconfType={} queryExecutionPlanId={} "
                      "threadPool->getNumberOfThreads()={} qep {}",
                      sourceId,
                      magic_enum::enum_name(ReconfigurationType::FailEndOfStream),
                      sink->get()->getParentPlanId(),
                      threadPool->getNumberOfThreads(),
                      sink->get()->getSharedQueryId());
        }
    }
    return true;
}

bool AbstractQueryManager::addEndOfStream(DataSourcePtr source, Runtime::QueryTerminationType terminationType) {
    std::unique_lock queryLock(queryMutex);
    NES_WARNING("AbstractQueryManager: AbstractQueryManager::addEndOfStream for source operator {} terminationType={}",
                source->getOperatorId(),
                terminationType);
    NES_ASSERT2_FMT(threadPool->isRunning(), "thread pool no longer running");
    NES_ASSERT2_FMT(sourceToQEPMapping.contains(source->getOperatorId()), "invalid source " << source->getOperatorId());

    bool success = false;
    switch (terminationType) {
        case Runtime::QueryTerminationType::Graceful: {
            success = addSoftEndOfStream(source);
            break;
        }
        case Runtime::QueryTerminationType::HardStop: {
            success = addHardEndOfStream(source);
            break;
        }
        case Runtime::QueryTerminationType::Failure: {
            success = addFailureEndOfStream(source);
            break;
        }
        default: {
            NES_ASSERT2_FMT(false, "Invalid termination type");
        }
    }

    return success;
}

bool AbstractQueryManager::startNewExecutableQueryPlanAndPropagateMarker(
    const ReconfigurationMarkerPtr marker,
    DecomposedQueryIdWithVersion decomposedQueryIdWithVersion) {
    if (auto eventOptional = marker->getReconfigurationEvent(decomposedQueryIdWithVersion)) {
        auto event = eventOptional.value();
        auto metadata = event->reconfigurationMetadata;
        if (metadata->instanceOf<UpdateAndDrainQueryMetadata>()) {
            // if plan's id is decomposedQueryId and event is drain and update
            auto info = metadata->as<UpdateAndDrainQueryMetadata>();
            auto newPlan =
                getQueryExecutionPlan(DecomposedQueryIdWithVersion(info->decomposedQueryId, info->decomposedQueryPlanVersion));

            if (newPlan->getStatus() != Execution::ExecutableQueryPlanStatus::Finished) {
                // start new plan
                auto result = startExecutableQueryPlan(newPlan);

                if (result) {
                    // propagate marker to newly started plan
                    for (auto& newDecomposedQueryPlanSource : newPlan->getSources()) {
                        newDecomposedQueryPlanSource->handleReconfigurationMarker(marker);
                    }
                }

                return result;
            }
        }
    }

    return false;
}

bool AbstractQueryManager::propagateReconfigurationMarker(const ReconfigurationMarkerPtr& marker, DataSourcePtr source) {
    auto sourceId = source->getOperatorId();
    markSourceAsDrained(marker, sourceId);
    auto pipelineSuccessors = source->getExecutableSuccessors();

    if (auto netSource = std::dynamic_pointer_cast<Network::NetworkSource>(source); netSource != nullptr) {
        //add soft eaos for network source
        auto reconfMessage = ReconfigurationMessage(INVALID_SHARED_QUERY_ID,
                                                    INVALID_DECOMPOSED_QUERY_PLAN_ID,
                                                    INVALID_DECOMPOSED_QUERY_PLAN_VERSION,
                                                    ReconfigurationType::ReconfigurationMarker,
                                                    netSource,
                                                    marker);
        addReconfigurationMessage(INVALID_SHARED_QUERY_ID,
                                  INVALID_DECOMPOSED_QUERY_PLAN_ID,
                                  INVALID_DECOMPOSED_QUERY_PLAN_VERSION,
                                  reconfMessage,
                                  false);
    }

    for (auto successor : pipelineSuccessors) {
        // create reconfiguration message. If the successor is an executable pipeline we send the marker to the pipeline.
        if (auto* executablePipeline = std::get_if<Execution::ExecutablePipelinePtr>(&successor)) {
            auto reconfMessage = ReconfigurationMessage(executablePipeline->get()->getSharedQueryId(),
                                                        executablePipeline->get()->getDecomposedQueryId(),
                                                        executablePipeline->get()->getDecomposedQueryVersion(),
                                                        ReconfigurationType::ReconfigurationMarker,
                                                        (*executablePipeline),
                                                        marker);
            addReconfigurationMessage(executablePipeline->get()->getSharedQueryId(),
                                      executablePipeline->get()->getDecomposedQueryId(),
                                      executablePipeline->get()->getDecomposedQueryVersion(),
                                      reconfMessage,
                                      false);
            NES_DEBUG("Inserted reconfiguration to propagate reconfiguration marker to Pipeline opId={} reconfType={} "
                      "queryExecutionPlanId={} "
                      "threadPool->getNumberOfThreads()={} qep {}.{}",
                      sourceId,
                      magic_enum::enum_name(ReconfigurationType::ReconfigurationMarker),
                      executablePipeline->get()->getDecomposedQueryId(),
                      executablePipeline->get()->getDecomposedQueryVersion(),
                      threadPool->getNumberOfThreads(),
                      executablePipeline->get()->getSharedQueryId());
        } else if (auto* sink = std::get_if<DataSinkPtr>(&successor)) {
            // In case of a network sink we apply the reconfiguration to the sink.
            // Depending on the kind of reconfiguration event the sink will propagate the to the next worker if necessary.
            auto reconfMessageSink = ReconfigurationMessage(sink->get()->getSharedQueryId(),
                                                            sink->get()->getParentPlanId(),
                                                            sink->get()->getParentPlanVersion(),
                                                            ReconfigurationType::ReconfigurationMarker,
                                                            (*sink),
                                                            marker);
            addReconfigurationMessage(sink->get()->getSharedQueryId(),
                                      sink->get()->getParentPlanId(),
                                      sink->get()->getParentPlanVersion(),
                                      reconfMessageSink,
                                      false);
            NES_DEBUG("Inserted reconfiguration to propagate reconfiguration marker to Sink opId={} reconfType={} "
                      "queryExecutionPlanId={} threadPool->getNumberOfThreads()={} qep{}",
                      sourceId,
                      magic_enum::enum_name(ReconfigurationType::SoftEndOfStream),
                      sink->get()->getParentPlanId(),
                      threadPool->getNumberOfThreads(),
                      sink->get()->getSharedQueryId());
        }
    }
    return true;
}

void AbstractQueryManager::propagateReconfigurationMarkerToSuccessorList(
    const ReconfigurationMarkerPtr& marker,
    std::vector<Execution::SuccessorExecutablePipeline> pipelineSuccessors) {
    for (auto successor : pipelineSuccessors) {
        // create reconfiguration message. If the successor is an executable pipeline we send the marker to the pipeline.
        if (auto* executablePipeline = std::get_if<Execution::ExecutablePipelinePtr>(&successor)) {
            auto reconfMessage = ReconfigurationMessage(executablePipeline->get()->getSharedQueryId(),
                                                        executablePipeline->get()->getDecomposedQueryId(),
                                                        executablePipeline->get()->getDecomposedQueryVersion(),
                                                        ReconfigurationType::ReconfigurationMarker,
                                                        (*executablePipeline),
                                                        marker);
            addReconfigurationMessage(executablePipeline->get()->getSharedQueryId(),
                                      executablePipeline->get()->getDecomposedQueryId(),
                                      executablePipeline->get()->getDecomposedQueryVersion(),
                                      reconfMessage,
                                      false);
            NES_DEBUG(
                "Inserted reconfiguration to propagate reconfiguration marker to pipeline from list of successors reconfType={} "
                "queryExecutionPlanId={} "
                "threadPool->getNumberOfThreads()={} qep {}",
                magic_enum::enum_name(ReconfigurationType::ReconfigurationMarker),
                executablePipeline->get()->getDecomposedQueryId(),
                threadPool->getNumberOfThreads(),
                executablePipeline->get()->getSharedQueryId());
        } else if (auto* sink = std::get_if<DataSinkPtr>(&successor)) {
            // In case of a network sink we apply the reconfiguration to the sink.
            // Depending on the kind of reconfiguration event the sink will propagate the to the next worker if necessary.
            auto reconfMessageSink = ReconfigurationMessage(sink->get()->getSharedQueryId(),
                                                            sink->get()->getParentPlanId(),
                                                            sink->get()->getParentPlanVersion(),
                                                            ReconfigurationType::ReconfigurationMarker,
                                                            (*sink),
                                                            marker);
            addReconfigurationMessage(sink->get()->getSharedQueryId(),
                                      sink->get()->getParentPlanId(),
                                      sink->get()->getParentPlanVersion(),
                                      reconfMessageSink,
                                      false);
            NES_DEBUG(
                "Inserted reconfiguration to propagate reconfiguration marker to Sink from list of successors reconfType={} "
                "queryExecutionPlanId={} threadPool->getNumberOfThreads()={} qep{}",
                magic_enum::enum_name(ReconfigurationType::SoftEndOfStream),
                sink->get()->getParentPlanId(),
                threadPool->getNumberOfThreads(),
                sink->get()->getSharedQueryId());
        }
    }
}

void AbstractQueryManager::markSourceAsDrained(const ReconfigurationMarkerPtr& marker, OperatorId sourceId) {
    auto successorQEPs = sourceToQEPMapping[sourceId];
    for (auto qep : successorQEPs) {
        qep->increaseSourceDrainCounter(marker);
    }
}
}// namespace NES::Runtime
