/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <Catalogs/PhysicalStreamConfig.hpp>
#include <Network/NetworkSource.hpp>
#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LambdaSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Phases/ConvertLogicalToPhysicalSink.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <QueryCompiler/DefaultQueryCompiler.hpp>
#include <QueryCompiler/Phases/DefaultPhaseFactory.hpp>
#include <QueryCompiler/QueryCompilationRequest.hpp>
#include <QueryCompiler/QueryCompilationResult.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <Runtime/ErrorListener.hpp>
#include <Runtime/Execution/ExecutablePipeline.hpp>
#include <Runtime/Execution/ExecutableQueryPlan.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeStatsProvider.hpp>
#include <State/StateManager.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <string>
#include <utility>

namespace NES::Runtime {

extern void installGlobalErrorListener(std::shared_ptr<ErrorListener> const&);
extern void removeGlobalErrorListener(std::shared_ptr<ErrorListener> const&);

NodeEnginePtr create(const std::string& hostname, uint16_t port, PhysicalStreamConfigPtr config) {
    return NodeEngine::create(hostname, port, std::move(config), 1, 4096, 1024, 128, 12);
}

NodeStatsProviderPtr NodeEngine::getNodeStatsProvider() { return nodeStatsProvider; }

NodeEnginePtr NodeEngine::create(const std::string& hostname,
                                 uint16_t port,
                                 const PhysicalStreamConfigPtr& config,
                                 uint16_t numThreads,
                                 uint64_t bufferSize,
                                 uint64_t numberOfBuffersInGlobalBufferManager,
                                 uint64_t numberOfBuffersInSourceLocalBufferPool,
                                 uint64_t numberOfBuffersPerPipeline) {
    try {
        auto nodeEngineId = UtilityFunctions::getNextNodeEngineId();
        auto partitionManager = std::make_shared<Network::PartitionManager>();
        auto bufferManager = std::make_shared<BufferManager>(bufferSize, numberOfBuffersInGlobalBufferManager);
        auto queryManager = std::make_shared<QueryManager>(bufferManager, nodeEngineId, numThreads);
        auto stateManager = std::make_shared<StateManager>(nodeEngineId);
        if (!partitionManager) {
            NES_ERROR("Runtime: error while creating partition manager");
            throw Exception("Error while creating partition manager");
        }
        if (!bufferManager) {
            NES_ERROR("Runtime: error while creating buffer manager");
            throw Exception("Error while creating buffer manager");
        }
        if (!queryManager) {
            NES_ERROR("Runtime: error while creating queryManager");
            throw Exception("Error while creating queryManager");
        }
        if (!stateManager) {
            NES_ERROR("Runtime: error while creating stateManager");
            throw Exception("Error while creating stateManager");
        }
        auto compilerOptions = QueryCompilation::QueryCompilerOptions::createDefaultOptions();
        compilerOptions->setNumSourceLocalBuffers(numberOfBuffersInSourceLocalBufferPool);
        auto phaseFactory = QueryCompilation::Phases::DefaultPhaseFactory::create();
        auto compiler = QueryCompilation::DefaultQueryCompiler::create(compilerOptions, phaseFactory);
        if (!compiler) {
            NES_ERROR("Runtime: error while creating compiler");
            throw Exception("Error while creating compiler");
        }
        auto engine = std::make_shared<NodeEngine>(
            config,
            std::move(bufferManager),
            std::move(queryManager),
            [hostname, port, numThreads](const std::shared_ptr<NodeEngine>& engine) {
                return Network::NetworkManager::create(hostname,
                                                       port,
                                                       Network::ExchangeProtocol(engine->getPartitionManager(), engine),
                                                       engine->getBufferManager(),
                                                       numThreads);
            },
            std::move(partitionManager),
            std::move(compiler),
            std::move(stateManager),
            nodeEngineId,
            numberOfBuffersInGlobalBufferManager,
            numberOfBuffersInSourceLocalBufferPool,
            numberOfBuffersPerPipeline);
        installGlobalErrorListener(engine);
        return engine;
    } catch (std::exception& err) {
        NES_ERROR("Cannot start node engine " << err.what());
        NES_THROW_RUNTIME_ERROR("Cant start node engine");
    }
    return nullptr;
}

NodeEngine::NodeEngine(const PhysicalStreamConfigPtr& config,
                       BufferManagerPtr&& bufferManager,
                       QueryManagerPtr&& queryManager,
                       std::function<Network::NetworkManagerPtr(std::shared_ptr<NodeEngine>)>&& networkManagerCreator,
                       Network::PartitionManagerPtr&& partitionManager,
                       QueryCompilation::QueryCompilerPtr&& queryCompiler,
                       StateManagerPtr&& stateManager,
                       uint64_t nodeEngineId,
                       uint64_t numberOfBuffersInGlobalBufferManager,
                       uint64_t numberOfBuffersInSourceLocalBufferPool,
                       uint64_t numberOfBuffersPerPipeline)
    : nodeEngineId(nodeEngineId), numberOfBuffersInGlobalBufferManager(numberOfBuffersInGlobalBufferManager),
      numberOfBuffersInSourceLocalBufferPool(numberOfBuffersInSourceLocalBufferPool),
      numberOfBuffersPerPipeline(numberOfBuffersPerPipeline) {

    configs.push_back(config);
    NES_TRACE("Runtime() id=" << nodeEngineId);
    nodeStatsProvider = std::make_shared<NodeStatsProvider>();
    this->queryCompiler = std::move(queryCompiler);
    this->queryManager = std::move(queryManager);
    this->bufferManager = std::move(bufferManager);
    this->partitionManager = std::move(partitionManager);
    this->stateManager = std::move(stateManager);
    // here shared_from_this() does not work because of the machinery behind make_shared
    // as a result, we need to use a trick, i.e., a shared ptr that does not deallocate the node engine
    // plz make sure that ExchangeProtocol never leaks the impl pointer
    networkManager = networkManagerCreator(std::shared_ptr<NodeEngine>(this, [](NodeEngine*) {
        // nop
    }));
    if (!this->queryManager->startThreadPool()) {
        NES_ERROR("Runtime: error while start thread pool");
        throw Exception("Error while start thread pool");
    }
    NES_DEBUG("NodeEngine(): thread pool successfully started");

    isRunning.store(true);
}

NodeEngine::~NodeEngine() {
    NES_DEBUG("Destroying Runtime()");
    stop();
}

bool NodeEngine::deployQueryInNodeEngine(const Execution::ExecutableQueryPlanPtr& queryExecutionPlan) {
    std::unique_lock lock(engineMutex);
    NES_DEBUG("Runtime: deployQueryInNodeEngine query using qep " << queryExecutionPlan);
    bool successRegister = registerQueryInNodeEngine(queryExecutionPlan);
    if (!successRegister) {
        NES_ERROR("Runtime::deployQueryInNodeEngine: failed to register query");
        return false;
    }
    NES_DEBUG("Runtime::deployQueryInNodeEngine: successfully register query");

    bool successStart = startQuery(queryExecutionPlan->getQueryId());
    if (!successStart) {
        NES_ERROR("Runtime::deployQueryInNodeEngine: failed to start query");
        return false;
    }
    NES_DEBUG("Runtime::deployQueryInNodeEngine: successfully start query");

    return true;
}

std::optional<Execution::ExecutableQueryPlanPtr> NodeEngine::compileQuery(const QueryPlanPtr& queryPlan) {
    QueryId queryId = queryPlan->getQueryId();
    QueryId querySubPlanId = queryPlan->getQuerySubPlanId();
    NES_INFO("Creating ExecutableQueryPlan for " << queryId << " " << querySubPlanId);
    auto request = QueryCompilation::QueryCompilationRequest::create(queryPlan, inherited1::shared_from_this());
    request->enableDump();
    auto result = queryCompiler->compileQuery(request);
    try {
        auto executablePlan = result->getExecutableQueryPlan();
        return executablePlan;
    } catch (std::exception const& error) {
        NES_ERROR("Error while building query execution plan: " << error.what());
        NES_ASSERT(false, "Error while building query execution plan: " << error.what());
        return std::nullopt;
    }
}

bool NodeEngine::registerQueryInNodeEngine(const QueryPlanPtr& queryPlan) {
    std::unique_lock lock(engineMutex);
    auto maybeQep = compileQuery(queryPlan);
    if (!maybeQep) {
        return false;
    }
    return registerQueryInNodeEngine(*maybeQep);
}

bool NodeEngine::registerQueryInNodeEngine(const Execution::ExecutableQueryPlanPtr& queryExecutionPlan,
                                           bool lazyQueryManagerRegistration) {
    std::unique_lock lock(engineMutex);
    QueryId queryId = queryExecutionPlan->getQueryId();
    QuerySubPlanId querySubPlanId = queryExecutionPlan->getQuerySubPlanId();
    NES_DEBUG("Runtime: registerQueryInNodeEngine query " << queryExecutionPlan << " queryId=" << queryId
                                                          << " querySubPlanId =" << querySubPlanId);
    NES_ASSERT(queryManager->isThreadPoolRunning(), "Registering query but thread pool not running");
    if (deployedQEPs.find(querySubPlanId) != deployedQEPs.end()) {
        NES_DEBUG("NodeEngine: qep already exists. register failed" << querySubPlanId);
        return false;
    }
    auto found = queryIdToQuerySubPlanIds.find(queryId);
    if (found == queryIdToQuerySubPlanIds.end()) {
        queryIdToQuerySubPlanIds[queryId] = {querySubPlanId};
        NES_DEBUG("Runtime: register of QEP " << querySubPlanId << " as a singleton");
    } else {
        (*found).second.push_back(querySubPlanId);
        NES_DEBUG("Runtime: register of QEP " << querySubPlanId << " added");
    }
    if (!lazyQueryManagerRegistration) {
        return registerQueryExecutionPlanInQueryManagerForSources(queryExecutionPlan, queryExecutionPlan->getSources());
    }
    return true;
}

bool NodeEngine::registerQueryExecutionPlanInQueryManagerForSources(const Execution::ExecutableQueryPlanPtr& queryExecutionPlan,
                                                                    const std::vector<DataSourcePtr>& sources) {
    QuerySubPlanId querySubPlanId = queryExecutionPlan->getQuerySubPlanId();
    if (queryManager->registerQuery(queryExecutionPlan, sources)) {
        deployedQEPs[querySubPlanId] = queryExecutionPlan;
        NES_DEBUG("NodeEngine: register of subqep " << querySubPlanId << " succeeded");
        return true;
    } else {
        NES_DEBUG("NodeEngine: register of subqep " << querySubPlanId << " failed");
        return false;
    }
}

bool NodeEngine::registerQueryForReconfigurationInNodeEngine(const QueryPlanPtr& queryPlan) {
    std::unique_lock lock(engineMutex);
    NES_INFO("NodeEngine::registerQueryForReconfigurationInNodeEngine: Received request for registering qep with querySubPlanId: "
             << queryPlan->getQuerySubPlanId());
    if (reconfigurationQEPs.find(queryPlan->getQuerySubPlanId()) != reconfigurationQEPs.end()) {
        NES_DEBUG("NodeEngine::registerQueryForReconfigurationInNodeEngine: query with subPlanId"
                  << queryPlan->getQuerySubPlanId() << " already registered for reconfiguration.");
        return false;
    }
    auto maybeQep = compileQuery(queryPlan);
    if (!maybeQep) {
        return false;
    }
    auto qep = *maybeQep;
    NES_DEBUG("NodeEngine::registerQueryForReconfigurationInNodeEngine: adding qep with querySubPlanId: "
              << qep->getQuerySubPlanId() << " into QEP pending reconfiguration.");
    reconfigurationQEPs[qep->getQuerySubPlanId()] = qep;
    return true;
}

bool NodeEngine::startQueryReconfiguration(const QueryReconfigurationPlanPtr& queryReconfigurationPlan) {
    std::unique_lock lock(engineMutex);
    QueryId queryId = queryReconfigurationPlan->getQueryId();
    NES_DEBUG("NodeEngine::startQueryReconfiguration: Received Reconfiguration Plan: " << queryReconfigurationPlan->getId()
                                                                                       << " to apply on QueryId: " << queryId);
    if (queryIdToQuerySubPlanIds.find(queryId) == queryIdToQuerySubPlanIds.end()) {
        NES_ERROR("NodeEngine::startQueryReconfiguration: Query ID does not exists. Reconfiguration failed" << queryId);
        return false;
    }
    std::vector<QuerySubPlanId>& querySubPlanIds = queryIdToQuerySubPlanIds[queryId];
    if (querySubPlanIds.empty()) {
        NES_ERROR("NodeEngine::startQueryReconfiguration: Unable to find SubQueryPlans for the query "
                  << queryId << ". Reconfiguration failed.");
        queryIdToQuerySubPlanIds.erase(queryId);
        return false;
    }
    std::set<QuerySubPlanId> reconfiguredSubPlans;
    // At source only replace QEP for stopping at source, use stop query endpoint

    if (deployedQEPs.find(queryReconfigurationPlan->getOldQuerySubPlanId()) == deployedQEPs.end()) {
        NES_ERROR("NodeEngine::startQueryReconfiguration: Reconfiguration failed. SubQuery ID does not exists in deployedQEPs:  "
                  << queryReconfigurationPlan->getOldQuerySubPlanId());
        return false;
    }
    if (reconfigurationQEPs.find(queryReconfigurationPlan->getNewQuerySubPlanId()) == reconfigurationQEPs.end()) {
        NES_ERROR("NodeEngine::startQueryReconfiguration: Reconfiguration failed. SubQuery ID does not exists in "
                  "reconfigurationQEPs:  "
                  << queryReconfigurationPlan->getNewQuerySubPlanId());
        return false;
    }
    auto oldQep = deployedQEPs[queryReconfigurationPlan->getOldQuerySubPlanId()];
    auto newQep = reconfigurationQEPs[queryReconfigurationPlan->getNewQuerySubPlanId()];

    if (queryReconfigurationPlan->getReconfigurationType() == DATA_SOURCE) {
        return reconfigureDataSource(oldQep, newQep);
    }
    if (queryReconfigurationPlan->getReconfigurationType() == DATA_SINK) {
        return reconfigureDataSink(oldQep, newQep);
    }

    NES_ERROR("NodeEngine::startQueryReconfiguration: Reconfiguration failed. Unknown reconfiguration type: "
              << queryReconfigurationPlan->getReconfigurationType());
    return false;
}

bool NodeEngine::reconfigureDataSink(const std::shared_ptr<Execution::ExecutableQueryPlan>& oldQep,
                                     std::shared_ptr<Execution::ExecutableQueryPlan>& newQep) {
    NES_DEBUG("NodeEngine::reconfigureDataSink: Triggering data sink reconfiguration with oldQep: "
              << oldQep->getQuerySubPlanId() << " and newQep: " << newQep->getQuerySubPlanId());
    std::unordered_map<OperatorId, DataSinkPtr> oldSinkLogicalMapping;
    std::unordered_map<OperatorId, DataSinkPtr> newSinkLogicalMapping;

    for (const auto& sink : oldQep->getSinks()) {
        oldSinkLogicalMapping[sink->getLogicalOperatorId()] = sink;
    }

    for (const auto& sink : newQep->getSinks()) {
        newSinkLogicalMapping[sink->getLogicalOperatorId()] = sink;
    }

    std::vector<DataSinkPtr> sinksToDelete;
    std::vector<DataSinkPtr> sinksToAdd;

    for (auto& newSink : newSinkLogicalMapping) {
        if (!oldSinkLogicalMapping.contains(newSink.first)) {
            sinksToAdd.emplace_back(newSink.second);
        }
    }

    for (auto& oldSink : oldSinkLogicalMapping) {
        if (!newSinkLogicalMapping.contains(oldSink.first)) {
            sinksToDelete.emplace_back(oldSink.second);
        }
    }

    // increment producer counts for old sinks
    for (const auto& sink : newQep->getSinks()) {
        if (oldSinkLogicalMapping.contains(sink->getLogicalOperatorId())) {
            sink->addNewProducer();
        }
    }

    for (const auto& sinkToDelete : sinksToDelete) {
        sinkOperatorCache.erase(sinkToDelete->getLogicalOperatorId());
    }

    registerQueryInNodeEngine(newQep, false);
    queryManager->startQuery(newQep, stateManager, newQep->getSources(), sinksToAdd);

    return true;
}

bool NodeEngine::reconfigureDataSource(const std::shared_ptr<Execution::ExecutableQueryPlan>& oldQep,
                                       std::shared_ptr<Execution::ExecutableQueryPlan>& newQep) {
    // If qep has data sinks, sink reconfiguration, else if data source source reconfiguration
    // Ensure new data sources are not added
    std::unordered_map<OperatorId, DataSourcePtr> oldQepDataSourceMapping;
    std::unordered_map<OperatorId, DataSourcePtr> newQepDataSourceMapping;
    for (const auto& source : oldQep->getSources()) {
        if (std::dynamic_pointer_cast<Network::NetworkSource>(source)) {
            continue;
        }
        oldQepDataSourceMapping[source->getLogicalSourceOperatorId()] = source;
    }
    for (const auto& source : newQep->getSources()) {
        if (!std::dynamic_pointer_cast<Network::NetworkSource>(source)) {
            newQepDataSourceMapping[source->getLogicalSourceOperatorId()] = source;
        }
    }
    NES_ASSERT2_FMT(oldQepDataSourceMapping.size() == newQepDataSourceMapping.size(),
                    "NodeEngine::reconfigureDataSource: Failed to replace qep: " << oldQep->getQuerySubPlanId()
                                                                                 << " with qep:" << newQep->getQuerySubPlanId()
                                                                                 << " as number of sources are different");
    // check if no new data sources are added
    for (const auto& newQepDatasource : newQepDataSourceMapping) {
        if (oldQepDataSourceMapping.find(newQepDatasource.first) == oldQepDataSourceMapping.end()) {
            NES_ERROR("NodeEngine::reconfigureDataSource: Reconfiguration failed. New QEP can't have new source: "
                      << newQepDatasource.first);
            return false;
        }
    }

    std::vector<DataSourcePtr> oldSources = oldQep->getSources();
    std::unordered_map<OperatorId, DataSourcePtr> logicalOperatorIdSourceMapping;

    std::vector<ReconfigurationMessage> replaceDataEmitterMessages;
    std::vector<std::vector<Execution::SuccessorExecutablePipeline>> oldPipelineSuccessors;

    for (const auto& newSource : newQep->getSources()) {
        OperatorId logicalSourceOperatorId = newSource->getLogicalSourceOperatorId();
        if (oldQepDataSourceMapping.find(logicalSourceOperatorId) == oldQepDataSourceMapping.end()) {
            NES_ERROR("NodeEngine::reconfigureDataSource: Source with logicalOperatorId: "
                      << logicalSourceOperatorId << " not found in oldQep: " << oldQep->getQuerySubPlanId());
        }
        DataSourcePtr oldSource = oldQepDataSourceMapping[logicalSourceOperatorId];
        auto updatedSuccessors = newSource->getExecutableSuccessors();
        auto updatedSuccessorPipeline = std::make_any<std::vector<Execution::SuccessorExecutablePipeline>>(updatedSuccessors);
        replaceDataEmitterMessages.emplace_back(
            ReconfigurationMessage(newQep->getQueryId(), ReplaceDataEmitter, oldSource, std::move(updatedSuccessorPipeline)));
        oldPipelineSuccessors.emplace_back(oldSource->getExecutableSuccessors());
    }

    // Replace sources in new QEP with sources in old QEP
    newQep->setSources(oldQep->getSources());
    // Register source mapping in Query Manager
    registerQueryInNodeEngine(newQep, false);

    // No need to start the sources but we need to start new QEP and setup sinks
    queryManager->startQuery(newQep, stateManager, std::vector<DataSourcePtr>{}, newQep->getSinks());

    for (const auto& replaceDataEmitterMessage : replaceDataEmitterMessages) {
        queryManager->addReconfigurationMessage(newQep->getQueryId(), replaceDataEmitterMessage, false);
    }

    for (auto oldPipelineSuccessor : oldPipelineSuccessors) {
        queryManager->propagateViaSuccessorPipelines(
            SoftEndOfStream,
            [](const Execution::ExecutableQueryPlanPtr& executableQueryPlan) {
                return std::make_any<std::weak_ptr<Execution::ExecutableQueryPlan>>(executableQueryPlan);
            },
            oldQep,
            oldPipelineSuccessor);
    }

    reconfigurationQEPs.erase(newQep->getQuerySubPlanId());
    deployedQEPs.erase(oldQep->getQuerySubPlanId());

    std::vector<QuerySubPlanId>& subPlanIds = queryIdToQuerySubPlanIds[oldQep->getQueryId()];
    subPlanIds.erase(std::remove_if(subPlanIds.begin(),
                                    subPlanIds.end(),
                                    [oldQep](QuerySubPlanId x) {
                                        return x == oldQep->getQuerySubPlanId();
                                    }),
                     subPlanIds.end());
    return true;
}

std::map<OperatorId, DataSinkPtr>& NodeEngine::getSinkOperatorCache() { return sinkOperatorCache; }

bool NodeEngine::startQuery(QueryId queryId) {
    std::unique_lock lock(engineMutex);
    NES_DEBUG("Runtime: startQuery=" << queryId);
    if (queryIdToQuerySubPlanIds.find(queryId) != queryIdToQuerySubPlanIds.end()) {

        std::vector<QuerySubPlanId> querySubPlanIds = queryIdToQuerySubPlanIds[queryId];
        if (querySubPlanIds.empty()) {
            NES_ERROR("Runtime: Unable to find qep ids for the query " << queryId << ". Start failed.");
            return false;
        }

        for (auto querySubPlanId : querySubPlanIds) {
            Execution::ExecutableQueryPlanStatus status = deployedQEPs[querySubPlanId]->getStatus();
            if (status == Execution::ExecutableQueryPlanStatus::Created) {
                if (queryManager->startQuery(deployedQEPs[querySubPlanId], stateManager)) {
                    NES_DEBUG("Runtime: start of QEP " << querySubPlanId << " succeeded");
                } else {
                    NES_DEBUG("Runtime: start of QEP " << querySubPlanId << " failed");
                    return false;
                }
            }
        }
        return true;
    }
    NES_ERROR("Runtime: qep does not exists. start failed" << queryId);
    return false;
}

bool NodeEngine::undeployQuery(QueryId queryId) {
    std::unique_lock lock(engineMutex);
    NES_DEBUG("Runtime: undeployQuery query=" << queryId);
    bool successStop = stopQuery(queryId);
    if (!successStop) {
        NES_ERROR("Runtime::undeployQuery: failed to stop query");
        return false;
    }
    NES_DEBUG("Runtime::undeployQuery: successfully stop query");

    bool successUnregister = unregisterQuery(queryId);
    if (!successUnregister) {
        NES_ERROR("Runtime::undeployQuery: failed to unregister query");
        return false;
    }
    NES_DEBUG("Runtime::undeployQuery: successfully unregister query");
    return true;
}

bool NodeEngine::unregisterQuery(QueryId queryId) {
    std::unique_lock lock(engineMutex);
    NES_DEBUG("Runtime: unregisterQuery query=" << queryId);
    if (queryIdToQuerySubPlanIds.find(queryId) != queryIdToQuerySubPlanIds.end()) {

        std::vector<QuerySubPlanId> querySubPlanIds = queryIdToQuerySubPlanIds[queryId];
        if (querySubPlanIds.empty()) {
            NES_ERROR("Runtime: Unable to find qep ids for the query " << queryId << ". Start failed.");
            return false;
        }

        for (auto querySubPlanId : querySubPlanIds) {
            if (queryManager->deregisterQuery(deployedQEPs[querySubPlanId])) {
                deployedQEPs.erase(querySubPlanId);
                NES_DEBUG("Runtime: unregister of query " << querySubPlanId << " succeeded");
            } else {
                NES_ERROR("Runtime: unregister of QEP " << querySubPlanId << " failed");
                return false;
            }
        }
        queryIdToQuerySubPlanIds.erase(queryId);
        return true;
    }

    NES_ERROR("Runtime: qep does not exists. unregister failed" << queryId);
    return false;
}

bool NodeEngine::stopQuery(QueryId queryId, bool graceful) {
    std::unique_lock lock(engineMutex);
    NES_DEBUG("Runtime::stopQuery for Query: " << queryId);
    auto it = queryIdToQuerySubPlanIds.find(queryId);
    if (it != queryIdToQuerySubPlanIds.end()) {
        std::vector<QuerySubPlanId> querySubPlanIds = it->second;
        if (querySubPlanIds.empty()) {
            NES_ERROR("Runtime: Unable to find qep ids for the query " << queryId << ". Start failed.");
            return false;
        }

        for (auto querySubPlanId : querySubPlanIds) {
            std::shared_ptr<Execution::ExecutableQueryPlan>& qep = deployedQEPs[querySubPlanId];
            for (auto sink : qep->getSinks()) {
                sinkOperatorCache.erase(sink->getLogicalOperatorId());
            }
            if (queryManager->stopQuery(qep, graceful)) {
                NES_DEBUG("Runtime: stop of QEP " << querySubPlanId << " succeeded");
            } else {
                NES_ERROR("Runtime: stop of QEP " << querySubPlanId << " failed");
                return false;
            }
        }
        return true;
    }

    NES_ERROR("Runtime: qep does not exists. stop failed " << queryId);
    return false;
}

QueryManagerPtr NodeEngine::getQueryManager() { return queryManager; }

bool NodeEngine::stop(bool markQueriesAsFailed) {
    //TODO: add check if still queries are running
    //TODO @Steffen: does it make sense to have force stop still?
    //TODO @all: imho, when this method terminates, nothing must be running still and all resources must be returned to the engine
    //TODO @all: error handling, e.g., is it an error if the query is stopped but not undeployed? @Steffen?

    bool expected = true;
    if (!isRunning.compare_exchange_strong(expected, false)) {
        NES_WARNING("Runtime::stop: engine already stopped");
        return true;
    }
    NES_DEBUG("Runtime::stop: going to stop the node engine");
    std::unique_lock lock(engineMutex);
    bool withError = false;

    // release all deployed queries
    for (auto it = deployedQEPs.begin(); it != deployedQEPs.end();) {
        auto& [querySubPlanId, queryExecutionPlan] = *it;
        try {
            if (markQueriesAsFailed) {
                if (queryManager->failQuery(queryExecutionPlan)) {
                    NES_DEBUG("Runtime: fail of QEP " << querySubPlanId << " succeeded");
                } else {
                    NES_ERROR("Runtime: fail of QEP " << querySubPlanId << " failed");
                    withError = true;
                }
            } else {
                if (queryManager->stopQuery(queryExecutionPlan)) {
                    NES_DEBUG("Runtime: stop of QEP " << querySubPlanId << " succeeded");
                } else {
                    NES_ERROR("Runtime: stop of QEP " << querySubPlanId << " failed");
                    withError = true;
                }
            }
        } catch (std::exception const& err) {
            NES_ERROR("Runtime: stop of QEP " << querySubPlanId << " failed: " << err.what());
            withError = true;
        }
        try {
            if (queryManager->deregisterQuery(queryExecutionPlan)) {
                NES_DEBUG("Runtime: deregisterQuery of QEP " << querySubPlanId << " succeeded");
                it = deployedQEPs.erase(it);
            } else {
                NES_ERROR("Runtime: deregisterQuery of QEP " << querySubPlanId << " failed");
                withError = true;
                ++it;
            }
        } catch (std::exception const& err) {
            NES_ERROR("Runtime: deregisterQuery of QEP " << querySubPlanId << " failed: " << err.what());
            withError = true;
            ++it;
        }
    }
    // release components
    // TODO do not touch the sequence here as it will lead to errors in the shutdown sequence
    deployedQEPs.clear();
    sinkOperatorCache.clear();
    queryIdToQuerySubPlanIds.clear();
    queryManager->destroy();
    networkManager->destroy();
    bufferManager->destroy();
    stateManager->destroy();
    return !withError;
}

BufferManagerPtr NodeEngine::getBufferManager() { return bufferManager; }

StateManagerPtr NodeEngine::getStateManager() { return stateManager; }

uint64_t NodeEngine::getNodeEngineId() { return nodeEngineId; }

Network::NetworkManagerPtr NodeEngine::getNetworkManager() { return networkManager; }

QueryCompilation::QueryCompilerPtr NodeEngine::getCompiler() { return queryCompiler; }

Execution::ExecutableQueryPlanStatus NodeEngine::getQueryStatus(QueryId queryId) {
    std::unique_lock lock(engineMutex);
    if (queryIdToQuerySubPlanIds.find(queryId) != queryIdToQuerySubPlanIds.end()) {
        std::vector<QuerySubPlanId> querySubPlanIds = queryIdToQuerySubPlanIds[queryId];
        if (querySubPlanIds.empty()) {
            NES_ERROR("Runtime: Unable to find qep ids for the query " << queryId << ". Start failed.");
            return Execution::ExecutableQueryPlanStatus::Invalid;
        }

        for (auto querySubPlanId : querySubPlanIds) {
            //FIXME: handle vector of statistics properly in #977
            return deployedQEPs[querySubPlanId]->getStatus();
        }
    }
    return Execution::ExecutableQueryPlanStatus::Invalid;
}

void NodeEngine::onDataBuffer(Network::NesPartition, TupleBuffer&) {
    // nop :: kept as legacy
}

void NodeEngine::onEndOfStream(Network::Messages::EndOfStreamMessage msg) {
    // propagate EOS to the locally running QEPs that use the network source
    NES_DEBUG("Going to inject eos for " << msg.getChannelId().getNesPartition());
    queryManager->addEndOfStream(msg.getChannelId().getNesPartition().getOperatorId(), msg.isGraceful());
}

void NodeEngine::onServerError(Network::Messages::ErrorMessage err) {
    if (err.getErrorType() == Network::Messages::ErrorType::kPartitionNotRegisteredError) {
        NES_WARNING("Runtime: Unable to find the NES Partition " << err.getChannelId());
        return;
    }
    NES_THROW_RUNTIME_ERROR(err.getErrorTypeAsString());
}

void NodeEngine::onChannelError(Network::Messages::ErrorMessage err) {
    if (err.getErrorType() == Network::Messages::ErrorType::kPartitionNotRegisteredError) {
        NES_WARNING("Runtime: Unable to find the NES Partition " << err.getChannelId() << ": please, retry later or abort.");
        return;
    }
    NES_THROW_RUNTIME_ERROR(err.getErrorTypeAsString());
}

std::vector<QueryStatisticsPtr> NodeEngine::getQueryStatistics(QueryId queryId) {
    NES_INFO("QueryManager: Get query statistics for query " << queryId);
    std::unique_lock lock(engineMutex);
    std::vector<QueryStatisticsPtr> queryStatistics;

    NES_DEBUG("QueryManager: Check if query is registered");
    auto foundQuerySubPlanIds = queryIdToQuerySubPlanIds.find(queryId);
    NES_DEBUG("Founded members = " << foundQuerySubPlanIds->second.size());
    if (foundQuerySubPlanIds == queryIdToQuerySubPlanIds.end()) {
        NES_ERROR("QueryManager::getQueryStatistics: query does not exists " << queryId);
        return queryStatistics;
    }

    NES_DEBUG("QueryManager: Extracting query execution ids for the input query " << queryId);
    std::vector<QuerySubPlanId> querySubPlanIds = (*foundQuerySubPlanIds).second;
    for (auto querySubPlanId : querySubPlanIds) {
        NES_DEBUG("querySubPlanId=" << querySubPlanId << " stat="
                                    << queryManager->getQueryStatistics(querySubPlanId)->getQueryStatisticsAsString());
        queryStatistics.emplace_back(queryManager->getQueryStatistics(querySubPlanId));
    }
    return queryStatistics;
}

Network::PartitionManagerPtr NodeEngine::getPartitionManager() { return partitionManager; }

SourceDescriptorPtr NodeEngine::createLogicalSourceDescriptor(const SourceDescriptorPtr& sourceDescriptor) {
    NES_INFO("NodeEngine: Updating the default Logical Source Descriptor to the Logical Source Descriptor supported by the node");

    //we have to decide where many cases
    // 1.) if we specify a build-in source like default source, then we have only one config but call this for each source
    // 2.) if we have really two sources, then we would have two real configurations here
    if (configs.size() > 1) {
        NES_ASSERT(!configs.empty(), "no config for Lambda source");
        auto conf = configs.back();
        configs.pop_back();
        return conf->build(sourceDescriptor->getSchema());
    }
    NES_ASSERT(configs[0], "physical source config is not specified");
    return configs[0]->build(sourceDescriptor->getSchema());
}

void NodeEngine::setConfig(const AbstractPhysicalStreamConfigPtr& config) {
    NES_ASSERT(config, "physical source config is not specified");
    this->configs.emplace_back(config);
}

void NodeEngine::onFatalError(int signalNumber, std::string callstack) {
    NES_ERROR("onFatalError: signal [" << signalNumber << "] error [" << strerror(errno) << "] callstack " << callstack);
    std::cerr << "Runtime failed fatally" << std::endl;// it's necessary for testing and it wont harm us to write to stderr
    std::cerr << "Error: " << strerror(errno) << std::endl;
    std::cerr << "Signal: " << std::to_string(signalNumber) << std::endl;
    std::cerr << "Callstack:\n " << callstack << std::endl;
}

void NodeEngine::onFatalException(const std::shared_ptr<std::exception> exception, std::string callstack) {
    NES_ERROR("onFatalException: exception=[" << exception->what() << "] callstack=\n" << callstack);
    std::cerr << "Runtime failed fatally" << std::endl;
    std::cerr << "Error: " << strerror(errno) << std::endl;
    std::cerr << "Exception: " << exception->what() << std::endl;
    std::cerr << "Callstack:\n " << callstack << std::endl;
}

}// namespace NES::Runtime
