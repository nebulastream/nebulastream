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
#include <NodeEngine/ErrorListener.hpp>
#include <NodeEngine/Execution/ExecutableQueryPlan.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <NodeEngine/NodeStatsProvider.hpp>
#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LambdaSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Phases/ConvertLogicalToPhysicalSink.hpp>
#include <Phases/ConvertLogicalToPhysicalSource.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <QueryCompiler/GeneratableOperators/TranslateToGeneratableOperatorPhase.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <string>

namespace NES::NodeEngine {

extern void installGlobalErrorListener(std::shared_ptr<ErrorListener>);
extern void removeGlobalErrorListener(std::shared_ptr<ErrorListener>);

NodeEnginePtr create(const std::string& hostname, uint16_t port, PhysicalStreamConfigPtr config) {
    return NodeEngine::create(hostname, port, config, 1, 4096, 1024, 128, 12);
}

NodeStatsProviderPtr NodeEngine::getNodeStatsProvider() { return nodeStatsProvider; }

NodeEnginePtr NodeEngine::create(const std::string& hostname, uint16_t port, PhysicalStreamConfigPtr config, uint16_t numThreads,
                                 uint64_t bufferSize, uint64_t numberOfBuffersInGlobalBufferManager,
                                 uint64_t numberOfBuffersInSourceLocalBufferPool, uint64_t numberOfBuffersPerPipeline) {
    try {
        auto nodeEngineId = UtilityFunctions::getNextNodeEngineId();
        auto partitionManager = std::make_shared<Network::PartitionManager>();
        auto bufferManager = std::make_shared<BufferManager>(bufferSize, numberOfBuffersInGlobalBufferManager);
        auto queryManager = std::make_shared<QueryManager>(bufferManager, nodeEngineId, numThreads);
        if (!partitionManager) {
            NES_ERROR("NodeEngine: error while creating partition manager");
            throw Exception("Error while creating partition manager");
        }
        if (!bufferManager) {
            NES_ERROR("NodeEngine: error while creating buffer manager");
            throw Exception("Error while creating buffer manager");
        }
        if (!queryManager) {
            NES_ERROR("NodeEngine: error while creating queryManager");
            throw Exception("Error while creating queryManager");
        }
        auto compiler = createDefaultQueryCompiler(numberOfBuffersPerPipeline);
        if (!compiler) {
            NES_ERROR("NodeEngine: error while creating compiler");
            throw Exception("Error while creating compiler");
        }
        auto engine = std::make_shared<NodeEngine>(
            config, std::move(bufferManager), std::move(queryManager),
            [hostname, port, numThreads](std::shared_ptr<NodeEngine> engine) {
                return Network::NetworkManager::create(hostname, port,
                                                       Network::ExchangeProtocol(engine->getPartitionManager(), engine),
                                                       engine->getBufferManager(), numThreads);
            },
            std::move(partitionManager), std::move(compiler), nodeEngineId, numberOfBuffersInGlobalBufferManager,
            numberOfBuffersInSourceLocalBufferPool, numberOfBuffersPerPipeline);
        installGlobalErrorListener(engine);
        return engine;
    } catch (std::exception& err) {
        NES_ERROR("Cannot start node engine " << err.what());
        NES_THROW_RUNTIME_ERROR("Cant start node engine");
    }
    return nullptr;
}

NodeEngine::NodeEngine(PhysicalStreamConfigPtr config, BufferManagerPtr&& bufferManager, QueryManagerPtr&& queryManager,
                       std::function<Network::NetworkManagerPtr(std::shared_ptr<NodeEngine>)>&& networkManagerCreator,
                       Network::PartitionManagerPtr&& partitionManager, QueryCompilerPtr&& queryCompiler, uint64_t nodeEngineId,
                       uint64_t numberOfBuffersInGlobalBufferManager, uint64_t numberOfBuffersInSourceLocalBufferPool,
                       uint64_t numberOfBuffersPerPipeline)
    : inherited0(), inherited1(), inherited2(), nodeEngineId(nodeEngineId),
      numberOfBuffersInGlobalBufferManager(numberOfBuffersInGlobalBufferManager),
      numberOfBuffersInSourceLocalBufferPool(numberOfBuffersInSourceLocalBufferPool),
      numberOfBuffersPerPipeline(numberOfBuffersPerPipeline) {

    configs.push_back(config);
    NES_TRACE("NodeEngine() id=" << nodeEngineId);
    nodeStatsProvider = std::make_shared<NodeStatsProvider>();
    this->queryCompiler = std::move(queryCompiler);
    this->queryManager = std::move(queryManager);
    this->bufferManager = std::move(bufferManager);
    this->partitionManager = std::move(partitionManager);
    // here shared_from_this() does not work because of the machinery behind make_shared
    // as a result, we need to use a trick, i.e., a shared ptr that does not deallocate the node engine
    // plz make sure that ExchangeProtocol never leaks the impl pointer
    networkManager = networkManagerCreator(std::shared_ptr<NodeEngine>(this, [](NodeEngine*) {
        // nop
    }));
    if (!this->queryManager->startThreadPool()) {
        NES_ERROR("NodeEngine: error while start thread pool");
        throw Exception("Error while start thread pool");
    } else {
        NES_DEBUG("NodeEngine(): thread pool successfully started");
    }
    isRunning.store(true);
}

NodeEngine::~NodeEngine() {
    NES_DEBUG("Destroying NodeEngine()");
    stop();
}

bool NodeEngine::deployQueryInNodeEngine(Execution::ExecutableQueryPlanPtr queryExecutionPlan) {
    std::unique_lock lock(engineMutex);
    NES_DEBUG("NodeEngine: deployQueryInNodeEngine query using qep " << queryExecutionPlan);
    bool successRegister = registerQueryInNodeEngine(queryExecutionPlan);
    if (!successRegister) {
        NES_ERROR("NodeEngine::deployQueryInNodeEngine: failed to register query");
        return false;
    } else {
        NES_DEBUG("NodeEngine::deployQueryInNodeEngine: successfully register query");
    }
    bool successStart = startQuery(queryExecutionPlan->getQueryId());
    if (!successStart) {
        NES_ERROR("NodeEngine::deployQueryInNodeEngine: failed to start query");
        return false;
    } else {
        NES_DEBUG("NodeEngine::deployQueryInNodeEngine: successfully start query");
    }
    return true;
}

bool NodeEngine::registerQueryInNodeEngine(QueryPlanPtr queryPlan) {
    std::unique_lock lock(engineMutex);
    QueryId queryId = queryPlan->getQueryId();
    QueryId querySubPlanId = queryPlan->getQuerySubPlanId();
    NES_INFO("Creating ExecutableQueryPlan for " << queryId << " " << querySubPlanId);
    try {
        // Translate the query operators in their legacy representation
        // todo this is not required if later the query compiler can handle it by it self.
        //auto translationPhase = TranslateToLegacyPlanPhase::create();
        auto translationPhase = TranslateToGeneratableOperatorPhase::create();
        //ToDo: At present assume that the query plan has two different root operators with
        // same upstream operator chain.
        auto queryOperators = queryPlan->getRootOperators()[0];
        auto generatableOperatorPlan = translationPhase->transform(queryOperators);

        // Compile legacy operators with qep builder.
        auto qepBuilder = GeneratedQueryExecutionPlanBuilder::create()
                              .setQueryManager(queryManager)
                              .setBufferManager(bufferManager)
                              .setCompiler(queryCompiler)
                              .setQueryId(queryId)
                              .setQuerySubPlanId(querySubPlanId)
                              .addOperatorQueryPlan(generatableOperatorPlan);

        std::vector<SourceLogicalOperatorNodePtr> sourceOperators = queryPlan->getSourceOperators();
        std::vector<SinkLogicalOperatorNodePtr> sinkOperators = queryPlan->getSinkOperators();

        NodeEnginePtr self = this->inherited1::shared_from_this();

        // Translate all operator source to the physical sources and add them to the query plan
        for (const auto& sources : sourceOperators) {
            auto operatorId = sources->getId();
            auto sourceDescriptor = sources->getSourceDescriptor();
            //perform the operation only for Logical stream source descriptor
            if (sourceDescriptor->instanceOf<LogicalStreamSourceDescriptor>()) {
                sourceDescriptor = createLogicalSourceDescriptor(sourceDescriptor);
            }
            auto legacySource = ConvertLogicalToPhysicalSource::createDataSource(operatorId, sourceDescriptor, self,
                                                                                 numberOfBuffersInSourceLocalBufferPool);
            qepBuilder.addSource(legacySource);
            NES_DEBUG("ExecutableTransferObject:: add source" << legacySource->toString());
        }

        // Translate all operator sink to the physical sink and add them to the query plan

        for (const auto& sink : sinkOperators) {
            NES_ASSERT(sink, "Got invalid sink in query " << qepBuilder.getQueryId());
            auto sinkDescriptor = sink->getSinkDescriptor();
            auto schema = sink->getOutputSchema();
            // todo use the correct schema
            auto legacySink = ConvertLogicalToPhysicalSink::createDataSink(schema, sinkDescriptor, self, querySubPlanId);
            qepBuilder.addSink(legacySink);
            NES_DEBUG("NodeEngine::registerQueryInNodeEngine: add source" << legacySink->toString());
        }

        return registerQueryInNodeEngine(qepBuilder.build());
    } catch (std::exception& error) {
        NES_ERROR("Error while building query execution plan " << error.what());
        NES_ASSERT(false, "Error while building query execution plan " << error.what());
        return false;
    }
}

bool NodeEngine::registerQueryInNodeEngine(Execution::ExecutableQueryPlanPtr queryExecutionPlan) {
    std::unique_lock lock(engineMutex);
    QueryId queryId = queryExecutionPlan->getQueryId();
    QuerySubPlanId querySubPlanId = queryExecutionPlan->getQuerySubPlanId();
    NES_DEBUG("NodeEngine: registerQueryInNodeEngine query " << queryExecutionPlan << " queryId=" << queryId
                                                             << " querySubPlanId =" << querySubPlanId);
    NES_ASSERT(queryManager->isThreadPoolRunning(), "Registering query but thread pool not running");
    if (deployedQEPs.find(querySubPlanId) == deployedQEPs.end()) {
        auto found = queryIdToQuerySubPlanIds.find(queryId);
        if (found == queryIdToQuerySubPlanIds.end()) {
            queryIdToQuerySubPlanIds[queryId] = {querySubPlanId};
            NES_DEBUG("NodeEngine: register of QEP " << querySubPlanId << " as a singleton");
        } else {
            (*found).second.push_back(querySubPlanId);
            NES_DEBUG("NodeEngine: register of QEP " << querySubPlanId << " added");
        }
        if (queryManager->registerQuery(queryExecutionPlan)) {
            deployedQEPs[querySubPlanId] = queryExecutionPlan;
            NES_DEBUG("NodeEngine: register of subqep " << querySubPlanId << " succeeded");
            return true;
        } else {
            NES_DEBUG("NodeEngine: register of subqep " << querySubPlanId << " failed");
            return false;
        }
    } else {
        NES_DEBUG("NodeEngine: qep already exists. register failed" << querySubPlanId);
        return false;
    }
}

bool NodeEngine::startQuery(QueryId queryId) {
    std::unique_lock lock(engineMutex);
    NES_DEBUG("NodeEngine: startQuery=" << queryId);
    if (queryIdToQuerySubPlanIds.find(queryId) != queryIdToQuerySubPlanIds.end()) {

        std::vector<QuerySubPlanId> querySubPlanIds = queryIdToQuerySubPlanIds[queryId];
        if (querySubPlanIds.empty()) {
            NES_ERROR("NodeEngine: Unable to find qep ids for the query " << queryId << ". Start failed.");
            return false;
        }

        for (auto querySubPlanId : querySubPlanIds) {
            if (queryManager->startQuery(deployedQEPs[querySubPlanId])) {
                NES_DEBUG("NodeEngine: start of QEP " << querySubPlanId << " succeeded");
            } else {
                NES_DEBUG("NodeEngine: start of QEP " << querySubPlanId << " failed");
                return false;
            }
        }
        return true;
    }
    NES_ERROR("NodeEngine: qep does not exists. start failed" << queryId);
    return false;
}

bool NodeEngine::undeployQuery(QueryId queryId) {
    std::unique_lock lock(engineMutex);
    NES_DEBUG("NodeEngine: undeployQuery query=" << queryId);
    bool successStop = stopQuery(queryId);
    if (!successStop) {
        NES_ERROR("NodeEngine::undeployQuery: failed to stop query");
        return false;
    } else {
        NES_DEBUG("NodeEngine::undeployQuery: successfully stop query");
    }

    bool successUnregister = unregisterQuery(queryId);
    if (!successUnregister) {
        NES_ERROR("NodeEngine::undeployQuery: failed to unregister query");
        return false;
    } else {
        NES_DEBUG("NodeEngine::undeployQuery: successfully unregister query");
        return true;
    }
}

bool NodeEngine::unregisterQuery(QueryId queryId) {
    std::unique_lock lock(engineMutex);
    NES_DEBUG("NodeEngine: unregisterQuery query=" << queryId);
    if (queryIdToQuerySubPlanIds.find(queryId) != queryIdToQuerySubPlanIds.end()) {

        std::vector<QuerySubPlanId> querySubPlanIds = queryIdToQuerySubPlanIds[queryId];
        if (querySubPlanIds.empty()) {
            NES_ERROR("NodeEngine: Unable to find qep ids for the query " << queryId << ". Start failed.");
            return false;
        }

        for (auto querySubPlanId : querySubPlanIds) {
            if (queryManager->deregisterQuery(deployedQEPs[querySubPlanId])) {
                deployedQEPs.erase(querySubPlanId);
                NES_DEBUG("NodeEngine: unregister of query " << querySubPlanId << " succeeded");
            } else {
                NES_ERROR("NodeEngine: unregister of QEP " << querySubPlanId << " failed");
                return false;
            }
        }
        queryIdToQuerySubPlanIds.erase(queryId);
        return true;
    }

    NES_ERROR("NodeEngine: qep does not exists. unregister failed" << queryId);
    return false;
}

bool NodeEngine::stopQuery(QueryId queryId, bool graceful) {
    std::unique_lock lock(engineMutex);
    NES_DEBUG("NodeEngine:stopQuery for qep" << queryId);
    if (queryIdToQuerySubPlanIds.find(queryId) != queryIdToQuerySubPlanIds.end()) {
        std::vector<QuerySubPlanId> querySubPlanIds = queryIdToQuerySubPlanIds[queryId];
        if (querySubPlanIds.empty()) {
            NES_ERROR("NodeEngine: Unable to find qep ids for the query " << queryId << ". Start failed.");
            return false;
        }

        for (auto querySubPlanId : querySubPlanIds) {
            if (queryManager->stopQuery(deployedQEPs[querySubPlanId], graceful)) {
                NES_DEBUG("NodeEngine: stop of QEP " << querySubPlanId << " succeeded");
            } else {
                NES_ERROR("NodeEngine: stop of QEP " << querySubPlanId << " failed");
                return false;
            }
        }
        return true;
    }

    NES_ERROR("NodeEngine: qep does not exists. stop failed " << queryId);
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
        NES_WARNING("NodeEngine::stop: engine already stopped");
        return false;
    }
    NES_DEBUG("NodeEngine::stop: going to stop the node engine");
    std::unique_lock lock(engineMutex);
    bool withError = false;

    // release all deployed queries
    for (auto it = deployedQEPs.begin(); it != deployedQEPs.end();) {
        auto& [querySubPlanId, queryExecutionPlan] = *it;
        try {
            if (markQueriesAsFailed) {
                if (queryManager->failQuery(queryExecutionPlan)) {
                    NES_DEBUG("NodeEngine: fail of QEP " << querySubPlanId << " succeeded");
                } else {
                    NES_ERROR("NodeEngine: fail of QEP " << querySubPlanId << " failed");
                    withError = true;
                }
            } else {
                if (queryManager->stopQuery(queryExecutionPlan)) {
                    NES_DEBUG("NodeEngine: stop of QEP " << querySubPlanId << " succeeded");
                } else {
                    NES_ERROR("NodeEngine: stop of QEP " << querySubPlanId << " failed");
                    withError = true;
                }
            }
        } catch (std::exception& err) {
            NES_ERROR("NodeEngine: stop of QEP " << querySubPlanId << " failed: " << err.what());
            withError = true;
        }
        try {
            if (queryManager->deregisterQuery(queryExecutionPlan)) {
                NES_DEBUG("NodeEngine: deregisterQuery of QEP " << querySubPlanId << " succeeded");
                it = deployedQEPs.erase(it);
            } else {
                NES_ERROR("NodeEngine: deregisterQuery of QEP " << querySubPlanId << " failed");
                withError = true;
                ++it;
            }
        } catch (std::exception& err) {
            NES_ERROR("NodeEngine: deregisterQuery of QEP " << querySubPlanId << " failed: " << err.what());
            withError = true;
            ++it;
        }
    }
    // release components
    // TODO do not touch the sequence here as it will lead to errors in the shutdown sequence
    deployedQEPs.clear();
    queryIdToQuerySubPlanIds.clear();
    queryManager->destroy();
    networkManager->destroy();
    bufferManager->destroy();
    return !withError;
}

BufferManagerPtr NodeEngine::getBufferManager() { return bufferManager; }

Network::NetworkManagerPtr NodeEngine::getNetworkManager() { return networkManager; }

QueryCompilerPtr NodeEngine::getCompiler() { return queryCompiler; }

Execution::ExecutableQueryPlanStatus NodeEngine::getQueryStatus(QueryId queryId) {
    std::unique_lock lock(engineMutex);
    if (queryIdToQuerySubPlanIds.find(queryId) != queryIdToQuerySubPlanIds.end()) {
        std::vector<QuerySubPlanId> querySubPlanIds = queryIdToQuerySubPlanIds[queryId];
        if (querySubPlanIds.empty()) {
            NES_ERROR("NodeEngine: Unable to find qep ids for the query " << queryId << ". Start failed.");
            return Execution::ExecutableQueryPlanStatus::Invalid;
        }

        for (auto querySubPlanId : querySubPlanIds) {
            //FIXME: handle vector of statistics properly in #977
            return deployedQEPs[querySubPlanId]->getStatus();
        }
    }
    return Execution::ExecutableQueryPlanStatus::Invalid;
}

void NodeEngine::onDataBuffer(Network::NesPartition nesPartition, TupleBuffer& buffer) {
    // TODO analyze performance penalty here because of double lock
    if (partitionManager->isRegistered(nesPartition)) {
        // create a string for logging of the identity which corresponds to the
        // queryId::operatorId::partitionId::subpartitionId
        //TODO: dont use strings for lookups
        NES_DEBUG("NodeEngine::onDataBuffer addWork operator=" << nesPartition.getOperatorId() << " buffer=" << buffer
                                                               << " numberOfTuples=" << buffer.getNumberOfTuples()
                                                               << " orid=" << buffer.getOriginId());
        queryManager->addWork(nesPartition.getOperatorId(), buffer);
    } else {
        // partition is not registered, discard the buffer
        buffer.release();
        NES_ERROR("DataBuffer for " + nesPartition.toString() + " is not registered and was discarded!");
        //        NES_THROW_RUNTIME_ERROR("NES Network Error: unhandled message");
    }
}

void NodeEngine::onEndOfStream(Network::Messages::EndOfStreamMessage msg) {
    // propagate EOS to the locally running QEPs that use the network source
    NES_DEBUG("Going to inject eos for " << msg.getChannelId().getNesPartition());
    queryManager->addEndOfStream(msg.getChannelId().getNesPartition().getOperatorId(), msg.isGraceful());
}

void NodeEngine::onQueryReconfiguration(Network::Messages::QueryReconfigurationMessage queryReconfigurationMessage) {
    auto operatorId = queryReconfigurationMessage.getChannelId().getNesPartition().getOperatorId();
    for (std::pair<QuerySubPlanId, QuerySubPlanId> element : queryReconfigurationMessage.getQuerySubPlansIdToReplace()) {
        auto foundQEPNeedingReplacement = deployedQEPs.find(element.first);
        if (foundQEPNeedingReplacement != deployedQEPs.end()) {
            auto foundReplacementQEP = reconfigurationQEPs.find(element.second);
            queryManager->addQueryReconfiguration(operatorId, foundReplacementQEP->second, foundQEPNeedingReplacement->second,
                                                  queryReconfigurationMessage);
        }
    }
    for (auto querySubPlanId : queryReconfigurationMessage.getQuerySubPlansToStart()) {
        auto foundQueryReconfigurationQEP = reconfigurationQEPs.find(querySubPlanId);
        if (foundQueryReconfigurationQEP != reconfigurationQEPs.end()) {
            queryManager->addQueryReconfiguration(operatorId, foundQueryReconfigurationQEP->second, nullptr,
                                                  queryReconfigurationMessage);
        }
    }
    for (auto querySubPlanId : queryReconfigurationMessage.getQuerySubPlansToStop()) {
        auto foundQEPToStop = deployedQEPs.find(querySubPlanId);
        if (foundQEPToStop != reconfigurationQEPs.end()) {
            // propagate, deRegister and stop
        }
    }
}

void NodeEngine::onServerError(Network::Messages::ErrorMessage err) {
    if (err.getErrorType() == Network::Messages::ErrorType::kPartitionNotRegisteredError) {
        NES_WARNING("NodeEngine: Unable to find the NES Partition " << err.getChannelId());
        return;
    }
    NES_THROW_RUNTIME_ERROR(err.getErrorTypeAsString());
}

void NodeEngine::onChannelError(Network::Messages::ErrorMessage err) {
    if (err.getErrorType() == Network::Messages::ErrorType::kPartitionNotRegisteredError) {
        NES_WARNING("NodeEngine: Unable to find the NES Partition " << err.getChannelId() << ": please, retry later or abort.");
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
    if (foundQuerySubPlanIds == queryIdToQuerySubPlanIds.end()) {
        NES_ERROR("QueryManager::getQueryStatistics: query does not exists " << queryId);
        return queryStatistics;
    }

    NES_DEBUG("QueryManager: Extracting query execution ids for the input query " << queryId);
    std::vector<QuerySubPlanId> querySubPlanIds = (*foundQuerySubPlanIds).second;
    for (auto querySubPlanId : querySubPlanIds) {
        queryStatistics.emplace_back(queryManager->getQueryStatistics(querySubPlanId));
    }
    return queryStatistics;
}

Network::PartitionManagerPtr NodeEngine::getPartitionManager() { return partitionManager; }

SourceDescriptorPtr NodeEngine::createLogicalSourceDescriptor(SourceDescriptorPtr sourceDescriptor) {
    NES_INFO("NodeEngine: Updating the default Logical Source Descriptor to the Logical Source Descriptor supported by the node");

    //we have to decide where many cases
    // 1.) if we specify a build-in source like default source, then we have only one config but call this for each source
    // 2.) if we have really two sources, then we would have two real configurations here
    if (configs.size() > 1) {
        NES_ASSERT(!configs.empty(), "no config for Lambda source");
        auto conf = configs.back();
        configs.pop_back();
        return conf->build(sourceDescriptor->getSchema());
    } else {
        NES_ASSERT(configs[0], "physical source config is not specified");
        return configs[0]->build(sourceDescriptor->getSchema());
    }
}

void NodeEngine::setConfig(AbstractPhysicalStreamConfigPtr config) {
    NES_ASSERT(config, "physical source config is not specified");
    this->configs.emplace_back(config);
}

void NodeEngine::onFatalError(int signalNumber, std::string callstack) {
    NES_ERROR("onFatalError: signal [" << signalNumber << "] error [" << strerror(errno) << "] callstack " << callstack);
    std::cerr << "NodeEngine failed fatally" << std::endl;// it's necessary for testing and it wont harm us to write to stderr
    std::cerr << "Error: " << strerror(errno) << std::endl;
    std::cerr << "Signal: " << std::to_string(signalNumber) << std::endl;
    std::cerr << "Callstack:\n " << callstack << std::endl;
}

void NodeEngine::onFatalException(const std::shared_ptr<std::exception> exception, std::string callstack) {
    NES_ERROR("onFatalException: exception=[" << exception->what() << "] callstack=\n" << callstack);
    std::cerr << "NodeEngine failed fatally" << std::endl;
    std::cerr << "Error: " << strerror(errno) << std::endl;
    std::cerr << "Exception: " << exception->what() << std::endl;
    std::cerr << "Callstack:\n " << callstack << std::endl;
}

}// namespace NES::NodeEngine
