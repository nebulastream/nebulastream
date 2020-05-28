#include <NodeEngine/NodeEngine.hpp>
#include <NodeEngine/NodeProperties.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Nodes/Operators/OperatorNode.hpp>
#include <Nodes/Phases/ConvertLogicalToPhysicalSink.hpp>
#include <Nodes/Phases/ConvertLogicalToPhysicalSource.hpp>
#include <Nodes/Phases/TranslateToLegacyPlanPhase.hpp>
#include <Util/Logger.hpp>
#include <string>
using namespace std;
namespace NES {

static constexpr size_t DEFAULT_BUFFER_SIZE = 4096;
static constexpr size_t DEFAULT_NUM_BUFFERS = 1024;

JSON NodeEngine::getNodePropertiesAsJSON() {
    props->readMemStats();
    props->readCpuStats();
    props->readNetworkStats();
    props->readDiskStats();

    return props->getExistingMetrics();
}

std::string NodeEngine::getNodePropertiesAsString() {
    props->readMemStats();
    props->readCpuStats();
    props->readNetworkStats();
    props->readDiskStats();

    return props->dump();
}

NodeProperties* NodeEngine::getNodeProperties() {
    return props.get();
}

NodeEngine::NodeEngine() {
    NES_DEBUG("NodeEngine()");
    props = std::make_shared<NodeProperties>();
    isRunning = false;
}

NodeEngine::~NodeEngine() {
    NES_DEBUG("~NodeEngine()");
    stop(true);
    queryIdToQepMap.clear();
    qepToStatusMap.clear();
}

bool NodeEngine::deployQueryInNodeEngine(std::string executableTransferObject) {
    NES_DEBUG("NodeEngine::deployQueryInNodeEngine eto " << executableTransferObject);

    ExecutableTransferObject eto = SerializationTools::parse_eto(
        executableTransferObject);
    NES_DEBUG(
        "WorkerActor::running() eto after parse=" << eto.toString());
    QueryExecutionPlanPtr qep = eto.toQueryExecutionPlan(queryCompiler);
    NES_DEBUG("WorkerActor::running()  add query to queries map");
    deployQueryInNodeEngine(qep);
}

bool NodeEngine::deployQueryInNodeEngine(QueryExecutionPlanPtr qep) {
    NES_DEBUG("NodeEngine: deployQueryInNodeEngine query using qep " << qep);
    bool successRegister = registerQueryInNodeEngine(qep);
    if (!successRegister) {
        NES_ERROR("NodeEngine::deployQueryInNodeEngine: failed to register query");
        return false;
    } else {
        NES_DEBUG("NodeEngine::deployQueryInNodeEngine: successfully register query");
    }
    bool successStart = startQuery(qep->getQueryId());
    if (!successStart) {
        NES_ERROR("NodeEngine::deployQueryInNodeEngine: failed to start query");
        return false;
    } else {
        NES_DEBUG("NodeEngine::deployQueryInNodeEngine: successfully start query");
    }
    return true;
}

bool NodeEngine::registerQueryInNodeEngine(std::string queryId, OperatorNodePtr operatorTree) {

    NES_DEBUG(
        "WorkerActor::running() queryId after " << queryId);

    NES_INFO("*** Creating QueryExecutionPlan for " << queryId);
    auto translationPhase = TranslateToLegacyPlanPhase::create();
    auto legacyOperatorPlan = translationPhase->transform(operatorTree);
    QueryExecutionPlanPtr qep = queryCompiler->compile(legacyOperatorPlan);
    qep->setQueryId(queryId);

    for (const auto& sources : operatorTree->getNodesByType<SourceLogicalOperatorNode>()) {
        auto sourceDescriptor = sources->getSourceDescriptor();
        auto legacySource = ConvertLogicalToPhysicalSource::createDataSource(sourceDescriptor);
        qep->addDataSource(legacySource);
        NES_DEBUG("ExecutableTransferObject:: add source" << legacySource->toString());
    }

    for (const auto& sink : operatorTree->getNodesByType<SinkLogicalOperatorNode>()) {
        auto sinkDescriptor = sink->getSinkDescriptor();
        // todo use the correct schema
        auto legacySink = ConvertLogicalToPhysicalSink::createDataSink(qep->getSources()[0]->getSchema(), sinkDescriptor);
        qep->addDataSink(legacySink);
        NES_DEBUG("ExecutableTransferObject:: add source" << legacySink->toString());
    }

    return registerQueryInNodeEngine(qep);
}

bool NodeEngine::registerQueryInNodeEngine(QueryExecutionPlanPtr qep) {
    NES_DEBUG("NodeEngine: registerQueryInNodeEngine query " << qep << " queryId=" << qep->getQueryId());
    qep->setBufferManager(bufferManager);

    if (queryIdToQepMap.find(qep->getQueryId()) == queryIdToQepMap.end()) {
        if (queryManager->registerQuery(qep)) {
            queryIdToQepMap.insert({qep->getQueryId(), qep});
            qepToStatusMap.insert({qep, NodeEngineQueryStatus::registered});
            NES_DEBUG("NodeEngine: register of QEP " << qep << " succeeded");
            return true;
        } else {
            NES_DEBUG("NodeEngine: register of QEP " << qep << " failed");
            return false;
        }
    } else {
        NES_DEBUG("NodeEngine: qep already exists. register failed" << qep);
        return false;
    }
}

bool NodeEngine::startQuery(std::string queryId) {
    NES_DEBUG("NodeEngine: startQuery=" << queryId);
    if (queryIdToQepMap.find(queryId) != queryIdToQepMap.end()) {
        if (queryManager->startQuery(queryIdToQepMap[queryId])) {
            NES_DEBUG("NodeEngine: start of QEP " << queryId << " succeeded");
            qepToStatusMap[queryIdToQepMap[queryId]] = NodeEngineQueryStatus::started;
            return true;
        } else {
            NES_DEBUG("NodeEngine: start of QEP " << queryId << " failed");
            return false;
        }
    } else {
        NES_DEBUG("NodeEngine: qep does not exists. start failed" << queryId);
        return false;
    }
}

bool NodeEngine::undeployQuery(std::string queryId) {
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

bool NodeEngine::unregisterQuery(std::string queryId) {
    NES_DEBUG("NodeEngine: unregisterQuery query=" << queryId);
    if (queryIdToQepMap.find(queryId) != queryIdToQepMap.end()) {
        if (queryManager->deregisterQuery(queryIdToQepMap[queryId])) {
            size_t delCnt1 = qepToStatusMap.erase(queryIdToQepMap[queryId]);
            NES_DEBUG("NodeEngine: unregister of QEP " << queryId << " succeeded with cnt=" << delCnt1);

            size_t delCnt2 = queryIdToQepMap.erase(queryId);
            NES_DEBUG("NodeEngine: unregister of query " << queryId << " succeeded with cnt=" << delCnt2);

            if (delCnt1 == 1 && delCnt2 == 1) {
                return true;
            } else {
                NES_ERROR("NodeEngine::unregisterQuery: error while unregister query");
                return false;
            }
        } else {
            NES_ERROR("NodeEngine: unregister of QEP " << queryId << " failed");
            return false;
        }
    } else {
        NES_DEBUG("NodeEngine: qep does not exists. unregister failed" << queryId);
        return false;
    }
}

bool NodeEngine::stopQuery(std::string queryId) {
    NES_DEBUG("NodeEngine:stopQuery for qep" << queryId);
    if (queryIdToQepMap.find(queryId) != queryIdToQepMap.end()) {
        if (queryManager->stopQuery(queryIdToQepMap[queryId])) {
            NES_DEBUG("NodeEngine: stop of QEP " << queryId << " succeeded");
            qepToStatusMap[queryIdToQepMap[queryId]] = NodeEngineQueryStatus::stopped;
            return true;
        } else {
            NES_DEBUG("NodeEngine: stop of QEP " << queryId << " failed");
            return false;
        }
    } else {
        NES_DEBUG("NodeEngine: qep does not exists. stop failed" << queryId);
        return false;
    }
}

QueryManagerPtr NodeEngine::getQueryManager() {
    return queryManager;
}

bool NodeEngine::start() {
    if (!isRunning) {
        NES_DEBUG("NodeEngine: start thread pool");
        bool successTp = startQueryManager();
        NES_DEBUG("NodeEngine: start thread pool success=" << successTp);

        NES_DEBUG("NodeEngine:start reset query manager");
        queryManager->resetQueryManager();

        NES_DEBUG("NodeEngine: create buffer manager");
        bool successBm = createBufferManager();
        NES_DEBUG("NodeEngine: create buffer manager success=" << successBm);

        if (successTp && successBm) {
            isRunning = true;
            return true;
        } else {
            return false;
        }
    } else {
        NES_WARNING("NodeEngine::start: is already running");
        return true;
    }
}

bool NodeEngine::stop(bool forceStop) {
    //TODO: add check if still queries are running
    if (isRunning) {
        NES_DEBUG("NodeEngine:stop stop NodeEngine, undeploy " << qepToStatusMap.size() << " queries");
        for (auto entry : qepToStatusMap) {
            if (entry.second == NodeEngineQueryStatus::started && !forceStop) {
                NES_ERROR("NodeEngine::stop: cannot stop as query " << entry.first << " still running");
                return false;
            }
        }

        //extract key column from map
        std::vector<std::string> copyOfVec;
        std::transform(queryIdToQepMap.begin(), queryIdToQepMap.end(), std::back_inserter(copyOfVec),
                       [](std::pair<std::string, QueryExecutionPlanPtr> const& dev) {
                           return dev.first;
                       });

        for (std::string qId : copyOfVec) {
            bool success = undeployQuery(qId);
            if (success) {
                NES_DEBUG("NodeEngine:stop undeployQuery query " << qId << " successfully");

            } else {
                NES_ERROR("NodeEngine:stop undeploy query " << qId << " failed");
                return false;
            }
        }

        NES_DEBUG("NodeEngine:stop undeploy successful");
        queryManager->resetQueryManager();
        copyOfVec.clear();
        isRunning = false;

        bool successTp = stopQueryManager();
        NES_DEBUG("NodeEngine:stop stop threadpool with success=" << successTp);

        bool successBm = stopBufferManager();
        NES_DEBUG("NodeEngine:stop stop buffer manager with success=" << successBm);

        return successTp && successBm;
    } else {
        NES_WARNING("NodeEngine::stop: engine already stopped");
        return true;
    }
}

BufferManagerPtr NodeEngine::getBufferManager() {
    return bufferManager;
}

bool NodeEngine::createBufferManager() {
    if (bufferManager) {
        NES_ERROR("NodeEngine::createBufferManager: buffer manager already exists");
        return true;
    }
    NES_DEBUG("createBufferManager: setup buffer manager");
    bufferManager = std::make_shared<BufferManager>(DEFAULT_BUFFER_SIZE, DEFAULT_NUM_BUFFERS);
    return bufferManager->isReady();
}

bool NodeEngine::createBufferManager(size_t bufferSize, size_t numBuffers) {
    if (bufferManager) {
        NES_ERROR("NodeEngine::createBufferManager: buffer manager already exists");
        return true;
    }

    NES_DEBUG("createBufferManager: setup buffer manager");
    bufferManager = std::make_shared<BufferManager>(bufferSize, numBuffers);
    return bufferManager->isReady();
}

bool NodeEngine::stopBufferManager() {
    if (!bufferManager) {
        NES_ERROR("NodeEngine::stopBufferManager buffer manager does not exists");
        throw Exception("Error while stop buffer manager");
    }
    NES_DEBUG("QueryManager::stopBufferManager: stop");
    return true;
}

bool NodeEngine::startQueryManager() {
    NES_DEBUG("startQueryManager: setup query manager");
    if (queryManager) {
        NES_ERROR("NodeEngine::startQueryManager: query manager already exists");
        return true;
    }
    NES_DEBUG("startQueryManager: setup query manager");
    queryManager = std::make_shared<QueryManager>();
    if (queryManager) {
        NES_DEBUG("NodeEngine::startQueryManager(): successful");
        NES_DEBUG("QueryManager(): start thread pool");
        bool success = queryManager->startThreadPool();
        if (!success) {
            NES_ERROR("QueryManager: error while start thread pool");
            throw Exception("Error while start thread pool");
        } else {
            NES_DEBUG("QueryManager(): thread pool successfully started");

            NES_DEBUG("NodeEngine::startQueryManager create compiler");
            queryCompiler = createDefaultQueryCompiler(queryManager);
            return true;
        }
    } else {
        NES_ERROR("NodeEngine::startQueryManager(): query manager could not be starterd");
        return false;
    }
}

bool NodeEngine::stopQueryManager() {
    if (!queryManager) {
        NES_ERROR("NodeEngine::stopQueryManager query manager does not exists");
        throw Exception("Error while stop query manager");
    }
    NES_DEBUG("stopQueryManager: stop query manager");

    bool success = queryManager->stopThreadPool();
    if (!success) {
        NES_ERROR("NodeEngine::stopQueryManager: could not stop thread pool");
        throw Exception("Error while stopping thread pool");
    }

    return true;
}

QueryStatisticsPtr NodeEngine::getQueryStatistics(std::string queryId) {
    QueryExecutionPlanPtr qep = queryIdToQepMap[queryId];
    if (!qep) {
        NES_ERROR("QueryManager::getNumberOfProcessedBuffer: query does not exists");
        throw Exception("Query does not exists");
    }
    return queryManager->getQueryStatistics(qep);
}

}// namespace NES
