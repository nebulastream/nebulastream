#include <Catalogs/QueryCatalog.hpp>
#include <Components/NesCoordinator.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Nodes/Operators/OperatorNode.hpp>
#include <Nodes/Phases/ConvertPhysicalToLogicalSink.hpp>
#include <Nodes/Phases/ConvertPhysicalToLogicalSource.hpp>
#include <Nodes/Phases/TranslateFromLegacyPlanPhase.hpp>
#include <REST/usr_interrupt_handler.hpp>
#include <Topology/NESTopologyEntry.hpp>
#include <Topology/TopologyManager.hpp>
#include <Util/Logger.hpp>
#include <future>
#include <thread>

//GRPC Includes
#include <GRPC/CoordinatorRPCServer.hpp>
#include <grpcpp/health_check_service_interface.h>
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

namespace NES {

NesCoordinator::NesCoordinator() {
    NES_DEBUG("NesCoordinator()");
    restPort = 8081;
    serverIp = "localhost";
    rpcPort = 4000;
    stopped = false;

    topologyManager = std::make_shared<TopologyManager>();
    streamCatalog = std::make_shared<StreamCatalog>();
    queryCatalog = std::make_shared<QueryCatalog>(topologyManager, streamCatalog);
    workerRPCClient = std::make_shared<WorkerRPCClient>();
    queryDeployer = std::make_shared<QueryDeployer>(queryCatalog, topologyManager);
    coordinatorEngine = std::make_shared<CoordinatorEngine>(streamCatalog, topologyManager);
}

NesCoordinator::NesCoordinator(string serverIp, uint16_t restPort, uint16_t rpcPort)
    : serverIp(serverIp),
      restPort(restPort),
      rpcPort(rpcPort) {
    NES_DEBUG("NesCoordinator() serverIp=" << serverIp << " restPort=" << restPort << " rpcPort=" << rpcPort);
    stopped = false;

    topologyManager = std::make_shared<TopologyManager>();
    streamCatalog = std::make_shared<StreamCatalog>();
    queryCatalog = std::make_shared<QueryCatalog>(topologyManager, streamCatalog);
    workerRPCClient = std::make_shared<WorkerRPCClient>();
    queryDeployer = std::make_shared<QueryDeployer>(queryCatalog, topologyManager);
    coordinatorEngine = std::make_shared<CoordinatorEngine>(streamCatalog, topologyManager);
}

NesCoordinator::~NesCoordinator() {
    NES_DEBUG("NesCoordinator::~NesCoordinator()");
    stopCoordinator(true);
    NES_DEBUG("NesCoordinator::~NesCoordinator() clear map");
    currentDeployments.clear();
    NES_DEBUG("NesCoordinator::~NesCoordinator() map cleared");
    streamCatalog->reset();
    queryCatalog->clearQueries();
    topologyManager->resetNESTopologyPlan();
}

/**
 * @brief this method starts the rest server and then blocks
 */
void startRestServer(std::shared_ptr<RestServer> restServer,
                     std::string restHost, uint16_t restPort, NesCoordinatorPtr coordinator, std::promise<bool>& prom) {

    prom.set_value(true);
    restServer->start();//this call is blocking
    NES_DEBUG("NesCoordinator: startRestServer thread terminates");
}

/**
 * @brief this method will start the RPC Coordinator service which is responsible for reacting to calls from the
 * CoordinatorRPCClient which will be send by the worker
 */
void startCoordinatorRPCServer(std::shared_ptr<Server>& rpcServer,
                               std::string address,
                               std::promise<bool>& prom,
                               CoordinatorEnginePtr coordinatorEngine) {
    NES_DEBUG("startCoordinatorRPCServer");
    grpc::ServerBuilder builder;
    CoordinatorRPCServer service(coordinatorEngine);

    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    rpcServer = std::move(server);
    prom.set_value(true);
    NES_DEBUG("startCoordinatorRPCServer: Server listening on address=" << address);
    rpcServer->Wait();//blocking call
    NES_DEBUG("startCoordinatorRPCServer: end listening");
}

size_t NesCoordinator::startCoordinator(bool blocking) {
    NES_DEBUG("NesCoordinator start");

    std::string address = serverIp + ":" + ::to_string(rpcPort);
    NES_DEBUG("startCoordinatorRPCServer for address=" << address);

    //Start RPC server that listen to calls form the clients
    std::promise<bool> promRPC;//promise to make sure we wait until the server is started
    rpcThread = std::make_shared<std::thread>(([&]() {
        startCoordinatorRPCServer(rpcServer, address, promRPC, coordinatorEngine);
    }));
    promRPC.get_future().get();
    NES_DEBUG("NESWorker::startCoordinatorRPCServer: ready");

    //start the coordinator worker that is the sink for all queries
    NES_DEBUG("NesCoordinator::startCoordinator: start nes worker");
    worker = std::make_shared<NesWorker>(serverIp,
                                         std::to_string(rpcPort),
                                         serverIp,
                                         std::to_string(rpcPort + 1),
                                         NESNodeType::Worker);
    worker->start(/**blocking*/ false, /**withConnect*/ true);

    //Start rest that accepts quiers form the outsides
    NES_DEBUG("NesCoordinator starting rest server");
    std::promise<bool> promRest;
    std::shared_ptr<RestServer> restServer = std::make_shared<RestServer>(serverIp, restPort, this->shared_from_this(),
                                                                          queryCatalog, streamCatalog, topologyManager);

    restThread = std::make_shared<std::thread>(([&]() {
        startRestServer(restServer, serverIp, restPort, this->shared_from_this(), promRest);
    }));

    promRest.get_future().get();//promise to make sure we wait until the server is started
    NES_DEBUG("NESWorker::startCoordinatorRPCServer: ready");

    stopped = false;
    if (blocking) {//blocking is for the starter to wait here for user to send query
        NES_DEBUG("NesCoordinator started, join now and waiting for work");
        restThread->join();
        NES_DEBUG("NesCoordinator Required stopping");
    } else {//non-blocking is used for tests to continue execution
        NES_DEBUG(
            "NesCoordinator started, return without blocking on port " << rpcPort);
        return rpcPort;
    }

    NES_DEBUG("NesCoordinator startCoordinator succeed");
}

bool NesCoordinator::stopCoordinator(bool force) {
    NES_DEBUG("NesCoordinator: stopCoordinator force=" << force);
    if (!stopped) {
        NES_DEBUG("NesCoordinator: stopping rest server");
        bool successStopRest = restServer->stop();
        if (!successStopRest) {
            NES_ERROR("NesCoordinator::stopCoordinator: error while stopping restServer");
            throw Exception("Error while stopping NesCoordinator");
        }

        NES_DEBUG("NesCoordinator: rest server stopped " << successStopRest);
        if (restThread->joinable()) {
            NES_DEBUG("NesCoordinator: join restThread");
            restThread->join();
        } else {
            NES_ERROR("NesCoordinator: rest thread not joinable");
            throw Exception("Error while stopping restThread->join");
        }

        NES_DEBUG("NesCoordinator: stopping rpc server");
        rpcServer->Shutdown();
        if (rpcThread->joinable()) {
            NES_DEBUG("NesCoordinator: join rpcThread");
            rpcThread->join();
        } else {
            NES_ERROR("NesCoordinator: rpc thread not joinable");
            throw Exception("Error while stopping thread->join");
        }

        bool successShutdownWorker = worker->stop(force);
        if (!successShutdownWorker) {
            NES_ERROR("NesCoordinator::stop node engine stop not successful");
            throw Exception("NesCoordinator::stop error while stopping node engine");
        } else {
            NES_DEBUG("NesCoordinator::stop Node engine stopped successfully");
        }

        stopped = true;
        return true;
    } else {
        NES_DEBUG("NesCoordinator: already stopped");
        return true;
    }
}

void NesCoordinator::setServerIp(std::string serverIp) {
    this->serverIp = serverIp;
}

QueryStatisticsPtr NesCoordinator::getQueryStatistics(std::string queryId) {
    return worker->getNodeEngine()->getQueryStatistics(queryId);
}

string NesCoordinator::addQuery(const string queryString, const string strategy) {
    NES_DEBUG("NesCoordinator:addQuery queryString=" << queryString << " strategy=" << strategy);

    NES_DEBUG("NesCoordinator:addQuery: register query");
    string queryId = registerQuery(queryString, strategy);
    if (queryId != "") {
        NES_DEBUG("NesCoordinator:addQuery: register query successful");
    } else {
        NES_ERROR("NesCoordinator:addQuery: register query failed");
        return "";
    }

    NES_DEBUG("NesCoordinator:addQuery: create Deployment");
    QueryDeployment deployments = queryDeployer->generateDeployment(queryId);
    NES_DEBUG("NESCoordinator::addQuery" << deployments.size() << " objects topology before"
                                           << topologyManager->getNESTopologyPlanString());
    stringstream ss;
    for (auto element : deployments) {
        ss << "nodeID=" << element.first->getId();
        ss << " nodeIP=" << element.first->getIp();
        ss << " eto=" << element.second.toString() << endl;
    }
    NES_DEBUG("NesCoordinator:addQuery: deployments =" << ss.str());
    currentDeployments[queryId] = deployments;

    NES_DEBUG("NesCoordinator:addQuery: deploy query");
    bool successDeploy = deployQuery(queryId);
    if (successDeploy) {
        NES_DEBUG("NesCoordinator:addQuery: deploy query successful");
    } else {
        NES_ERROR("NesCoordinator:addQuery: deploy query failed");
        return "";
    }

    NES_DEBUG("NesCoordinator:addQuery: start query");
    bool successStart = startQuery(queryId);
    if (successStart) {
        NES_DEBUG("NesCoordinator:addQuery: start query successful");
    } else {
        NES_ERROR("NesCoordinator:addQuery: start query failed");
        return "";
    }

    return queryId;
}

bool NesCoordinator::removeQuery(const string queryId) {
    NES_DEBUG("NesCoordinator:removeQuery queryId=" << queryId);

    NES_DEBUG("NesCoordinator:removeQuery: stop query");
    bool successStop = stopQuery(queryId);
    if (successStop) {
        NES_DEBUG("NesCoordinator:removeQuery: stop query successful");
    } else {
        NES_ERROR("NesCoordinator:removeQuery: stop query failed");
        return false;
    }

    NES_DEBUG("NesCoordinator:removeQuery: undeploy query");
    bool successUndeploy = undeployQuery(queryId);
    if (successUndeploy) {
        NES_DEBUG("NesCoordinator:removeQuery: undeploy query successful");
    } else {
        NES_ERROR("NesCoordinator:removeQuery: undeploy query failed");
        return false;
    }

    NES_DEBUG("NesCoordinator:removeQuery: unregister query");
    bool successUnregister = unregisterQuery(queryId);
    if (successUnregister) {
        NES_DEBUG("NesCoordinator:removeQuery: unregister query successful");
    } else {
        NES_ERROR("NesCoordinator:removeQuery: unregister query failed");
        return false;
    }

    return true;
}

string NesCoordinator::registerQuery(const string queryString, const string strategy) {
    NES_DEBUG("NesCoordinator:registerQuery queryString=" << queryString << " strategy=" << strategy);
    return queryCatalog->registerQuery(queryString, strategy);
}

bool NesCoordinator::unregisterQuery(const string queryId) {
    NES_DEBUG("NesCoordinator:unregisterQuery queryId=" << queryId);
    return queryCatalog->deleteQuery(queryId);
}

bool NesCoordinator::deployQuery(std::string queryId) {
    NES_DEBUG("NesCoordinator:deployQuery queryId=" << queryId);

    QueryDeployment& deployments = currentDeployments[queryId];
    auto translationUnit = TranslateFromLegacyPlanPhase::create();

    for (auto x = deployments.rbegin(); x != deployments.rend(); x++) {
        NES_DEBUG("NESCoordinator::registerQueryInNodeEngine serialize " << x->first << " id=" << x->first->getId()
                                                                           << " eto="
                                                                           << x->second.toString());

        auto executionTransferObject = x->second;

        // In the following code we translate the executable transfer object in the new logical node representations,
        // because we want to use this for serialization.
        // Later we want to replace this if we we can store the right representation directly in the query deployment map.
        // This procedure follows three steps.
        // 1. we have to find the root of the operator tree, as we always serialize from the parent to child.
        auto rootOperator = executionTransferObject.getOperatorTree();
        while (rootOperator->getParent() != nullptr) {
            rootOperator = rootOperator->getParent();
        }
        // 2. we translate the legacy operator into the new node representation
        auto queryOperators = translationUnit->transform(rootOperator);

        // 3. the operator plan contains the wrong source and sink descriptor, because the executionTransferObject
        // is storing them separately.
        // As a consequence we update the source and sink descriptors in the following

        // Translate source descriptor and add to sink operator.
        // todo we currently assume that we only have one sink per query plan.
        auto sourceDescriptor = ConvertPhysicalToLogicalSource::createSourceDescriptor(executionTransferObject.getSources()[0]);
        auto sourceOperator = queryOperators->getNodesByType<SourceLogicalOperatorNode>()[0];
        sourceOperator->setSourceDescriptor(sourceDescriptor);

        // Translate sink descriptor and add to sink operator.
        // todo we currently assume that we only have one sink per query plan.
        auto sinkDescriptor = ConvertPhysicalToLogicalSink::createSinkDescriptor(executionTransferObject.getDestinations()[0]);
        auto sinkOperator = queryOperators->getNodesByType<SinkLogicalOperatorNode>()[0];
        sinkOperator->setSinkDescriptor(sinkDescriptor);

        NES_DEBUG(
            "NesCoordinator:deployQuery: " << queryId << " to " << x->first->getIp());

        bool success = workerRPCClient->registerQuery(x->first->getIp(), queryId, queryOperators);
        if (success) {
            NES_DEBUG("NesCoordinator:deployQuery: " << queryId << " to " << x->first->getIp() << " successful");
        } else {
            NES_ERROR("NesCoordinator:deployQuery: " << queryId << " to " << x->first->getIp() << "  failed");
            return false;
        }
    }

    return true;
}

bool NesCoordinator::undeployQuery(std::string queryId) {
    NES_DEBUG("NesCoordinator:undeployQuery queryId=" << queryId);

    QueryDeployment& deployments = currentDeployments[queryId];
    for (auto x = deployments.rbegin(); x != deployments.rend(); x++) {
        NES_DEBUG("NESCoordinator::undeployQuery serialize " << x->first << " id=" << x->first->getId() << " eto="
                                                               << x->second.toString());
        NES_DEBUG(
            "NESCoordinator::undeployQuery " << queryId << " to " << x->first->getIp());

        bool success = workerRPCClient->unregisterQuery(x->first->getIp(), queryId);
        if (success) {
            NES_DEBUG("NesCoordinator:undeployQuery: " << queryId << " to " << x->first->getIp() << " successful");
        } else {
            NES_ERROR("NesCoordinator:undeployQuery: " << queryId << " to " << x->first->getIp() << "  failed");
            return false;
        }
    }
    return true;
}

bool NesCoordinator::startQuery(std::string queryId) {
    NES_DEBUG("NesCoordinator:startQuery queryId=" << queryId);

    QueryDeployment& deployments = currentDeployments[queryId];
    for (auto x = deployments.rbegin(); x != deployments.rend(); x++) {
        NES_DEBUG("NESCoordinator::startQuery serialize " << x->first << " id=" << x->first->getId()
                                                            << " eto="
                                                            << x->second.toString());
        NES_DEBUG(
            "NESCoordinator::startQuery " << queryId << " to " << x->first->getIp());

        bool success = workerRPCClient->startQuery(x->first->getIp(), queryId);
        if (success) {
            NES_DEBUG("NesCoordinator:startQuery: " << queryId << " to " << x->first->getIp() << " successful");
        } else {
            NES_ERROR("NesCoordinator:startQuery: " << queryId << " to " << x->first->getIp() << "  failed");
            return false;
        }
    }

    return true;
}

bool NesCoordinator::stopQuery(std::string queryId) {
    NES_DEBUG("NesCoordinator:stopQuery queryId=" << queryId);

    QueryDeployment& deployments = currentDeployments[queryId];
    for (auto x = deployments.rbegin(); x != deployments.rend(); x++) {
        NES_DEBUG("NESCoordinator::stopQuery serialize " << x->first << " id=" << x->first->getId()
                                                           << " eto="
                                                           << x->second.toString());
        NES_DEBUG(
            "NESCoordinator::stopQuery " << queryId << " to " << x->first->getIp());

        bool success = workerRPCClient->stopQuery(x->first->getIp(), queryId);
        if (success) {
            NES_DEBUG("NesCoordinator:stopQuery: " << queryId << " to " << x->first->getIp() << " successful");
        } else {
            NES_ERROR("NesCoordinator:stopQuery: " << queryId << " to " << x->first->getIp() << "  failed");
            return false;
        }
    }

    return true;
}

}// namespace NES
