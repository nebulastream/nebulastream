#include <Catalogs/QueryCatalog.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Deployer/QueryDeployer.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Nodes/Operators/OperatorNode.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <REST/RestServer.hpp>
#include <REST/usr_interrupt_handler.hpp>
#include <Services/QueryRequestProcessorService.hpp>
#include <Services/QueryService.hpp>
#include <Topology/NESTopologyEntry.hpp>
#include <Topology/TopologyManager.hpp>
#include <Util/Logger.hpp>
#include <WorkQueues/QueryRequestQueue.hpp>
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

NesCoordinator::NesCoordinator(string serverIp, uint16_t restPort, uint16_t rpcPort)
    : serverIp(serverIp), restPort(restPort), rpcPort(rpcPort) {
    NES_DEBUG("NesCoordinator() serverIp=" << serverIp << " restPort=" << restPort << " rpcPort=" << rpcPort);
    stopped = false;

    topologyManager = std::make_shared<TopologyManager>();
    streamCatalog = std::make_shared<StreamCatalog>();
    globalExecutionPlan = GlobalExecutionPlan::create();
    queryCatalog = std::make_shared<QueryCatalog>();
    coordinatorEngine = std::make_shared<CoordinatorEngine>(streamCatalog, topologyManager);
    WorkerRPCClientPtr workerRpcClient = std::make_shared<WorkerRPCClient>();
    QueryDeployerPtr queryDeployer = std::make_shared<QueryDeployer>(queryCatalog, topologyManager, globalExecutionPlan);
    QueryRequestQueuePtr queryRequestQueue = std::make_shared<QueryRequestQueue>();
    queryRequestProcessorService = std::make_shared<QueryRequestProcessorService>(globalExecutionPlan, topologyManager->getNESTopologyPlan(),
                                                                                  queryCatalog, streamCatalog, workerRpcClient, queryDeployer, queryRequestQueue);
    queryService = std::make_shared<QueryService>(queryCatalog, queryRequestQueue);
}

NesCoordinator::~NesCoordinator() {
    NES_DEBUG("NesCoordinator::~NesCoordinator()");
    stopCoordinator(true);
    NES_DEBUG("NesCoordinator::~NesCoordinator() map cleared");
    streamCatalog->reset();
    queryCatalog->clearQueries();
    topologyManager->resetNESTopologyPlan();
}

size_t NesCoordinator::startCoordinator(bool blocking) {
    NES_DEBUG("NesCoordinator start");

    queryRequestProcessorThread = std::make_shared<std::thread>(([&]() {
        NES_INFO("NesCoordinator: started queryRequestProcessor");
        queryRequestProcessorService->start();
        NES_WARNING("NesCoordinator: finished queryRequestProcessor");
    }));

    NES_DEBUG("startCoordinatorRPCServer: Building GRPC Server");

    rpcThread = std::make_shared<std::thread>(([&]() {
        NES_DEBUG("startCoordinatorRPCServer");
        buildAndStartGRPCServer();
        NES_DEBUG("startCoordinatorRPCServer: end listening");
    }));

    NES_DEBUG("NESWorker::startCoordinatorRPCServer: ready");

    //start the coordinator worker that is the sink for all queries
    NES_DEBUG("NesCoordinator::startCoordinator: start nes worker");
    worker = std::make_shared<NesWorker>(serverIp, std::to_string(rpcPort), serverIp,
                                         std::to_string(rpcPort + 1), NESNodeType::Worker);
    worker->start(/**blocking*/ false, /**withConnect*/ true);

    //Start rest that accepts queries form the outsides
    NES_DEBUG("NesCoordinator starting rest server");
    restServer = std::make_shared<RestServer>(serverIp, restPort, this->shared_from_this(), queryCatalog,
                                              streamCatalog, topologyManager, globalExecutionPlan, queryService);
    restThread = std::make_shared<std::thread>(([&]() {
        restServer->start();//this call is blocking
        NES_DEBUG("NesCoordinator: startRestServer thread terminates");
    }));
    NES_DEBUG("NESWorker::startCoordinatorRESTServer: ready");

    stopped = false;
    if (blocking) {//blocking is for the starter to wait here for user to send query
        NES_DEBUG("NesCoordinator started, join now and waiting for work");
        restThread->join();
        NES_DEBUG("NesCoordinator Required stopping");
    } else {//non-blocking is used for tests to continue execution
        NES_DEBUG("NesCoordinator started, return without blocking on port " << rpcPort);
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
            throw Exception("Error while stopping thread->join");
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

        queryRequestProcessorService->shutDown();
        if (queryRequestProcessorThread->joinable()) {
            NES_DEBUG("NesCoordinator: join rpcThread");
            queryRequestProcessorThread->join();
        } else {
            NES_ERROR("NesCoordinator: query processor thread not joinable");
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

void NesCoordinator::buildAndStartGRPCServer() {
    grpc::ServerBuilder builder;
    CoordinatorRPCServer service(coordinatorEngine);

    std::string address = serverIp + ":" + ::to_string(rpcPort);
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    rpcServer = builder.BuildAndStart();
    NES_DEBUG("startCoordinatorRPCServer: Server listening on address=" << address);
    rpcServer->Wait();//blocking call
}

void NesCoordinator::setServerIp(std::string serverIp) {
    this->serverIp = serverIp;
}

QueryStatisticsPtr NesCoordinator::getQueryStatistics(std::string queryId) {
    return worker->getNodeEngine()->getQueryStatistics(queryId);
}

QueryServicePtr NesCoordinator::getQueryService() {
    return queryService;
}

QueryCatalogPtr NesCoordinator::getQueryCatalog() {
    return queryCatalog;
}

}// namespace NES
