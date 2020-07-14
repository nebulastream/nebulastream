#include <Catalogs/QueryCatalog.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Deployer/QueryDeployer.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Nodes/Operators/OperatorNode.hpp>
#include <Phases/ConvertPhysicalToLogicalSink.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <REST/RestServer.hpp>
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
    globalExecutionPlan = GlobalExecutionPlan::create();
    queryCatalog = std::make_shared<QueryCatalog>();
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
    globalExecutionPlan = GlobalExecutionPlan::create();
    queryCatalog = std::make_shared<QueryCatalog>();
    coordinatorEngine = std::make_shared<CoordinatorEngine>(streamCatalog, topologyManager);
}

NesCoordinator::~NesCoordinator() {
    NES_DEBUG("NesCoordinator::~NesCoordinator()");
    stopCoordinator(true);
    NES_DEBUG("NesCoordinator::~NesCoordinator() map cleared");
    streamCatalog->reset();
    queryCatalog->clearQueries();
    topologyManager->resetNESTopologyPlan();
}

/**
 * @brief this method starts the rest server and then blocks
 */
void startRestServer(std::shared_ptr<RestServer> restServer, std::string restHost, uint16_t restPort,
                     NesCoordinatorPtr coordinator, std::promise<bool>& prom) {

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
                                                                          queryCatalog, streamCatalog, topologyManager, globalExecutionPlan);

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

}// namespace NES
