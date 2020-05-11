#include <Components/NesCoordinator.hpp>
#include <Topology/NESTopologyEntry.hpp>
#include <Util/Logger.hpp>
#include <caf/actor_cast.hpp>
#include <caf/io/all.hpp>
#include <thread>

//GRPC Includes
#include <GRPC/CoordinatorRPCServer.hpp>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
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
}

NesCoordinator::NesCoordinator(string serverIp, uint16_t restPort, uint16_t rpcPort)
    : serverIp(serverIp),
      restPort(restPort),
      rpcPort(rpcPort) {
    NES_DEBUG("NesCoordinator() serverIp=" << serverIp << " restPort=" << restPort << " rpcPort=" << rpcPort);
    stopped = false;
}

NesCoordinator::~NesCoordinator() {
    NES_DEBUG("NesCoordinator::~NesCoordinator()");
    stopCoordinator(true);
}

size_t NesCoordinator::getRandomPort(size_t base) {
    //TODO will be removed once the new network stack is in place
    return base - 12 + time(0)*321*rand()%10000;
}

void startRestServer(RestServer* restServer,
                     std::string restHost, uint16_t restPort) {
    infer_handle_from_class_t<CoordinatorActor> coordinatorActorHandle;
    restServer = new RestServer(restHost, restPort, coordinatorActorHandle);
    restServer->start();//this call is blocking
    NES_DEBUG("NesCoordinator: startRestServer thread terminates");
}

void startCoordinatorRPCServer(std::shared_ptr<Server>& rpcServer) {

    NES_DEBUG("startCoordinatorRPCServer: Server listening");
    rpcServer->Wait();
    NES_DEBUG("startCoordinatorRPCServer: end listening");
}

bool NesCoordinator::stopCoordinator(bool force) {
    NES_DEBUG("NesCoordinator: stopCoordinator force=" << force);
    if (!stopped) {
        bool successShutdownWorker = wrk->stop(force);
        if (!successShutdownWorker) {
            NES_ERROR("NesCoordinator::stop node engine stop not successful");
            throw Exception("NesCoordinator::stop error while stopping node engine");
        } else {
            NES_DEBUG("NesCoordinator::stop Node engine stopped successfully");
        }

        NES_DEBUG("NesCoordinator: stopping rest server");
        bool successStopRest = restServer->stop();
        if (!successStopRest) {
            NES_ERROR("NesCoordinator::stopCoordinator: error while stopping restServer");
            throw Exception("Error while stopping NesCoordinator");
        }
        NES_DEBUG("NesCoordinator: rest server stopped " << successStopRest);
        if (restThread.joinable()) {
            NES_DEBUG("NesCoordinator: join restThread");
            restThread.join();
        }

        NES_DEBUG("NesCoordinator: stopping rpc server");
        rpcServer->Shutdown();
        if (rpcThread.joinable()) {
            NES_DEBUG("NesCoordinator: join rpcThread");
            rpcThread.join();
        }

        stopped = true;
        return true;
    } else {
        NES_DEBUG("NesCoordinator: already stopped");
        return true;
    }
}

size_t NesCoordinator::startCoordinator(bool blocking) {
    NES_DEBUG("NesCoordinator start");

    std::string address = serverIp + ":" + ::to_string(rpcPort);
    NES_DEBUG("startCoordinatorRPCServer for address=" << address);
    CoordinatorRPCServer service;

    ServerBuilder builder;
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    rpcServer = std::move(server);

    std::thread threadRPC([&]() {
      startCoordinatorRPCServer(rpcServer);
    });

    rpcThread = std::move(threadRPC);

    NES_DEBUG("NesCoordinator::startCoordinator: start nes worker");
    wrk = std::make_shared<NesWorker>(serverIp,
                                      std::to_string(rpcPort),
                                      serverIp,
                                      std::to_string(rpcPort + 1),
                                      NESNodeType::Worker);
    wrk->start(/**blocking*/false, /**withConnect*/true);

    NES_DEBUG("NesCoordinator starting rest server");
    std::thread th0(startRestServer, restServer,
                    serverIp, restPort);
    restThread = std::move(th0);

    stopped = false;
    if (blocking) {
        NES_DEBUG("NesCoordinator started, join now and waiting for work");
        restThread.join();
        NES_DEBUG("NesCoordinator Required stopping");
    } else {
        NES_DEBUG(
            "NesCoordinator started, return without blocking on port " << rpcPort);
        return rpcPort;
    }

    NES_DEBUG("NesCoordinator startCoordinator succeed");
}

void NesCoordinator::setServerIp(std::string serverIp) {
    this->serverIp = serverIp;
}

QueryStatisticsPtr NesCoordinator::getQueryStatistics(std::string queryId) {
    return wrk->getNodeEngine()->getQueryStatistics(queryId);
}

string NesCoordinator::deployQuery(const string queryString, const string strategy) {
    NES_DEBUG("NesCoordinator:deployQuery queryString=" << queryString << " strategy=" << strategy);
//    return crdActor->registerAndDeployQuery(0, queryString, strategy);
    return "";
}

bool NesCoordinator::undeployQuery(const string queryId) {
    NES_DEBUG("NesCoordinator:undeployQuery queryId=" << queryId);
//    return crdActor->deregisterAndUndeployQuery(0, queryId);
    return true;
}

string NesCoordinator::registerQuery(const string queryString, const string strategy) {
    NES_DEBUG("NesCoordinator:registerQuery queryString=" << queryString << " strategy=" << strategy);
//    return crdActor->registerAndDeployQuery(0, queryString, strategy);
    return "";
}

bool NesCoordinator::unregisterQuery(const string queryId) {
    NES_DEBUG("NesCoordinator:unregisterQuery queryId=" << queryId);
//    return crdActor->deregisterAndUndeployQuery(0, queryId);
    return true;
}

bool NesCoordinator::startQuery(const string queryId) {
    NES_DEBUG("NesCoordinator:startQuery queryId=" << queryId);
//    return crdActor->deregisterAndUndeployQuery(0, queryId);
    return true;
}

bool NesCoordinator::stopQuery(const string queryId) {
    NES_DEBUG("NesCoordinator:stopQuery queryId=" << queryId);
    return true;
//    return crdActor->stopQuery(0, queryId);
}

}// namespace NES
