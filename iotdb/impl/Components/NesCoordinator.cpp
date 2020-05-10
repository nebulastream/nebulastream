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
    restPort = 8081;
    restHost = "localhost";
    actorPort = 0;
    serverIp = "localhost";
    stopped = false;
}

NesCoordinator::~NesCoordinator() {
    NES_DEBUG("NesCoordinator::~NesCoordinator()");
    stopCoordinator(true);
}

infer_handle_from_class_t<CoordinatorActor> NesCoordinator::getActorHandle() {
    return coordinatorActorHandle;
}

void startRestServer(infer_handle_from_class_t<CoordinatorActor> handle,
                     actor_system* actorSystem, RestServer* restServer,
                     std::string restHost, uint16_t restPort, uint16_t* port) {
    restServer = new RestServer(restHost, restPort, handle);
    restServer->start();//this call is blocking
    NES_DEBUG("NesCoordinator: startRestServer thread terminates");
}

void startCoordinatorRPCServer(std::string ip, std::string port, std::unique_ptr<Server>& rpcServer) {
    std::string address = ip + ":" + port;
    NES_DEBUG("startCoordinatorRPCServer for address=" << address);
    CoordinatorRPCServer service;

    ServerBuilder builder;
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    rpcServer = builder.BuildAndStart();
    std::cout << "startCoordinatorRPCServer: Server listening on " << address << std::endl;
    rpcServer->Wait();
}

string NesCoordinator::deployQuery(const string& queryString, const string& strategy) {
    NES_DEBUG("NesCoordinator:registerAndDeployQuery queryString=" << queryString << " strategy=" << strategy);
    return crdActor->registerAndDeployQuery(0, queryString, strategy);
}

bool NesCoordinator::undeployQuery(const string& queryId) {
    NES_DEBUG("NesCoordinator:unddeployQuery queryId=" << queryId);
    return crdActor->deregisterAndUndeployQuery(0, queryId);
}

bool NesCoordinator::stopCoordinator(bool force) {
    NES_DEBUG("NesCoordinator: stopCoordinator force=" << force);
    if (!stopped) {
        NES_DEBUG("NesCoordinator: stopping worker actor");
        bool successStopWorkerActor = wrkActor->shutdown(force);
        if (!successStopWorkerActor) {
            NES_ERROR("NesCoordinator::stopCoordinator: error while stopping worker actor");
            throw Exception("Error while stopping NesCoordinator");
        } else {
            anon_send(workerActorHandle, exit_reason::user_shutdown);
        }
        bool successShutdownNodeEngine = nodeEngine->stop(force);
        if (!successShutdownNodeEngine) {
            NES_ERROR("NesCoordinator::stop node engine stop not successful");
            throw Exception("NesCoordinator::stop error while stopping node engine");
        } else {
            NES_DEBUG("NesCoordinator::stop Node engine stopped successfully");
        }

        bool successWorker = successStopWorkerActor && successShutdownNodeEngine;
        NES_DEBUG("NesCoordinator::stopCoordinator worker  success=" << successWorker);

        NES_DEBUG("NesCoordinator: stopping coordinator actor");
        bool successStopCoordinatorActor = crdActor->shutdown();
        if (!successStopCoordinatorActor) {
            NES_ERROR("NesCoordinator::stopCoordinator: error while stopping coordinator actor");
            throw Exception("Error while stopping NesCoordinator");
        }
        anon_send(coordinatorActorHandle, exit_reason::user_shutdown);
        NES_DEBUG("NesCoordinator::stopCoordinator coordinator actor success=" << successStopCoordinatorActor);

        NES_DEBUG("NesCoordinator: stopping rest server");
        bool successStopRest = restServer->stop();
        if (!successStopRest) {
            NES_ERROR("NesCoordinator::stopCoordinator: error while stopping restServer");
            throw Exception("Error while stopping NesCoordinator");
        }
        NES_DEBUG("NesCoordinator: rest server stopped " << successStopRest);

        if (restThread.joinable()) {
            restThread.join();
        }

        NES_DEBUG("NesCoordinator: thread joined");
        stopped = true;
        return true;
    } else {
        NES_DEBUG("NesCoordinator: already stopped");
        return true;
    }
}

void NesCoordinator::startCoordinator(bool blocking, uint16_t port) {
    actorPort = port;
    startCoordinator(blocking);
}

size_t NesCoordinator::getRandomPort(size_t base) {
    //TODO will be removed once the new network stack is in place
    return base - 12 + time(0)*321*rand()%10000;
}

uint16_t NesCoordinator::startCoordinator(bool blocking) {
    NES_DEBUG("NesCoordinator start");

    NES_DEBUG("NesCoordinator::start: start NodeEngine");
    nodeEngine = std::make_shared<NodeEngine>();
    bool successNodeEngine = nodeEngine->start();
    if (!successNodeEngine) {
        NES_ERROR("NesCoordinator: node engine could not be started");
        throw Exception("NesCoordinator error while starting node engine");
    } else {
        NES_DEBUG("NesCoordinator: Node engine started successfully");
    }

    NES_DEBUG("NesCoordinator starting coordinator actor");
    coordinatorCfg.load<io::middleman>();
    if (serverIp != "localhost") {
        NES_DEBUG("NesCoordinator: set server ip=" << serverIp);
        coordinatorCfg.ip = serverIp;
    }
    coordinatorCfg.printCfg();
    actorSystemCoordinator = new actor_system{coordinatorCfg};
    coordinatorActorHandle = actorSystemCoordinator->spawn<CoordinatorActor>(serverIp);

    abstract_actor* abstractActorCoordinator = caf::actor_cast<abstract_actor*>(coordinatorActorHandle);
    crdActor = dynamic_cast<CoordinatorActor*>(abstractActorCoordinator);
    NES_DEBUG("NesCoordinator: coordinator actor created");

    io::unpublish(coordinatorActorHandle, actorPort);
    auto expectedPortCoordinator = io::publish(coordinatorActorHandle, actorPort, nullptr,
                                               true);
    if (!expectedPortCoordinator) {
        NES_ERROR("NesCoordinator: publish expectedPortCoordinator failed: " << expectedPortCoordinator);
        throw Exception("NesCoordinator start failed");
    }
    NES_DEBUG("NesCoordinator: coordinator handle with id"
                  << coordinatorActorHandle->id() << " successfully published at port " << expectedPortCoordinator);
    actorPort = *expectedPortCoordinator;

    NES_DEBUG("NesCoordinator starting worker actor");
    workerCfg.load<io::middleman>();
    workerCfg.host = "localhost";

    workerCfg.publish_port = getRandomPort(workerCfg.publish_port);
    workerCfg.receive_port = getRandomPort(workerCfg.receive_port);

    workerCfg.printCfg();

    actorSystemWorker = new actor_system{workerCfg};

    workerActorHandle = actorSystemWorker->spawn<NES::WorkerActor>(workerCfg.ip,
                                                                   workerCfg.publish_port,
                                                                   workerCfg.receive_port,
                                                                   NESNodeType::Worker,
                                                                   nodeEngine);

    abstract_actor* abstractActorWorker = caf::actor_cast<abstract_actor*>(workerActorHandle);
    wrkActor = dynamic_cast<WorkerActor*>(abstractActorWorker);

    io::unpublish(workerActorHandle, workerCfg.receive_port);
    auto expectedPortWorker = io::publish(workerActorHandle, workerCfg.receive_port, nullptr,
                                          true);
    if (!expectedPortWorker) {
        NES_ERROR("NesCoordinator: publish expectedPortWorker failed: " << expectedPortWorker);
        throw Exception("NesCoordinator start failed");
    }
    NES_DEBUG("NesCoordinator: worker handle with id"
                  << workerActorHandle->id() << " successfully published at port " << expectedPortWorker);


    startCoordinatorRPCServer(workerCfg.ip, ::to_string(workerCfg.receive_port + 123), rpcServer);

    NES_DEBUG("NesCoordinator: connection worker to coordinator");
    bool successNodeConnect = wrkActor->connecting(workerCfg.host, actorPort);
    if (successNodeConnect) {
        NES_DEBUG("NesCoordinator and NesWorker successfully connected");
    } else {
        NES_ERROR("NesCoordinator: failed to connect coordinator and worker");
        throw Exception("NesCoordinator and NesWorker connection failed");
    }

    NES_DEBUG("NesCoordinator starting rest server");
    std::thread th0(startRestServer, coordinatorActorHandle, actorSystemCoordinator, restServer,
                    restHost, restPort, &actorPort);
    restThread = std::move(th0);

    stopped = false;
    if (blocking) {
        NES_DEBUG("NesCoordinator started, join now and waiting for work");
        restThread.join();
    } else {
        NES_DEBUG(
            "NesCoordinator started, return without blocking on port " << actorPort);
        return actorPort;
    }
}

void NesCoordinator::setRestConfiguration(std::string host, uint16_t port) {
    this->restHost = host;
    this->restPort = port;
}

void NesCoordinator::setServerIp(std::string serverIp) {
    this->serverIp = serverIp;
}

QueryStatisticsPtr NesCoordinator::getQueryStatistics(std::string queryId) {
    return nodeEngine->getQueryStatistics(queryId);
}

}// namespace NES
