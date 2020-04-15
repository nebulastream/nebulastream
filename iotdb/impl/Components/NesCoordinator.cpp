#include <Components/NesCoordinator.hpp>
#include <Util/Logger.hpp>
#include <caf/io/all.hpp>
#include <thread>

namespace NES {

NesCoordinator::NesCoordinator() {
    restPort = 8081;
    restHost = "localhost";
    actorPort = 0;
    serverIp = "localhost";
}

infer_handle_from_class_t<CoordinatorActor> NesCoordinator::getActorHandle() {
    return coordinatorActorHandle;
}

void starter(infer_handle_from_class_t<CoordinatorActor> handle,
             actor_system* actorSystem, RestServer* restServer,
             std::string restHost, uint16_t restPort, uint16_t* port) {
    restServer = new RestServer(restHost, restPort, handle);
    restServer->start();  //this call is blocking
    NES_DEBUG("NesCoordinator: starter thread terminates")
}

bool NesCoordinator::stopCoordinator() {
    NES_DEBUG("NesCoordinator: stop")
    bool success = crd->shutdown();
    if(!success)
    {
        NES_ERROR("NesCoordinator::stopCoordinator: error while stopping coordinator")
        throw new Exception("Error while stopping NesCoordinator");
    }
    anon_send(coordinatorActorHandle, exit_reason::user_shutdown);
    NES_DEBUG("NesCoordinator::stopCoordinator success=" << success)

    NES_DEBUG("NesCoordinator: stopping rest server")
    bool retStopRest = restServer->stop();
    if(!retStopRest)
    {
        NES_ERROR("NesCoordinator::stopCoordinator: error while stopping restServer")
        throw new Exception("Error while stopping NesCoordinator");
    }
    NES_DEBUG("NesCoordinator: rest server stopped")

    restThread.join();
    NES_DEBUG("NesCoordinator: thread joined")
    return true;
}

void NesCoordinator::startCoordinator(bool blocking, uint16_t port) {
    actorPort = port;
    startCoordinator(blocking);
}

uint16_t NesCoordinator::startCoordinator(bool blocking) {
    NES_DEBUG("NesCoordinator start")

    actorCoordinatorConfig.load<io::middleman>();
    if (serverIp != "localhost") {
        NES_DEBUG("NesCoordinator: set server ip=" << serverIp)
        actorCoordinatorConfig.ip = serverIp;
    }
    actorCoordinatorConfig.printCfg();
    actorSystem = new actor_system{actorCoordinatorConfig};

    coordinatorActorHandle = actorSystem->spawn<CoordinatorActor>(serverIp);

    abstract_actor* abstractActor = caf::actor_cast<abstract_actor*>(coordinatorActorHandle);
    crd = dynamic_cast<CoordinatorActor*>(abstractActor);
    NES_DEBUG("NesCoordinator: actor handle created")

    io::unpublish(coordinatorActorHandle, actorPort);

    auto expected_port = io::publish(coordinatorActorHandle, actorPort, nullptr,
                                     true);
    if (!expected_port) {
        NES_ERROR("NesCoordinator: publish failed: " << expected_port)
        throw new Exception("NesCoordinator start failed");
    }
    NES_DEBUG(
        "NesCoordinator: coordinator successfully published at port " << expected_port)

    actorPort = *expected_port;
    std::thread th0(starter, coordinatorActorHandle, actorSystem, restServer,
                    restHost, restPort, &actorPort);
    restThread = std::move(th0);

    if (blocking) {
        NES_DEBUG("NesCoordinator started, join now and waiting for work")
        restThread.join();
    } else {
        NES_DEBUG(
            "NesCoordinator started, return without blocking on port " << actorPort)
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

}
