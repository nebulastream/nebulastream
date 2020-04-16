#include <Components/NesCoordinator.hpp>
#include <Util/Logger.hpp>
#include <caf/io/all.hpp>
#include <thread>
#include <caf/actor_cast.hpp>
#include <Topology/NESTopologyEntry.hpp>

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

void startRestServer(infer_handle_from_class_t<CoordinatorActor> handle,
                     actor_system* actorSystem, RestServer* restServer,
                     std::string restHost, uint16_t restPort, uint16_t* port) {
    restServer = new RestServer(restHost, restPort, handle);
    restServer->start();  //this call is blocking
    NES_DEBUG("NesCoordinator: startRestServer thread terminates")
}

bool NesCoordinator::stopCoordinator() {
    NES_DEBUG("NesCoordinator: stop")

    NES_DEBUG("NesCoordinator: stopping worker actor")
    bool success1 = wrkPtr->shutdown();
    if (!success1) {
        NES_ERROR("NesCoordinator::stopCoordinator: error while stopping worker actor")
        throw new Exception("Error while stopping NesCoordinator");
    }
    anon_send(workerActorHandle, exit_reason::user_shutdown);
    NES_DEBUG("NesCoordinator::stopCoordinator worker actor success=" << success1)

    NES_DEBUG("NesCoordinator: stopping coordinator actor")
    bool success2 = crdPtr->shutdown();
    if (!success2) {
        NES_ERROR("NesCoordinator::stopCoordinator: error while stopping coordinator actor")
        throw new Exception("Error while stopping NesCoordinator");
    }
    anon_send(coordinatorActorHandle, exit_reason::user_shutdown);
    NES_DEBUG("NesCoordinator::stopCoordinator coordinator actor success=" << success2)

    NES_DEBUG("NesCoordinator: stopping rest server")
    bool retStopRest = restServer->stop();
    if (!retStopRest) {
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

    NES_DEBUG("NesCoordinator starting coordinator actor")
    coordinatorCfg.load<io::middleman>();
    if (serverIp != "localhost") {
        NES_DEBUG("NesCoordinator: set server ip=" << serverIp)
        coordinatorCfg.ip = serverIp;
    }
    coordinatorCfg.printCfg();
    actorSystemCoordinator = new actor_system{coordinatorCfg};
    coordinatorActorHandle = actorSystemCoordinator->spawn<CoordinatorActor>(serverIp);

    abstract_actor* abstractActorCoordinator = caf::actor_cast<abstract_actor*>(coordinatorActorHandle);
    crdPtr = dynamic_cast<CoordinatorActor*>(abstractActorCoordinator);
    NES_DEBUG("NesCoordinator: coordinator actor created")

    io::unpublish(coordinatorActorHandle, actorPort);
    auto expectedPortCoordinator = io::publish(coordinatorActorHandle, actorPort, nullptr,
                                               true);
    if (!expectedPortCoordinator) {
        NES_ERROR("NesCoordinator: publish expectedPortCoordinator failed: " << expectedPortCoordinator)
        throw new Exception("NesCoordinator start failed");
    }
    NES_DEBUG("NesCoordinator: coordinator handle with id  "
                  << coordinatorActorHandle->id() << " successfully published at port " << expectedPortCoordinator)
    actorPort = *expectedPortCoordinator;

    NES_DEBUG("NesCoordinator starting worker actor")
    workerCfg.load<io::middleman>();
    workerCfg.host = "localhost";
    workerCfg.publish_port = workerCfg.publish_port -1;
    workerCfg.receive_port = workerCfg.receive_port -1;
    workerCfg.printCfg();

    actorSystemWorker = new actor_system{workerCfg};

    workerActorHandle = actorSystemWorker->spawn<NES::WorkerActor>(workerCfg.ip,
                                                                   workerCfg.publish_port,
                                                                   workerCfg.receive_port,
                                                                   NESNodeType::Worker);

    abstract_actor* abstractActorWorker = caf::actor_cast<abstract_actor*>(workerActorHandle);
    wrkPtr = dynamic_cast<WorkerActor*>(abstractActorWorker);

    auto expectedPortWorker = io::publish(workerActorHandle, workerCfg.receive_port, nullptr,
                                          true);
    if (!expectedPortWorker) {
        NES_ERROR("NesCoordinator: publish expectedPortWorker failed: " << expectedPortWorker)
        throw new Exception("NesCoordinator start failed");
    }
    NES_DEBUG("NesCoordinator: worker handle with id  "
                  << workerActorHandle->id() << " successfully published at port " << expectedPortWorker)

    //TODO connect
    NES_DEBUG("NesCoordinator: connection worker to coordinator")
    bool success = wrkPtr->connecting(workerCfg.host, actorPort);
    if (success) {
        NES_DEBUG("NesCoordinator and NesWorker successfully connected")
    } else {
        NES_ERROR("NesCoordinator: failed to connect coordinator and worker")
        throw new Exception("NesCoordinator and NesWorker connection failed");
    }

    NES_DEBUG("NesCoordinator starting rest server")
    std::thread th0(startRestServer, coordinatorActorHandle, actorSystemCoordinator, restServer,
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
