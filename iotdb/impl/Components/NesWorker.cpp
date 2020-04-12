#include <Components/NesWorker.hpp>
#include <Util/Logger.hpp>
#include "Actors/WorkerActor.hpp"

#include <signal.h> //  our new library
#include <caf/actor_cast.hpp>

volatile sig_atomic_t flag = 0;

void termFunc(int sig) {
    flag = 1;
}

namespace NES {

NesWorker::NesWorker() {
    connected = false;
    withRegisterStream = false;
    withParent = false;
    coordinatorPort = 0;
    NES_DEBUG("NesWorker: constructed")

    // Register signals
    signal(SIGINT, termFunc);
}

bool NesWorker::setWitRegister(PhysicalStreamConfig pConf) {
    withRegisterStream = true;
    conf = pConf;
    return true;
}

bool NesWorker::setWithParent(std::string parentId) {
    withParent = true;
    this->parentId = parentId;
    return true;
}

bool NesWorker::start(bool blocking, bool withConnect, uint16_t port, std::string serverIp) {
    NES_DEBUG("NesWorker: start with blocking " << blocking << " serverIp=" << serverIp << " port=" << port)

    workerCfg.load<io::middleman>();
    workerCfg.printCfg();
    workerCfg.host = serverIp;
    coordinatorPort = port;
    actorSystem = new actor_system{workerCfg};

    workerHandle = actorSystem->spawn<NES::WorkerActor>(workerCfg.ip,
                                                        workerCfg.publish_port,
                                                        workerCfg.receive_port);

    abstract_actor* abstractActor = caf::actor_cast<abstract_actor*>(workerHandle);
    wrk = dynamic_cast<WorkerActor*>(abstractActor);

    auto expectedPort = io::publish(workerHandle, workerCfg.receive_port, nullptr,
                                    true);
    cout << "spawn handle with id=" << workerHandle.id() << " port=" << expectedPort << endl;

    sleep(1);
    if (withConnect) {
        NES_DEBUG("NesWorker: start with connect")
        connect();
    }
    if (withRegisterStream) {
        NES_DEBUG("NesWorker: start with register stream")
        registerPhysicalStream(conf);
    }
    if (withParent) {
        NES_DEBUG("NesWorker: add parent id=" << parentId)
        addParent(parentId);
    }

    if (blocking) {
        NES_DEBUG("NesWorker: started, join now and waiting for work")
        while (true) {
            if (flag) {
                NES_DEBUG("NesWorker: caught signal terminating worker")
                flag = 0;
                break;
            } else {
                cout << "NesWorker wait" << endl;
                sleep(5);
            }
        }
        NES_DEBUG("NesWorker: joined, return")
        return true;
    } else {
        NES_DEBUG("NesWorker: started, return without blocking")
        return true;
    }
}

bool NesWorker::stop() {
    NES_DEBUG("NesWorker: stop")
    bool success = wrk->shutdown();
    anon_send(workerHandle, exit_reason::user_shutdown);
    NES_DEBUG("NesWorker::connect success=" << success)
    return success;
}

bool NesWorker::connect() {
    bool success = wrk->connecting(workerCfg.host, coordinatorPort);
    NES_DEBUG("NesWorker::connect success=" << success)
    return success;
}

bool NesWorker::disconnect() {
    bool success = wrk->disconnecting();
    NES_DEBUG("NesWorker::disconnect success=" << success)
    return success;
}

bool NesWorker::registerLogicalStream(std::string name, std::string path) {
    bool success = wrk->registerLogicalStream(name, path);
    NES_DEBUG("NesWorker::disconnect success=" << success)
    return success;
}

bool NesWorker::deregisterLogicalStream(std::string logicalName) {
    bool success = wrk->removeLogicalStream(logicalName);
    NES_DEBUG("NesWorker::deregisterLogicalStream success=" << success)
    return success;
}

bool NesWorker::deregisterPhysicalStream(std::string logicalName, std::string physicalName) {
    bool success = wrk->removePhysicalStream(logicalName, physicalName);
    NES_DEBUG("NesWorker::deregisterPhysicalStream success=" << success)
    return success;
}

bool NesWorker::registerPhysicalStream(PhysicalStreamConfig conf) {
    bool success = wrk->registerPhysicalStream(conf);
    NES_DEBUG("NesWorker::registerPhysicalStream success=" << success)
    return success;
}

std::string NesWorker::getIdFromServer() {
    string id = wrk->getIdFromServer();
    NES_DEBUG("NesWorker::getIdFromServer id=" << id)
    return id;
}

bool NesWorker::addNewLink(std::string childId, std::string parentId) {
    bool success = wrk->addNewParentToSensorNode(childId, parentId);
    NES_DEBUG("NesWorker::addNewLink(child and parent) success=" << success)
    return success;
}

bool NesWorker::addParent(std::string parentId) {
    bool success = wrk->addNewParentToSensorNode(getIdFromServer(), parentId);
    NES_DEBUG("NesWorker::addNewLink(parent only) success=" << success)
    return success;
}

bool NesWorker::removeParent(std::string parentId) {
    bool success = wrk->removeParentFromSensorNode(getIdFromServer(), parentId);
    NES_DEBUG("NesWorker::removeLink(parent only) success=" << success)
    return success;
}

bool NesWorker::removeLink(std::string childId, std::string parentId) {
    bool success = wrk->removeParentFromSensorNode(childId, parentId);
    NES_DEBUG("NesWorker::removeLink(child and parent) success=" << success)
    return success;
}

}
