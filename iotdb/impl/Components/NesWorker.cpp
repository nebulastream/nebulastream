#include <Components/NesWorker.hpp>
#include <Util/Logger.hpp>
#include "Actors/WorkerActor.hpp"
#include <Topology/NESTopologyEntry.hpp>

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
    this->type = NESNodeType::Sensor;
    NES_DEBUG("NesWorker: constructed")

    // Register signals
    signal(SIGINT, termFunc);
}

NesWorker::NesWorker(NESNodeType type) {
    connected = false;
    withRegisterStream = false;
    withParent = false;
    coordinatorPort = 0;
    this->type = type;
    NES_DEBUG("NesWorker: constructed")
    stopped = false;
    // Register signals
    signal(SIGINT, termFunc);
}
NesWorker::~NesWorker() {
    NES_DEBUG("NesWorker::~NesWorker()")
    stop(true);
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
    workerCfg.host = serverIp;
    workerCfg.printCfg();

    coordinatorPort = port;
    actorSystem = new actor_system{workerCfg};

    size_t ts = time(0);
    workerCfg.publish_port = workerCfg.publish_port + ts%10000;
    workerCfg.receive_port = workerCfg.receive_port + ts%10000;
    workerHandle = actorSystem->spawn<NES::WorkerActor>(workerCfg.ip,
                                                        workerCfg.publish_port,
                                                        workerCfg.receive_port,
                                                        type);

    abstract_actor* abstractActor = caf::actor_cast<abstract_actor*>(workerHandle);
    wrk = dynamic_cast<WorkerActor*>(abstractActor);

    auto expectedPort = io::publish(workerHandle, workerCfg.receive_port, nullptr,
                                    true);
    NES_DEBUG("spawn handle with id=" << workerHandle.id() << " port=" << expectedPort)

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

    stopped = false;
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

bool NesWorker::stop(bool force) {
    NES_DEBUG("NesWorker: stop")
    if (!stopped) {

        bool success = wrk->shutdown(force);
        if (success) {
            anon_send(workerHandle, exit_reason::user_shutdown);
        }
        NES_DEBUG("NesWorker::stop success=" << success)
        return success;
    }
    else
    {
        NES_WARNING("NesWorker::stop: already stopped")
        return true;
    }
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

bool NesWorker::addParent(std::string parentId) {
    bool success = wrk->addNewParentToSensorNode(parentId);
    NES_DEBUG("NesWorker::addNewLink(parent only) success=" << success)
    return success;
}

bool NesWorker::removeParent(std::string parentId) {
    bool success = wrk->removeParentFromSensorNode(parentId);
    NES_DEBUG("NesWorker::removeLink(parent only) success=" << success)
    return success;
}

size_t NesWorker::getNumberOfProcessedBuffer(std::string queryId) {
    return wrk->getNumberOfProcessedBuffer(queryId);
}

size_t NesWorker::getNumberOfProcessedTasks(std::string queryId) {
    return wrk->getNumberOfProcessedTasks(queryId);
}


}
