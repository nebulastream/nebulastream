#include <Components/NesWorker.hpp>
#include <Util/Logger.hpp>

#include <caf/actor_cast.hpp>
#include <signal.h>//  our new library

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
    NES_DEBUG("NesWorker: constructed");

    // Register signals
    signal(SIGINT, termFunc);
}

NesWorker::NesWorker(NESNodeType type) {
    connected = false;
    withRegisterStream = false;
    withParent = false;
    coordinatorPort = 0;
    this->type = type;
    NES_DEBUG("NesWorker: constructed");

    // Register signals
    signal(SIGINT, termFunc);
}
NesWorker::~NesWorker() {
    NES_DEBUG("NesWorker::~NesWorker()");
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

size_t NesWorker::getRandomPort(size_t base) {
    //TODO will be removed once the new network stack is in place
    return base - 12 + time(0) * 321 * rand() % 10000;
}

bool NesWorker::start(bool blocking, bool withConnect, uint16_t port, std::string serverIp) {
    NES_DEBUG("NesWorker: start with blocking " << blocking << " serverIp=" << serverIp << " port=" << port);

    NES_DEBUG("NesWorker::start: start NodeEngine");
    nodeEngine = std::make_shared<NodeEngine>();
    bool success = nodeEngine->start();
    if (!success) {
        NES_ERROR("NesWorker: node engine could not be started");
        throw Exception("NesWorker error while starting node engine");
    } else {
        NES_DEBUG("NesWorker: Node engine started successfully serverIp=" << serverIp);
    }
//
//    workerCfg.load<io::middleman>();
//    workerCfg.host = serverIp;
//    workerCfg.printCfg();
//
//    coordinatorPort = port;
//    actorSystem = new actor_system{workerCfg};
//
//    workerCfg.publish_port = getRandomPort(workerCfg.publish_port);
//    workerCfg.receive_port = getRandomPort(workerCfg.receive_port);
//
//    workerHandle = actorSystem->spawn<NES::WorkerActor>(workerCfg.ip,
//                                                        workerCfg.publish_port,
//                                                        workerCfg.receive_port,
//                                                        type,
//                                                        nodeEngine);
//
//    abstract_actor* abstractActor = caf::actor_cast<abstract_actor*>(workerHandle);
//    workerActor = dynamic_cast<WorkerActor*>(abstractActor);
//
//    auto expectedPort = io::publish(workerHandle, workerCfg.receive_port, nullptr,
//                                    true);
//    NES_DEBUG("spawn handle with id=" << workerHandle.id() << " port=" << expectedPort);

    if (withConnect) {
        NES_DEBUG("NesWorker: start with connect");
        connect();
    }
    if (withRegisterStream) {
        NES_DEBUG("NesWorker: start with register stream");
        registerPhysicalStream(conf);
    }
    if (withParent) {
        NES_DEBUG("NesWorker: add parent id=" << parentId);
        addParent(parentId);
    }

    stopped = false;
    if (blocking) {
        NES_DEBUG("NesWorker: started, join now and waiting for work");
        while (true) {
            if (flag) {
                NES_DEBUG("NesWorker: caught signal terminating worker");
                flag = 0;
                break;
            } else {
                cout << "NesWorker wait" << endl;
                sleep(5);
            }
        }
        NES_DEBUG("NesWorker: joined, return");
        return true;
    } else {
        NES_DEBUG("NesWorker: started, return without blocking");
        return true;
    }
}

bool NesWorker::stop(bool force) {
    NES_DEBUG("NesWorker: stop");
    if (!stopped) {
        bool successShutdownActor = workerActor->shutdown(force);
        if (successShutdownActor) {
            anon_send(workerHandle, exit_reason::user_shutdown);
        }

        bool successShutdownNodeEngine = nodeEngine->stop(force);
        if (!successShutdownNodeEngine) {
            NES_ERROR("NesWorker::stop node engine stop not successful");
            throw Exception("NesWorker::stop  error while stopping node engine");
        } else {
            NES_DEBUG("NesWorker::stop : Node engine stopped successfully");
        }

        bool success = successShutdownActor && successShutdownNodeEngine;
        NES_DEBUG("NesWorker::stop success=" << success);
        return successShutdownActor && successShutdownNodeEngine;
    } else {
        NES_WARNING("NesWorker::stop: already stopped");
        return true;
    }
}

bool NesWorker::connect() {
    coordinatorRpcClient = std::make_shared<CoordinatorRPCClient>(
        workerCfg.host,
        workerCfg.receive_port,
        workerCfg.publish_port,
        2,
        this->type,
        nodeEngine->getNodePropertiesAsString()
        );

    bool successPRCConnect = coordinatorRpcClient->connect();
    if(successPRCConnect)
    {
        NES_DEBUG("NesWorker::connect rpc connect success");
    } else
    {
        NES_DEBUG("NesWorker::connect rpc connect failed");
        return false;
    }

    bool successPRCRegister = coordinatorRpcClient->registerNode();
    if(successPRCRegister)
    {
        NES_DEBUG("NesWorker::registerNode rpc register success");
        return true;
    } else
    {
        NES_DEBUG("NesWorker::registerNode rpc register failed");
        return false;
    }

//
//    bool success = workerActor->connecting(workerCfg.host, coordinatorPort);
//    NES_DEBUG("NesWorker::connect success=" << success);
//    return success;
}

bool NesWorker::disconnect() {
    bool success = workerActor->disconnecting();
    NES_DEBUG("NesWorker::disconnect success=" << success);
    return success;
}

bool NesWorker::registerLogicalStream(std::string name, std::string path) {
    bool success = workerActor->registerLogicalStream(name, path);
    NES_DEBUG("NesWorker::disconnect success=" << success);
    return success;
}

bool NesWorker::deregisterLogicalStream(std::string logicalName) {
    bool success = workerActor->removeLogicalStream(logicalName);
    NES_DEBUG("NesWorker::deregisterLogicalStream success=" << success);
    return success;
}

bool NesWorker::deregisterPhysicalStream(std::string logicalName, std::string physicalName) {
    bool success = workerActor->removePhysicalStream(logicalName, physicalName);
    NES_DEBUG("NesWorker::deregisterPhysicalStream success=" << success);
    return success;
}

bool NesWorker::registerPhysicalStream(PhysicalStreamConfig conf) {
    bool success = workerActor->registerPhysicalStream(conf);
    NES_DEBUG("NesWorker::registerPhysicalStream success=" << success);
    return success;
}

bool NesWorker::addParent(std::string parentId) {
    bool success = workerActor->addNewParentToSensorNode(parentId);
    NES_DEBUG("NesWorker::addNewLink(parent only) success=" << success);
    return success;
}

bool NesWorker::removeParent(std::string parentId) {
    bool success = workerActor->removeParentFromSensorNode(parentId);
    NES_DEBUG("NesWorker::removeLink(parent only) success=" << success);
    return success;
}

QueryStatisticsPtr NesWorker::getQueryStatistics(std::string queryId) {
    return nodeEngine->getQueryStatistics(queryId);
}

}// namespace NES
