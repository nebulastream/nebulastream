#include <Components/NesWorker.hpp>
#include <Util/Logger.hpp>

#include <caf/actor_cast.hpp>
#include <signal.h>

#include <GRPC/WorkerRPCServer.hpp>



volatile sig_atomic_t flag = 0;

void termFunc(int sig) {
    cout << "termfunc" << endl;
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

}
NesWorker::NesWorker(NESNodeType type) {
    connected = false;
    withRegisterStream = false;
    withParent = false;
    coordinatorPort = 0;
    this->type = type;
    NES_DEBUG("NesWorker: constructed");
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

void startWorkerRPCServer(std::shared_ptr<Server>& rpcServer) {

    NES_DEBUG("startWorkerRPCServer: Server listening");
    rpcServer->Wait();
    NES_DEBUG("startWorkerRPCServer: end listening");
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

    std::string address = workerCfg.ip + ":" + ::to_string(workerCfg.receive_port+1);
    NES_DEBUG("startWorkerRPCServer for address=" << address);
    WorkerRPCServer service(nodeEngine);

    ServerBuilder builder;
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    rpcServer = std::move(server);

    std::thread threadRPC([&]()
                          {
                            startWorkerRPCServer(rpcServer);
                          });

    rpcThread = std::move(threadRPC);
    NES_DEBUG("NesWorker: rpcThread successfully started");


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
        signal(SIGINT, termFunc);
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

NodeEnginePtr NesWorker::getNodeEngine()
{
    return nodeEngine;
}

bool NesWorker::stop(bool force) {
    NES_DEBUG("NesWorker: stop");
    if (!stopped) {
        NES_DEBUG("NesWorker: stopping rpc server");
        rpcServer->Shutdown();
        if (rpcThread.joinable()) {
            NES_DEBUG("NesWorker: join rpcThread");
            rpcThread.join();
        }

        bool successShutdownNodeEngine = nodeEngine->stop(force);
        if (!successShutdownNodeEngine) {
            NES_ERROR("NesWorker::stop node engine stop not successful");
            throw Exception("NesWorker::stop  error while stopping node engine");
        } else {
            NES_DEBUG("NesWorker::stop : Node engine stopped successfully");
        }
        return successShutdownNodeEngine;
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
