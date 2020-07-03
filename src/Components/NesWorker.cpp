#include <Components/NesWorker.hpp>
#include <GRPC/WorkerRPCServer.hpp>
#include <Util/Logger.hpp>
#include <future>
#include <signal.h>

volatile sig_atomic_t flag = 0;

void termFunc(int sig) {
    cout << "termfunc" << endl;
    flag = 1;
}

namespace NES {

NesWorker::NesWorker(std::string coordinatorIp, std::string coordinatorPort, std::string localIp, std::string localWorkerPort, NESNodeType type)
    : coordinatorIp(coordinatorIp),
      coordinatorPort(coordinatorPort),
      localWorkerIp(localIp),
      localWorkerPort(localWorkerPort),
      type(type) {
    connected = false;
    withRegisterStream = false;
    withParent = false;
    NES_DEBUG("NesWorker: constructed");
}

NesWorker::~NesWorker() {
    NES_DEBUG("NesWorker::~NesWorker()");
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

void startWorkerRPCServer(std::shared_ptr<Server>& rpcServer, std::string address, NodeEnginePtr nodeEngine, std::promise<bool>& prom) {
    NES_DEBUG("startWorkerRPCServer");

    WorkerRPCServer service(nodeEngine);
    ServerBuilder builder;
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    rpcServer = std::move(server);
    prom.set_value(true);
    NES_DEBUG("startWorkerRPCServer: Server listening on address " << address);
    rpcServer->Wait();
    NES_DEBUG("startWorkerRPCServer: end listening");
}

bool NesWorker::start(bool blocking, bool withConnect) {
    NES_DEBUG("NesWorker: start with blocking " << blocking << " coordinatorIp=" << coordinatorIp
                                                << " coordinatorPort=" << coordinatorPort << " localWorkerIp=" << localWorkerIp << " localWorkerPort=" << localWorkerPort
                                                << " type=" << type);

    NES_DEBUG("NesWorker::start: start NodeEngine");
    nodeEngine = std::make_shared<NodeEngine>();
    bool success = nodeEngine->start();
    if (!success) {
        NES_ERROR("NesWorker: node engine could not be started");
        throw Exception("NesWorker error while starting node engine");
    } else {
        NES_DEBUG("NesWorker: Node engine started successfully");
    }

    std::string address = localWorkerIp + ":" + localWorkerPort;
    NES_DEBUG("startWorkerRPCServer for accepting messages for address=" << address);
    std::promise<bool> promRPC;
    std::thread threadRPC([&]() {
        startWorkerRPCServer(rpcServer, address, nodeEngine, promRPC);
    });
    promRPC.get_future().get();
    rpcThread = std::move(threadRPC);
    NES_DEBUG("NesWorker: rpcThread successfully started");

    if (withConnect) {
        NES_DEBUG("NesWorker: start with connect");
        assert(connect());
    }
    if (withRegisterStream) {
        NES_DEBUG("NesWorker: start with register stream");
        assert(registerPhysicalStream(conf));
    }
    if (withParent) {
        NES_DEBUG("NesWorker: add parent id=" << parentId);
        assert(addParent(atoi(parentId.c_str())));
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

NodeEnginePtr NesWorker::getNodeEngine() {
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
        stopped = true;
        return successShutdownNodeEngine;
    } else {
        NES_WARNING("NesWorker::stop: already stopped");
        return true;
    }
}

bool NesWorker::connect() {
    std::string address = coordinatorIp + ":" + coordinatorPort;

    coordinatorRpcClient = std::make_shared<CoordinatorRPCClient>(
        address);
    std::string localAddress = localWorkerIp + ":" + localWorkerPort;

    NES_DEBUG("NesWorker::connect() with server address= " << address << " localaddres=" << localAddress);
    bool successPRCRegister = coordinatorRpcClient->registerNode(localAddress, 2,
                                                                 type, nodeEngine->getNodePropertiesAsString());
    if (successPRCRegister) {
        NES_DEBUG("NesWorker::registerNode rpc register success");
        return true;
    } else {
        NES_DEBUG("NesWorker::registerNode rpc register failed");
        return false;
    }
}

bool NesWorker::disconnect() {
    NES_DEBUG("NesWorker::disconnect()");
    bool successPRCRegister = coordinatorRpcClient->unregisterNode();
    if (successPRCRegister) {
        NES_DEBUG("NesWorker::registerNode rpc unregister success");
        return true;
    } else {
        NES_DEBUG("NesWorker::registerNode rpc unregister failed");
        return false;
    }
}

bool NesWorker::registerLogicalStream(std::string name, std::string path) {
    bool success = coordinatorRpcClient->registerLogicalStream(name, path);
    NES_DEBUG("NesWorker::registerLogicalStream success=" << success);
    return success;
}

bool NesWorker::unregisterLogicalStream(std::string logicalName) {
    bool success = coordinatorRpcClient->unregisterLogicalStream(logicalName);
    NES_DEBUG("NesWorker::unregisterLogicalStream success=" << success);
    return success;
}

bool NesWorker::unregisterPhysicalStream(std::string logicalName, std::string physicalName) {
    bool success = coordinatorRpcClient->unregisterPhysicalStream(logicalName, physicalName);
    NES_DEBUG("NesWorker::unregisterPhysicalStream success=" << success);
    return success;
}

bool NesWorker::registerPhysicalStream(PhysicalStreamConfig conf) {
    bool success = coordinatorRpcClient->registerPhysicalStream(conf);
    NES_DEBUG("NesWorker::registerPhysicalStream success=" << success);
    return success;
}

bool NesWorker::addParent(size_t parentId) {
    bool success = coordinatorRpcClient->addParent(parentId);
    NES_DEBUG("NesWorker::addNewLink(parent only) success=" << success);
    return success;
}

bool NesWorker::removeParent(size_t parentId) {
    bool success = coordinatorRpcClient->removeParent(parentId);
    NES_DEBUG("NesWorker::removeLink(parent only) success=" << success);
    return success;
}

QueryStatisticsPtr NesWorker::getQueryStatistics(std::string queryId) {
    return nodeEngine->getQueryStatistics(queryId);
}

}// namespace NES
