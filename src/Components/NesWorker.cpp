#include <Components/NesWorker.hpp>
#include <GRPC/WorkerRPCServer.hpp>
#include <Util/Logger.hpp>
#include <future>
#include <signal.h>
#include <utility>

using namespace std;
volatile sig_atomic_t flag = 0;

void termFunc(int) {
    cout << "termfunc" << endl;
    flag = 1;
}

namespace NES {

NesWorker::NesWorker(
    std::string coordinatorIp,
    std::string coordinatorPort,
    std::string localIp,
    uint16_t localWorkerRpcPort,
    uint16_t localWorkerZmqPort,
    uint16_t numberOfSlots,
    NodeType type)
    : coordinatorIp(std::move(coordinatorIp)),
      coordinatorPort(coordinatorPort),
      localWorkerIp(std::move(localIp)),
      localWorkerRpcPort(localWorkerRpcPort),
      localWorkerZmqPort(localWorkerZmqPort),
      numberOfSlots(numberOfSlots),
      type(type) {
    connected = false;
    withRegisterStream = false;
    withParent = false;
    MDC::put("threadName", "NesWorker");
    NES_DEBUG("NesWorker: constructed");
}

// constructor with default numberOfSlots set to the number of processors
NesWorker::NesWorker(
    std::string coordinatorIp,
    std::string coordinatorPort,
    std::string localWorkerIp,
    uint16_t localWorkerRpcPort,
    uint16_t localWorkerZmqPort,
    NodeType type) : NesWorker(coordinatorIp,
                               coordinatorPort,
                               localWorkerIp,
                               localWorkerRpcPort,
                               localWorkerZmqPort,
                               std::thread::hardware_concurrency(),
                               type) {}

NesWorker::~NesWorker() {
    NES_DEBUG("NesWorker::~NesWorker()");
    NES_DEBUG("NesWorker::~NesWorker() use count of node engine: " << nodeEngine.use_count());
    stop(true);
}

bool NesWorker::setWitRegister(PhysicalStreamConfigPtr pConf) {
    withRegisterStream = true;
    conf = pConf;
    return true;
}

bool NesWorker::setWithParent(std::string parentId) {
    withParent = true;
    this->parentId = parentId;
    return true;
}

void NesWorker::buildAndStartGRPCServer(std::promise<bool>& prom) {
    WorkerRPCServer service(nodeEngine);
    ServerBuilder builder;
    builder.AddListeningPort(rpcAddress, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    rpcServer = builder.BuildAndStart();
    prom.set_value(true);
    NES_DEBUG("NesWorker: buildAndStartGRPCServer Server listening on address " << rpcAddress);
    rpcServer->Wait();
    NES_DEBUG("NesWorker: buildAndStartGRPCServer end listening");
}

bool NesWorker::start(bool blocking, bool withConnect) {
    NES_DEBUG("NesWorker: start with blocking "
              << blocking << " coordinatorIp=" << coordinatorIp
              << " coordinatorPort=" << coordinatorPort << " localWorkerIp="
              << localWorkerIp << " localWorkerRpcPort=" << localWorkerRpcPort
              << " localWorkerZmqPort=" << localWorkerZmqPort
              << " type=" << type);

    NES_DEBUG("NesWorker::start: start NodeEngine");
    try {
        nodeEngine = NodeEngine::create(localWorkerIp, localWorkerZmqPort, conf);
        NES_DEBUG("NesWorker: Node engine started successfully");
    } catch (std::exception& err) {
        NES_ERROR("NesWorker: node engine could not be started");
        throw Exception("NesWorker error while starting node engine");
    }

    rpcAddress = localWorkerIp + ":" + std::to_string(localWorkerRpcPort);
    NES_DEBUG("NesWorker: startWorkerRPCServer for accepting messages for address=" << rpcAddress);
    std::promise<bool> promRPC;

    rpcThread = std::make_shared<std::thread>(([&]() {
        NES_DEBUG("NesWorker: buildAndStartGRPCServer");
        buildAndStartGRPCServer(promRPC);
        NES_DEBUG("NesWorker: buildAndStartGRPCServer: end listening");
    }));
    promRPC.get_future().get();
    NES_DEBUG("NesWorker:buildAndStartGRPCServer: ready");

    if (withConnect) {
        NES_DEBUG("NesWorker: start with connect");
        bool con = connect();
        NES_DEBUG("connected= " << con);
        NES_ASSERT(con, "cannot connect");
    }
    if (withRegisterStream) {
        NES_DEBUG("NesWorker: start with register stream");
        bool success = registerPhysicalStream(conf);
        NES_DEBUG("registered= " << success);
        NES_ASSERT(success, "cannot register");
    }
    if (withParent) {
        NES_DEBUG("NesWorker: add parent id=" << parentId);
        bool success = addParent(atoi(parentId.c_str()));
        NES_DEBUG("parent add= " << success);
        NES_ASSERT(success, "cannot addParent");
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

bool NesWorker::stop(bool) {
    NES_DEBUG("NesWorker: stop");
    if (!stopped) {
        NES_DEBUG("NesWorker: stopping rpc server");
        rpcServer->Shutdown();

        if (rpcThread->joinable()) {
            NES_DEBUG("NesWorker: join rpcThread");
            rpcThread->join();
        }
        rpcServer.reset();
        rpcThread.reset();

        bool successShutdownNodeEngine = nodeEngine->stop();
        if (!successShutdownNodeEngine) {
            NES_ERROR("NesWorker::stop node engine stop not successful");
            throw Exception("NesWorker::stop  error while stopping node engine");
        } else {
            NES_DEBUG("NesWorker::stop : Node engine stopped successfully");
        }
        nodeEngine.reset();
        stopped = true;
        return successShutdownNodeEngine;
    } else {
        NES_WARNING("NesWorker::stop: already stopped");
        return true;
    }
}

bool NesWorker::connect() {
    std::string address = coordinatorIp + ":" + coordinatorPort;

    coordinatorRpcClient = std::make_shared<CoordinatorRPCClient>(address);
    std::string localAddress = localWorkerIp + ":" + std::to_string(localWorkerRpcPort);

    NES_DEBUG("NesWorker::connect() with server address= " << address << " localaddres=" << localAddress);
    // todo add nodeEngine->getNodePropertiesAsString()
    auto nodeStatsProvider = nodeEngine->getNodeStatsProvider();
    nodeStatsProvider->update();
    auto nodeStats = nodeStatsProvider->getNodeStats();
    bool successPRCRegister = coordinatorRpcClient->registerNode(localWorkerIp, localWorkerRpcPort, localWorkerZmqPort, numberOfSlots, type, nodeStats);
    if (successPRCRegister) {
        NES_DEBUG("NesWorker::registerNode rpc register success");
        connected = true;
        return true;
    } else {
        NES_DEBUG("NesWorker::registerNode rpc register failed");
        connected = false;
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
    bool con = waitForConnect();
    NES_DEBUG("connected= " << con);
    assert(con);
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

bool NesWorker::registerPhysicalStream(PhysicalStreamConfigPtr conf) {
    bool con = waitForConnect();
    NES_DEBUG("connected= " << con);
    assert(con);
    bool success = coordinatorRpcClient->registerPhysicalStream(conf);
    NES_DEBUG("NesWorker::registerPhysicalStream success=" << success);
    return success;
}

bool NesWorker::addParent(size_t parentId) {
    bool con = waitForConnect();
    NES_DEBUG("connected= " << con);
    assert(con);
    bool success = coordinatorRpcClient->addParent(parentId);
    NES_DEBUG("NesWorker::addNewLink(parent only) success=" << success);
    return success;
}

bool NesWorker::removeParent(size_t parentId) {
    bool con = waitForConnect();
    NES_DEBUG("connected= " << con);
    assert(con);
    bool success = coordinatorRpcClient->removeParent(parentId);
    NES_DEBUG("NesWorker::removeLink(parent only) success=" << success);
    return success;
}

std::vector<QueryStatisticsPtr> NesWorker::getQueryStatistics(QueryId queryId) {
    return nodeEngine->getQueryStatistics(queryId);
}

bool NesWorker::waitForConnect() {
    NES_DEBUG("NesWorker::waitForConnect()");
    auto timeoutInSec = std::chrono::seconds(3);
    auto start_timestamp = std::chrono::system_clock::now();
    auto now = start_timestamp;
    while ((now = std::chrono::system_clock::now()) < start_timestamp + timeoutInSec) {
        NES_DEBUG("waitForConnect: check connect");
        if (!connected) {
            NES_DEBUG("waitForConnect: not connected, sleep");
            sleep(1);
        } else {
            NES_DEBUG("waitForConnect: connected");
            return true;
        }
    }
    NES_DEBUG("waitForConnect: not connected after timeout");
    return false;
}

}// namespace NES
