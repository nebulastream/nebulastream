/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <Components/NesWorker.hpp>
#include <Configurations/ConfigOption.hpp>
#include <Configurations/ConfigOptions/WorkerConfig.hpp>
#include <CoordinatorRPCService.pb.h>
#include <GRPC/CallData.hpp>
#include <GRPC/CoordinatorRPCClient.hpp>
#include <GRPC/WorkerRPCServer.hpp>
#include <Mobility/LocationHTTPClient.h>
#include <Monitoring/MetricValues/MetricValueType.hpp>
#include <Monitoring/Metrics/MetricCatalog.hpp>
#include <Monitoring/Metrics/MonitoringPlan.hpp>
#include <Monitoring/MonitoringAgent.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <Util/Logger.hpp>
#include <csignal>
#include <future>
#include <utility>

using namespace std;
volatile sig_atomic_t flag = 0;

void termFunc(int) {
    cout << "termfunc" << endl;
    flag = 1;
}

namespace NES {

NesWorker::NesWorker(const WorkerConfigPtr& workerConfig, NesNodeType type)
    : conf(PhysicalStreamConfig::createEmpty()), withRegisterLocation(workerConfig->getRegisterLocation()->getValue() == 0),
      coordinatorIp(workerConfig->getCoordinatorIp()->getValue()),
      localWorkerIp(workerConfig->getLocalWorkerIp()->getValue()),
      coordinatorPort(workerConfig->getCoordinatorPort()->getValue()),
      coordinatorRestPort(workerConfig->getCoordinatorRestPort()->getValue()),
      localWorkerRpcPort(workerConfig->getRpcPort()->getValue()),
      localWorkerZmqPort(workerConfig->getDataPort()->getValue()), numberOfSlots(workerConfig->getNumberOfSlots()->getValue()),
      numWorkerThreads(workerConfig->getNumWorkerThreads()->getValue()),
      numberOfBuffersInGlobalBufferManager(workerConfig->getNumberOfBuffersInGlobalBufferManager()->getValue()),
      numberOfBuffersPerPipeline(workerConfig->getnumberOfBuffersPerPipeline()->getValue()),
      numberOfBuffersInSourceLocalBufferPool(workerConfig->getNumberOfBuffersInSourceLocalBufferPool()->getValue()),
      bufferSizeInBytes(workerConfig->getBufferSizeInBytes()->getValue()), type(type),
      workerName(workerConfig->getWorkerName()->getValue()) {
    MDC::put("threadName", "NesWorker");
    NES_DEBUG("NesWorker: constructed");
}

NesWorker::~NesWorker() {
    NES_DEBUG("NesWorker::~NesWorker()");
    NES_DEBUG("NesWorker::~NesWorker() use count of node engine: " << nodeEngine.use_count());
    stop(true);
}

bool NesWorker::setWithRegister(PhysicalStreamConfigPtr conf) {
    withRegisterStream = true;
    this->conf = std::move(conf);
    return true;
}

bool NesWorker::setWithParent(std::string parentId) {
    withParent = true;
    this->parentId = std::move(parentId);
    return true;
}

void NesWorker::setWithRegisterLocation(bool withRegisterLocationValue) {
    withRegisterLocation = withRegisterLocationValue;
}

void NesWorker::handleRpcs(WorkerRPCServer& service) {
    //TODO: somehow we need this although it is not called at all
    // Spawn a new CallData instance to serve new clients.

    CallData call(service, completionQueue.get());
    call.proceed();
    void* tag = nullptr;// uniquely identifies a request.
    bool ok = 0;        //
    while (true) {
        // Block waiting to read the next event from the completion queue. The
        // event is uniquely identified by its tag, which in this case is the
        // memory address of a CallData instance.
        // The return value of Next should always be checked. This return value
        // tells us whether there is any kind of event or completionQueue is shutting down.
        bool ret = completionQueue->Next(&tag, &ok);
        NES_DEBUG("handleRpcs got item from queue with ret=" << ret);
        if (!ret) {
            //we are going to shut down
            return;
        }
        NES_ASSERT(ok, "handleRpcs got invalid message");
        static_cast<CallData*>(tag)->proceed();
    }
}

void NesWorker::buildAndStartGRPCServer(const std::shared_ptr<std::promise<bool>>& prom) {
    WorkerRPCServer service(nodeEngine, monitoringAgent);
    ServerBuilder builder;
    builder.AddListeningPort(rpcAddress, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    completionQueue = builder.AddCompletionQueue();
    rpcServer = builder.BuildAndStart();
    prom->set_value(true);
    NES_DEBUG("NesWorker: buildAndStartGRPCServer Server listening on address " << rpcAddress);
    //this call is already blocking
    handleRpcs(service);
    rpcServer->Wait();
    NES_DEBUG("NesWorker: buildAndStartGRPCServer end listening");
}

uint64_t NesWorker::getWorkerId() { return coordinatorRpcClient->getId(); }

bool NesWorker::start(bool blocking, bool withConnect) {
    NES_DEBUG("NesWorker: start with blocking " << blocking << " coordinatorIp=" << coordinatorIp
                                                << " coordinatorPort=" << coordinatorPort << " localWorkerIp=" << localWorkerIp
                                                << " localWorkerRpcPort=" << localWorkerRpcPort
                                                << " localWorkerZmqPort=" << localWorkerZmqPort << " type=" << type);

    NES_DEBUG("NesWorker::start: start NodeEngine");
    auto expected = false;
    if (!isRunning.compare_exchange_strong(expected, true)) {
        NES_ASSERT2_FMT(false, "cannot start nes worker");
    }

    try {
        nodeEngine = NodeEngine::NodeEngine::create(localWorkerIp,
                                                    localWorkerZmqPort,
                                                    conf,
                                                    numWorkerThreads,
                                                    bufferSizeInBytes,
                                                    numberOfBuffersInGlobalBufferManager,
                                                    numberOfBuffersInSourceLocalBufferPool,
                                                    numberOfBuffersPerPipeline);
        NES_DEBUG("NesWorker: Node engine started successfully");
        monitoringAgent = MonitoringAgent::create();
        NES_DEBUG("NesWorker: MonitoringAgent configured with default values");
    } catch (std::exception& err) {
        NES_ERROR("NesWorker: node engine could not be started");
        throw Exception("NesWorker error while starting node engine");
    }

    rpcAddress = localWorkerIp + ":" + std::to_string(localWorkerRpcPort);
    NES_DEBUG("NesWorker: startWorkerRPCServer for accepting messages for address=" << rpcAddress);
    std::shared_ptr<std::promise<bool>> promRPC = std::make_shared<std::promise<bool>>();

    rpcThread = std::make_shared<std::thread>(([this, promRPC]() {
        NES_DEBUG("NesWorker: buildAndStartGRPCServer");
        buildAndStartGRPCServer(promRPC);
        NES_DEBUG("NesWorker: buildAndStartGRPCServer: end listening");
    }));
    promRPC->get_future().get();
    NES_DEBUG("NesWorker:buildAndStartGRPCServer: ready");

    if (withConnect) {
        NES_DEBUG("NesWorker: start with connect");
        bool con = connect();
        NES_DEBUG("connected= " << con);
        NES_ASSERT(con, "cannot connect");
    }
    if (withRegisterLocation) {
        NES_DEBUG("NesWorker: start with register location");
        LocationHTTPClient locationHttpClient(coordinatorIp, coordinatorRestPort);
        bool success = locationHttpClient.addSource(workerName);
        NES_DEBUG("registered= " << success);
        NES_ASSERT(success, "cannot register");
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

    if (blocking) {
        NES_DEBUG("NesWorker: started, join now and waiting for work");
        signal(SIGINT, termFunc);
        while (true) {
            if (flag) {
                NES_DEBUG("NesWorker: caught signal terminating worker");
                flag = 0;
                break;
            }
            cout << "NesWorker wait" << endl;
            sleep(5);
        }
        NES_DEBUG("NesWorker: joined, return");
        return true;
    }
    NES_DEBUG("NesWorker: started, return without blocking");
    return true;
}

NodeEngine::NodeEnginePtr NesWorker::getNodeEngine() { return nodeEngine; }

bool NesWorker::isWorkerRunning() const noexcept { return isRunning; }

bool NesWorker::stop(bool) {
    NES_DEBUG("NesWorker: stop");
    auto expected = true;
    if (isRunning.compare_exchange_strong(expected, false)) {
        bool successShutdownNodeEngine = nodeEngine->stop();
        if (!successShutdownNodeEngine) {
            NES_ERROR("NesWorker::stop node engine stop not successful");
            throw Exception("NesWorker::stop  error while stopping node engine");
        }
        NES_DEBUG("NesWorker::stop : Node engine stopped successfully");

        nodeEngine.reset();
        NES_DEBUG("NesWorker: stopping rpc server");
        rpcServer->Shutdown();
        //shut down the async queue
        completionQueue->Shutdown();

        if (rpcThread->joinable()) {
            NES_DEBUG("NesWorker: join rpcThread");
            rpcThread->join();
        }
        rpcServer.reset();
        rpcThread.reset();

        return successShutdownNodeEngine;
    }
    NES_WARNING("NesWorker::stop: already stopped");
    return true;
}

bool NesWorker::connect() {
    std::string address = coordinatorIp + ":" + std::to_string(coordinatorPort);

    coordinatorRpcClient = std::make_shared<CoordinatorRPCClient>(address);
    std::string localAddress = localWorkerIp + ":" + std::to_string(localWorkerRpcPort);

    NES_DEBUG("NesWorker::connect() with server address= " << address << " localaddress=" << localAddress);
    // todo add nodeEngine->getNodePropertiesAsString()
    auto nodeStatsProvider = nodeEngine->getNodeStatsProvider();
    nodeStatsProvider->update();
    auto nodeStats = nodeStatsProvider->getNodeStats();

    bool successPRCRegister = false;
    if (type == NesNodeType::Sensor) {
        successPRCRegister = coordinatorRpcClient->registerNode(localWorkerIp,
                                                                localWorkerRpcPort,
                                                                localWorkerZmqPort,
                                                                numberOfSlots,
                                                                NodeType::Sensor,
                                                                nodeStats);
    } else if (type == NesNodeType::Worker) {
        successPRCRegister = coordinatorRpcClient->registerNode(localWorkerIp,
                                                                localWorkerRpcPort,
                                                                localWorkerZmqPort,
                                                                numberOfSlots,
                                                                NodeType::Worker,
                                                                nodeStats);
    } else {
        NES_NOT_IMPLEMENTED();
    }
    NES_DEBUG("NesWorker::connect() got id=" << coordinatorRpcClient->getId());
    topologyNodeId = coordinatorRpcClient->getId();
    if (successPRCRegister) {
        NES_DEBUG("NesWorker::registerNode rpc register success");
        connected = true;
        return true;
    }
    NES_DEBUG("NesWorker::registerNode rpc register failed");
    connected = false;
    return false;
}

bool NesWorker::disconnect() {
    NES_DEBUG("NesWorker::disconnect()");
    bool successPRCRegister = coordinatorRpcClient->unregisterNode();
    if (successPRCRegister) {
        NES_DEBUG("NesWorker::registerNode rpc unregister success");
        return true;
    }
    NES_DEBUG("NesWorker::registerNode rpc unregister failed");
    return false;
}

bool NesWorker::registerLogicalStream(std::string name, std::string path) {
    bool con = waitForConnect();
    NES_DEBUG("connected= " << con);
    NES_ASSERT(con, "Connection failed");
    bool success = coordinatorRpcClient->registerLogicalStream(std::move(name), std::move(path));
    NES_DEBUG("NesWorker::registerLogicalStream success=" << success);
    return success;
}

bool NesWorker::unregisterLogicalStream(std::string logicalName) {
    bool success = coordinatorRpcClient->unregisterLogicalStream(std::move(logicalName));
    NES_DEBUG("NesWorker::unregisterLogicalStream success=" << success);
    return success;
}

bool NesWorker::unregisterPhysicalStream(std::string logicalName, std::string physicalName) {
    bool success = coordinatorRpcClient->unregisterPhysicalStream(std::move(logicalName), std::move(physicalName));
    NES_DEBUG("NesWorker::unregisterPhysicalStream success=" << success);
    return success;
}

bool NesWorker::registerPhysicalStream(const AbstractPhysicalStreamConfigPtr& conf) {
    NES_ASSERT(conf, "invalid configuration");
    bool con = waitForConnect();
    NES_DEBUG("connected= " << con);
    NES_ASSERT(con, "cannot connect");
    bool success = coordinatorRpcClient->registerPhysicalStream(conf);
    NES_ASSERT(success, "failed to register stream");
    // TODO we need to get rid of this
    nodeEngine->setConfig(conf);
    NES_DEBUG("NesWorker::registerPhysicalStream success=" << success);
    return success;
}

bool NesWorker::addParent(uint64_t parentId) {
    bool con = waitForConnect();
    NES_DEBUG("connected= " << con);
    NES_ASSERT(con, "Connection failed");
    bool success = coordinatorRpcClient->addParent(parentId);
    NES_DEBUG("NesWorker::addNewLink(parent only) success=" << success);
    return success;
}

bool NesWorker::replaceParent(uint64_t oldParentId, uint64_t newParentId) {
    bool con = waitForConnect();
    NES_DEBUG("connected= " << con);
    NES_ASSERT(con, "Connection failed");
    bool success = coordinatorRpcClient->replaceParent(oldParentId, newParentId);
    if (!success) {
        NES_WARNING("NesWorker::replaceParent() failed to replace oldParent=" << oldParentId
                                                                              << " with newParentId=" << newParentId);
    }
    NES_DEBUG("NesWorker::replaceParent() success=" << success);
    return success;
}

bool NesWorker::removeParent(uint64_t parentId) {
    bool con = waitForConnect();
    NES_DEBUG("connected= " << con);
    NES_ASSERT(con, "Connection failed");
    bool success = coordinatorRpcClient->removeParent(parentId);
    NES_DEBUG("NesWorker::removeLink(parent only) success=" << success);
    return success;
}

std::vector<NodeEngine::QueryStatisticsPtr> NesWorker::getQueryStatistics(QueryId queryId) {
    return nodeEngine->getQueryStatistics(queryId);
}

bool NesWorker::waitForConnect() const {
    NES_DEBUG("NesWorker::waitForConnect()");
    auto timeoutInSec = std::chrono::seconds(3);
    auto start_timestamp = std::chrono::system_clock::now();
    while (std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
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

TopologyNodeId NesWorker::getTopologyNodeId() const { return topologyNodeId; }

}// namespace NES
