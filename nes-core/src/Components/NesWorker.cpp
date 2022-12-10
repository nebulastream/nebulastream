/*
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

#include <Catalogs/Source/PhysicalSource.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Worker/WorkerMobilityConfiguration.hpp>
#include <CoordinatorRPCService.pb.h>
#include <GRPC/CallData.hpp>
#include <GRPC/CoordinatorRPCClient.hpp>
#include <GRPC/HealthCheckRPCServer.hpp>
#include <GRPC/WorkerRPCServer.hpp>
#include <Monitoring/Metrics/Gauge/RegistrationMetrics.hpp>
#include <Monitoring/MonitoringAgent.hpp>
#include <Monitoring/MonitoringPlan.hpp>
#include <Monitoring/Storage/AbstractMetricStore.hpp>
#include <Network/NetworkManager.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Services/WorkerHealthCheckService.hpp>
#include <Spatial/Index/Location.hpp>
#include <Spatial/Index/Waypoint.hpp>
#include <Spatial/Mobility/LocationProvider.hpp>
#include <Spatial/Mobility/ReconnectConfigurator.hpp>
#include <Spatial/Mobility/TrajectoryPredictor.hpp>
#include <Util/Experimental/NodeType.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/ThreadNaming.hpp>
#include <csignal>
#include <future>
#include <grpcpp/ext/health_check_service_server_builder_option.h>
#include <grpcpp/health_check_service_interface.h>
#include <iomanip>

#include <utility>
using namespace std;
volatile sig_atomic_t flag = 0;

void termFunc(int) {
    cout << "termfunc" << endl;
    flag = 1;
}

namespace NES {

NesWorker::NesWorker(Configurations::WorkerConfigurationPtr&& workerConfig, Monitoring::MetricStorePtr metricStore)
    : workerConfig(workerConfig), localWorkerRpcPort(workerConfig->rpcPort), topologyNodeId(INVALID_TOPOLOGY_NODE_ID),
      metricStore(metricStore), parentId(workerConfig->parentId),
      mobilityConfig(std::make_shared<NES::Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfiguration>(
          workerConfig->mobilityConfiguration)) {
    setThreadName("NesWorker");
    NES_DEBUG("NesWorker: constructed");
    NES_ASSERT2_FMT(workerConfig->coordinatorPort > 0, "Cannot use 0 as coordinator port");
    rpcAddress = workerConfig->localWorkerIp.getValue() + ":" + std::to_string(localWorkerRpcPort);
}

NesWorker::~NesWorker() { stop(true); }

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

void NesWorker::buildAndStartGRPCServer(const std::shared_ptr<std::promise<int>>& portPromise) {
    WorkerRPCServer service(nodeEngine, monitoringAgent, locationProvider, trajectoryPredictor);
    ServerBuilder builder;
    int actualRpcPort;
    builder.AddListeningPort(rpcAddress, grpc::InsecureServerCredentials(), &actualRpcPort);
    builder.RegisterService(&service);
    completionQueue = builder.AddCompletionQueue();

    std::unique_ptr<grpc::HealthCheckServiceInterface> healthCheckServiceInterface;
    std::unique_ptr<grpc::ServerBuilderOption> option(
        new grpc::HealthCheckServiceServerBuilderOption(std::move(healthCheckServiceInterface)));
    builder.SetOption(std::move(option));
    HealthCheckRPCServer healthCheckServiceImpl;
    healthCheckServiceImpl.SetStatus(
        HEALTH_SERVICE_NAME,
        grpc::health::v1::HealthCheckResponse_ServingStatus::HealthCheckResponse_ServingStatus_SERVING);
    builder.RegisterService(&healthCheckServiceImpl);

    rpcServer = builder.BuildAndStart();
    portPromise->set_value(actualRpcPort);
    NES_DEBUG("NesWorker: buildAndStartGRPCServer Server listening on address " << rpcAddress << ":" << actualRpcPort);
    //this call is already blocking
    handleRpcs(service);

    rpcServer->Wait();
    NES_DEBUG("NesWorker: buildAndStartGRPCServer end listening");
}

uint64_t NesWorker::getWorkerId() { return coordinatorRpcClient->getId(); }

bool NesWorker::start(bool blocking, bool withConnect) {
    NES_DEBUG("NesWorker: start with blocking "
              << blocking << " coordinatorIp=" << workerConfig->coordinatorIp.getValue() << " coordinatorPort="
              << workerConfig->coordinatorPort.getValue() << " localWorkerIp=" << workerConfig->localWorkerIp.getValue()
              << " localWorkerRpcPort=" << localWorkerRpcPort << " localWorkerZmqPort=" << workerConfig->dataPort.getValue()
              << " windowStrategy=" << workerConfig->queryCompiler.windowingStrategy);
    NES_DEBUG("NesWorker::start: start Runtime");
    auto expected = false;
    if (!isRunning.compare_exchange_strong(expected, true)) {
        NES_ASSERT2_FMT(false, "cannot start nes worker");
    }

    try {
        NES_DEBUG("NesWorker: MonitoringAgent configured with monitoring=" << workerConfig->enableMonitoring.getValue());
        monitoringAgent = Monitoring::MonitoringAgent::create(workerConfig->enableMonitoring.getValue());
        monitoringAgent->addMonitoringStreams(workerConfig);

        nodeEngine =
            Runtime::NodeEngineBuilder::create(workerConfig).setQueryStatusListener(this->inherited0::shared_from_this()).build();
        if (metricStore != nullptr) {
            nodeEngine->setMetricStore(metricStore);
        }
        NES_DEBUG("NesWorker: Node engine started successfully");
    } catch (std::exception& err) {
        NES_ERROR("NesWorker: node engine could not be started");
        throw Exceptions::RuntimeException("NesWorker error while starting node engine");
    }

    NES_DEBUG("NesWorker: request startWorkerRPCServer for accepting messages for address=" << rpcAddress << ":"
                                                                                            << localWorkerRpcPort.load());
    std::shared_ptr<std::promise<int>> promRPC = std::make_shared<std::promise<int>>();

    if (workerConfig->nodeSpatialType.getValue() != NES::Spatial::Index::Experimental::NodeType::NO_LOCATION) {
        locationProvider = NES::Spatial::Mobility::Experimental::LocationProvider::create(workerConfig);
        if (locationProvider->getNodeType() == NES::Spatial::Index::Experimental::NodeType::MOBILE_NODE) {
            trajectoryPredictor = std::make_shared<NES::Spatial::Mobility::Experimental::TrajectoryPredictor>(locationProvider,
                                                                                                              mobilityConfig,
                                                                                                              parentId);
        }
    }

    rpcThread = std::make_shared<std::thread>(([this, promRPC]() {
        NES_DEBUG("NesWorker: buildAndStartGRPCServer");
        buildAndStartGRPCServer(promRPC);
        NES_DEBUG("NesWorker: buildAndStartGRPCServer: end listening");
    }));
    localWorkerRpcPort.store(promRPC->get_future().get());
    rpcAddress = workerConfig->localWorkerIp.getValue() + ":" + std::to_string(localWorkerRpcPort.load());
    NES_DEBUG("NesWorker: startWorkerRPCServer ready for accepting messages for address=" << rpcAddress << ":"
                                                                                          << localWorkerRpcPort.load());

    if (withConnect) {
        NES_DEBUG("NesWorker: start with connect");
        bool con = connect();

        NES_ASSERT(con, "cannot connect");
    }

    if (parentId > NesCoordinator::NES_COORDINATOR_ID) {
        NES_DEBUG("NesWorker: add parent id=" << parentId);
        bool success = replaceParent(NesCoordinator::NES_COORDINATOR_ID, parentId);
        NES_DEBUG("parent add= " << success);
        NES_ASSERT(success, "cannot addParent");
    }

    if (coordinatorRpcClient && trajectoryPredictor) {
        NES_DEBUG("Worker has spatial type MOBILE_NODE, creating trajectory predictor")
        trajectoryPredictor->setUpReconnectPlanning(reconnectConfigurator);
    }

    if (workerConfig->enableStatisticOuput) {
        statisticOutputThread = std::make_shared<std::thread>(([this]() {
            NES_DEBUG("NesWorker: start statistic collection");
            while (isRunning) {
                auto ts = std::chrono::system_clock::now();
                auto timeNow = std::chrono::system_clock::to_time_t(ts);
                auto stats = nodeEngine->getQueryStatistics(true);
                for (auto& query : stats) {
                    std::cout << "Statistics " << std::put_time(std::localtime(&timeNow), "%Y-%m-%d %X") << " =>"
                              << query.getQueryStatisticsAsString() << std::endl;
                }
                sleep(1);
            }
            NES_DEBUG("NesWorker: statistic collection end");
        }));
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
            //cout << "NesWorker wait" << endl;
            sleep(5);
        }
    }

    NES_DEBUG("NesWorker: started, return");
    return true;
}

Runtime::NodeEnginePtr NesWorker::getNodeEngine() { return nodeEngine; }

bool NesWorker::isWorkerRunning() const noexcept { return isRunning; }

bool NesWorker::stop(bool) {
    NES_DEBUG("NesWorker: stop");

    auto expected = true;
    if (isRunning.compare_exchange_strong(expected, false)) {
        NES_DEBUG("NesWorker::stopping health check");
        if (healthCheckService) {
            healthCheckService->stopHealthCheck();
        } else {
            NES_WARNING("No health check service was created");
        }

        if (locationProvider && locationProvider->getNodeType() == NES::Spatial::Index::Experimental::NodeType::MOBILE_NODE) {
            if (trajectoryPredictor) {
                trajectoryPredictor->stopReconnectPlanning();
                NES_TRACE("triggered stopping of reconnect planner thread");
            }
            if (reconnectConfigurator) {
                reconnectConfigurator->stopPeriodicUpdating();
                NES_TRACE("triggered stopping of location update push thread");
            }
        }
        bool successShutdownNodeEngine = nodeEngine->stop();
        if (!successShutdownNodeEngine) {
            NES_ERROR("NesWorker::stop node engine stop not successful");
            NES_THROW_RUNTIME_ERROR("NesWorker::stop  error while stopping node engine");
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
        if (statisticOutputThread && statisticOutputThread->joinable()) {
            NES_DEBUG("NesWorker: statistic collection thread join");
            statisticOutputThread->join();
        }
        statisticOutputThread.reset();

        return successShutdownNodeEngine;
    }
    NES_WARNING("NesWorker::stop: already stopped");
    return true;
}

bool NesWorker::connect() {
    std::string coordinatorAddress = workerConfig->coordinatorIp.getValue() + ":" + std::to_string(workerConfig->coordinatorPort);
    coordinatorRpcClient = std::make_shared<CoordinatorRPCClient>(coordinatorAddress);
    std::string localAddress = workerConfig->localWorkerIp.getValue() + ":" + std::to_string(localWorkerRpcPort);
    auto registrationMetrics = monitoringAgent->getRegistrationMetrics();
    NES::Spatial::Index::Experimental::Location fixedCoordinates = {};
    if (locationProvider && locationProvider->getNodeType() == NES::Spatial::Index::Experimental::NodeType::FIXED_LOCATION) {
        fixedCoordinates = *locationProvider->getWaypoint()->getLocation();
    }

    NES_DEBUG("NesWorker::connect() with server coordinatorAddress= " << coordinatorAddress << " localaddress=" << localAddress);

    bool successPRCRegister = coordinatorRpcClient->registerNode(workerConfig->localWorkerIp.getValue(),
                                                                 localWorkerRpcPort.load(),
                                                                 nodeEngine->getNetworkManager()->getServerDataPort(),
                                                                 workerConfig->numberOfSlots,
                                                                 registrationMetrics,
                                                                 fixedCoordinates,
                                                                 workerConfig->nodeSpatialType.getValue(),
                                                                 /*isTfInstalled*/ false);

    NES_DEBUG("NesWorker::connect() got id=" << coordinatorRpcClient->getId());
    topologyNodeId = coordinatorRpcClient->getId();
    monitoringAgent->setNodeId(topologyNodeId);
    if (successPRCRegister) {
        NES_DEBUG("NesWorker::registerNode rpc register success with id " << topologyNodeId);
        connected = true;
        nodeEngine->setNodeId(topologyNodeId);
        healthCheckService = std::make_shared<WorkerHealthCheckService>(coordinatorRpcClient,
                                                                        HEALTH_SERVICE_NAME,
                                                                        this->inherited0::shared_from_this());
        NES_DEBUG("NesWorker start health check");
        healthCheckService->startHealthCheck();

        if (locationProvider) {
            locationProvider->setCoordinatorRPCCLient(coordinatorRpcClient);
            if (locationProvider->getNodeType() == NES::Spatial::Index::Experimental::NodeType::MOBILE_NODE) {
                reconnectConfigurator =
                    std::make_shared<NES::Spatial::Mobility::Experimental::ReconnectConfigurator>(*this,
                                                                                                  coordinatorRpcClient,
                                                                                                  mobilityConfig);
            }
        }

        auto configPhysicalSources = workerConfig->physicalSources.getValues();
        if (!configPhysicalSources.empty()) {
            std::vector<PhysicalSourcePtr> physicalSources;
            for (auto physicalSource : configPhysicalSources) {
                physicalSources.push_back(physicalSource);
            }
            NES_DEBUG("NesWorker: start with register source");
            bool success = registerPhysicalSources(physicalSources);
            NES_DEBUG("registered= " << success);
            NES_ASSERT(success, "cannot register");
        }

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
        connected = false;
        NES_DEBUG("NesWorker::stop health check");
        healthCheckService->stopHealthCheck();
        NES_DEBUG("NesWorker::stop health check successful");
        return true;
    }
    NES_DEBUG("NesWorker::registerNode rpc unregister failed");
    return false;
}

bool NesWorker::unregisterPhysicalSource(std::string logicalName, std::string physicalName) {
    bool success = coordinatorRpcClient->unregisterPhysicalSource(std::move(logicalName), std::move(physicalName));
    NES_DEBUG("NesWorker::unregisterPhysicalSource success=" << success);
    return success;
}

const Configurations::WorkerConfigurationPtr& NesWorker::getWorkerConfiguration() const { return workerConfig; }

bool NesWorker::registerPhysicalSources(const std::vector<PhysicalSourcePtr>& physicalSources) {
    NES_ASSERT(!physicalSources.empty(), "invalid physical sources");
    bool con = waitForConnect();

    NES_ASSERT(con, "cannot connect");
    bool success = coordinatorRpcClient->registerPhysicalSources(physicalSources);
    NES_ASSERT(success, "failed to register source");
    NES_DEBUG("NesWorker::registerPhysicalSources success=" << success);
    return success;
}

bool NesWorker::addParent(uint64_t parentId) {
    bool con = waitForConnect();

    NES_ASSERT(con, "Connection failed");
    bool success = coordinatorRpcClient->addParent(parentId);
    NES_DEBUG("NesWorker::addNewLink(parent only) success=" << success);
    return success;
}

bool NesWorker::replaceParent(uint64_t oldParentId, uint64_t newParentId) {
    bool con = waitForConnect();

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

    NES_ASSERT(con, "Connection failed");
    bool success = coordinatorRpcClient->removeParent(parentId);
    NES_DEBUG("NesWorker::removeLink(parent only) success=" << success);
    return success;
}

std::vector<Runtime::QueryStatisticsPtr> NesWorker::getQueryStatistics(QueryId queryId) {
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

bool NesWorker::notifyQueryStatusChange(QueryId queryId,
                                        QuerySubPlanId subQueryId,
                                        Runtime::Execution::ExecutableQueryPlanStatus newStatus) {
    NES_ASSERT(waitForConnect(), "cannot connect");
    NES_ASSERT2_FMT(newStatus != Runtime::Execution::ExecutableQueryPlanStatus::Stopped,
                    "Hard Stop called for query=" << queryId << " subQueryId=" << subQueryId
                                                  << " should not call notifyQueryStatusChange");
    if (newStatus == Runtime::Execution::ExecutableQueryPlanStatus::Finished) {
        NES_DEBUG("NesWorker " << getWorkerId() << " about to notify soft stop completion for query " << queryId << " subPlan "
                               << subQueryId);
        return coordinatorRpcClient->notifySoftStopCompleted(queryId, subQueryId);
    } else if (newStatus == Runtime::Execution::ExecutableQueryPlanStatus::ErrorState) {
        return true;// rpc to coordinator executed from async runner
    }
    return false;
}

bool NesWorker::canTriggerEndOfStream(QueryId queryId,
                                      QuerySubPlanId subPlanId,
                                      OperatorId sourceId,
                                      Runtime::QueryTerminationType terminationType) {
    NES_ASSERT(waitForConnect(), "cannot connect");
    NES_ASSERT(terminationType == Runtime::QueryTerminationType::Graceful, "invalid termination type");
    return coordinatorRpcClient->checkAndMarkForSoftStop(queryId, subPlanId, sourceId);
}

bool NesWorker::notifySourceTermination(QueryId queryId,
                                        QuerySubPlanId subPlanId,
                                        OperatorId sourceId,
                                        Runtime::QueryTerminationType queryTermination) {
    NES_ASSERT(waitForConnect(), "cannot connect");
    return coordinatorRpcClient->notifySourceStopTriggered(queryId, subPlanId, sourceId, queryTermination);
}

bool NesWorker::notifyQueryFailure(uint64_t queryId, uint64_t subQueryId, std::string errorMsg) {
    bool con = waitForConnect();
    NES_ASSERT(con, "Connection failed");
    bool success = coordinatorRpcClient->notifyQueryFailure(queryId, subQueryId, getWorkerId(), 0, errorMsg);
    NES_DEBUG("NesWorker::notifyQueryFailure success=" << success);
    return success;
}

bool NesWorker::notifyEpochTermination(uint64_t timestamp, uint64_t queryId) {
    bool con = waitForConnect();
    NES_ASSERT(con, "Connection failed");
    bool success = coordinatorRpcClient->notifyEpochTermination(timestamp, queryId);
    NES_DEBUG("NesWorker::propagatePunctuation success=" << success);
    return success;
}

bool NesWorker::notifyErrors(uint64_t workerId, std::string errorMsg) {
    bool con = waitForConnect();
    NES_ASSERT(con, "Connection failed");
    NES_DEBUG("NesWorker::sendErrors worker " << workerId << " going to send error=" << errorMsg);
    bool success = coordinatorRpcClient->sendErrors(workerId, errorMsg);
    NES_DEBUG("NesWorker::sendErrors success=" << success);
    return success;
}

void NesWorker::onFatalError(int signalNumber, std::string callstack) {
    NES_ERROR("onFatalError: signal [" << signalNumber << "] error [" << strerror(errno) << "] callstack " << callstack);
    std::string errorMsg;
    std::cerr << "NesWorker failed fatally" << std::endl;// it's necessary for testing and it wont harm us to write to stderr
    std::cerr << "Error: " << strerror(errno) << std::endl;
    std::cerr << "Signal: " << std::to_string(signalNumber) << std::endl;
    std::cerr << "Callstack:\n " << callstack << std::endl;
    // save errors in errorMsg
    errorMsg =
        "onFatalError: signal [" + std::to_string(signalNumber) + "] error [" + strerror(errno) + "] callstack " + callstack;
    //send it to Coordinator
    notifyErrors(getWorkerId(), errorMsg);
#ifdef ENABLE_CORE_DUMPER
    detail::createCoreDump();
#endif
}

void NesWorker::onFatalException(std::shared_ptr<std::exception> ptr, std::string callstack) {
    NES_ERROR("onFatalException: exception=[" << ptr->what() << "] callstack=\n" << callstack);
    std::string errorMsg;
    std::cerr << "NesWorker failed fatally" << std::endl;
    std::cerr << "Error: " << strerror(errno) << std::endl;
    std::cerr << "Exception: " << ptr->what() << std::endl;
    std::cerr << "Callstack:\n " << callstack << std::endl;
    // save errors in errorMsg
    errorMsg = "onFatalException: exception=[" + std::string(ptr->what()) + "] callstack=\n" + callstack;
    //send it to Coordinator
    this->notifyErrors(this->getWorkerId(), errorMsg);
#ifdef ENABLE_CORE_DUMPER
    detail::createCoreDump();
#endif
}

TopologyNodeId NesWorker::getTopologyNodeId() const { return topologyNodeId; }

NES::Spatial::Mobility::Experimental::LocationProviderPtr NesWorker::getLocationProvider() { return locationProvider; }

NES::Spatial::Mobility::Experimental::TrajectoryPredictorPtr NesWorker::getTrajectoryPredictor() { return trajectoryPredictor; }

}// namespace NES