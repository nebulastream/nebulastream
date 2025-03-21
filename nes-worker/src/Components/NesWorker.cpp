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

#include <Components/NesWorker.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <CoordinatorRPCService.pb.h>
#include <GRPC/CallData.hpp>
#include <GRPC/CoordinatorRPCClient.hpp>
#include <GRPC/HealthCheckRPCServer.hpp>
#include <GRPC/WorkerRPCServer.hpp>
#include <Mobility/LocationProviders/LocationProvider.hpp>
#include <Mobility/ReconnectSchedulePredictors/ReconnectSchedule.hpp>
#include <Mobility/ReconnectSchedulePredictors/ReconnectSchedulePredictor.hpp>
#include <Mobility/WorkerMobilityHandler.hpp>
#include <Monitoring/Metrics/Gauge/RegistrationMetrics.hpp>
#include <Monitoring/MonitoringAgent.hpp>
#include <Monitoring/MonitoringPlan.hpp>
#include <Monitoring/Storage/AbstractMetricStore.hpp>
#include <Network/NetworkManager.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Runtime/OpenCLManager.hpp>
#include <Runtime/QueryStatistics.hpp>
#include <Services/WorkerHealthCheckService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Mobility/SpatialTypeUtility.hpp>
#include <Util/ThreadNaming.hpp>
#include <Util/magicenum/magic_enum.hpp>
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

constexpr WorkerId NES_COORDINATOR_ID = WorkerId(1);

NesWorker::NesWorker(Configurations::WorkerConfigurationPtr workerConfig, Monitoring::MetricStorePtr metricStore)
    : workerConfig(workerConfig), localWorkerRpcPort(workerConfig->rpcPort), metricStore(metricStore),
      parentId(workerConfig->parentId),
      mobilityConfig(std::make_shared<NES::Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfiguration>(
          workerConfig->mobilityConfiguration)) {
    setThreadName("NesWorker");
    NES_DEBUG("NesWorker: constructed");
    NES_ASSERT2_FMT(workerConfig->coordinatorPort > 0, "Cannot use 0 as coordinator port");
    rpcAddress = workerConfig->localWorkerHost.getValue() + ":" + std::to_string(localWorkerRpcPort);
}

NesWorker::~NesWorker() {
    try {
        stop(true);
    } catch (...) {
        NES_ERROR("An error occurred while shutting down the worker");
    }
}

void NesWorker::handleRpcs(WorkerRPCServer& service) {
    //TODO: somehow we need this although it is not called at all
    // Spawn a new CallData instance to serve new clients.

    CallData call(service);
    call.proceed();
    void* tag = nullptr;// uniquely identifies a request.
    bool ok = false;    //
    while (true) {
        // Block waiting to read the next event from the completion queue. The
        // event is uniquely identified by its tag, which in this case is the
        // memory address of a CallData instance.
        // The return value of Next should always be checked. This return value
        // tells us whether there is any kind of event or completionQueue is shutting down.
        bool ret = completionQueue->Next(&tag, &ok);
        NES_DEBUG("handleRpcs got item from queue with ret={}", ret);
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
    NES_DEBUG("NesWorker: buildAndStartGRPCServer Server listening on address {}: {}", rpcAddress, actualRpcPort);
    //this call is already blocking
    handleRpcs(service);

    rpcServer->Wait();
    NES_DEBUG("NesWorker: buildAndStartGRPCServer end listening");
}

WorkerId NesWorker::getWorkerId() { return coordinatorRpcClient->getId(); }

bool NesWorker::start(bool blocking, bool withConnect) {
    NES_DEBUG("NesWorker: start with blocking {} workerId={} coordinatorHost={} coordinatorPort={} localWorkerHost={} "
              "localWorkerRpcPort={} "
              "localWorkerZmqPort={} windowStrategy={}",
              blocking,
              workerConfig->workerId.getValue(),
              workerConfig->coordinatorHost.getValue(),
              workerConfig->coordinatorPort.getValue(),
              workerConfig->localWorkerHost.getValue(),
              localWorkerRpcPort,
              workerConfig->dataPort.getValue(),
              magic_enum::enum_name(workerConfig->queryCompiler.windowingStrategy.getValue()));

    NES_DEBUG("NesWorker::start: start Runtime");
    auto expected = false;
    if (!isRunning.compare_exchange_strong(expected, true)) {
        NES_ASSERT2_FMT(false, "cannot start nes worker");
    }

    // load all plugins from the default folder.
    pluginLoader.loadDefaultPlugins();

    try {
        NES_DEBUG("NesWorker: MonitoringAgent configured with monitoring={}", workerConfig->enableMonitoring.getValue());
        monitoringAgent = Monitoring::MonitoringAgent::create(workerConfig->enableMonitoring.getValue());
        monitoringAgent->addMonitoringStreams(workerConfig);

        nodeEngine =
            Runtime::NodeEngineBuilder::create(workerConfig).setQueryStatusListener(this->inherited0::shared_from_this()).build();
        if (metricStore != nullptr) {
            nodeEngine->setMetricStore(metricStore);
        }
        NES_DEBUG("NesWorker: Node engine started successfully");
    } catch (std::exception& err) {
        NES_ERROR("NesWorker: node engine could not be started with error {}", err.what());
        throw Exceptions::RuntimeException("NesWorker error while starting node engine");
    }

    NES_DEBUG("NesWorker: request startWorkerRPCServer for accepting messages for address={}: {}",
              rpcAddress,
              localWorkerRpcPort.load());
    auto promRPC = std::make_shared<std::promise<int>>();

    if (workerConfig->nodeSpatialType.getValue() != NES::Spatial::Experimental::SpatialType::NO_LOCATION) {
        locationProvider = NES::Spatial::Mobility::Experimental::LocationProvider::create(workerConfig);
        if (locationProvider->getSpatialType() == NES::Spatial::Experimental::SpatialType::MOBILE_NODE) {
            //is s2 is activated, create a reconnect schedule predictor
//            trajectoryPredictor = NES::Spatial::Mobility::Experimental::ReconnectSchedulePredictor::create(mobilityConfig);
        }
    }

    rpcThread = std::make_shared<std::thread>(([this, promRPC]() {
        NES_DEBUG("NesWorker: buildAndStartGRPCServer");
        buildAndStartGRPCServer(promRPC);
        NES_DEBUG("NesWorker: buildAndStartGRPCServer: end listening");
    }));
    localWorkerRpcPort.store(promRPC->get_future().get());
    rpcAddress = workerConfig->localWorkerHost.getValue() + ":" + std::to_string(localWorkerRpcPort.load());
    NES_DEBUG("NesWorker: startWorkerRPCServer ready for accepting messages for address={}: {}",
              rpcAddress,
              localWorkerRpcPort.load());

    if (withConnect) {
        NES_DEBUG("NesWorker: start with connect");
        bool con = connect();
        NES_ASSERT(con, "cannot connect");
    }

    if (parentId.getRawValue() > NES_COORDINATOR_ID.getRawValue()) {
        NES_DEBUG("NesWorker: add parent id={}", parentId);
        bool success = replaceParent(NES_COORDINATOR_ID, parentId);
        NES_DEBUG("parent add= {}", success);
        NES_ASSERT(success, "cannot addParent");
    }

    if (withConnect && locationProvider
        && locationProvider->getSpatialType() == NES::Spatial::Experimental::SpatialType::MOBILE_NODE) {
//        workerMobilityHandler =
//            std::make_shared<NES::Spatial::Mobility::Experimental::WorkerMobilityHandler>(locationProvider,
//                                                                                          coordinatorRpcClient,
//                                                                                          nodeEngine,
//                                                                                          mobilityConfig);
        //FIXME: currently the worker mobility handler will only work with exactly one parent
        auto parentIds = coordinatorRpcClient->getParents(workerId);
        if (parentIds.size() > 1) {
            NES_WARNING("Attempting to start worker mobility handler for worker with multiple parents. This is"
                        "currently not supported, mobility handler will not be started");
        } else {
            //todo: do not use mobility handler on this branch
            //workerMobilityHandler->start(parentIds);
        }
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
            sleep(5);
        }
    }

    NES_DEBUG("NesWorker: started, return");
    return true;
}

Runtime::NodeEnginePtr NesWorker::getNodeEngine() { return nodeEngine; }

bool NesWorker::stop(bool) {
    NES_DEBUG("NesWorker: stop");

    auto expected = true;
    if (isRunning.compare_exchange_strong(expected, false)) {

        NES_INFO("Sending disconnect request to coordinator");
        disconnect();

        if (workerMobilityHandler) {
            workerMobilityHandler->stop();
            NES_INFO("triggered stopping of location update push thread");
        }
        bool successShutdownNodeEngine = nodeEngine->stop();
        if (!successShutdownNodeEngine) {
            NES_ERROR("NesWorker::stop node engine stop not successful");
            NES_THROW_RUNTIME_ERROR("NesWorker::stop  error while stopping node engine");
        }
        NES_INFO("NesWorker::stop : Node engine stopped successfully");
        nodeEngine.reset();

        NES_INFO("NesWorker: stopping rpc server");
        rpcServer->Shutdown();
        //shut down the async queue
        completionQueue->Shutdown();

        if (rpcThread->joinable()) {
            NES_INFO("NesWorker: join rpcThread");
            rpcThread->join();
        }

        rpcServer.reset();
        rpcThread.reset();
        if (statisticOutputThread && statisticOutputThread->joinable()) {
            NES_INFO("NesWorker: statistic collection thread join");
            statisticOutputThread->join();
        }
        statisticOutputThread.reset();

        return successShutdownNodeEngine;
    }
    NES_WARNING("NesWorker::stop: already stopped");
    return true;
}

void serializeOpenCLDeviceInfo(const NES::Runtime::OpenCLDeviceInfo& deviceInfo,
                               unsigned deviceIndex,
                               ::SerializedOpenCLDeviceInfo* serializedDeviceInfo) {
    serializedDeviceInfo->set_deviceid(deviceIndex);
    serializedDeviceInfo->set_platformvendor(deviceInfo.platformVendor);
    serializedDeviceInfo->set_platformname(deviceInfo.platformName);
    serializedDeviceInfo->set_devicename(deviceInfo.deviceName);
    serializedDeviceInfo->set_doublefpsupport(deviceInfo.doubleFPSupport);
    serializedDeviceInfo->add_maxworkitems(deviceInfo.maxWorkItems[0]);
    serializedDeviceInfo->add_maxworkitems(deviceInfo.maxWorkItems[1]);
    serializedDeviceInfo->add_maxworkitems(deviceInfo.maxWorkItems[2]);
    serializedDeviceInfo->set_deviceaddressbits(deviceInfo.deviceAddressBits);
    serializedDeviceInfo->set_devicetype(deviceInfo.deviceType);
    serializedDeviceInfo->set_deviceextensions(deviceInfo.deviceExtensions);
    serializedDeviceInfo->set_availableprocessors(deviceInfo.availableProcessors);
    serializedDeviceInfo->set_globalmemory(deviceInfo.globalMemory);
}

bool NesWorker::connect() {

    std::string coordinatorAddress =
        workerConfig->coordinatorHost.getValue() + ":" + std::to_string(workerConfig->coordinatorPort);
    NES_DEBUG("NesWorker::connect() Registering worker with coordinator at {}", coordinatorAddress);
    coordinatorRpcClient = CoordinatorRPCClient::create(coordinatorAddress);

    RegisterWorkerRequest registrationRequest;
    registrationRequest.set_workerid(workerConfig->workerId.getValue().getRawValue());
    registrationRequest.set_address(workerConfig->localWorkerHost.getValue());
    registrationRequest.set_grpcport(localWorkerRpcPort.load());
    registrationRequest.set_dataport(nodeEngine->getNetworkManager()->getServerDataPort());
    registrationRequest.set_numberofslots(workerConfig->numberOfSlots.getValue());
    registrationRequest.set_bandwidthinmbps(workerConfig->bandwidth.getValue());
    registrationRequest.set_latencyinms(workerConfig->latency.getValue());
    registrationRequest.mutable_registrationmetrics()->Swap(monitoringAgent->getRegistrationMetrics().serialize().get());
    registrationRequest.set_javaudfsupported(workerConfig->isJavaUDFSupported.getValue());
    registrationRequest.set_spatialtype(
        NES::Spatial::Util::SpatialTypeUtility::toProtobufEnum(workerConfig->nodeSpatialType.getValue()));
    for (auto i = 0u; i < nodeEngine->getOpenCLManager()->getDevices().size(); ++i) {
        serializeOpenCLDeviceInfo(nodeEngine->getOpenCLManager()->getDevices()[i].deviceInfo,
                                  i,
                                  registrationRequest.add_opencldevices());
    }

//    if (locationProvider) {
//        auto waypoint = registrationRequest.mutable_waypoint();
//        auto currentWaypoint = locationProvider->getCurrentWaypoint();
//        if (currentWaypoint.getTimestamp()) {
//            waypoint->set_timestamp(currentWaypoint.getTimestamp().value());
//        }
//        auto geolocation = waypoint->mutable_geolocation();
//        geolocation->set_lat(currentWaypoint.getLocation().getLatitude());
//        geolocation->set_lng(currentWaypoint.getLocation().getLongitude());
//    }

    bool successPRCRegister = coordinatorRpcClient->registerWorker(registrationRequest);

    NES_DEBUG("NesWorker::connect() Worker registered successfully and got id={}", coordinatorRpcClient->getId());
    workerId = coordinatorRpcClient->getId();
    monitoringAgent->setNodeId(workerId);
    if (successPRCRegister) {
        if (workerId != workerConfig->workerId) {
            if (workerConfig->workerId == INVALID_WORKER_NODE_ID) {
                // workerId value is written in the yaml for the first time
                NES_DEBUG("NesWorker::connect() Persisting workerId={} in yaml file for the first time.", workerId);
                bool success =
                    getWorkerConfiguration()->persistWorkerIdInYamlConfigFile(workerConfig->configPath, workerId, false);
                if (!success) {
                    NES_WARNING("NesWorker::connect() Could not persist workerId in yaml config file");
                } else {
                    NES_DEBUG("NesWorker::connect() Persisted workerId={} successfully in yaml file.", workerId);
                }
            } else {
                // a value was in the yaml file but it's being overwritten, because the coordinator assigns a new value
                NES_DEBUG("NesWorker::connect() Coordinator assigned new workerId value. Persisting workerId={} in yaml file",
                          workerId);
                bool success =
                    getWorkerConfiguration()->persistWorkerIdInYamlConfigFile(workerConfig->configPath, workerId, true);
                if (!success) {
                    NES_WARNING("NesWorker::connect() Could not persist workerId in yaml config file");
                } else {
                    NES_DEBUG("NesWorker::connect() Persisted workerId={} successfully in yaml file.", workerId);
                }
            }
            workerConfig->workerId = workerId;
        }
        NES_DEBUG("NesWorker::registerWorker rpc register success with id {}", workerId);
        connected = true;
        nodeEngine->setNodeId(workerId);
        healthCheckService = std::make_unique<WorkerHealthCheckService>(coordinatorRpcClient,
                                                                        HEALTH_SERVICE_NAME,
                                                                        this->inherited0::shared_from_this());
        NES_DEBUG("NesWorker start health check");
        healthCheckService->startHealthCheck();

        auto configPhysicalSourceTypes = workerConfig->physicalSourceTypes.getValues();
        if (!configPhysicalSourceTypes.empty()) {
            std::vector<PhysicalSourceTypePtr> physicalSourceTypes;
            for (const auto& physicalSourceType : configPhysicalSourceTypes) {
                physicalSourceTypes.push_back(physicalSourceType);
            }
            NES_DEBUG("NesWorker: start with register source");
            bool success = registerPhysicalSources(physicalSourceTypes);
            NES_DEBUG("registered= {}", success);
            NES_ASSERT(success, "cannot register");
        }
        return true;
    }
    NES_DEBUG("NesWorker::registerWorker rpc register failed");
    connected = false;
    return connected;
}

bool NesWorker::disconnect() {
    NES_DEBUG("NesWorker::disconnect()");
    bool successfulPRCUnregister = coordinatorRpcClient->unregisterNode();
    if (successfulPRCUnregister) {
        auto configPhysicalSourceTypes = workerConfig->physicalSourceTypes.getValues();
        if (!configPhysicalSourceTypes.empty()) {
            std::vector<PhysicalSourceTypePtr> physicalSourceTypes;
            for (auto& physicalSourceType : configPhysicalSourceTypes) {
                physicalSourceTypes.push_back(physicalSourceType);
            }
            NES_WARNING("NesWorker: stopping worker after de-registering the registered physical sources.");
            bool success = unregisterPhysicalSource(physicalSourceTypes);
            NES_INFO("unregistered = {}", success);
            NES_ASSERT(success, "cannot register");
        }

        NES_INFO("NesWorker::stopping health check");
        if (healthCheckService) {
            healthCheckService->stopHealthCheck();
        } else {
            NES_WARNING("No health check service was created");
        }

        NES_DEBUG("NesWorker::registerWorker rpc unregister success");
        connected = false;
        return true;
    }
    NES_DEBUG("NesWorker::registerWorker rpc unregister failed");
    return false;
}

bool NesWorker::unregisterPhysicalSource(const std::vector<PhysicalSourceTypePtr>& physicalSources) {
    bool success = coordinatorRpcClient->unregisterPhysicalSource(physicalSources);
    NES_DEBUG("NesWorker::unregisterPhysicalSource success={}", success);
    return success;
}

const Configurations::WorkerConfigurationPtr& NesWorker::getWorkerConfiguration() const { return workerConfig; }

bool NesWorker::registerPhysicalSources(const std::vector<PhysicalSourceTypePtr>& physicalSourceTypes) {
    NES_ASSERT(!physicalSourceTypes.empty(), "invalid physical sources");
    bool con = waitForConnect();
    NES_ASSERT(con, "cannot connect");
    NES_ASSERT(coordinatorRpcClient->registerPhysicalSources(physicalSourceTypes), "Worker failed to register physical sources");
    NES_DEBUG("NesWorker::registerPhysicalSources was succesfull");
    return true;
}

bool NesWorker::addParent(WorkerId pParentId) {
    bool con = waitForConnect();

    NES_ASSERT(con, "Connection failed");
    bool success = coordinatorRpcClient->addParent(pParentId);
    NES_DEBUG("NesWorker::addNewLink(parent only) success={}", success);
    return success;
}

bool NesWorker::replaceParent(WorkerId oldParentId, WorkerId newParentId) {
    bool con = waitForConnect();

    NES_ASSERT(con, "Connection failed");
    bool success = coordinatorRpcClient->replaceParent(oldParentId, newParentId);
    if (!success) {
        NES_WARNING("NesWorker::replaceParent() failed to replace oldParent={} with newParentId={}", oldParentId, newParentId);
    }
    NES_DEBUG("NesWorker::replaceParent() success={}", success);
    return success;
}

bool NesWorker::removeParent(WorkerId pParentId) {
    bool con = waitForConnect();

    NES_ASSERT(con, "Connection failed");
    bool success = coordinatorRpcClient->removeParent(pParentId);
    NES_DEBUG("NesWorker::removeLink(parent only) success={}", success);
    return success;
}

std::vector<Runtime::QueryStatisticsPtr> NesWorker::getQueryStatistics(SharedQueryId sharedQueryId) {
    return nodeEngine->getQueryStatistics(sharedQueryId);
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

bool NesWorker::notifyQueryStatusChange(SharedQueryId sharedQueryId,
                                        DecomposedQueryId decomposedQueryId,
                                        Runtime::Execution::ExecutableQueryPlanStatus newStatus) {
    NES_ASSERT(waitForConnect(), "cannot connect");
    NES_ASSERT2_FMT(newStatus != Runtime::Execution::ExecutableQueryPlanStatus::Stopped,
                    "Hard Stop called for query=" << sharedQueryId << " subQueryId=" << decomposedQueryId
                                                  << " should not call notifyQueryStatusChange");
    if (newStatus == Runtime::Execution::ExecutableQueryPlanStatus::Finished) {
        NES_DEBUG("NesWorker {} about to notify soft stop completion for query {} subPlan {}",
                  getWorkerId(),
                  sharedQueryId,
                  decomposedQueryId);
        return coordinatorRpcClient->notifySoftStopCompleted(sharedQueryId, decomposedQueryId);
    } else if (newStatus == Runtime::Execution::ExecutableQueryPlanStatus::ErrorState) {
        return true;// rpc to coordinator executed from async runner
    }
    return false;
}

bool NesWorker::canTriggerEndOfStream(SharedQueryId sharedQueryId,
                                      DecomposedQueryId decomposedQueryId,
                                      OperatorId sourceId,
                                      Runtime::QueryTerminationType terminationType) {
    NES_ASSERT(waitForConnect(), "cannot connect");
    NES_ASSERT(terminationType == Runtime::QueryTerminationType::Graceful, "invalid termination type");
    return coordinatorRpcClient->checkAndMarkForSoftStop(sharedQueryId, decomposedQueryId, sourceId);
}

bool NesWorker::notifySourceTermination(SharedQueryId sharedQueryId,
                                        DecomposedQueryId decomposedQueryId,
                                        OperatorId sourceId,
                                        Runtime::QueryTerminationType queryTermination) {
    NES_ASSERT(waitForConnect(), "cannot connect");
    if (queryTermination == Runtime::QueryTerminationType::Reconfiguration) {
        //todo #5133: notify coordinator of drain
        return true;
    } else {
        return coordinatorRpcClient->notifySourceStopTriggered(sharedQueryId, decomposedQueryId, sourceId, queryTermination);
    }
}

bool NesWorker::notifyQueryFailure(SharedQueryId sharedQueryId, DecomposedQueryId subQueryId, std::string errorMsg) {
    bool con = waitForConnect();
    NES_ASSERT(con, "Connection failed");
    bool success =
        coordinatorRpcClient->notifyQueryFailure(sharedQueryId, subQueryId, getWorkerId(), INVALID_OPERATOR_ID, errorMsg);
    NES_DEBUG("NesWorker::notifyQueryFailure success={}", success);
    return success;
}

bool NesWorker::notifyEpochTermination(uint64_t timestamp, uint64_t querySubPlanId) {
    bool con = waitForConnect();
    NES_ASSERT(con, "Connection failed");
    bool success = coordinatorRpcClient->notifyEpochTermination(timestamp, querySubPlanId);
    NES_DEBUG("NesWorker::propagatePunctuation success={}", success);
    return success;
}

bool NesWorker::notifyErrors(WorkerId pWorkerId, std::string errorMsg) {
    bool con = waitForConnect();
    NES_ASSERT(con, "Connection failed");
    NES_DEBUG("NesWorker::sendErrors worker {} going to send error={}", pWorkerId, errorMsg);
    bool success = coordinatorRpcClient->sendErrors(pWorkerId, errorMsg);
    NES_DEBUG("NesWorker::sendErrors success={}", success);
    return success;
}

void NesWorker::onFatalError(int signalNumber, std::string callstack) {
    std::string errorMsg;
    if (callstack.empty()) {
        NES_ERROR("onFatalError: signal [{}] error [{}] (enable NES_DEBUG to view stacktrace)", signalNumber, strerror(errno));
        std::cerr << "NesWorker failed fatally" << std::endl;// it's necessary for testing and it wont harm us to write to stderr
        std::cerr << "Error: " << strerror(errno) << std::endl;
        std::cerr << "Signal:" << std::to_string(signalNumber) << std::endl;
        // save errors in errorMsg
        errorMsg = "onFatalError: signal [" + std::to_string(signalNumber) + "] error [" + strerror(errno) + "] ";
    } else {
        NES_ERROR("onFatalError: signal [{}] error [{}] callstack {} ", signalNumber, strerror(errno), callstack);
        std::cerr << "NesWorker failed fatally" << std::endl;// it's necessary for testing and it wont harm us to write to stderr
        std::cerr << "Error: " << strerror(errno) << std::endl;
        std::cerr << "Signal: " << std::to_string(signalNumber) << std::endl;
        std::cerr << "Callstack:\n " << callstack << std::endl;
        // save errors in errorMsg
        errorMsg =
            "onFatalError: signal [" + std::to_string(signalNumber) + "] error [" + strerror(errno) + "] callstack " + callstack;
    }

    //send it to Coordinator
    notifyErrors(getWorkerId(), errorMsg);
#ifdef ENABLE_CORE_DUMPER
    detail::createCoreDump();
#endif
}

void NesWorker::onFatalException(std::shared_ptr<std::exception> ptr, std::string callstack) {
    std::string errorMsg;
    if (callstack.empty()) {
        NES_ERROR("onFatalException: exception=[{}] (enable NES_DEBUG to view stacktrace)", ptr->what());
        std::cerr << "NesWorker failed fatally" << std::endl;
        std::cerr << "Error: " << strerror(errno) << std::endl;
        std::cerr << "Exception: " << ptr->what() << std::endl;
        // save errors in errorMsg
        errorMsg = "onFatalException: exception=[" + std::string(ptr->what()) + "] ";
    } else {
        NES_ERROR("onFatalException: exception=[{}] callstack={}", ptr->what(), callstack);
        std::cerr << "NesWorker failed fatally" << std::endl;
        std::cerr << "Error: " << strerror(errno) << std::endl;
        std::cerr << "Exception: " << ptr->what() << std::endl;
        std::cerr << "Callstack:\n " << callstack << std::endl;
        // save errors in errorMsg
        errorMsg = "onFatalException: exception=[" + std::string(ptr->what()) + "] callstack=\n" + callstack;
    }
    //send it to Coordinator
    this->notifyErrors(this->getWorkerId(), errorMsg);
#ifdef ENABLE_CORE_DUMPER
    detail::createCoreDump();
#endif
}

WorkerId NesWorker::getWorkerId() const { return workerId; }

NES::Spatial::Mobility::Experimental::LocationProviderPtr NesWorker::getLocationProvider() { return locationProvider; }

NES::Spatial::Mobility::Experimental::ReconnectSchedulePredictorPtr NesWorker::getTrajectoryPredictor() {
    return trajectoryPredictor;
}

NES::Spatial::Mobility::Experimental::WorkerMobilityHandlerPtr NesWorker::getMobilityHandler() { return workerMobilityHandler; }

void NesWorker::notifyCheckpointToCoordinator(SharedQueryId sharedQueryId, uint64_t minWatermark) {
   coordinatorRpcClient->notifyCheckpoint(sharedQueryId, minWatermark);
}

}// namespace NES
