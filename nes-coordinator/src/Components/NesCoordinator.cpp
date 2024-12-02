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

#include <Catalogs/Query/QueryCatalog.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Topology/Index/LocationIndex.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/LogicalSourceType.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Exceptions/ErrorListener.hpp>
#include <GRPC/CoordinatorRPCServer.hpp>
#include <GRPC/HealthCheckRPCServer.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Health.pb.h>
#include <Monitoring/MonitoringManager.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <REST/RestServer.hpp>
#include <RequestProcessor/AsyncRequestProcessor.hpp>
#include <RequestProcessor/StorageHandles/StorageDataStructures.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryStatistics.hpp>
#include <Services/CoordinatorHealthCheckService.hpp>
#include <Services/MonitoringService.hpp>
#include <Services/PlacementAmendment/PlacementAmendmentHandler.hpp>
#include <Services/QueryParsingService.hpp>
#include <Services/RequestHandlerService.hpp>
#include <StatisticCollection/QueryGeneration/DefaultStatisticQueryGenerator.hpp>
#include <StatisticCollection/StatisticCache/DefaultStatisticCache.hpp>
#include <StatisticCollection/StatisticProbeHandling/DefaultStatisticProbeGenerator.hpp>
#include <StatisticCollection/StatisticProbeHandling/StatisticProbeHandler.hpp>
#include <StatisticCollection/StatisticRegistry/StatisticRegistry.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/ThreadNaming.hpp>
#include <csignal>
#include <fstream>
#include <grpcpp/ext/health_check_service_server_builder_option.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/server_builder.h>
#include <memory>
#include <thread>
#include <z3++.h>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

namespace NES {

using namespace Configurations;

extern void Exceptions::installGlobalErrorListener(std::shared_ptr<ErrorListener> const&);

NesCoordinator::NesCoordinator(CoordinatorConfigurationPtr coordinatorConfiguration)
    : coordinatorConfiguration(std::move(coordinatorConfiguration)), restIp(this->coordinatorConfiguration->restIp),
      restPort(this->coordinatorConfiguration->restPort), rpcIp(this->coordinatorConfiguration->coordinatorHost),
      rpcPort(this->coordinatorConfiguration->rpcPort), enableMonitoring(this->coordinatorConfiguration->enableMonitoring),
      numberOfBuffersPerEpoch(this->coordinatorConfiguration->numberOfBuffersPerEpoch) {
    NES_DEBUG("NesCoordinator() restIp={} restPort={} rpcIp={} rpcPort={}", restIp, restPort, rpcIp, rpcPort);
    setThreadName("NesCoordinator");

    // TODO make compiler backend configurable
    auto cppCompiler = Compiler::CPPCompiler::create();
    auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
    queryParsingService = QueryParsingService::create(jitCompiler);
    sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
    globalExecutionPlan = Optimizer::GlobalExecutionPlan::create();
    queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();

    topology = Topology::create();
    coordinatorHealthCheckService =
        std::make_shared<CoordinatorHealthCheckService>(topology, HEALTH_SERVICE_NAME, this->coordinatorConfiguration);
    globalQueryPlan = GlobalQueryPlan::create();

    z3::config cfg;
    cfg.set("timeout", 1000);
    cfg.set("model", false);
    cfg.set("type_check", false);
    auto z3Context = std::make_shared<z3::context>(cfg);

    // For now, we hardcode the usage of the DefaultStatisticQueryGenerator, see issue #4687
    auto statisticRegistry = Statistic::StatisticRegistry::create();
    statisticProbeHandler = Statistic::StatisticProbeHandler::create(statisticRegistry,
                                                                     Statistic::DefaultStatisticProbeGenerator::create(),
                                                                     Statistic::DefaultStatisticCache::create(),
                                                                     topology);
    RequestProcessor::StorageDataStructures storageDataStructures = {this->coordinatorConfiguration,
                                                                     topology,
                                                                     globalExecutionPlan,
                                                                     globalQueryPlan,
                                                                     queryCatalog,
                                                                     sourceCatalog,
                                                                     udfCatalog,
                                                                     statisticProbeHandler};

    auto asyncRequestExecutor = std::make_shared<RequestProcessor::AsyncRequestProcessor>(storageDataStructures);

    placementAmendmentHandler = std::make_shared<Optimizer::PlacementAmendmentHandler>(
        this->coordinatorConfiguration->optimizer.placementAmendmentThreadCount.getValue());

    requestHandlerService = std::make_shared<RequestHandlerService>(this->coordinatorConfiguration->optimizer,
                                                                    queryParsingService,
                                                                    queryCatalog,
                                                                    sourceCatalog,
                                                                    udfCatalog,
                                                                    asyncRequestExecutor,
                                                                    z3Context,
                                                                    Statistic::DefaultStatisticQueryGenerator::create(),
                                                                    statisticRegistry,
                                                                    placementAmendmentHandler);

    udfCatalog = Catalogs::UDF::UDFCatalog::create();

    monitoringService = std::make_shared<MonitoringService>(topology, requestHandlerService, queryCatalog, enableMonitoring);
    monitoringService->getMonitoringManager()->registerLogicalMonitoringStreams(this->coordinatorConfiguration);
}

NesCoordinator::~NesCoordinator() {
    stopCoordinator(true);
    NES_DEBUG("NesCoordinator::~NesCoordinator() map cleared");
    sourceCatalog->reset();
    queryCatalog.reset();
}

NesWorkerPtr NesCoordinator::getNesWorker() { return worker; }

Statistic::StatisticProbeHandlerPtr NesCoordinator::getStatisticProbeHandler() const { return statisticProbeHandler; }

Runtime::NodeEnginePtr NesCoordinator::getNodeEngine() { return worker->getNodeEngine(); }

bool NesCoordinator::isCoordinatorRunning() { return isRunning; }

uint64_t NesCoordinator::startCoordinator(bool blocking) {
    NES_DEBUG("NesCoordinator start");

    auto expected = false;
    if (!isRunning.compare_exchange_strong(expected, true)) {
        NES_ASSERT2_FMT(false, "cannot start nes coordinator");
    }

    std::shared_ptr<std::promise<bool>> promRPC = std::make_shared<std::promise<bool>>();

    rpcThread = std::make_shared<std::thread>(([this, promRPC]() {
        setThreadName("nesRPC");

        NES_DEBUG("NesCoordinator: buildAndStartGRPCServer");
        buildAndStartGRPCServer(promRPC);
        NES_DEBUG("NesCoordinator: buildAndStartGRPCServer: end listening");
    }));
    promRPC->get_future().get();
    NES_DEBUG("NesCoordinator:buildAndStartGRPCServer: ready");

    NES_DEBUG("NesCoordinator: Register Logical sources");
    for (const auto& logicalSourceType : coordinatorConfiguration->logicalSourceTypes.getValues()) {
        auto schema = Schema::createFromSchemaType(logicalSourceType.getValue()->getSchemaType());
        NES_ASSERT(requestHandlerService->queueRegisterLogicalSourceRequest(logicalSourceType.getValue()->getLogicalSourceName(),
                                                                            schema),
                   "Could not create logical source");
    }
    NES_DEBUG("NesCoordinator: Finished Registering Logical source");

    //start the coordinator worker that is the sink for all queryIdAndCatalogEntryMapping
    NES_DEBUG("NesCoordinator::startCoordinator: start nes worker");
    // Unconditionally set IP of internal worker and set IP and port of coordinator.
    coordinatorConfiguration->worker.coordinatorHost = rpcIp;
    coordinatorConfiguration->worker.coordinatorPort = rpcPort;
    // Ensure that coordinator and internal worker enable/disable monitoring together.
    coordinatorConfiguration->worker.enableMonitoring = enableMonitoring;
    coordinatorConfiguration->worker.numberOfBuffersPerEpoch = numberOfBuffersPerEpoch;
    // Create a copy of the worker configuration to pass to the NesWorker.
    auto workerConfig = std::make_shared<WorkerConfiguration>(coordinatorConfiguration->worker);
    worker = std::make_shared<NesWorker>(std::move(workerConfig), monitoringService->getMonitoringManager()->getMetricStore());
    worker->start(/**blocking*/ false, /**withConnect*/ true);

    NES::Exceptions::installGlobalErrorListener(worker);

    //Start rest that accepts queryIdAndCatalogEntryMapping form the outsides
    NES_DEBUG("NesCoordinator starting rest server");

    //setting the allowed origins for http request to the rest server
    std::optional<std::string> allowedOrigin = std::nullopt;
    auto originString = coordinatorConfiguration->restServerCorsAllowedOrigin.getValue();
    if (!originString.empty()) {
        NES_INFO("CORS: allow origin: {}", originString);
        allowedOrigin = originString;
    }

    restServer = std::make_shared<RestServer>(restIp,
                                              restPort,
                                              this->inherited0::weak_from_this(),
                                              queryCatalog,
                                              topology,
                                              globalExecutionPlan,
                                              requestHandlerService,
                                              monitoringService,
                                              queryParsingService,
                                              globalQueryPlan,
                                              udfCatalog,
                                              worker->getNodeEngine()->getBufferManager(),
                                              allowedOrigin);
    restThread = std::make_shared<std::thread>(([&]() {
        setThreadName("nesREST");
        auto result = restServer->start();//this call is blocking
        if (!result) {
            NES_THROW_RUNTIME_ERROR("Error while staring rest server!");
        }
    }));
    NES_DEBUG("NesCoordinator::startCoordinatorRESTServer: ready");

    NES_DEBUG("NesCoordinator start health check");
    coordinatorHealthCheckService->startHealthCheck();

    statisticOutputThread = std::make_shared<std::thread>(([this]() {
        NES_DEBUG("NesWorker: start statistic collection");
        std::ofstream statisticsFile;
        statisticsFile.open("coordinator.csv", std::ios::out);
        if (statisticsFile.is_open()) {
            statisticsFile << "timestamp,";
            statisticsFile << "queryId,";
            statisticsFile << "subPlanId,";
            statisticsFile << "processedTasks,";
            statisticsFile << "processedTuple,";
            statisticsFile << "processedBuffers,";
            statisticsFile << "processedWatermarks,";
            statisticsFile << "latencyAVG,";
            statisticsFile << "queueSizeAVG,";
            statisticsFile << "availableGlobalBufferAVG,";
            statisticsFile << "availableFixedBufferAVG\n";
            while (isRunning) {
                auto ts = std::chrono::system_clock::now();
                auto timeNow = std::chrono::system_clock::to_time_t(ts);
                auto stats = worker->getNodeEngine()->getQueryStatistics(true);
                for (auto& query : stats) {
                    statisticsFile << std::put_time(std::localtime(&timeNow), "%Y-%m-%d %X") << ","
                                   << query.getQueryStatisticsAsString() << "\n";
                    statisticsFile.flush();
                }
                sleep(1);
            }
        }
        NES_DEBUG("NesWorker: statistic collection end");
        statisticsFile.close();
    }));

    // Start placement amendment handler
    placementAmendmentHandler->start();

    if (blocking) {//blocking is for the starter to wait here for user to send query
        NES_DEBUG("NesCoordinator started, join now and waiting for work");
        restThread->join();
        NES_DEBUG("NesCoordinator Required stopping");
    } else {//non-blocking is used for tests to continue execution
        NES_DEBUG("NesCoordinator started, return without blocking on port {}", rpcPort);
        return rpcPort;
    }
    return 0UL;
}

Catalogs::Source::SourceCatalogPtr NesCoordinator::getSourceCatalog() const { return sourceCatalog; }

TopologyPtr NesCoordinator::getTopology() const { return topology; }

bool NesCoordinator::stopCoordinator(bool force) {
    NES_DEBUG("NesCoordinator: stopCoordinator force={}", force);
    auto expected = true;
    if (isRunning.compare_exchange_strong(expected, false)) {

        NES_DEBUG("NesCoordinator::stop placement amendment handler");
        //Terminate placement amendment handler
        placementAmendmentHandler->shutDown();

        NES_DEBUG("NesCoordinator::stop health check");
        coordinatorHealthCheckService->stopHealthCheck();

        bool successShutdownWorker = worker->stop(force);
        if (!successShutdownWorker) {
            NES_ERROR("NesCoordinator::stop node engine stop not successful");
            NES_THROW_RUNTIME_ERROR("NesCoordinator::stop error while stopping node engine");
        }
        NES_DEBUG("NesCoordinator::stop Node engine stopped successfully");

        NES_DEBUG("NesCoordinator: stopping rest server");
        bool successStopRest = restServer->stop();
        if (!successStopRest) {
            NES_ERROR("NesCoordinator::stopCoordinator: error while stopping restServer");
            NES_THROW_RUNTIME_ERROR("Error while stopping NesCoordinator");
        }
        NES_DEBUG("NesCoordinator: rest server stopped {}", successStopRest);

        if (restThread->joinable()) {
            NES_DEBUG("NesCoordinator: join restThread");
            restThread->join();
        } else {
            NES_ERROR("NesCoordinator: rest thread not joinable");
            NES_THROW_RUNTIME_ERROR("Error while stopping thread->join");
        }

        NES_DEBUG("NesCoordinator: stopping rpc server");
        rpcServer->Shutdown();
        rpcServer->Wait();

        if (rpcThread->joinable()) {
            NES_DEBUG("NesCoordinator: join rpcThread");
            rpcThread->join();
            rpcThread.reset();
            if (statisticOutputThread && statisticOutputThread->joinable()) {
                NES_DEBUG("NesWorker: statistic collection thread join");
                statisticOutputThread->join();
            }
            statisticOutputThread.reset();
        } else {
            NES_ERROR("NesCoordinator: rpc thread not joinable");
            NES_THROW_RUNTIME_ERROR("Error while stopping thread->join");
        }
        return true;
    }
    NES_DEBUG("NesCoordinator: already stopped");
    return true;
}

void NesCoordinator::buildAndStartGRPCServer(const std::shared_ptr<std::promise<bool>>& prom) {
    grpc::ServerBuilder builder;
    NES_ASSERT(topology, "null topology");

    CoordinatorRPCServer service(requestHandlerService,
                                 topology,
                                 queryCatalog,
                                 monitoringService->getMonitoringManager(),
                                 queryParsingService,
                                 coordinatorHealthCheckService);

    std::string address = rpcIp + ":" + std::to_string(rpcPort);
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<grpc::HealthCheckServiceInterface> healthCheckServiceInterface;
    std::unique_ptr<grpc::ServerBuilderOption> option(
        new grpc::HealthCheckServiceServerBuilderOption(std::move(healthCheckServiceInterface)));
    builder.SetOption(std::move(option));
    HealthCheckRPCServer healthCheckServiceImpl;
    healthCheckServiceImpl.SetStatus(
        HEALTH_SERVICE_NAME,
        grpc::health::v1::HealthCheckResponse_ServingStatus::HealthCheckResponse_ServingStatus_SERVING);
    builder.RegisterService(&healthCheckServiceImpl);

    // Create a signal handler to give a meaningful error message in case the rpcServer cannot be built.
    auto signalHandler = [](int signal) {
        std::cerr << "Error with signal: " << signal
                  << " while building and starting an GRPC server. "
                     "Possible reason: LLD compilation flag is true.\n";
        _exit(1);// Exit immediately, avoiding complex logic in signal handler.
    };
    signal(SIGSEGV, signalHandler);

    rpcServer = builder.BuildAndStart();
    prom->set_value(true);
    NES_DEBUG("NesCoordinator: buildAndStartGRPCServerServer listening on address={}", address);
    rpcServer->Wait();//blocking call
    NES_DEBUG("NesCoordinator: buildAndStartGRPCServer end listening");
}

std::vector<Runtime::QueryStatisticsPtr> NesCoordinator::getQueryStatistics(SharedQueryId sharedQueryId) {
    NES_INFO("NesCoordinator: Get query statistics for shared query Id {}", sharedQueryId);
    return worker->getNodeEngine()->getQueryStatistics(sharedQueryId);
}

RequestHandlerServicePtr NesCoordinator::getRequestHandlerService() { return requestHandlerService; }

Catalogs::Query::QueryCatalogPtr NesCoordinator::getQueryCatalog() { return queryCatalog; }

Catalogs::UDF::UDFCatalogPtr NesCoordinator::getUDFCatalog() { return udfCatalog; }

MonitoringServicePtr NesCoordinator::getMonitoringService() { return monitoringService; }

GlobalQueryPlanPtr NesCoordinator::getGlobalQueryPlan() { return globalQueryPlan; }

void NesCoordinator::onFatalError(int, std::string) {}

void NesCoordinator::onFatalException(const std::shared_ptr<std::exception>, std::string) {}

LocationServicePtr NesCoordinator::getLocationService() const { return locationService; }

Optimizer::GlobalExecutionPlanPtr NesCoordinator::getGlobalExecutionPlan() const { return globalExecutionPlan; }

}// namespace NES
