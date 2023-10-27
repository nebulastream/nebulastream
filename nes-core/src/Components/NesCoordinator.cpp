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
#include <Catalogs/Query/QueryCatalogService.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Source/SourceCatalogService.hpp>
#include <Catalogs/Topology/Index/LocationIndex.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyManagerService.hpp>
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
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <REST/RestServer.hpp>
#include <RequestProcessor/AsyncRequestProcessor.hpp>
#include <RequestProcessor/StorageHandles/StorageDataStructures.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Services/CoordinatorHealthCheckService.hpp>
#include <Services/LocationService.hpp>
#include <Services/MonitoringService.hpp>
#include <Services/QueryParsingService.hpp>
#include <Services/QueryService.hpp>
#include <Services/ReplicationService.hpp>
#include <Services/RequestProcessorService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/ThreadNaming.hpp>
#include <WorkQueues/RequestQueue.hpp>
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
      restPort(this->coordinatorConfiguration->restPort), rpcIp(this->coordinatorConfiguration->coordinatorIp),
      rpcPort(this->coordinatorConfiguration->rpcPort), enableMonitoring(this->coordinatorConfiguration->enableMonitoring) {
    NES_DEBUG("NesCoordinator() restIp={} restPort={} rpcIp={} rpcPort={}", restIp, restPort, rpcIp, rpcPort);
    setThreadName("NesCoordinator");
    topology = Topology::create();

    // TODO make compiler backend configurable
    auto cppCompiler = Compiler::CPPCompiler::create();
    auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
    queryParsingService = QueryParsingService::create(jitCompiler);
    auto locationIndex = std::make_shared<NES::Spatial::Index::Experimental::LocationIndex>();
    sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
    globalExecutionPlan = GlobalExecutionPlan::create();
    queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();

    sourceCatalogService = std::make_shared<SourceCatalogService>(sourceCatalog);
    topologyManagerService = std::make_shared<TopologyManagerService>(topology, locationIndex);
    queryRequestQueue = std::make_shared<RequestQueue>(this->coordinatorConfiguration->optimizer.queryBatchSize);
    globalQueryPlan = GlobalQueryPlan::create();

    queryCatalogService = std::make_shared<QueryCatalogService>(queryCatalog);

    z3::config cfg;
    cfg.set("timeout", 1000);
    cfg.set("model", false);
    cfg.set("type_check", false);
    auto z3Context = std::make_shared<z3::context>(cfg);

    queryRequestProcessorService = std::make_shared<RequestProcessorService>(globalExecutionPlan,
                                                                             topology,
                                                                             queryCatalogService,
                                                                             globalQueryPlan,
                                                                             sourceCatalog,
                                                                             udfCatalog,
                                                                             queryRequestQueue,
                                                                             this->coordinatorConfiguration,
                                                                             z3Context);

    RequestProcessor::Experimental::StorageDataStructures storageDataStructures = {this->coordinatorConfiguration,
                                                                                   topology,
                                                                                   globalExecutionPlan,
                                                                                   queryCatalogService,
                                                                                   globalQueryPlan,
                                                                                   sourceCatalog,
                                                                                   udfCatalog};

    auto asyncRequestExecutor = std::make_shared<RequestProcessor::Experimental::AsyncRequestProcessor>(storageDataStructures);
    bool enableNewRequestExecutor = this->coordinatorConfiguration->enableNewRequestExecutor.getValue();
    queryService = std::make_shared<QueryService>(enableNewRequestExecutor,
                                                  this->coordinatorConfiguration->optimizer,
                                                  queryCatalogService,
                                                  queryRequestQueue,
                                                  sourceCatalog,
                                                  queryParsingService,
                                                  udfCatalog,
                                                  asyncRequestExecutor,
                                                  z3Context);

    udfCatalog = Catalogs::UDF::UDFCatalog::create();
    locationService = std::make_shared<NES::LocationService>(topology, locationIndex);

    monitoringService = std::make_shared<MonitoringService>(topology, queryService, queryCatalogService, enableMonitoring);
    monitoringService->getMonitoringManager()->registerLogicalMonitoringStreams(this->coordinatorConfiguration);
}

NesCoordinator::~NesCoordinator() {
    stopCoordinator(true);
    NES_DEBUG("NesCoordinator::~NesCoordinator() map cleared");
    sourceCatalogService->reset();
    queryCatalogService->clearQueries();
}

NesWorkerPtr NesCoordinator::getNesWorker() { return worker; }

Runtime::NodeEnginePtr NesCoordinator::getNodeEngine() { return worker->getNodeEngine(); }
bool NesCoordinator::isCoordinatorRunning() { return isRunning; }

uint64_t NesCoordinator::startCoordinator(bool blocking) {
    NES_DEBUG("NesCoordinator start");

    auto expected = false;
    if (!isRunning.compare_exchange_strong(expected, true)) {
        NES_ASSERT2_FMT(false, "cannot start nes coordinator");
    }

    queryRequestProcessorThread = std::make_shared<std::thread>(([&]() {
        setThreadName("RqstProc");

        NES_INFO("NesCoordinator: started queryRequestProcessor");
        queryRequestProcessorService->start();
        NES_WARNING("NesCoordinator: finished queryRequestProcessor");
    }));

    NES_DEBUG("NesCoordinator: startCoordinatorRPCServer: Building GRPC Server");
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
        sourceCatalogService->registerLogicalSource(logicalSourceType.getValue()->getLogicalSourceName(), schema);
    }
    NES_DEBUG("NesCoordinator: Finished Registering Logical source");

    //start the coordinator worker that is the sink for all queryIdAndCatalogEntryMapping
    NES_DEBUG("NesCoordinator::startCoordinator: start nes worker");
    // Unconditionally set IP of internal worker and set IP and port of coordinator.
    coordinatorConfiguration->worker.coordinatorIp = rpcIp;
    coordinatorConfiguration->worker.coordinatorPort = rpcPort;
    coordinatorConfiguration->worker.localWorkerIp = rpcIp;
    // Ensure that coordinator and internal worker enable/disable monitoring together.
    coordinatorConfiguration->worker.enableMonitoring = enableMonitoring;
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
                                              queryCatalogService,
                                              sourceCatalogService,
                                              topologyManagerService,
                                              globalExecutionPlan,
                                              queryService,
                                              monitoringService,
                                              queryParsingService,
                                              globalQueryPlan,
                                              udfCatalog,
                                              worker->getNodeEngine()->getBufferManager(),
                                              locationService,
                                              allowedOrigin);
    restThread = std::make_shared<std::thread>(([&]() {
        setThreadName("nesREST");
        auto result = restServer->start();//this call is blocking
        if (!result) {
            NES_THROW_RUNTIME_ERROR("Error while staring rest server!");
        }
    }));

    NES_DEBUG("NesCoordinator::startCoordinatorRESTServer: ready");

    healthCheckService =
        std::make_shared<CoordinatorHealthCheckService>(topologyManagerService, HEALTH_SERVICE_NAME, coordinatorConfiguration);
    topologyManagerService->setHealthService(healthCheckService);
    NES_DEBUG("NesCoordinator start health check");
    healthCheckService->startHealthCheck();

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

ReplicationServicePtr NesCoordinator::getReplicationService() const { return replicationService; }

TopologyPtr NesCoordinator::getTopology() const { return topology; }

bool NesCoordinator::stopCoordinator(bool force) {
    NES_DEBUG("NesCoordinator: stopCoordinator force={}", force);
    auto expected = true;
    if (isRunning.compare_exchange_strong(expected, false)) {

        NES_DEBUG("NesCoordinator::stop health check");
        healthCheckService->stopHealthCheck();

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

        queryRequestProcessorService->shutDown();
        if (queryRequestProcessorThread->joinable()) {
            NES_DEBUG("NesCoordinator: join queryRequestProcessorThread");
            queryRequestProcessorThread->join();
            NES_DEBUG("NesCoordinator: joined queryRequestProcessorThread");
        } else {
            NES_ERROR("NesCoordinator: query processor thread not joinable");
            NES_THROW_RUNTIME_ERROR("Error while stopping thread->join");
        }

        NES_DEBUG("NesCoordinator: stopping rpc server");
        rpcServer->Shutdown();
        rpcServer->Wait();

        if (rpcThread->joinable()) {
            NES_DEBUG("NesCoordinator: join rpcThread");
            rpcThread->join();
            rpcThread.reset();

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
    NES_ASSERT(sourceCatalogService, "null sourceCatalogService");
    NES_ASSERT(topologyManagerService, "null topologyManagerService");
    this->replicationService = std::make_shared<ReplicationService>(this->inherited0::shared_from_this());
    CoordinatorRPCServer service(queryService,
                                 topologyManagerService,
                                 sourceCatalogService,
                                 queryCatalogService,
                                 monitoringService->getMonitoringManager(),
                                 this->replicationService,
                                 locationService,
                                 queryParsingService);

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

    rpcServer = builder.BuildAndStart();
    prom->set_value(true);
    NES_DEBUG("NesCoordinator: buildAndStartGRPCServerServer listening on address={}", address);
    rpcServer->Wait();//blocking call
    NES_DEBUG("NesCoordinator: buildAndStartGRPCServer end listening");
}

std::vector<Runtime::QueryStatisticsPtr> NesCoordinator::getQueryStatistics(QueryId queryId) {
    NES_INFO("NesCoordinator: Get query statistics for query Id {}", queryId);
    return worker->getNodeEngine()->getQueryStatistics(queryId);
}

QueryServicePtr NesCoordinator::getQueryService() { return queryService; }

QueryCatalogServicePtr NesCoordinator::getQueryCatalogService() { return queryCatalogService; }

Catalogs::UDF::UDFCatalogPtr NesCoordinator::getUDFCatalog() { return udfCatalog; }

MonitoringServicePtr NesCoordinator::getMonitoringService() { return monitoringService; }

GlobalQueryPlanPtr NesCoordinator::getGlobalQueryPlan() { return globalQueryPlan; }

void NesCoordinator::onFatalError(int, std::string) {}

void NesCoordinator::onFatalException(const std::shared_ptr<std::exception>, std::string) {}

SourceCatalogServicePtr NesCoordinator::getSourceCatalogService() const { return sourceCatalogService; }

TopologyManagerServicePtr NesCoordinator::getTopologyManagerService() const { return topologyManagerService; }

LocationServicePtr NesCoordinator::getLocationService() const { return locationService; }

GlobalExecutionPlanPtr NesCoordinator::getGlobalExecutionPlan() const { return globalExecutionPlan; }

}// namespace NES
