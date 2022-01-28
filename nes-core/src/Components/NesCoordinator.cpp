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
#include <Catalogs/UDF/UdfCatalog.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <REST/RestServer.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Services/QueryService.hpp>
#include <Services/RequestProcessorService.hpp>
#include <Services/StreamCatalogService.hpp>
#include <Services/TopologyManagerService.hpp>
#include <Util/Logger.hpp>
#include <WorkQueues/RequestQueue.hpp>
#include <grpcpp/server_builder.h>
#include <memory>
#include <thread>

//GRPC Includes
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Configurations/ConfigurationOption.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <GRPC/CoordinatorRPCServer.hpp>
#include <Optimizer/Phases/MemoryLayoutSelectionPhase.hpp>
#include <Services/MaintenanceService.hpp>
#include <Services/MonitoringService.hpp>
#include <Services/QueryParsingService.hpp>
#include <Services/StreamCatalogService.hpp>
#include <Services/TopologyManagerService.hpp>
#include <Topology/Topology.hpp>
#include <Util/ThreadNaming.hpp>
#include <grpcpp/health_check_service_interface.h>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

namespace NES {

using namespace Configurations;

NesCoordinator::NesCoordinator(CoordinatorConfigurationPtr coordinatorConfig, WorkerConfigurationPtr workerConfiguration)
    : NesCoordinator(std::move(coordinatorConfig)) {
    workerConfig = std::move(workerConfiguration);
}

NesCoordinator::NesCoordinator(CoordinatorConfigurationPtr coordinatorConfiguration)
    : coordinatorConfiguration(std::move(coordinatorConfiguration)),
      restIp(this->coordinatorConfiguration->getRestIp()->getValue()),
      restPort(this->coordinatorConfiguration->getRestPort()->getValue()),
      rpcIp(this->coordinatorConfiguration->getCoordinatorIp()->getValue()),
      rpcPort(this->coordinatorConfiguration->getRpcPort()->getValue()),
      numberOfSlots(this->coordinatorConfiguration->getNumberOfSlots()->getValue()),
      numberOfWorkerThreads(this->coordinatorConfiguration->getNumWorkerThreads()->getValue()),
      numberOfBuffersInGlobalBufferManager(this->coordinatorConfiguration->getNumberOfBuffersInGlobalBufferManager()->getValue()),
      numberOfBuffersPerWorker(this->coordinatorConfiguration->getNumberOfBuffersPerWorker()->getValue()),
      numberOfBuffersInSourceLocalBufferPool(
          this->coordinatorConfiguration->getNumberOfBuffersInSourceLocalBufferPool()->getValue()),
      bufferSizeInBytes(this->coordinatorConfiguration->getBufferSizeInBytes()->getValue()),
      enableMonitoring(this->coordinatorConfiguration->getEnableMonitoring()->getValue()) {
    NES_DEBUG("NesCoordinator() restIp=" << restIp << " restPort=" << restPort << " rpcIp=" << rpcIp << " rpcPort=" << rpcPort);
    MDC::put("threadName", "NesCoordinator");
    topology = Topology::create();
    workerRpcClient = std::make_shared<WorkerRPCClient>();
    monitoringService = std::make_shared<MonitoringService>(workerRpcClient, topology, enableMonitoring);

    // TODO make compiler backend configurable
    auto cppCompiler = Compiler::CPPCompiler::create();
    auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
    auto queryParsingService = QueryParsingService::create(jitCompiler);

    streamCatalog = std::make_shared<SourceCatalog>(queryParsingService);
    globalExecutionPlan = GlobalExecutionPlan::create();
    queryCatalog = std::make_shared<QueryCatalog>();

    streamCatalogService = std::make_shared<StreamCatalogService>(streamCatalog);
    topologyManagerService = std::make_shared<TopologyManagerService>(topology);
    workerRpcClient = std::make_shared<WorkerRPCClient>();
    queryRequestQueue = std::make_shared<RequestQueue>(this->coordinatorConfiguration->getQueryBatchSize()->getValue());
    globalQueryPlan = GlobalQueryPlan::create();

    auto memoryLayoutPolicyString = this->coordinatorConfiguration->getMemoryLayoutPolicy()->getValue();
    if (!Optimizer::stringToMemoryLayoutPolicy.contains(memoryLayoutPolicyString)) {
        NES_FATAL_ERROR("Unrecognized MemoryLayoutPolicy Detected " << memoryLayoutPolicyString);
    }
    auto memoryLayoutPolicy = Optimizer::stringToMemoryLayoutPolicy.find(memoryLayoutPolicyString)->second;

    std::string queryMergerRuleName = this->coordinatorConfiguration->getQueryMergerRule()->getValue();
    auto found = Optimizer::stringToMergerRuleEnum.find(queryMergerRuleName);

    bool performOnlySourceOperatorExpansion = this->coordinatorConfiguration->getPerformOnlySourceOperatorExpansion()->getValue();
    if (found != Optimizer::stringToMergerRuleEnum.end()) {
        queryRequestProcessorService = std::make_shared<RequestProcessorService>(globalExecutionPlan,
                                                                                 topology,
                                                                                 queryCatalog,
                                                                                 globalQueryPlan,
                                                                                 streamCatalog,
                                                                                 workerRpcClient,
                                                                                 queryRequestQueue,
                                                                                 found->second,
                                                                                 memoryLayoutPolicy,
                                                                                 performOnlySourceOperatorExpansion);
    } else {
        NES_FATAL_ERROR("Unrecognized Query Merger Rule Detected " << queryMergerRuleName);
    }

    queryService = std::make_shared<QueryService>(queryCatalog,
                                                  queryRequestQueue,
                                                  streamCatalog,
                                                  queryParsingService,
                                                  this->coordinatorConfiguration->getEnableSemanticQueryValidation()->getValue());

    udfCatalog = Catalogs::UdfCatalog::create();
    maintenanceService =
        std::make_shared<NES::Experimental::MaintenanceService>(topology, queryCatalog, queryRequestQueue, globalExecutionPlan);
}

NesCoordinator::~NesCoordinator() {
    NES_DEBUG("NesCoordinator::~NesCoordinator()");
    NES_DEBUG("NesCoordinator::~NesCoordinator() ptr usage=" << workerRpcClient.use_count());

    stopCoordinator(true);
    NES_DEBUG("NesCoordinator::~NesCoordinator() map cleared");
    streamCatalog->reset();
    queryCatalog->clearQueries();

    topology.reset();
    streamCatalog.reset();
    globalExecutionPlan.reset();
    queryCatalog.reset();
    workerRpcClient.reset();
    queryRequestQueue.reset();
    queryRequestProcessorService.reset();
    queryService.reset();
    monitoringService.reset();
    maintenanceService.reset();
    queryRequestProcessorThread.reset();
    worker.reset();
    streamCatalogService.reset();
    topologyManagerService.reset();
    restThread.reset();
    restServer.reset();
    rpcThread.reset();
    coordinatorConfiguration.reset();
    workerConfig.reset();

    NES_ASSERT(topology.use_count() == 0, "NesCoordinator topology leaked");
    NES_ASSERT(streamCatalog.use_count() == 0, "NesCoordinator sourceCatalog leaked");
    NES_ASSERT(globalExecutionPlan.use_count() == 0, "NesCoordinator globalExecutionPlan leaked");
    NES_ASSERT(queryCatalog.use_count() == 0, "NesCoordinator queryCatalog leaked");
    NES_ASSERT(workerRpcClient.use_count() == 0, "NesCoordinator workerRpcClient leaked");
    NES_ASSERT(queryRequestQueue.use_count() == 0, "NesCoordinator queryRequestQueue leaked");
    NES_ASSERT(queryRequestProcessorService.use_count() == 0, "NesCoordinator queryRequestProcessorService leaked");
    NES_ASSERT(queryService.use_count() == 0, "NesCoordinator queryService leaked");

    NES_ASSERT(rpcThread.use_count() == 0, "NesCoordinator rpcThread leaked");
    NES_ASSERT(queryRequestProcessorThread.use_count() == 0, "NesCoordinator queryRequestProcessorThread leaked");
    NES_ASSERT(worker.use_count() == 0, "NesCoordinator worker leaked");
    NES_ASSERT(restServer.use_count() == 0, "NesCoordinator restServer leaked");
    NES_ASSERT(restThread.use_count() == 0, "NesCoordinator restThread leaked");
    NES_ASSERT(streamCatalogService.use_count() == 0, "NesCoordinator streamCatalogService leaked");
    NES_ASSERT(topologyManagerService.use_count() == 0, "NesCoordinator topologyManagerService leaked");
    NES_ASSERT(maintenanceService.use_count() == 0, "NesCoordinator maintenanceService leaked");
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

    NES_DEBUG("NesCoordinator: Register Logical source");
    for (auto& logicalSource : coordinatorConfiguration->getLogicalSources()) {
        streamCatalogService->registerLogicalSource(logicalSource->getLogicalSourceName(), logicalSource->getSchema());
    }
    NES_DEBUG("NesCoordinator: Finished Registering Logical source");

    //start the coordinator worker that is the sink for all queries
    NES_DEBUG("NesCoordinator::startCoordinator: start nes worker");
    if (workerConfig) {
        NES_DEBUG("Use provided external worker config");
    } else {
        NES_DEBUG("Use provided default worker config");
        workerConfig = WorkerConfiguration::create();
        workerConfig->resetWorkerOptions();
        workerConfig->setCoordinatorIp(rpcIp);
        workerConfig->setLocalWorkerIp(rpcIp);
        workerConfig->setCoordinatorPort(rpcPort);
        workerConfig->setRpcPort(rpcPort + 1);
        workerConfig->setDataPort(rpcPort + 2);
        workerConfig->setNumberOfSlots(numberOfSlots);
        workerConfig->setNumWorkerThreads(numberOfWorkerThreads);
        workerConfig->setBufferSizeInBytes(bufferSizeInBytes);
        workerConfig->setNumberOfBuffersInSourceLocalBufferPool(numberOfBuffersInSourceLocalBufferPool);
        workerConfig->setNumberOfBuffersPerWorker(numberOfBuffersPerWorker);
        workerConfig->setNumberOfBuffersInGlobalBufferManager(numberOfBuffersInGlobalBufferManager);
        workerConfig->setEnableMonitoring(enableMonitoring);
    }

    worker = std::make_shared<NesWorker>(workerConfig);
    worker->start(/**blocking*/ false, /**withConnect*/ true);

    //Start rest that accepts queries form the outsides
    NES_DEBUG("NesCoordinator starting rest server");
    restServer = std::make_shared<RestServer>(restIp,
                                              restPort,
                                              this->inherited0::weak_from_this(),
                                              queryCatalog,
                                              streamCatalog,
                                              topology,
                                              globalExecutionPlan,
                                              queryService,
                                              monitoringService,
                                              maintenanceService,
                                              globalQueryPlan,
                                              udfCatalog,
                                              worker->getNodeEngine()->getBufferManager());
    restThread = std::make_shared<std::thread>(([&]() {
        setThreadName("nesREST");
        restServer->start();//this call is blocking
        NES_DEBUG("NesCoordinator: startRestServer thread terminates");
    }));
    NES_DEBUG("NESWorker::startCoordinatorRESTServer: ready");

    if (blocking) {//blocking is for the starter to wait here for user to send query
        NES_DEBUG("NesCoordinator started, join now and waiting for work");
        restThread->join();
        NES_DEBUG("NesCoordinator Required stopping");
    } else {//non-blocking is used for tests to continue execution
        NES_DEBUG("NesCoordinator started, return without blocking on port " << rpcPort);
        return rpcPort;
    }
    return 0UL;
}

bool NesCoordinator::stopCoordinator(bool force) {
    NES_DEBUG("NesCoordinator: stopCoordinator force=" << force);
    auto expected = true;
    if (isRunning.compare_exchange_strong(expected, false)) {
        NES_DEBUG("NesCoordinator: stopping rest server");
        bool successStopRest = restServer->stop();
        if (!successStopRest) {
            NES_ERROR("NesCoordinator::stopCoordinator: error while stopping restServer");
            throw Exception("Error while stopping NesCoordinator");
        }
        NES_DEBUG("NesCoordinator: rest server stopped " << successStopRest);

        if (restThread->joinable()) {
            NES_DEBUG("NesCoordinator: join restThread");
            restThread->join();
        } else {
            NES_ERROR("NesCoordinator: rest thread not joinable");
            throw Exception("Error while stopping thread->join");
        }

        queryRequestProcessorService->shutDown();
        if (queryRequestProcessorThread->joinable()) {
            NES_DEBUG("NesCoordinator: join rpcThread");
            queryRequestProcessorThread->join();
        } else {
            NES_ERROR("NesCoordinator: query processor thread not joinable");
            throw Exception("Error while stopping thread->join");
        }

        bool successShutdownWorker = worker->stop(force);
        if (!successShutdownWorker) {
            NES_ERROR("NesCoordinator::stop node engine stop not successful");
            throw Exception("NesCoordinator::stop error while stopping node engine");
        }
        NES_DEBUG("NesCoordinator::stop Node engine stopped successfully");

        NES_DEBUG("NesCoordinator: stopping rpc server");
        rpcServer->Shutdown();

        rpcServer->Wait();

        if (rpcThread->joinable()) {
            NES_DEBUG("NesCoordinator: join rpcThread");
            rpcThread->join();
            rpcThread.reset();
        } else {
            NES_ERROR("NesCoordinator: rpc thread not joinable");
            throw Exception("Error while stopping thread->join");
        }
        return true;
    }
    NES_DEBUG("NesCoordinator: already stopped");
    return true;
}

void NesCoordinator::buildAndStartGRPCServer(const std::shared_ptr<std::promise<bool>>& prom) {
    grpc::ServerBuilder builder;
    NES_ASSERT(streamCatalogService, "null streamCatalogService");
    NES_ASSERT(topologyManagerService, "null topologyManagerService");

    CoordinatorRPCServer service(topologyManagerService, streamCatalogService, monitoringService->getMonitoringManager());

    std::string address = rpcIp + ":" + std::to_string(rpcPort);
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    rpcServer = builder.BuildAndStart();
    prom->set_value(true);
    NES_DEBUG("NesCoordinator: buildAndStartGRPCServerServer listening on address=" << address);
    rpcServer->Wait();//blocking call
    NES_DEBUG("NesCoordinator: buildAndStartGRPCServer end listening");
}

std::vector<Runtime::QueryStatisticsPtr> NesCoordinator::getQueryStatistics(QueryId queryId) {
    NES_INFO("NesCoordinator: Get query statistics for query Id " << queryId);
    return worker->getNodeEngine()->getQueryStatistics(queryId);
}

QueryServicePtr NesCoordinator::getQueryService() { return queryService; }

QueryCatalogPtr NesCoordinator::getQueryCatalog() { return queryCatalog; }

Catalogs::UdfCatalogPtr NesCoordinator::getUdfCatalog() { return udfCatalog; }

MonitoringServicePtr NesCoordinator::getMonitoringService() { return monitoringService; }

GlobalQueryPlanPtr NesCoordinator::getGlobalQueryPlan() { return globalQueryPlan; }

NES::Experimental::MaintenanceServicePtr NesCoordinator::getMaintenanceService() { return maintenanceService; }

void NesCoordinator::onFatalError(int, std::string) {}

void NesCoordinator::onFatalException(const std::shared_ptr<std::exception>, std::string) {}
StreamCatalogServicePtr NesCoordinator::getStreamCatalogService() const { return streamCatalogService; }
TopologyManagerServicePtr NesCoordinator::getTopologyManagerService() const { return topologyManagerService; }

}// namespace NES
