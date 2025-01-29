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

#include <Util/BenchmarkUtils.hpp>
#include <Exceptions/ErrorListener.hpp>
#include <Catalogs/Query/QueryCatalog.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/AddPhysicalSourcesEvent.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Services/QueryParsingService.hpp>
#include <fstream>
#include <Util/magicenum/magic_enum.hpp>
#include <Util/yaml/Yaml.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Components/NesCoordinator.hpp>
#include <detail/PortDispatcher.hpp>
#include <unistd.h>
#include <BorrowedPort.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/LambdaSourceType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Components/NesWorker.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/Execution/ExecutableQueryPlan.hpp>
#include <Reconfiguration/Metadata/DrainQueryMetadata.hpp>
#include <Reconfiguration/ReconfigurationMarker.hpp>
#include <Sources/DataSource.hpp>

using namespace NES;
using namespace NES::Benchmark;
using std::filesystem::directory_iterator;

class ErrorHandler : public Exceptions::ErrorListener {
  public:
    virtual void onFatalError(int signalNumber, std::string callstack) override {
        if (callstack.empty()) {
            std::cout << "onFatalError: signal [" << signalNumber << "] error [" << strerror(errno) << "] ";
        } else {
            std::cout << "onFatalError: signal [" << signalNumber << "] error [" << strerror(errno) << "] callstack "
                      << callstack;
        }
    }

    virtual void onFatalException(std::shared_ptr<std::exception> exception, std::string callstack) override {
        if (callstack.empty()) {
            std::cout << "onFatalException: exception=[" << exception->what() << "] ";
        } else {
            std::cout << "onFatalException: exception=[" << exception->what() << "] callstack=\n" << callstack;
        }
    }
};

/**
 * @brief Set up the physical sources for the benchmark
 * @param leafWorkerIds: leaf worker ids
 */
void setupLogicalSources(uint64_t numberOfSources, RequestHandlerServicePtr requestHandlerService) {
    std::cout << "set up logical sources" << std::endl;
    NES::SchemaPtr schema1 = NES::Schema::create()
                                ->addField("id1", BasicType::UINT64)
                                ->addField("value1", BasicType::UINT64)
                                ->addField("secretValue1", BasicType::UINT64)
                                ->addField("timestamp", BasicType::UINT64);

    NES::SchemaPtr schema2 = NES::Schema::create()
                                 ->addField("id2", BasicType::UINT64)
                                 ->addField("value2", BasicType::UINT64)
                                 ->addField("secretValue2", BasicType::UINT64)
                                 ->addField("timestamp", BasicType::UINT64);

    for (uint64_t sourceIdx = 1; sourceIdx <= numberOfSources; sourceIdx++) {
        const auto& logicalSourceName = "example" + std::to_string(sourceIdx);
        if (sourceIdx == 1) {
            requestHandlerService->queueRegisterLogicalSourceRequest(logicalSourceName, schema1);
        } else {
            requestHandlerService->queueRegisterLogicalSourceRequest(logicalSourceName, schema2);
        }
    }
}

NES::Testing::BorrowedPortPtr rpcCoordinatorPort;
NES::Testing::BorrowedPortPtr restPort;
NES::NesCoordinatorPtr crd;

void startCoordinator(uint64_t numberOfBuffersToProduce, uint64_t bufferSize) {
    std::cout << "starting coordinator" << std::endl;
    auto coordinatorConfiguration = NES::Configurations::CoordinatorConfiguration::createDefault();
    restPort = NES::Testing::detail::getPortDispatcher().getNextPort();
    rpcCoordinatorPort = NES::Testing::detail::getPortDispatcher().getNextPort();
    coordinatorConfiguration->rpcPort = *rpcCoordinatorPort;
    coordinatorConfiguration->restPort = *restPort;
    coordinatorConfiguration->logLevel = NES::LogLevel::LOG_NONE;
    std::string configPath = std::filesystem::path(TEST_DATA_DIRECTORY) / "emptyWorker.yaml";
    coordinatorConfiguration->worker.configPath = configPath;
    if (numberOfBuffersToProduce > 1024) {
        coordinatorConfiguration->worker.numberOfBuffersInGlobalBufferManager = 10 * numberOfBuffersToProduce + 50;
        coordinatorConfiguration->worker.numberOfBuffersInSourceLocalBufferPool = 5 * numberOfBuffersToProduce + 50;
        coordinatorConfiguration->worker.numberOfBuffersPerWorker = numberOfBuffersToProduce + 50;
    } else {
        coordinatorConfiguration->worker.numberOfBuffersInGlobalBufferManager = 10 * 1024 + 50;
        coordinatorConfiguration->worker.numberOfBuffersInSourceLocalBufferPool = 5 * 1024 + 50;
        coordinatorConfiguration->worker.numberOfBuffersPerWorker = 1024 + 50;
    }
    coordinatorConfiguration->worker.numberOfBuffersToProduce = numberOfBuffersToProduce;
    coordinatorConfiguration->worker.numWorkerThreads.setValue(4);
    coordinatorConfiguration->worker.bufferSizeInBytes = bufferSize;
    // coordinatorConfiguration->worker.queryCompiler.nautilusBackend = QueryCompilation::NautilusBackend::INTERPRETER;

    NES::Configurations::OptimizerConfiguration optimizerConfiguration;
    optimizerConfiguration.queryMergerRule = Optimizer::QueryMergerRule::HashSignatureBasedCompleteQueryMergerRule;
    optimizerConfiguration.enableIncrementalPlacement = true;
    optimizerConfiguration.placementAmendmentThreadCount = 4;
    coordinatorConfiguration->optimizer = optimizerConfiguration;

    crd = std::make_shared<NesCoordinator>(coordinatorConfiguration);
    crd->startCoordinator(false);
    std::cout << "coordinator started" << std::endl;
}

std::vector<NesWorkerPtr> sourceNodes;

NesWorkerPtr startSourceWorker(uint64_t bufferSize, uint64_t numberOfBuffersToProduce, uint64_t workerIdx) {
    std::cout << "start source node " << workerIdx << std::endl;
    WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();
    wrkConf->coordinatorPort.setValue(*rpcCoordinatorPort);
    auto wrkDataPort = NES::Testing::detail::getPortDispatcher().getNextPort();
    wrkConf->dataPort = *wrkDataPort;
    wrkConf->numWorkerThreads.setValue(4);
    // wrkConf->connectSinksAsync.setValue(true);
    // wrkConf->connectSourceEventChannelsAsync.setValue(true);
    wrkConf->bufferSizeInBytes.setValue(bufferSize);
    std::string configPath = std::filesystem::path(TEST_DATA_DIRECTORY) / "emptyWorker.yaml";
    wrkConf->configPath = configPath;
    wrkConf->numberOfBuffersToProduce = numberOfBuffersToProduce;
    if (numberOfBuffersToProduce > 1024) {
        wrkConf->numberOfBuffersInGlobalBufferManager = 10 * numberOfBuffersToProduce + 50;
        wrkConf->numberOfBuffersInSourceLocalBufferPool = 5 * numberOfBuffersToProduce + 50;
        wrkConf->numberOfBuffersPerWorker = numberOfBuffersToProduce + 50;
    } else {
        wrkConf->numberOfBuffersInGlobalBufferManager = 10 * 1024 + 50;
        wrkConf->numberOfBuffersInSourceLocalBufferPool = 5 * 1024 + 50;
        wrkConf->numberOfBuffersPerWorker = 1024 + 50;
    }

//    auto func = [workerIdx](NES::Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce) {
//        struct Record {
//            uint64_t id;
//            uint64_t value;
//            uint64_t secretValue;
//            uint64_t timestamp;
//        };
//
//        static std::vector<uint64_t> counters(3, 0);
//        auto& counter = counters[workerIdx];
//
//        auto* records = buffer.getBuffer<Record>();
//        for (auto u = 0u; u < numberOfTuplesToProduce; ++u) {
//            records[u].id = workerIdx;
//            records[u].value = counter;
//            records[u].secretValue = counter;
//            records[u].timestamp = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
//        }
//        counter++;
//        // std::cout<<"Record " << counter << "produced by " << workerIdx << std::endl;
////        if (counter >= numberOfBuffersToProduce + 1) {
////            sleep(30);
////        }
//    };

    const auto& logicalSourceName = "example" + std::to_string(workerIdx);
//    auto lambdaSourceType = LambdaSourceType::create(logicalSourceName,
//                                                     "phy_" + logicalSourceName,
//                                                     std::move(func),
//                                                     numberOfBuffersToProduce + 50,
//                                                     /* gatheringValue */ 1,
//                                                     GatheringMode::INTERVAL_MODE);
    const auto physicalSourceName = "phy_" + logicalSourceName;
    std::map<std::string, std::string> sourceConfig {
        {Configurations::FILE_PATH_CONFIG, "/Users/danilaferentz/Desktop/nebulastream/cmake-build-debug/nes-benchmark/start_source.csv"},
        {Configurations::SOURCE_GATHERING_INTERVAL_CONFIG, "0"},
        {Configurations::NUMBER_OF_BUFFER_TO_PRODUCE, std::to_string(numberOfBuffersToProduce)}
    };
    auto csvSourceType = CSVSourceType::create(logicalSourceName, physicalSourceName, sourceConfig);
    wrkConf->physicalSourceTypes.add(csvSourceType);

    NesWorkerPtr wrk = std::make_shared<NesWorker>(std::move(wrkConf));
    bool resStart = wrk->start(/**blocking**/ false, /**withConnect**/ true);

    if (!resStart) {
        NES_ERROR("intermediate worker {} failed to start", workerIdx);
    }
    sourceNodes.push_back(wrk);
    return wrk;
}

std::vector<NesWorkerPtr> intermediateNodes;

NesWorkerPtr startIntermediateWorker(uint64_t bufferSize, uint64_t numberOfBuffersToProduce, uint64_t workerIdx) {
    std::cout << "start intermediate node " << workerIdx << std::endl;
    WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();
    wrkConf->coordinatorPort.setValue(*rpcCoordinatorPort);
    auto wrkDataPort = NES::Testing::detail::getPortDispatcher().getNextPort();
    wrkConf->dataPort = *wrkDataPort;
    wrkConf->numWorkerThreads.setValue(4);
    // wrkConf->connectSinksAsync.setValue(true);
    // wrkConf->connectSourceEventChannelsAsync.setValue(true);
    wrkConf->bufferSizeInBytes.setValue(bufferSize);
    std::string configPath = std::filesystem::path(TEST_DATA_DIRECTORY) / "emptyWorker.yaml";
    wrkConf->configPath = configPath;
    if (numberOfBuffersToProduce > 1024) {
        wrkConf->numberOfBuffersInGlobalBufferManager = 10 * numberOfBuffersToProduce + 50;
        wrkConf->numberOfBuffersInSourceLocalBufferPool = 5 * numberOfBuffersToProduce + 50;
        wrkConf->numberOfBuffersPerWorker = numberOfBuffersToProduce + 50;
    } else {
        wrkConf->numberOfBuffersInGlobalBufferManager = 10 * 1024 + 50;
        wrkConf->numberOfBuffersInSourceLocalBufferPool = 5 * 1024 + 50;
        wrkConf->numberOfBuffersPerWorker = 1024 + 50;
    }

    NesWorkerPtr wrk = std::make_shared<NesWorker>(std::move(wrkConf));
    bool resStart = wrk->start(/**blocking**/ false, /**withConnect**/ true);

    if (!resStart) {
        NES_ERROR("intermediate worker {} failed to start", workerIdx);
    }
    intermediateNodes.push_back(wrk);
    return wrk;
}
void setUp(uint64_t bufferSize, uint64_t numberOfBuffersToProduce, uint64_t numberOfIntermediateNodes) {
    startCoordinator(numberOfBuffersToProduce, bufferSize);
    setupLogicalSources(2, crd->getRequestHandlerService());

    // start sources
    std::vector<NesWorkerPtr> sourceWorkers;
    sourceWorkers.push_back(startSourceWorker(bufferSize, numberOfBuffersToProduce, 1));
    sourceWorkers.push_back(startSourceWorker(bufferSize, numberOfBuffersToProduce, 2));

    // start intermediate nodes
    std::vector<NesWorkerPtr> intermediateWorkers;
    for (uint64_t intermediateNodeIdx = 0; intermediateNodeIdx < numberOfIntermediateNodes; intermediateNodeIdx++) {
        intermediateWorkers.push_back(startIntermediateWorker(bufferSize, numberOfBuffersToProduce, intermediateNodeIdx + 1));
    }

    // replace parent for sources
    if (numberOfIntermediateNodes >= 1) {
        for (const auto& wrk: sourceWorkers) {
            wrk->replaceParent(crd->getNesWorker()->getWorkerId(), intermediateWorkers[0]->getWorkerId());
        }

        // set intermediate nodes as a chain
        for (uint64_t intermediateNodeIdx = 0; intermediateNodeIdx < numberOfIntermediateNodes - 1; intermediateNodeIdx++) {
            intermediateWorkers[intermediateNodeIdx]->replaceParent(crd->getNesWorker()->getWorkerId(), intermediateWorkers[intermediateNodeIdx + 1]->getWorkerId());
        }
    }
}

/**
 * @brief Load provided configuration file
 * @param filePath : location of the configuration file
 */
Yaml::Node loadConfigFromYAMLFile(const std::string& filePath) {
    if (!filePath.empty() && std::filesystem::exists(filePath)) {
        try {
            Yaml::Node config = *(new Yaml::Node());
            Yaml::Parse(config, filePath.c_str());
            return config;
        } catch (std::exception& e) {
            NES_ERROR("BenchmarkIncrementalPlacement: Error while initializing configuration parameters from YAML file. {}",
                      e.what());
            throw e;
        }
    }
    NES_THROW_RUNTIME_ERROR("BenchmarkIncrementalPlacement: No file path was provided or file could not be found at the path: "
                            + filePath);
}

static constexpr auto sleepDuration = std::chrono::milliseconds(250);

bool waitForQueryStatus(QueryId queryId, const Catalogs::Query::QueryCatalogPtr& queryCatalog, QueryState state, std::chrono::seconds timeoutInSec) {
    NES_TRACE("TestUtils: wait till the query {} gets into Running status.", queryId);
    auto start_timestamp = std::chrono::system_clock::now();

    NES_TRACE("TestUtils: Keep checking the status of query {} until a fixed time out", queryId);
    while (std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        QueryState queryState = queryCatalog->getQueryState(queryId);
        NES_DEBUG("TestUtils: Query {} is now in status {}", queryId, magic_enum::enum_name(queryState));
        switch (queryState) {
            case QueryState::MARKED_FOR_HARD_STOP:
            case QueryState::MARKED_FOR_SOFT_STOP:
            case QueryState::SOFT_STOP_TRIGGERED: {
                continue;
            }
            case QueryState::STOPPED: {
                if (state == NES::QueryState::STOPPED) {
                    return true;
                }
            }
            case QueryState::SOFT_STOP_COMPLETED: {
                if (state == NES::QueryState::SOFT_STOP_COMPLETED) {
                    return true;
                }
            }
            case QueryState::RUNNING: {
                NES_TRACE("TestUtils: Query {} is now in status {}", queryId, magic_enum::enum_name(queryState));
                if (state == NES::QueryState::RUNNING) {
                    return true;
                }
                continue;
            }
            case QueryState::FAILED: {
                NES_ERROR("Query failed to start. Expected: Running or Optimizing but found {}",
                          magic_enum::enum_name(queryState));
                return false;
            }
            default: {
                NES_WARNING("Expected: Running or Scheduling but found {}", magic_enum::enum_name(queryState));
            }
        }

        std::this_thread::sleep_for(sleepDuration);
    }
    NES_TRACE("checkCompleteOrTimeout: waitForStart expected results are not reached after timeout");
    return false;
}

bool checkRemovedDecomposedQueryOrTimeoutAtWorker(DecomposedQueryId decomposedQueryId,
                                                  DecomposedQueryPlanVersion decomposedQueryVersion,
                                                  NesWorkerPtr worker,
                                                  std::chrono::seconds timeout) {
    auto timeoutInSec = std::chrono::seconds(timeout);
    auto start_timestamp = std::chrono::system_clock::now();
    while (std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        NES_TRACE("checkStoppedOrTimeout: check decomposed query plan with id {}.{}", decomposedQueryId, decomposedQueryVersion);
        const auto currentPlan = worker->getNodeEngine()->getExecutableQueryPlan(decomposedQueryId, decomposedQueryVersion);
        if (currentPlan == NULL) {
            NES_TRACE("No such plan with id {}.{} yet registered", decomposedQueryId, decomposedQueryVersion);
            return false;
        }
        if (currentPlan->getStatus() == Runtime::Execution::ExecutableQueryPlanStatus::Finished) {
            NES_TRACE("checkStoppedOrTimeout: finished status for {}.{} reached stopped",
                      decomposedQueryId,
                      decomposedQueryVersion);
            return true;
        }
        std::string status;
        switch (currentPlan->getStatus()) {
            case Runtime::Execution::ExecutableQueryPlanStatus::Created: status = "created"; break;
            case Runtime::Execution::ExecutableQueryPlanStatus::Deployed: status = "deployed"; break;
            case Runtime::Execution::ExecutableQueryPlanStatus::Running: status = "running"; break;
            case Runtime::Execution::ExecutableQueryPlanStatus::Finished: status = "finished"; break;
            case Runtime::Execution::ExecutableQueryPlanStatus::Stopped: status = "stopped"; break;
            case Runtime::Execution::ExecutableQueryPlanStatus::ErrorState: status = "error"; break;
            case Runtime::Execution::ExecutableQueryPlanStatus::Invalid: status = "invalid"; break;
        }
        NES_DEBUG("checkStoppedOrTimeout: status not reached for {}.{} at worker {}, status {}.",
                  decomposedQueryId,
                  decomposedQueryVersion,
                  worker->getWorkerId(),
                  status);
        std::this_thread::sleep_for(sleepDuration);
    }
    NES_TRACE("checkStoppedOrTimeout: expected status not reached within set timeout");
    return false;
}

bool checkStoppedOrTimeout(QueryId queryId, const Catalogs::Query::QueryCatalogPtr& queryCatalog, std::chrono::seconds timeout) {
    auto timeoutInSec = std::chrono::seconds(timeout);
    auto start_timestamp = std::chrono::system_clock::now();
    while (std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        NES_TRACE("checkStoppedOrTimeout: check query status for {}", queryId);
        QueryState queryState = queryCatalog->getQueryState(queryId);
        if (queryState == QueryState::STOPPED) {
            NES_DEBUG("checkStoppedOrTimeout: status for {} reached stopped", queryId);
            return true;
        }
        NES_WARNING("checkStoppedOrTimeout: status Stopped not reached for {} as status is={}",
                    queryId,
                    magic_enum::enum_name(queryState));
        std::this_thread::sleep_for(sleepDuration);
    }
    NES_TRACE("checkStoppedOrTimeout: expected status not reached within set timeout");
    return false;
}

void runDataBenchmark(uint64_t defaultNumberOfIntermediateNodes, uint64_t minNUmberOfBuffersToProduce, uint64_t maxNumberOfBuffersToProduce, uint64_t bufferSize) {
    for (uint64_t tries = 1, numberOfBuffersToProduce = minNUmberOfBuffersToProduce; numberOfBuffersToProduce <= maxNumberOfBuffersToProduce; numberOfBuffersToProduce *= (1 + (tries++ % 3 == 0))) {
        NES_ERROR("Next round numberOfBuffersToProduce = {}, numberOfIntermediateNodes = {}",
                  numberOfBuffersToProduce,
                  defaultNumberOfIntermediateNodes);

        std::cout << "Setting up nodes." << std::endl;
        setUp(bufferSize, numberOfBuffersToProduce, defaultNumberOfIntermediateNodes);

        auto requestHandlerService = crd->getRequestHandlerService();
        auto topology = crd->getTopology();
        // std::cout << topology->toString();

        auto fileSinkDescriptor = FileSinkDescriptor::create("benchmark_output.csv", "CSV_FORMAT", "OVERWRITE");
        auto query = Query::from("example1")
                         .joinWith(Query::from("example2"))
                         .where(Attribute("value1") == Attribute("value2"))
                         .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
                         .sink(fileSinkDescriptor);

        QueryId addedQueryId =
            requestHandlerService->validateAndQueueAddQueryRequest(query.getQueryPlan(), Optimizer::PlacementStrategy::TopDown);
        waitForQueryStatus(addedQueryId, crd->getQueryCatalog(), NES::QueryState::RUNNING, std::chrono::seconds(120));
        std::cout<<"query is running"<<std::endl;
        for (auto& wrk : sourceNodes) {
            auto decompPlanIds = wrk->getNodeEngine()->getDecomposedQueryIds(SharedQueryId(addedQueryId.getRawValue()));
            auto [decompPlanIdToCheck, decompPlanVersionToCheck] = decompPlanIds.front();
            checkRemovedDecomposedQueryOrTimeoutAtWorker(decompPlanIdToCheck, decompPlanVersionToCheck, wrk, std::chrono::seconds(120));
        }
        for (auto& wrk : intermediateNodes) {
            auto decompPlanIds = wrk->getNodeEngine()->getDecomposedQueryIds(SharedQueryId(addedQueryId.getRawValue()));
            auto [decompPlanIdToCheck, decompPlanVersionToCheck] = decompPlanIds.front();
            checkRemovedDecomposedQueryOrTimeoutAtWorker(decompPlanIdToCheck, decompPlanVersionToCheck, wrk, std::chrono::seconds(120));
        }

        auto decompPlanIds = crd->getNesWorker()->getNodeEngine()->getDecomposedQueryIds(SharedQueryId(addedQueryId.getRawValue()));
        auto [decompPlanIdToCheck, decompPlanVersionToCheck] = decompPlanIds.front();
        auto reconfigMarker = ReconfigurationMarker::create();
        auto metadata = std::make_shared<DrainQueryMetadata>(1);
        auto event = ReconfigurationMarkerEvent::create(QueryState::RUNNING, metadata);
        reconfigMarker->addReconfigurationEvent(decompPlanIds.front(), event);
        auto wrkSource = crd->getNesWorker()->getNodeEngine()->getExecutableQueryPlan(decompPlanIdToCheck, decompPlanVersionToCheck)->getSources().front();
        wrkSource->handleReconfigurationMarker(reconfigMarker);
        checkRemovedDecomposedQueryOrTimeoutAtWorker(decompPlanIdToCheck, decompPlanVersionToCheck, crd->getNesWorker(), std::chrono::seconds(120));
        sleep(5);

        std::cout << "Stop source nodes" << std::endl;
        for (auto& wrk : sourceNodes) {
            wrk->stop(true);
        }
        std::cout << "Stop intermediate nodes" << std::endl;
        for (auto& wrk : intermediateNodes) {
            wrk->stop(true);
        }
        std::cout << "Stop coordinator" << std::endl;
        crd->stopCoordinator(true);
        sourceNodes.clear();
        intermediateNodes.clear();
        sleep(10);
    }
}

void runNodesBenchmark(uint64_t defaultNumberOfBuffersToProduce, uint64_t nmberOfIntermediateNodes, uint64_t bufferSize) {
    NES_ERROR("Next round numberOfBuffersToProduce = {}, numberOfIntermediateNodes = {}",
              defaultNumberOfBuffersToProduce,
              nmberOfIntermediateNodes);

    std::cout << "Setting up nodes." << std::endl;
    setUp(bufferSize, defaultNumberOfBuffersToProduce, nmberOfIntermediateNodes);

    auto requestHandlerService = crd->getRequestHandlerService();
    auto topology = crd->getTopology();
    // std::cout << topology->toString();

    auto fileSinkDescriptor = FileSinkDescriptor::create("benchmark_output.csv", "CSV_FORMAT", "OVERWRITE");
    auto query = Query::from("example1")
                     .joinWith(Query::from("example2"))
                     .where(Attribute("value1") == Attribute("value2"))
                     .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
                     .sink(fileSinkDescriptor);

    QueryId addedQueryId =
        requestHandlerService->validateAndQueueAddQueryRequest(query.getQueryPlan(), Optimizer::PlacementStrategy::TopDown);
    waitForQueryStatus(addedQueryId, crd->getQueryCatalog(), NES::QueryState::RUNNING, std::chrono::seconds(120));
    for (auto& wrk : sourceNodes) {
        auto decompPlanIds = wrk->getNodeEngine()->getDecomposedQueryIds(SharedQueryId(addedQueryId.getRawValue()));
        auto [decompPlanIdToCheck, decompPlanVersionToCheck] = decompPlanIds.front();
        checkRemovedDecomposedQueryOrTimeoutAtWorker(decompPlanIdToCheck, decompPlanVersionToCheck, wrk, std::chrono::seconds(120));
    }
    for (auto& wrk : intermediateNodes) {
        auto decompPlanIds = wrk->getNodeEngine()->getDecomposedQueryIds(SharedQueryId(addedQueryId.getRawValue()));
        auto [decompPlanIdToCheck, decompPlanVersionToCheck] = decompPlanIds.front();
        checkRemovedDecomposedQueryOrTimeoutAtWorker(decompPlanIdToCheck, decompPlanVersionToCheck, wrk, std::chrono::seconds(120));
    }

    auto decompPlanIds = crd->getNesWorker()->getNodeEngine()->getDecomposedQueryIds(SharedQueryId(addedQueryId.getRawValue()));
    auto [decompPlanIdToCheck, decompPlanVersionToCheck] = decompPlanIds.front();
    auto reconfigMarker = ReconfigurationMarker::create();
    auto metadata = std::make_shared<DrainQueryMetadata>(1);
    auto event = ReconfigurationMarkerEvent::create(QueryState::RUNNING, metadata);
    reconfigMarker->addReconfigurationEvent(decompPlanIds.front(), event);
    auto wrkSource = crd->getNesWorker()->getNodeEngine()->getExecutableQueryPlan(decompPlanIdToCheck, decompPlanVersionToCheck)->getSources().front();
    wrkSource->handleReconfigurationMarker(reconfigMarker);
    checkRemovedDecomposedQueryOrTimeoutAtWorker(decompPlanIdToCheck, decompPlanVersionToCheck, crd->getNesWorker(), std::chrono::seconds(120));
    sleep(5);

    std::cout << "Stop source nodes" << std::endl;
    for (auto& wrk : sourceNodes) {
        wrk->stop(true);
    }
    std::cout << "Stop intermediate nodes" << std::endl;
    for (auto& wrk : intermediateNodes) {
        wrk->stop(true);
    }
    std::cout << "Stop coordinator" << std::endl;
    crd->stopCoordinator(true);
    sourceNodes.clear();
    intermediateNodes.clear();
    sleep(10);
}

void runBuffersBenchmark(uint64_t defaultNumberOfBuffersToProduce, uint64_t defaultNumberOfIntermediateNodes, uint64_t bufferSize) {
    NES_ERROR("Next round numberOfBuffersToProduce = {}, numberOfIntermediateNodes = {}, bufferSize = {}",
              defaultNumberOfBuffersToProduce,
              defaultNumberOfIntermediateNodes,
              bufferSize);

    std::cout << "Setting up nodes." << std::endl;
    setUp(bufferSize, defaultNumberOfBuffersToProduce, defaultNumberOfIntermediateNodes);

    auto requestHandlerService = crd->getRequestHandlerService();
    auto topology = crd->getTopology();
    // std::cout << topology->toString();

    auto fileSinkDescriptor = FileSinkDescriptor::create("benchmark_output.csv", "CSV_FORMAT", "OVERWRITE");
    auto query = Query::from("example1")
                     .joinWith(Query::from("example2"))
                     .where(Attribute("value1") == Attribute("value2"))
                     .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
                     .sink(fileSinkDescriptor);

    QueryId addedQueryId =
        requestHandlerService->validateAndQueueAddQueryRequest(query.getQueryPlan(), Optimizer::PlacementStrategy::TopDown);
    waitForQueryStatus(addedQueryId, crd->getQueryCatalog(), NES::QueryState::RUNNING, std::chrono::seconds(120));
    for (auto& wrk : sourceNodes) {
        auto decompPlanIds = wrk->getNodeEngine()->getDecomposedQueryIds(SharedQueryId(addedQueryId.getRawValue()));
        auto [decompPlanIdToCheck, decompPlanVersionToCheck] = decompPlanIds.front();
        checkRemovedDecomposedQueryOrTimeoutAtWorker(decompPlanIdToCheck, decompPlanVersionToCheck, wrk, std::chrono::seconds(120));
    }
    for (auto& wrk : intermediateNodes) {
        auto decompPlanIds = wrk->getNodeEngine()->getDecomposedQueryIds(SharedQueryId(addedQueryId.getRawValue()));
        auto [decompPlanIdToCheck, decompPlanVersionToCheck] = decompPlanIds.front();
        checkRemovedDecomposedQueryOrTimeoutAtWorker(decompPlanIdToCheck, decompPlanVersionToCheck, wrk, std::chrono::seconds(120));
    }

    auto decompPlanIds = crd->getNesWorker()->getNodeEngine()->getDecomposedQueryIds(SharedQueryId(addedQueryId.getRawValue()));
    auto [decompPlanIdToCheck, decompPlanVersionToCheck] = decompPlanIds.front();
    auto reconfigMarker = ReconfigurationMarker::create();
    auto metadata = std::make_shared<DrainQueryMetadata>(1);
    auto event = ReconfigurationMarkerEvent::create(QueryState::RUNNING, metadata);
    reconfigMarker->addReconfigurationEvent(decompPlanIds.front(), event);
    auto wrkSource = crd->getNesWorker()->getNodeEngine()->getExecutableQueryPlan(decompPlanIdToCheck, decompPlanVersionToCheck)->getSources().front();
    wrkSource->handleReconfigurationMarker(reconfigMarker);
    checkRemovedDecomposedQueryOrTimeoutAtWorker(decompPlanIdToCheck, decompPlanVersionToCheck, crd->getNesWorker(), std::chrono::seconds(120));
    sleep(5);

    std::cout << "Stop source nodes" << std::endl;
    for (auto& wrk : sourceNodes) {
        wrk->stop(true);
    }
    std::cout << "Stop intermediate nodes" << std::endl;
    for (auto& wrk : intermediateNodes) {
        wrk->stop(true);
    }
    std::cout << "Stop coordinator" << std::endl;
    crd->stopCoordinator(true);
    sourceNodes.clear();
    intermediateNodes.clear();
    sleep(10);
}

int main(int argc, const char* argv[]) {
    auto listener = std::make_shared<ErrorHandler>();
    Exceptions::installGlobalErrorListener(listener);

    // Load all command line arguments
    std::map<std::string, std::string> commandLineParams;
    for (int i = 1; i < argc; ++i) {
        commandLineParams.insert(std::pair<std::string, std::string>(
            std::string(argv[i]).substr(0, std::string(argv[i]).find("=")),
            std::string(argv[i]).substr(std::string(argv[i]).find("=") + 1, std::string(argv[i]).length() - 1)));
    }

    // Location of the configuration file
    auto configPath = commandLineParams.find("--configPath");
    auto benchmarkType = commandLineParams.find("--benchmarkType");

    Yaml::Node configs;
    //Load the configuration file
    if (configPath != commandLineParams.end()) {
        configs = loadConfigFromYAMLFile(configPath->second);
    }
    else {
        NES_ERROR("Configuration file is not provided. Please use the option --configPath");
        return -1;
    }

    auto defaultNumberOfIntermediateNodes = configs["DefaultNumOfIntermediateNodes"].As<uint64_t>();
    auto defaultNumberOfBuffersToProduce = configs["DefaultNumberOfBuffersToProduce"].As<uint64_t>();
    auto defaultBufferSize = configs["DefaultBufferSize"].As<uint64_t>();
    //    auto numberOfIntermediateNodes = 1;
    //    auto numberOfBuffersToProduce = 500;
    //    auto bufferSize = 1024;
    if (benchmarkType != commandLineParams.end()) {
        std::cout << "Setup StateRecreationBenchmark test class." << std::endl;
        NES::Logger::setupLogging("state-recreation-" + benchmarkType->second + ".log", magic_enum::enum_cast<LogLevel>(3).value());
        if (benchmarkType->second == "data") {
            auto minNumberOfBuffersToProduce = configs["MinNumberOfBuffersToProduce"].As<uint64_t>();
            auto maxNumberOfBuffersToProduce = configs["MaxNumberOfBuffersToProduce"].As<uint64_t>();

            runDataBenchmark(1, 2048, 2048, 1024);
        } else if (benchmarkType->second == "nodes") {
            auto minNUmberOfIntermediateNodes = configs["MinNumberOfIntermediateNodes"].As<uint64_t>();
            auto maxNUmberOfIntermediateNodes = configs["MaxNumberOfIntermediateNodes"].As<uint64_t>();

            for (auto i = 0; i < 3; i++) {
                runNodesBenchmark(defaultNumberOfBuffersToProduce, 0, defaultBufferSize);
            }

            for (uint64_t numberOfIntermediateNodes = minNUmberOfIntermediateNodes, tries = 1; numberOfIntermediateNodes <= maxNUmberOfIntermediateNodes; numberOfIntermediateNodes *= (1 + (tries++ % 3 == 0))) {
                runNodesBenchmark(defaultNumberOfBuffersToProduce, numberOfIntermediateNodes, defaultBufferSize);
            }
        } else if (benchmarkType->second == "buffer") {
            auto minBufferSize = configs["MinBufferSize"].As<uint64_t>();
            auto maxBufferSize = configs["MaxBufferSize"].As<uint64_t>();

            for (uint64_t bufferSize = minBufferSize, tries = 1; bufferSize <= maxBufferSize; bufferSize *= (1 + (tries++ % 3 == 0))) {
                runBuffersBenchmark(defaultNumberOfBuffersToProduce, defaultNumberOfIntermediateNodes, bufferSize);
            }
        }
    }
    sleep(5);
    std::cout << "-----------" << std::endl;
    std::cout << "finished" << std::endl;
}


