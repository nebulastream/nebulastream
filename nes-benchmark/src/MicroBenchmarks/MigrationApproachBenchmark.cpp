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
#include <cpr/cpr.h>
#include <API/QueryAPI.hpp>
#include <utility>
#include <Operators/LogicalOperators/LogicalOperatorFactory.hpp>
#include <Util/Placement/PlacementConstants.hpp>
#include <Operators/Operator.hpp>
#include <Execution/Operators/ReorderTupleBuffersOperator.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinOperator.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Operators/LogicalOperators/LogicalUnaryOperator.hpp>

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
            const auto& logicalSourceName = "source_" + std::to_string(sourceIdx) + "_cord";
            if (sourceIdx == 1) {
                requestHandlerService->queueRegisterLogicalSourceRequest(logicalSourceName, schema1);
            } else {
                requestHandlerService->queueRegisterLogicalSourceRequest(logicalSourceName, schema2);
            }
        }

        for (uint64_t sourceIdx = 1; sourceIdx <= numberOfSources; sourceIdx++) {
            const auto& logicalSourceName = "source_" + std::to_string(sourceIdx) + "_start";
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
        coordinatorConfiguration->worker.numberOfBuffersInGlobalBufferManager = 200000;
        coordinatorConfiguration->worker.numberOfBuffersInSourceLocalBufferPool = 20000;
        coordinatorConfiguration->worker.numberOfBuffersPerWorker = 20000;
        // coordinatorConfiguration->worker.numberOfBuffersToProduce = numberOfBuffersToProduce;
        coordinatorConfiguration->worker.numWorkerThreads.setValue(4);
        coordinatorConfiguration->worker.bufferSizeInBytes = bufferSize;
        // coordinatorConfiguration->worker.queryCompiler.nautilusBackend = QueryCompilation::NautilusBackend::INTERPRETER;

        std::map<std::string, std::string> sourceConfig {
            {Configurations::FILE_PATH_CONFIG, "/Users/danilaferentz/Desktop/nebulastream/cmake-build-debug/nes-benchmark/fake.csv"},
            {Configurations::SOURCE_GATHERING_INTERVAL_CONFIG, "0"},
            {Configurations::NUMBER_OF_BUFFER_TO_PRODUCE, std::to_string(numberOfBuffersToProduce)}
        };
        std::string logicalSourceName1 = "source_1_cord";
        const auto physicalSourceName1 = "phy_" + logicalSourceName1;
        auto csvSourceType1 = CSVSourceType::create(logicalSourceName1, physicalSourceName1, sourceConfig);
        coordinatorConfiguration->worker.physicalSourceTypes.add(csvSourceType1);

        std::string logicalSourceName2 = "source_2_cord";
        const auto physicalSourceName2 = "phy_" + logicalSourceName2;
        auto csvSourceType2 = CSVSourceType::create(logicalSourceName2, physicalSourceName2, sourceConfig);
        coordinatorConfiguration->worker.physicalSourceTypes.add(csvSourceType2);

        NES::Configurations::OptimizerConfiguration optimizerConfiguration;
        optimizerConfiguration.queryMergerRule = Optimizer::QueryMergerRule::HashSignatureBasedCompleteQueryMergerRule;
        optimizerConfiguration.enableIncrementalPlacement = true;
        optimizerConfiguration.placementAmendmentThreadCount = 4;
        coordinatorConfiguration->optimizer = optimizerConfiguration;

        crd = std::make_shared<NesCoordinator>(coordinatorConfiguration);
        setupLogicalSources(2, crd->getRequestHandlerService());

        crd->startCoordinator(false);
        std::cout << "coordinator started" << std::endl;
    }

    NesWorkerPtr migrationJoinWorker;
    NesWorkerPtr startMigrationJoinWorker(uint64_t bufferSize, uint64_t numberOfBuffersToProduce) {
        std::cout << "start startMigrationJoinWorker" << std::endl;
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
        // wrkConf->numberOfBuffersToProduce = numberOfBuffersToProduce;
        wrkConf->numberOfBuffersInGlobalBufferManager = 200000;
        wrkConf->numberOfBuffersInSourceLocalBufferPool = 20000;
        wrkConf->numberOfBuffersPerWorker = 20000;

        std::map<std::string, std::string> sourceConfig {
            {Configurations::FILE_PATH_CONFIG, "/Users/danilaferentz/Desktop/nebulastream/cmake-build-debug/nes-benchmark/start_source.csv"},
            {Configurations::SOURCE_GATHERING_INTERVAL_CONFIG, "0"},
            {Configurations::NUMBER_OF_BUFFER_TO_PRODUCE, std::to_string(numberOfBuffersToProduce)}
        };
        std::string logicalSourceName1 = "source_1_start";
        const auto physicalSourceName1 = "phy_" + logicalSourceName1;
        auto csvSourceType1 = CSVSourceType::create(logicalSourceName1, physicalSourceName1, sourceConfig);
        wrkConf->physicalSourceTypes.add(csvSourceType1);

        std::string logicalSourceName2 = "source_2_start";
        const auto physicalSourceName2 = "phy_" + logicalSourceName2;
        auto csvSourceType2 = CSVSourceType::create(logicalSourceName2, physicalSourceName2, sourceConfig);
        wrkConf->physicalSourceTypes.add(csvSourceType2);

        NesWorkerPtr wrk = std::make_shared<NesWorker>(std::move(wrkConf));
        bool resStart = wrk->start(/**blocking**/ false, /**withConnect**/ true);

        if (!resStart) {
            NES_ERROR("intermediate worker failed to start");
        }
        migrationJoinWorker = wrk;
        return wrk;
    }

    std::vector<NesWorkerPtr> intermediateNodes;
    NesWorkerPtr startIntermediateWorker(uint64_t bufferSize, uint64_t, uint64_t workerIdx) {
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
        wrkConf->numberOfBuffersInGlobalBufferManager = 200000;
        wrkConf->numberOfBuffersInSourceLocalBufferPool = 20000;
        wrkConf->numberOfBuffersPerWorker = 20000;

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

        auto migrationJoinWorker = startMigrationJoinWorker(bufferSize, numberOfBuffersToProduce);

        // start intermediate nodes
        std::vector<NesWorkerPtr> intermediateWorkers;
        for (uint64_t intermediateNodeIdx = 0; intermediateNodeIdx < numberOfIntermediateNodes; intermediateNodeIdx++) {
            intermediateWorkers.push_back(startIntermediateWorker(bufferSize, numberOfBuffersToProduce, intermediateNodeIdx + 1));
        }

        // replace parent for sources
        if (numberOfIntermediateNodes >= 1) {
            migrationJoinWorker->replaceParent(crd->getNesWorker()->getWorkerId(), intermediateWorkers[0]->getWorkerId());

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

    void runNodesBenchmark(uint64_t defaultNumberOfBuffersToProduce, uint64_t nmberOfIntermediateNodes, uint64_t bufferSize) {
        NES_ERROR("Next round numberOfBuffersToProduce = {}, numberOfIntermediateNodes = {}",
                  defaultNumberOfBuffersToProduce,
                  nmberOfIntermediateNodes);

        std::cout << "Setting up nodes." << std::endl;
        setUp(bufferSize, defaultNumberOfBuffersToProduce, nmberOfIntermediateNodes);

        auto requestHandlerService = crd->getRequestHandlerService();
        auto topology = crd->getTopology();
        // std::cout << topology->toString();

        auto migrationFuture = cpr::PostAsync(cpr::Url{"http://127.0.0.1:" + std::to_string(*restPort) + "/v1/nes/query/place-migration-join"},
                                     cpr::Header{{"Content-Type", "application/json"}});
         migrationFuture.wait();
        auto migrationResponse = migrationFuture.get();

        waitForQueryStatus(QueryId(1), crd->getQueryCatalog(), NES::QueryState::RUNNING, std::chrono::seconds(120));


        auto future = cpr::PostAsync(cpr::Url{"http://127.0.0.1:" + std::to_string(*restPort) + "/v1/nes/query/place-recreation-join"},
                     cpr::Header{{"Content-Type", "application/json"}});
        future.wait();
        auto response = future.get();
//        auto nullOutputSinkDescriptor = NullOutputSinkDescriptor::create();
//        auto migrationQuery = Query::from("source_1_start")
//                         .joinWith(Query::from("source_2_start"))
//                         .where(Attribute("value1") == Attribute("value2"))
//                         .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
//                         .sink(nullOutputSinkDescriptor, WorkerId(2));
//        auto queryPlan = migrationQuery.getQueryPlan();
//        auto join = queryPlan->getOperatorByType<LogicalJoinOperator>()[0];
//        std::string recreationFileName = "recreation_file.bin";
//        auto migrateSinkOperator = LogicalOperatorFactory::createSinkOperator(
//            FileSinkDescriptor::create(recreationFileName, "MIGRATION_FORMAT", "OVERWRITE"));
//        migrateSinkOperator->addProperty(Optimizer::PINNED_WORKER_ID, WorkerId(1));
//        migrateSinkOperator->addProperty("MIGRATION_SINK", true);
//        join->addParent(migrateSinkOperator);
//        join->addProperty("MIGRATION_FLAG", true);
//        queryPlan->addRootOperator(migrateSinkOperator);

//        QueryId addedQueryId =
//            requestHandlerService->validateAndQueueAddQueryRequest(migrationQuery.getQueryPlan(), Optimizer::PlacementStrategy::BottomUp);
//        waitForQueryStatus(addedQueryId, crd->getQueryCatalog(), NES::QueryState::RUNNING, std::chrono::seconds(120));

//        auto query = Query::from("source_1_cord")
//                         .joinWith(Query::from("source_2_cord"))
//                         .where(Attribute("value1") == Attribute("value2"))
//                         .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
//                         .sink(nullOutputSinkDescriptor, WorkerId(1));
//        auto joins = query.getQueryPlan()->getOperatorByType<LogicalJoinOperator>();
//        joins[0]->addProperty("MIGRATION_FILE", recreationFileName + "-completed");
//        QueryId queryId = requestHandlerService->validateAndQueueAddQueryRequest(query.getQueryPlan(), Optimizer::PlacementStrategy::TopDown);

        auto coordinator = crd;
        auto startWrk = migrationJoinWorker;
        sleep(100);

        std::cout << "Stop source nodes" << std::endl;
        migrationJoinWorker->stop(true);
        std::cout << "Stop intermediate nodes" << std::endl;
        for (auto& wrk : intermediateNodes) {
            wrk->stop(true);
        }
        std::cout << "Stop coordinator" << std::endl;
        crd->stopCoordinator(true);
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
        // auto benchmarkType = commandLineParams.find("--benchmarkType");

//        Yaml::Node configs;
        //Load the configuration file
//        if (configPath != commandLineParams.end()) {
//            configs = loadConfigFromYAMLFile(configPath->second);
//        }
//        else {
//            NES_ERROR("Configuration file is not provided. Please use the option --configPath");
//            return -1;
//        }

        auto numberOfIntermediateNodes = 8;
        auto numberOfBuffersToProduce = 2048;
        auto defaultBufferSize = 1024;
        //    auto numberOfIntermediateNodes = 1;
        //    auto numberOfBuffersToProduce = 500;
        //    auto bufferSize = 1024;
//        if (benchmarkType != commandLineParams.end()) {
            std::cout << "Setup StateRecreationBenchmark test class." << std::endl;
            NES::Logger::setupLogging("state-recreation.log", magic_enum::enum_cast<LogLevel>(3).value());
//            if (benchmarkType->second == "data") {
//                auto minNumberOfBuffersToProduce = configs["MinNumberOfBuffersToProduce"].As<uint64_t>();
//                auto maxNumberOfBuffersToProduce = configs["MaxNumberOfBuffersToProduce"].As<uint64_t>();
//
//                runDataBenchmark(1, 2048, 2048, 1024);
//            if (benchmarkType->second == "nodes") {
//                auto minNUmberOfInterm§ediateNodes = configs["MinNumberOfIntermediateNodes"].As<uint64_t>();
//                auto maxNUmberOfIntermediateNodes = configs["MaxNumberOfIntermediateNodes"].As<uint64_t>();

//                for (auto i = 0; i < 3; i++) {
                    runNodesBenchmark(numberOfBuffersToProduce, numberOfIntermediateNodes, defaultBufferSize);
//                }

//                for (uint64_t numberOfIntermediateNodes = minNUmberOfIntermediateNodes, tries = 1; numberOfIntermediateNodes <= maxNUmberOfIntermediateNodes; numberOfIntermediateNodes *= (1 + (tries++ % 3 == 0))) {
//                    runNodesBenchmark(defaultNumberOfBuffersToProduce, numberOfIntermediateNodes, defaultBufferSize);
//                }
//            } else if (benchmarkType->second == "buffer") {
//                auto minBufferSize = configs["MinBufferSize"].As<uint64_t>();
//                auto maxBufferSize = configs["MaxBufferSize"].As<uint64_t>();
//
//                for (uint64_t bufferSize = minBufferSize, tries = 1; bufferSize <= maxBufferSize; bufferSize *= (1 + (tries++ % 3 == 0))) {
//                    runBuffersBenchmark(defaultNumberOfBuffersToProduce, defaultNumberOfIntermediateNodes, bufferSize);
//                }
//            }
//        }
        sleep(5);
        std::cout << "-----------" << std::endl;
        std::cout << "finished" << std::endl;
    }