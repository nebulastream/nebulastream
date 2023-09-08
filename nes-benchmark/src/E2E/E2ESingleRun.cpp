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

#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Configurations/Coordinator/LogicalSourceType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/KafkaSourceType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/LambdaSourceType.hpp>
#include <E2E/E2ESingleRun.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/QueryStatistics.hpp>
#include <Sources/LambdaSource.hpp>
#include <Util/BenchmarkUtils.hpp>
#include <Version/version.hpp>

#ifdef ENABLE_KAFKA_BUILD
#include <cppkafka/cppkafka.h>
#endif

#include <fstream>

namespace NES::Benchmark {

void E2ESingleRun::setupCoordinatorConfig() {
    NES_INFO("Creating coordinator and worker configuration...");
    coordinatorConf = Configurations::CoordinatorConfiguration::createDefault();

    // Coordinator configuration
    coordinatorConf->rpcPort = rpcPortSingleRun;
    coordinatorConf->restPort = restPortSingleRun;
    coordinatorConf->enableMonitoring = false;
    coordinatorConf->optimizer.distributedWindowChildThreshold = 100;
    coordinatorConf->optimizer.distributedWindowCombinerThreshold = 100;

    // Worker configuration
    coordinatorConf->worker.numWorkerThreads = configPerRun.numberOfWorkerThreads->getValue();
    coordinatorConf->worker.bufferSizeInBytes = configPerRun.bufferSizeInBytes->getValue();

    coordinatorConf->worker.numberOfBuffersInGlobalBufferManager = configPerRun.numberOfBuffersInGlobalBufferManager->getValue();
    coordinatorConf->worker.numberOfBuffersInSourceLocalBufferPool =
        configPerRun.numberOfBuffersInSourceLocalBufferPool->getValue();

    coordinatorConf->worker.coordinatorIp = coordinatorConf->coordinatorIp.getValue();
    coordinatorConf->worker.localWorkerIp = coordinatorConf->coordinatorIp.getValue();
    coordinatorConf->worker.queryCompiler.windowingStrategy = QueryCompilation::WindowingStrategy::SLICING;
    coordinatorConf->worker.numaAwareness = true;
    coordinatorConf->worker.queryCompiler.useCompilationCache = true;
    coordinatorConf->worker.enableMonitoring = false;
    coordinatorConf->worker.queryCompiler.queryCompilerType =
        QueryCompilation::QueryCompilerOptions::QueryCompiler::NAUTILUS_QUERY_COMPILER;
    coordinatorConf->worker.queryCompiler.queryCompilerDumpMode =
        QueryCompilation::QueryCompilerOptions::DumpMode::FILE_AND_CONSOLE;
    coordinatorConf->worker.queryCompiler.nautilusBackend = configPerRun.nautilusBackend->getValue();

    coordinatorConf->worker.queryCompiler.pageSize = configPerRun.pageSize->getValue();
    coordinatorConf->worker.queryCompiler.numberOfPartitions = configPerRun.numberOfPartitions->getValue();
    coordinatorConf->worker.queryCompiler.preAllocPageCnt = configPerRun.preAllocPageCnt->getValue();
    coordinatorConf->worker.queryCompiler.maxHashTableSize = configPerRun.maxHashTableSize->getValue();

    if (configOverAllRuns.joinStrategy->getValue() == "HASH_JOIN_LOCAL") {
        coordinatorConf->worker.queryCompiler.joinStrategy = QueryCompilation::StreamJoinStrategy::HASH_JOIN_LOCAL;
    } else if (configOverAllRuns.joinStrategy->getValue() == "HASH_JOIN_GLOBAL_LOCKING") {
        coordinatorConf->worker.queryCompiler.joinStrategy = QueryCompilation::StreamJoinStrategy::HASH_JOIN_GLOBAL_LOCKING;
    } else if (configOverAllRuns.joinStrategy->getValue() == "HASH_JOIN_GLOBAL_LOCK_FREE") {
        coordinatorConf->worker.queryCompiler.joinStrategy = QueryCompilation::StreamJoinStrategy::HASH_JOIN_GLOBAL_LOCK_FREE;
    } else if (configOverAllRuns.joinStrategy->getValue() == "NESTED_LOOP_JOIN") {
        coordinatorConf->worker.queryCompiler.joinStrategy = QueryCompilation::StreamJoinStrategy::NESTED_LOOP_JOIN;
    } else {
        NES_THROW_RUNTIME_ERROR("Join Strategy " << configOverAllRuns.joinStrategy->getValue() << " not supported");
    }

    NES_INFO("Using joinStrategy {}",
             magic_enum::enum_name<QueryCompilation::StreamJoinStrategy>(coordinatorConf->worker.queryCompiler.joinStrategy));

    if (configOverAllRuns.sourceSharing->getValue() == "on") {
        coordinatorConf->worker.enableSourceSharing = true;
        coordinatorConf->worker.queryCompiler.useCompilationCache = true;
    }

    NES_INFO("Created coordinator and worker configuration!");
}

void E2ESingleRun::createSources() {
    NES_INFO("Creating sources and the accommodating data generation and data providing...");

    size_t sourceCnt = 0;
    for (const auto& item : configOverAllRuns.sourceNameToDataGenerator) {
        auto logicalSourceName = item.first;
        auto dataGenerator = item.second.get();

        auto logicalSource = LogicalSourceType::create(logicalSourceName, dataGenerator->getSchemaType());
        coordinatorConf->logicalSourceTypes.add(logicalSource);

        auto numberOfPhysicalSrc = configPerRun.logicalSrcToNoPhysicalSrc[logicalSource->getLogicalSourceName()];
        auto numberOfTotalBuffers = configOverAllRuns.numberOfPreAllocatedBuffer->getValue() * numberOfPhysicalSrc;

        auto bufferManager =
            std::make_shared<Runtime::BufferManager>(configPerRun.bufferSizeInBytes->getValue(), numberOfTotalBuffers);
        dataGenerator->setBufferManager(bufferManager);
        allBufferManagers.emplace_back(bufferManager);

        NES_INFO("Creating {} physical sources for logical source {}",
                 numberOfPhysicalSrc,
                 logicalSource->getLogicalSourceName());

        for (uint64_t i = 0; i < numberOfPhysicalSrc; i++) {
            auto generatorName = dataGenerator->getName();
            auto physicalStreamName = "physical_input" + std::to_string(sourceCnt);
            auto createdBuffers = dataGenerator->createData(configOverAllRuns.numberOfPreAllocatedBuffer->getValue(),
                                                            configPerRun.bufferSizeInBytes->getValue());

            auto sourceConfig =
                createPhysicalSourceType(logicalSourceName, physicalStreamName, createdBuffers, sourceCnt, i, generatorName);
            coordinatorConf->worker.physicalSourceTypes.add(sourceConfig);

            sourceCnt++;
            NES_INFO("Created {} for {}", physicalStreamName, logicalSource->getLogicalSourceName());
        }
    }
    NES_INFO("Created sources and the accommodating data generation and data providing!");
}

void E2ESingleRun::submitQueries(RequestHandlerServicePtr requestHandlerService, QueryCatalogServicePtr queryCatalog) {
    for (size_t i = 0; i < configPerRun.numberOfQueriesToDeploy->getValue(); i++) {
        for (const auto& query : configOverAllRuns.queries) {

            // If custom delay is set introduce a delay before submitting
            std::this_thread::sleep_for(std::chrono::seconds(query.getCustomDelayInSeconds()));

            NES_INFO("E2EBase: Submitting query = {}", query.getQueryString());
            auto queryId = requestHandlerService->validateAndQueueAddQueryRequest(query.getQueryString(),
                                                                                  Optimizer::PlacementStrategy::BottomUp);
            submittedIds.push_back(queryId);

            if (!waitForQueryToStart(queryId, queryCatalog, defaultStartQueryTimeout)) {
                NES_THROW_RUNTIME_ERROR("E2EBase: Could not start query with id = " << queryId);
            }
            NES_INFO("E2EBase: Query with id = {} started", queryId);
        }
    }
}

void E2ESingleRun::runQueries() {
    NES_INFO("Starting nesCoordinator...");
    coordinator = std::make_shared<NesCoordinator>(coordinatorConf);
    auto rpcPort = coordinator->startCoordinator(/* blocking */ false);
    NES_INFO("Started nesCoordinator at {}", rpcPort);

    auto requestHandlerService = coordinator->getRequestHandlerService();
    auto queryCatalog = coordinator->getQueryCatalogService();

    submitQueries(requestHandlerService, queryCatalog);

    NES_DEBUG("Starting the data providers...");
    for (auto& dataProvider : allDataProviders) {
        dataProvider->start();
    }

    // Wait for the system to come to a steady state
    NES_INFO("Waiting for {} seconds to let the system reach a steady state!",
             configOverAllRuns.startupSleepIntervalInSeconds->getValue());
    sleep(configOverAllRuns.startupSleepIntervalInSeconds->getValue());

    // For now, we only support one way of collecting the measurements
    NES_INFO("Starting to collect measurements...");
    uint64_t found = 0;
    while (found != submittedIds.size()) {
        for (auto id : submittedIds) {
            auto stats = coordinator->getNodeEngine()->getQueryStatistics(id);
            for (auto iter : stats) {
                while (iter->getProcessedTuple() < 1) {
                    NES_DEBUG("Query with id = {} not ready with no. tuples = {}. Sleeping for a second now...",
                              id,
                              iter->getProcessedTuple());
                    sleep(1);
                }
                NES_INFO("Query with id = {} Ready with no. tuples = {}", id, iter->getProcessedTuple());
                ++found;
            }
        }
    }

    collectMeasurements();
    NES_INFO("Done measuring!");

    NES_INFO("Done with single run!");
}

void E2ESingleRun::stopQueries() {
    NES_INFO("Stopping the queries...");
    auto requestHandlerService = coordinator->getRequestHandlerService();
    auto queryCatalog = coordinator->getQueryCatalogService();

    for (auto id : submittedIds) {
        // Sending a stop request to the coordinator with a timeout of 30 seconds
        requestHandlerService->validateAndQueueStopQueryRequest(id);

        if (!waitForQueryToStop(id, queryCatalog, defaultStartQueryTimeout)) {
            NES_THROW_RUNTIME_ERROR("E2EBase: Could not stop query with id = " << id);
        }
        NES_INFO("E2EBase: Query with id = {} stopped", id);
    }

    NES_DEBUG("Stopping data providers...");
    for (auto& dataProvider : allDataProviders) {
        dataProvider->stop();
    }
    NES_DEBUG("Stopped data providers!");

    // Starting a new thread that waits
    auto stopPromiseCord = std::make_shared<std::promise<bool>>();
    std::thread waitThreadCoordinator([this, stopPromiseCord]() {
        auto stopFutureCord = stopPromiseCord->get_future();
        bool satisfied = false;
        while (!satisfied) {
            switch (stopFutureCord.wait_for(std::chrono::seconds(1))) {
                case std::future_status::ready: {
                    satisfied = true;
                }
                case std::future_status::timeout:
                case std::future_status::deferred: {
                    if (coordinator->isCoordinatorRunning()) {
                        NES_DEBUG("Waiting for stop wrk cause #tasks in the queue: {}",
                                  coordinator->getNodeEngine()->getQueryManager()->getNumberOfTasksInWorkerQueues());
                    } else {
                        NES_DEBUG("worker stopped");
                    }
                    break;
                }
            }
        }
    });

    NES_INFO("Stopping coordinator...");
    bool retStoppingCoord = coordinator->stopCoordinator(true);
    stopPromiseCord->set_value(retStoppingCoord);
    NES_ASSERT(stopPromiseCord, retStoppingCoord);
    waitThreadCoordinator.join();
    NES_INFO("Coordinator stopped!");

    NES_INFO("Stopped the queries!");
}

void E2ESingleRun::writeMeasurementsToCsv() {
    NES_INFO("Writing the measurements to {}", configOverAllRuns.outputFile->getValue());
    std::stringstream resultOnConsole;
    auto schemaSizeInB = configOverAllRuns.getTotalSchemaSize();
    std::string queryString;
    for (auto query : configOverAllRuns.queries) {
        queryString += query.getQueryString() + "/";
    }
    if (!queryString.empty()) {
        queryString.pop_back();// Remove the trailing "/"
    }
    std::replace(queryString.begin(), queryString.end(), ',', ' ');

    std::stringstream outputCsvStream;

    for (const auto& measurementsCsv :
         measurements.getMeasurementsAsCSV(schemaSizeInB, configPerRun.numberOfQueriesToDeploy->getValue())) {
        outputCsvStream << "\"" << configOverAllRuns.benchmarkName->getValue() << "\"";
        outputCsvStream << "," << NES_VERSION;
        outputCsvStream << "," << configPerRun.nautilusBackend;
        outputCsvStream << "," << schemaSizeInB;
        outputCsvStream << "," << measurementsCsv;
        outputCsvStream << "," << configPerRun.numberOfWorkerThreads->getValue();
        outputCsvStream << "," << configPerRun.numberOfQueriesToDeploy->getValue();
        outputCsvStream << ","
                        << "\"" << configPerRun.getStringLogicalSourceToNumberOfPhysicalSources() << "\"";
        outputCsvStream << "," << configPerRun.bufferSizeInBytes->getValue();
        outputCsvStream << "," << configOverAllRuns.inputType->getValue();
        outputCsvStream << "," << configOverAllRuns.dataProviderMode->getValue();
        outputCsvStream << ","
                        << "\"" << queryString << "\"";
        outputCsvStream << std::endl;
    }

    std::ofstream ofs;
    ofs.open(configOverAllRuns.outputFile->getValue(), std::ofstream::app);
    NES_DEBUG("Writing to file={}", configOverAllRuns.outputFile->getValue());
    ofs << outputCsvStream.str();
    ofs.close();

    NES_INFO("Done writing the measurements to {}", configOverAllRuns.outputFile->getValue());
    NES_INFO("Statistics are: {}", outputCsvStream.str())
    std::cout << "Throughput=" << measurements.getThroughputAsString() << std::endl;
}

E2ESingleRun::E2ESingleRun(E2EBenchmarkConfigPerRun& configPerRun,
                           E2EBenchmarkConfigOverAllRuns& configOverAllRuns,
                           uint16_t rpcPort,
                           uint16_t restPort)
    : configPerRun(configPerRun), configOverAllRuns(configOverAllRuns), rpcPortSingleRun(rpcPort), restPortSingleRun(restPort) {}

E2ESingleRun::~E2ESingleRun() {
    coordinatorConf.reset();
    coordinator.reset();

    for (auto& dataProvider : allDataProviders) {
        dataProvider.reset();
    }
    allDataProviders.clear();

    for (auto& bufferManager : allBufferManagers) {
        bufferManager.reset();
    }
    allBufferManagers.clear();
}

void E2ESingleRun::run() {
    setupCoordinatorConfig();
    createSources();
    runQueries();
    stopQueries();
    writeMeasurementsToCsv();
}

bool E2ESingleRun::waitForQueryToStart(QueryId queryId,
                                       const QueryCatalogServicePtr& queryCatalogService,
                                       std::chrono::seconds timeoutInSec) {
    NES_TRACE("checkCompleteOrTimeout: Wait until the query {} is running", queryId);
    auto queryCatalogEntry = queryCatalogService->getEntryForQuery(queryId);
    if (!queryCatalogEntry) {
        NES_ERROR("checkCompleteOrTimeout: Cannot find query with id = {} in the query catalog", queryId);
        return false;
    }
    auto startTimestamp = std::chrono::system_clock::now();

    while (std::chrono::system_clock::now() < startTimestamp + timeoutInSec) {
        NES_TRACE("checkCompleteOrTimeout: Query with id = {} is currently {}",
                  queryId,
                  queryCatalogEntry->getQueryStatusAsString());
        auto status = queryCatalogEntry->getQueryState();

        switch (status) {
            case QueryState::MARKED_FOR_HARD_STOP:
            case QueryState::MARKED_FOR_SOFT_STOP:
            case QueryState::SOFT_STOP_COMPLETED:
            case QueryState::SOFT_STOP_TRIGGERED:
            case QueryState::STOPPED:
            case QueryState::RUNNING: {
                return true;
            }
            case QueryState::FAILED: {
                NES_ERROR("Query failed to start. Expected: Running or Optimizing but found {}",
                          std::string(magic_enum::enum_name(status)));
                return false;
            }
            default: {
                NES_WARNING("Expected: Running or Scheduling but found {}", std::string(magic_enum::enum_name(status)));
                break;
            }
        }

        std::this_thread::sleep_for(sleepDuration);
    }

    NES_TRACE("checkCompleteOrTimeout: waitForStart expected status is not reached after timeout");
    return false;
}

bool E2ESingleRun::waitForQueryToStop(NES::QueryId queryId,
                                      const QueryCatalogServicePtr& queryCatalogService,
                                      std::chrono::seconds timeoutInSec) {
    NES_TRACE("checkCompleteOrTimeout: Wait until the query {} is stopped", queryId);
    auto queryCatalogEntry = queryCatalogService->getEntryForQuery(queryId);
    if (!queryCatalogEntry) {
        NES_ERROR("checkCompleteOrTimeout: Cannot find query with id = {} in the query catalog", queryId);
        return false;
    }
    auto startTimestamp = std::chrono::system_clock::now();

    while (std::chrono::system_clock::now() < startTimestamp + timeoutInSec) {
        NES_TRACE("checkCompleteOrTimeout: Query with id = {} is currently {}",
                  queryId,
                  queryCatalogEntry->getQueryStatusAsString());
        auto status = queryCatalogEntry->getQueryState();

        if (status == QueryState::STOPPED) {
            NES_TRACE("checkStoppedOrTimeout: Status for query with id = {} is stopped", queryId);
            return true;
        }

        NES_DEBUG("checkStoppedOrTimeout: Query with id = {} not stopped as status is {}",
                  queryId,
                  queryCatalogEntry->getQueryStatusAsString());

        std::this_thread::sleep_for(sleepDuration);
    }

    NES_TRACE("checkCompleteOrTimeout: waitForStop expected status is not reached after timeout");
    return false;
}

PhysicalSourceTypePtr E2ESingleRun::createPhysicalSourceType(std::string logicalSourceName,
                                                             std::string physicalSourceName,
                                                             std::vector<Runtime::TupleBuffer>& createdBuffers,
                                                             size_t sourceCnt,
                                                             uint64_t groupId,
                                                             std::string& generator) {
    if (generator == "YSBKafka") {
        ((void) groupId);// We have to do this, as on the macs, we have disabled Kafka
#ifdef ENABLE_KAFKA_BUILD
        //Kafka is not using a data provider as Kafka itself is the provider
        auto connectionStringVec =
            NES::Util::splitWithStringDelimiter<std::string>(configOverAllRuns.connectionString->getValue(), ",");

        auto kafkaSourceType = KafkaSourceType::create(logicalSourceName, physicalSourceName);
        kafkaSourceType->setBrokers(connectionStringVec[0]);
        kafkaSourceType->setTopic(connectionStringVec[1]);
        kafkaSourceType->setConnectionTimeout(1000);

        //we use the group id
        kafkaSourceType->setGroupId(std::to_string(groupId));
        kafkaSourceType->setNumberOfBuffersToProduce(configOverAllRuns.numberOfBuffersToProduce->getValue());
        kafkaSourceType->setBatchSize(configOverAllRuns.batchSize->getValue());
        return kafkaSourceType;
#else
        NES_THROW_RUNTIME_ERROR("Kafka not supported on OSX");
#endif
    } else {
        auto dataProvider =
            DataProvision::DataProvider::createProvider(/* sourceIndex */ sourceCnt, configOverAllRuns, createdBuffers);
        // Adding necessary items to the corresponding vectors
        allDataProviders.emplace_back(dataProvider);

        size_t generatorQueueIndex = 0;
        auto dataProvidingFunc = [this, sourceCnt, generatorQueueIndex](Runtime::TupleBuffer& buffer, uint64_t) {
            allDataProviders[sourceCnt]->provideNextBuffer(buffer, generatorQueueIndex);
        };

        size_t sourceAffinity = std::numeric_limits<uint64_t>::max();
        //TODO #3336: static query manager mode is currently not ported therefore only one queue
        size_t taskQueueId = 0;
        auto sourceConfig = LambdaSourceType::create(logicalSourceName,
                                                     physicalSourceName,
                                                     dataProvidingFunc,
                                                     configOverAllRuns.numberOfBuffersToProduce->getValue(),
                                                     /* gatheringValue */ 0,
                                                     GatheringMode::INTERVAL_MODE,
                                                     sourceAffinity,
                                                     taskQueueId);

        return sourceConfig;
    }
}

void E2ESingleRun::collectMeasurements() {
    // We have to measure once more than the required numMeasurementsToCollect as we calculate deltas later on
    for (uint64_t cnt = 0; cnt <= configOverAllRuns.numMeasurementsToCollect->getValue(); ++cnt) {
        // Calculate the next start time
        int64_t nextPeriodStartTime = configOverAllRuns.experimentMeasureIntervalInSeconds->getValue() * 1000;
        nextPeriodStartTime +=
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

        uint64_t timeStamp =
            std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        measurements.addNewTimestamp(timeStamp);

        for (auto id : submittedIds) {
            auto statisticsCoordinator = coordinator->getNodeEngine()->getQueryStatistics(id);
            size_t processedTasks = 0;
            size_t processedBuffers = 0;
            size_t processedTuples = 0;
            size_t latencySum = 0;
            size_t queueSizeSum = 0;
            size_t availGlobalBufferSum = 0;
            size_t availFixedBufferSum = 0;

            for (auto& subPlanStatistics : statisticsCoordinator) {
                if (subPlanStatistics->getProcessedBuffers() != 0) {
                    processedTasks += subPlanStatistics->getProcessedTasks();
                    processedBuffers += subPlanStatistics->getProcessedBuffers();
                    processedTuples += subPlanStatistics->getProcessedTuple();
                    latencySum += (subPlanStatistics->getLatencySum() / subPlanStatistics->getProcessedBuffers());
                    queueSizeSum += (subPlanStatistics->getQueueSizeSum() / subPlanStatistics->getProcessedBuffers());
                    availGlobalBufferSum +=
                        (subPlanStatistics->getAvailableGlobalBufferSum() / subPlanStatistics->getProcessedBuffers());
                    availFixedBufferSum +=
                        (subPlanStatistics->getAvailableFixedBufferSum() / subPlanStatistics->getProcessedBuffers());
                }

                printQuerySubplanStatistics(timeStamp, subPlanStatistics, processedTasks);
                measurements.addNewMeasurement(processedTasks,
                                               processedBuffers,
                                               processedTuples,
                                               latencySum,
                                               queueSizeSum,
                                               availGlobalBufferSum,
                                               availFixedBufferSum,
                                               timeStamp);
            }
        }

        // Calculate the time to sleep until the next period starts
        auto curTime =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        auto sleepTime = nextPeriodStartTime - curTime;
        std::this_thread::sleep_for(std::chrono::milliseconds(sleepTime));
    }
}

void E2ESingleRun::printQuerySubplanStatistics(uint64_t timestamp,
                                               const Runtime::QueryStatisticsPtr& subPlanStatistics,
                                               size_t processedTasks,
                                               std::ostream& outStream) {
    std::stringstream ss;
    ss << "time=" << timestamp << " subplan=" << subPlanStatistics->getSubQueryId() << " procTasks=" << processedTasks;
    auto lockedPipelineStatistic = subPlanStatistics->getPipelineIdToTaskMap().rlock();
    for (auto& pipe : *lockedPipelineStatistic) {
        for (auto& worker : pipe.second) {
            ss << " pipeNo:" << pipe.first << " worker=" << worker.first << " tasks=" << worker.second;
        }
    }
    outStream << ss.str() << std::endl;
}

const CoordinatorConfigurationPtr& E2ESingleRun::getCoordinatorConf() const { return coordinatorConf; }

Measurements::Measurements& E2ESingleRun::getMeasurements() { return measurements; }

}// namespace NES::Benchmark
