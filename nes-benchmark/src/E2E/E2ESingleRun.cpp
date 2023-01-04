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
#include <Catalogs/Source/PhysicalSourceTypes/KafkaSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/LambdaSourceType.hpp>
#include <E2E/E2ESingleRun.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Services/QueryService.hpp>
#include <Sources/LambdaSource.hpp>
#include <Util/BenchmarkUtils.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Version/version.hpp>

#ifdef ENABLE_KAFKA_BUILD
#include <cppkafka/cppkafka.h>
#endif

#include <fstream>

namespace NES::Benchmark {

void E2ESingleRun::setupCoordinatorConfig() {
    NES_INFO("Creating coordinator and worker configuration...");
    coordinatorConf = Configurations::CoordinatorConfiguration::create();

    // Coordinator configuration
    coordinatorConf->rpcPort = 5000 + portOffSet;
    coordinatorConf->restPort = 9082 + portOffSet;
    coordinatorConf->enableMonitoring = false;
    coordinatorConf->optimizer.distributedWindowChildThreshold = 100;
    coordinatorConf->optimizer.distributedWindowCombinerThreshold = 100;

    // Worker configuration
    coordinatorConf->worker.numWorkerThreads = configPerRun.numWorkerOfThreads->getValue();
    coordinatorConf->worker.bufferSizeInBytes = configPerRun.bufferSizeInBytes->getValue();

    coordinatorConf->worker.numberOfBuffersInGlobalBufferManager = configPerRun.numberOfBuffersInGlobalBufferManager->getValue();
    coordinatorConf->worker.numberOfBuffersInSourceLocalBufferPool =
        configPerRun.numberOfBuffersInSourceLocalBufferPool->getValue();
    coordinatorConf->worker.numberOfBuffersInSourceLocalBufferPool = configPerRun.numberOfBuffersPerPipeline->getValue();

    coordinatorConf->worker.rpcPort = coordinatorConf->rpcPort.getValue() + 1;
    coordinatorConf->worker.dataPort = coordinatorConf->rpcPort.getValue() + 2;
    coordinatorConf->worker.coordinatorIp = coordinatorConf->coordinatorIp.getValue();
    coordinatorConf->worker.localWorkerIp = coordinatorConf->coordinatorIp.getValue();
    coordinatorConf->worker.queryCompiler.windowingStrategy = QueryCompilation::QueryCompilerOptions::THREAD_LOCAL;
    coordinatorConf->worker.numaAwareness = true;
    coordinatorConf->worker.queryCompiler.useCompilationCache = true;
    coordinatorConf->worker.enableMonitoring = false;

    if (configOverAllRuns.sourceSharing->getValue() == "on") {
        coordinatorConf->worker.enableSourceSharing = true;
        coordinatorConf->worker.queryCompiler.useCompilationCache = true;
    }

    NES_INFO("Created coordinator and worker configuration!");
}

void E2ESingleRun::createSources() {
    size_t sourceCnt = 0;
    NES_INFO("Creating sources and the accommodating data generation and data providing...");
    auto dataGenerator =
        NES::Benchmark::DataGeneration::DataGenerator::createGeneratorByName(configOverAllRuns.dataGenerator->getValue(),
                                                                             Yaml::Node());
    auto bufferManager = std::make_shared<Runtime::BufferManager>(configPerRun.bufferSizeInBytes->getValue(),
                                                                  configOverAllRuns.numberOfPreAllocatedBuffer->getValue());

    dataGenerator->setBufferManager(bufferManager);
    auto schema = dataGenerator->getSchema();
    auto logicalSourceName = configOverAllRuns.logicalSourceName->getValue();
    auto logicalSource = LogicalSource::create(logicalSourceName, schema);
    coordinatorConf->logicalSources.add(logicalSource);

    for (uint64_t i = 0; i < configPerRun.numberOfSources->getValue(); i++) {

        auto physicalStreamName = "physical_input" + std::to_string(sourceCnt);

        auto createdBuffers = dataGenerator->createData(configOverAllRuns.numberOfPreAllocatedBuffer->getValue(),
                                                        configPerRun.bufferSizeInBytes->getValue());

        size_t sourceAffinity = std::numeric_limits<uint64_t>::max();

        //TODO: static query manager mode is currently not ported therefore only one queue
        size_t taskQueueId = 0;
        if (configOverAllRuns.dataGenerator->getValue() == "YSBKafka") {
#ifdef ENABLE_KAFKA_BUILD
            //Kafka is not using a data provider as Kafka itself is the provider
            auto connectionStringVec =
                NES::Util::splitWithStringDelimiter<std::string>(configOverAllRuns.connectionString->getValue(), ",");

            std::string destinationTopic;

            NES_DEBUG("Source no=" << sourceCnt << " connects to topic=" << connectionStringVec[1])
            auto kafkaSourceType = KafkaSourceType::create();
            kafkaSourceType->setBrokers(connectionStringVec[0]);
            kafkaSourceType->setTopic(connectionStringVec[1]);
            kafkaSourceType->setConnectionTimeout(1000);

            //we use the group id
            kafkaSourceType->setGroupId(std::to_string(i));
            kafkaSourceType->setNumberOfBuffersToProduce(configOverAllRuns.numberOfBuffersToProduce->getValue());
            kafkaSourceType->setBatchSize(configOverAllRuns.batchSize->getValue());

            auto physicalSource = PhysicalSource::create(logicalSourceName, physicalStreamName, kafkaSourceType);
            coordinatorConf->worker.physicalSources.add(physicalSource);

            allBufferManagers.emplace_back(bufferManager);
#else
            NES_THROW_RUNTIME_ERROR("Kafka not supported on OSX");
#endif
        } else {
            auto dataProvider =
                DataProviding::DataProvider::createProvider(/* sourceIndex */ sourceCnt, configOverAllRuns, createdBuffers);
            // Adding necessary items to the corresponding vectors
            allDataProviders.emplace_back(dataProvider);
            allBufferManagers.emplace_back(bufferManager);
            allDataGenerators.emplace_back(dataGenerator);

            size_t generatorQueueIndex = 0;
            auto dataProvidingFunc = [this, sourceCnt, generatorQueueIndex](Runtime::TupleBuffer& buffer, uint64_t) {
                allDataProviders[sourceCnt]->provideNextBuffer(buffer, generatorQueueIndex);
            };

            LambdaSourceTypePtr sourceConfig = LambdaSourceType::create(dataProvidingFunc,
                                                                        configOverAllRuns.numberOfBuffersToProduce->getValue(),
                                                                        /* gatheringValue */ 0,
                                                                        GatheringMode::INTERVAL_MODE,
                                                                        sourceAffinity,
                                                                        taskQueueId);

            auto physicalSource = PhysicalSource::create(logicalSourceName, physicalStreamName, sourceConfig);
            coordinatorConf->worker.physicalSources.add(physicalSource);
        }
        sourceCnt += 1;
    }
    NES_INFO("Created sources and the accommodating data generation and data providing!");
}

void E2ESingleRun::runQuery() {
    NES_INFO("Starting nesCoordinator...");
    coordinator = std::make_shared<NesCoordinator>(coordinatorConf);
    auto rpcPort = coordinator->startCoordinator(/* blocking */ false);
    NES_INFO("Started nesCoordinator at " << rpcPort << "!");

    auto queryService = coordinator->getQueryService();
    auto queryCatalog = coordinator->getQueryCatalogService();

    for (size_t i = 0; i < configPerRun.numberOfQueriesToDeploy->getValue(); i++) {
        QueryId queryId = 0;
        std::cout << "E2EBase: Submit query, source sharing for external data gen" << i << " ="
                  << configOverAllRuns.query->getValue() << std::endl;
        queryId = queryService->validateAndQueueAddQueryRequest(configOverAllRuns.query->getValue(), "BottomUp");

        submittedIds.push_back(queryId);

        for (auto id : submittedIds) {
            bool res = waitForQueryToStart(id, queryCatalog, std::chrono::seconds(180));
            if (!res) {
                NES_THROW_RUNTIME_ERROR("run does not succeed for id=" << id);
            }
            std::cout << "E2EBase: query started with id=" << id << std::endl;
        }
    }
    NES_DEBUG("Starting the data provider...");
    for (auto& dataProvider : allDataProviders) {
        dataProvider->start();
    }

    // Wait for the system to come to a steady state
    NES_INFO("Now waiting for " << configOverAllRuns.startupSleepIntervalInSeconds->getValue()
                                << "s to let the system come to a steady state!");
    sleep(configOverAllRuns.startupSleepIntervalInSeconds->getValue());

    // For now, we only support one way of collecting the measurements
    NES_INFO("Starting to collect measurements...");

    uint64_t found = 0;
    while (found != submittedIds.size()) {
        for (auto id : submittedIds) {
            auto stats = coordinator->getNodeEngine()->getQueryStatistics(id);
            for (auto iter : stats) {
                while (iter->getProcessedBuffers() < 1) {
                    NES_DEBUG("Query  with id " << id << " not ready with value= " << iter->getProcessedBuffers()
                                                << ". Sleeping for a second now...");
                    sleep(1);
                }
                std::cout << "Query  with id " << id << " Ready with value= " << iter->getProcessedBuffers() << std::endl;
                found++;
            }
        }
    }

    NES_INFO("Starting measurements...");
    // We have to measure once more than the required numMeasurementsToCollect as we calculate deltas later on
    for (uint64_t cnt = 0; cnt <= configOverAllRuns.numMeasurementsToCollect->getValue(); ++cnt) {
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
            for (auto subPlanStatistics : statisticsCoordinator) {
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

                measurements.addNewMeasurement(processedTasks,
                                               processedBuffers,
                                               processedTuples,
                                               latencySum,
                                               queueSizeSum,
                                               availGlobalBufferSum,
                                               availFixedBufferSum,
                                               timeStamp);
                std::cout << "Measurement queryId=" << id << " timestamp=" << timeStamp
                          << " processedBuffers=" << processedBuffers << " processedTuples=" << processedTuples
                          << " latencySum=" << latencySum << std::endl;
            }
        }

        // Calculating the time to sleep
        auto curTime =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        auto sleepTime = nextPeriodStartTime - curTime;
        std::this_thread::sleep_for(std::chrono::milliseconds(sleepTime));
    }

    NES_INFO("Done measuring!");
    NES_INFO("Done with single run!");
}

void E2ESingleRun::stopQuery() {
    NES_INFO("Stopping the query...");

    auto queryService = coordinator->getQueryService();
    auto queryCatalog = coordinator->getQueryCatalogService();

    for (auto id : submittedIds) {
        // Sending a stop request to the coordinator with a timeout of 30 seconds
        queryService->validateAndQueueStopQueryRequest(id);
        auto start_timestamp = std::chrono::system_clock::now();
        while (std::chrono::system_clock::now() < start_timestamp + stopQueryTimeoutInSec) {
            NES_TRACE("checkStoppedOrTimeout: check query status for " << id);
            if (queryCatalog->getEntryForQuery(id)->getQueryStatus() == QueryStatus::Stopped) {
                NES_TRACE("checkStoppedOrTimeout: status for " << id << " reached stopped");
                break;
            }
            NES_DEBUG("checkStoppedOrTimeout: status not reached for "
                      << id << " as status is=" << queryCatalog->getEntryForQuery(id)->getQueryStatusAsString());
            std::this_thread::sleep_for(stopQuerySleep);
        }
        NES_TRACE("checkStoppedOrTimeout: expected status not reached within set timeout");
    }

    NES_DEBUG("Stopping data provider...");
    for (auto& dataProvider : allDataProviders) {
        dataProvider->stop();
    }
    NES_DEBUG("Stopped data provider!");

    // Starting a new thread that waits
    std::shared_ptr<std::promise<bool>> stopPromiseCord = std::make_shared<std::promise<bool>>();
    std::thread waitThreadCoordinator([this, stopPromiseCord]() {
        std::future<bool> stopFutureCord = stopPromiseCord->get_future();
        bool satisfied = false;
        while (!satisfied) {
            switch (stopFutureCord.wait_for(std::chrono::seconds(1))) {
                case std::future_status::ready: {
                    satisfied = true;
                }
                case std::future_status::timeout:
                case std::future_status::deferred: {
                    if (coordinator->isCoordinatorRunning()) {
                        NES_DEBUG("Waiting for stop wrk cause #tasks in the queue: "
                                  << coordinator->getNodeEngine()->getQueryManager()->getNumberOfTasksInWorkerQueues());
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

    NES_INFO("Stopped the query!");
}

void E2ESingleRun::writeMeasurementsToCsv() {
    NES_INFO("Writing the measurements to " << configOverAllRuns.outputFile->getValue() << "...");

    auto schemaSizeInB = allDataGenerators[0]->getSchema()->getSchemaSizeInBytes();
    std::string queryString = configOverAllRuns.query->getValue();
    std::replace(queryString.begin(), queryString.end(), ',', ' ');

    std::stringstream header;
    header
        << "BenchmarkName,NES_VERSION,SchemaSize,timestamp,processedTasks,processedBuffers,processedTuples,latencySum,"
           "queueSizeSum,availGlobalBufferSum,availFixedBufferSum,"
           "tuplesPerSecond,tasksPerSecond,bufferPerSecond,mebiBPerSecond,"
           "numWorkerOfThreads,numberOfDeployedQueries,numberOfSources,bufferSizeInBytes,inputType,dataProviderMode,queryString"
        << std::endl;

    std::stringstream outputCsvStream;

    for (auto measurementsCsv :
         measurements.getMeasurementsAsCSV(schemaSizeInB, configPerRun.numberOfQueriesToDeploy->getValue())) {
        outputCsvStream << configOverAllRuns.benchmarkName->getValue();
        outputCsvStream << "," << NES_VERSION << "," << schemaSizeInB;
        outputCsvStream << "," << measurementsCsv;
        outputCsvStream << "," << configPerRun.numWorkerOfThreads->getValue();
        outputCsvStream << "," << configPerRun.numberOfQueriesToDeploy->getValue();
        outputCsvStream << "," << configPerRun.numberOfSources->getValue();
        outputCsvStream << "," << configPerRun.bufferSizeInBytes->getValue();
        outputCsvStream << "," << configOverAllRuns.inputType->getValue();
        outputCsvStream << "," << configOverAllRuns.dataProviderMode->getValue();
        outputCsvStream << ","
                        << "\"" << queryString << "\"";
        outputCsvStream << "\n";
    }

    std::ofstream ofs;
    ofs.open(configOverAllRuns.outputFile->getValue(), std::ofstream::out | std::ofstream::app);
    ofs << header.str();
    ofs << outputCsvStream.str();
    ofs.close();

    std::cout << "Measurements are:" << std::endl;
    std::cout << header.str() << outputCsvStream.str() << std::endl;

    NES_INFO("Done writing the measurements to " << configOverAllRuns.outputFile->getValue() << "!")
}

E2ESingleRun::E2ESingleRun(const E2EBenchmarkConfigPerRun& configPerRun,
                           const E2EBenchmarkConfigOverAllRuns& configOverAllRuns,
                           int portOffSet)
    : configPerRun(configPerRun), configOverAllRuns(configOverAllRuns), portOffSet(portOffSet) {}

E2ESingleRun::~E2ESingleRun() {
    coordinatorConf.reset();
    coordinator.reset();

    for (auto& dataProvider : allDataProviders) {
        dataProvider.reset();
    }
    allDataProviders.clear();

    for (auto& dataGenerator : allDataGenerators) {
        dataGenerator.reset();
    }
    allDataGenerators.clear();

    for (auto& bufferManager : allBufferManagers) {
        bufferManager.reset();
    }
    allBufferManagers.clear();
}

void E2ESingleRun::run() {
    setupCoordinatorConfig();
    createSources();
    runQuery();
    stopQuery();
    writeMeasurementsToCsv();
}

bool E2ESingleRun::waitForQueryToStart(QueryId queryId,
                                       const QueryCatalogServicePtr& queryCatalogService,
                                       std::chrono::seconds timeoutInSec) {
    NES_TRACE("TestUtils: wait till the query " << queryId << " gets into Running status.");
    auto start_timestamp = std::chrono::system_clock::now();

    NES_TRACE("TestUtils: Keep checking the status of query " << queryId << " until a fixed time out");
    while (std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        auto queryCatalogEntry = queryCatalogService->getEntryForQuery(queryId);
        if (!queryCatalogEntry) {
            NES_ERROR("TestUtils: unable to find the entry for query " << queryId << " in the query catalog.");
            return false;
        }
        NES_TRACE("TestUtils: Query " << queryId << " is now in status " << queryCatalogEntry->getQueryStatusAsString());
        QueryStatus::Value status = queryCatalogEntry->getQueryStatus();

        switch (queryCatalogEntry->getQueryStatus()) {
            case QueryStatus::MarkedForHardStop:
            case QueryStatus::MarkedForSoftStop:
            case QueryStatus::SoftStopCompleted:
            case QueryStatus::SoftStopTriggered:
            case QueryStatus::Stopped:
            case QueryStatus::Running: {
                return true;
            }
            case QueryStatus::Failed: {
                NES_ERROR("Query failed to start. Expected: Running or Optimizing but found " + QueryStatus::toString(status));
                return false;
            }
            default: {
                NES_WARNING("Expected: Running or Scheduling but found " + QueryStatus::toString(status));
                break;
            }
        }

        std::this_thread::sleep_for(E2ESingleRun::sleepDuration);
    }
    NES_TRACE("checkCompleteOrTimeout: waitForStart expected results are not reached after timeout");
    return false;
}

}// namespace NES::Benchmark
