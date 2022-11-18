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
#include <Version/version.hpp>
#include <algorithm>
#ifdef ENABLE_KAFKA_BUILD
#include <cppkafka/cppkafka.h>
#endif
#include <fstream>
#include <Util/UtilityFunctions.hpp>

namespace NES::Benchmark {

void E2ESingleRun::setupCoordinatorConfig() {
    NES_INFO("Creating coordinator and worker configuration...");
    coordinatorConf = Configurations::CoordinatorConfiguration::create();

    // Coordinator configuration
    coordinatorConf->rpcPort = 5000 + portOffSet;
    coordinatorConf->restPort = 9082 + portOffSet;
    coordinatorConf->enableMonitoring = false;

    // Worker configuration
    coordinatorConf->worker.numWorkerThreads = configPerRun.numWorkerThreads->getValue();
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
    NES_INFO("Created coordinator and worker configuration!");
}

void E2ESingleRun::createSources() {
    size_t sourceCnt = 0;
    NES_INFO("Creating sources and the accommodating data generation and data providing...");
    for (size_t sourceCnt = 0; sourceCnt < configOverAllRuns.numSources->getValue(); ++sourceCnt) {
        auto bufferManager = std::make_shared<Runtime::BufferManager>(configPerRun.bufferSizeInBytes->getValue(),
                                                                      configOverAllRuns.numberOfPreAllocatedBuffer->getValue());

        auto dataGenerator =
            DataGeneration::DataGenerator::createGeneratorByName(configOverAllRuns.dataGenerator->getValue(), bufferManager);

        auto createdBuffers = dataGenerator->createData(configOverAllRuns.numberOfPreAllocatedBuffer->getValue(),
                                                        configPerRun.bufferSizeInBytes->getValue());

        auto schema = dataGenerator->getSchema();
        auto logicalStreamName = configOverAllRuns.logicalStreamName->getValue();
        auto physicalStreamName = "physical_input" + std::to_string(sourceCnt);
        auto logicalSource = LogicalSource::create(logicalStreamName, schema);
        coordinatorConf->logicalSources.add(logicalSource);

        size_t sourceAffinity = std::numeric_limits<uint64_t>::max();
        size_t taskQueueId = sourceCnt;

        if (configOverAllRuns.dataGenerator->getValue() == "YSBKafka") {
#ifdef ENABLE_KAFKA_BUILD
            auto connectionStringVec = NES::Util::splitWithStringDelimiter<std::string>(configOverAllRuns.connectionString->getValue()configOverAllRuns.connectionString->getValue(), ",");
            //push data to kafka topic
            cppkafka::Configuration config = {{"metadata.broker.list", connectionStringVec[0]}};
            cppkafka::Producer producer(config);

            for (uint64_t cnt = 0; cnt < createdBuffers.size(); cnt++) {
                char* buffer = createdBuffers[cnt].getBuffer<char>();
                size_t len = createdBuffers[cnt].getBufferSize();
                std::vector<uint8_t> bytes(buffer, buffer + len);
                try {
                    producer.produce(cppkafka::MessageBuilder(connectionStringVec[1]).partition(0).payload(bytes));
                    producer.flush(std::chrono::seconds(100));
                } catch (const std::exception& ex) {
                    std::cout << ex.what() << std::endl;
                } catch (const std::string& ex) {
                    std::cout << ex << std::endl;
                } catch (...) {
                }
                if (cnt % 1000 == 0) {
                    std::cout << "number of buffers prod=" << cnt << std::endl;
                }
            }
            producer.flush(std::chrono::seconds(100));

            auto kafkaSourceType = KafkaSourceType::create();
            kafkaSourceType->setBrokers(connectionStringVec[0]);
            kafkaSourceType->setTopic(connectionStringVec[1]);
            kafkaSourceType->setGroupId(connectionStringVec[2]);
            kafkaSourceType->setNumberOfBuffersToProduce(configPerRun.numBuffersToProduce->getValue());

            auto physicalSource = PhysicalSource::create(logicalStreamName, physicalStreamName, kafkaSourceType);
            coordinatorConf->worker.physicalSources.add(physicalSource);

            allDataGenerators.emplace_back(dataGenerator);
            allBufferManagers.emplace_back(bufferManager);
#else
            NES_ASSERT(false, "Please add kafka compiler flag.");
#endif
        } else {
            auto dataProvider =
                DataProviding::DataProvider::createProvider(/* sourceIndex */ sourceCnt, configOverAllRuns, createdBuffers);
            // Adding necessary items to the corresponding vectors
            allDataProviders.emplace_back(dataProvider);
            allDataGenerators.emplace_back(dataGenerator);
            allBufferManagers.emplace_back(bufferManager);

            size_t generatorQueueIndex = 0;
            auto dataProvidingFunc = [this, sourceCnt, generatorQueueIndex](Runtime::TupleBuffer& buffer, uint64_t) {
                allDataProviders[sourceCnt]->provideNextBuffer(buffer, generatorQueueIndex);
            };

            LambdaSourceTypePtr sourceConfig = LambdaSourceType::create(dataProvidingFunc,
                                                                        configPerRun.numBuffersToProduce->getValue(),
                                                                        /* gatheringValue */ 0,
                                                                        GatheringMode::INTERVAL_MODE,
                                                                        sourceAffinity,
                                                                        taskQueueId);

            auto physicalSource = PhysicalSource::create(logicalStreamName, physicalStreamName, sourceConfig);
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

    queryId = queryService->validateAndQueueAddQueryRequest(configOverAllRuns.query->getValue(), "BottomUp");
    bool queryResult = Benchmark::Util::waitForQueryToStart(queryId, queryCatalog);
    if (!queryResult) {
        NES_THROW_RUNTIME_ERROR("E2ERunner: Query id=" << queryId << " did not start!");
    }
    NES_INFO("Started query with id=" << queryId);

    NES_DEBUG("Starting the data provider...");
    for (auto& dataProvider : allDataProviders) {
        dataProvider->start();
    }
    NES_DEBUG("Started the data provider!");

    // Wait for the system to come to a steady state
    NES_INFO("Now waiting for " << configOverAllRuns.startupSleepIntervalInSeconds->getValue()
                                << "s to let the system come to a steady state!");
    sleep(configOverAllRuns.startupSleepIntervalInSeconds->getValue());

    // For now, we only support one way of collecting the measurements
    NES_INFO("Starting to collect measurements...");
    bool found = false;
    while (!found) {
        auto stats = coordinator->getNodeEngine()->getQueryStatistics(queryId);
        for (auto iter : stats) {
            while (iter->getProcessedBuffers() < 1) {
                NES_DEBUG("Query  with id " << queryId << " not ready with value= " << iter->getProcessedBuffers()
                                            << ". Sleeping for a second now...");
                sleep(1);
            }
            found = true;
        }
    }

    NES_INFO("Starting measurements...");
    // We have to measure once more than the required numMeasurementsToCollect as we calculate deltas later on
    for (uint64_t cnt = 0; cnt <= configOverAllRuns.numMeasurementsToCollect->getValue(); ++cnt) {
        int64_t nextPeriodStartTime = configOverAllRuns.experimentMeasureIntervalInSeconds->getValue() * 1000;
        nextPeriodStartTime +=
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

        auto statisticsCoordinator = coordinator->getNodeEngine()->getQueryStatistics(queryId);
        uint64_t timeStamp =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
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
            std::cout << "measure_out=" << processedTasks << "," << processedBuffers << "," << processedTuples << ","
                      << latencySum << "," << queueSizeSum << "," << availGlobalBufferSum << "," << availFixedBufferSum << ","
                      << timeStamp << std::endl;

            measurements.addNewMeasurement(processedTasks,
                                           processedBuffers,
                                           processedTuples,
                                           latencySum,
                                           queueSizeSum,
                                           availGlobalBufferSum,
                                           availFixedBufferSum,
                                           timeStamp);
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

    // Sending a stop request to the coordinator with a timeout of 30 seconds
    queryService->validateAndQueueStopQueryRequest(queryId);
//    NES_ASSERT(queryService->validateAndQueueStopQueryRequest(queryId), "No valid stop request!");
    auto start_timestamp = std::chrono::system_clock::now();
    while (std::chrono::system_clock::now() < start_timestamp + stopQueryTimeoutInSec) {
        NES_TRACE("checkStoppedOrTimeout: check query status for " << queryId);
        if (queryCatalog->getEntryForQuery(queryId)->getQueryStatus() == QueryStatus::Stopped) {
            NES_TRACE("checkStoppedOrTimeout: status for " << queryId << " reached stopped");
            break;
        }
        NES_DEBUG("checkStoppedOrTimeout: status not reached for "
                  << queryId << " as status is=" << queryCatalog->getEntryForQuery(queryId)->getQueryStatusAsString());
        std::this_thread::sleep_for(stopQuerySleep);
    }
    NES_TRACE("checkStoppedOrTimeout: expected status not reached within set timeout");

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
    header << "BenchmarkName,NES_VERSION,SchemaSize,timestamp,processedTasks,processedBuffers,processedTuples,latencySum,"
              "queueSizeSum,availGlobalBufferSum,availFixedBufferSum,"
              "tuplesPerSecond,tasksPerSecond,bufferPerSecond,mebiBPerSecond,"
              "numWorkerThreads,numSources,bufferSizeInBytes,inputType,dataProviderMode,queryString"
           << std::endl;

    std::stringstream outputCsvStream;

    for (auto measurementsCsv : measurements.getMeasurementsAsCSV(schemaSizeInB)) {
        outputCsvStream << configOverAllRuns.benchmarkName->getValue();
        outputCsvStream << "," << NES_VERSION << "," << schemaSizeInB;
        outputCsvStream << "," << measurementsCsv;
        outputCsvStream << "," << configPerRun.numWorkerThreads->getValue();
        outputCsvStream << "," << configOverAllRuns.numSources->getValue();
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

}// namespace NES::Benchmark
