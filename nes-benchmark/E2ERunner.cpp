/*
Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#include <Runtime/NodeEngine.hpp>
#include <Components/NesCoordinator.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <DataGeneration/DataGenerator.hpp>
#include <DataProvider/DataProvider.hpp>
#include <E2EBenchmarkConfig.hpp>
#include <Exceptions/ErrorListener.hpp>
#include <Measurements.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Version/version.hpp>

#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/LambdaSourceType.hpp>
#include <Sources/LambdaSource.hpp>
#include <cstring>
#include <filesystem>
#include <fstream>

namespace NES::Exceptions {
    extern void installGlobalErrorListener(std::shared_ptr<ErrorListener> const&);
}
const std::string logo = "/********************************************************\n"
                         " *     _   _   ______    _____\n"
                         " *    | \\ | | |  ____|  / ____|\n"
                         " *    |  \\| | | |__    | (___\n"
                         " *    |     | |  __|    \\___ \\     Benchmark Runner\n"
                         " *    | |\\  | | |____   ____) |\n"
                         " *    |_| \\_| |______| |_____/\n"
                         " *\n"
                         " ********************************************************/";

class BenchmarkRunner : public NES::Exceptions::ErrorListener {
  public:
    void onFatalError(int signalNumber, std::string callStack) override {
        std::ostringstream fatalErrorMessage;
        fatalErrorMessage << "onFatalError: signal [" << signalNumber << "] error [" << strerror(errno) << "] callstack " << callStack;

        NES_FATAL_ERROR(fatalErrorMessage.str());
        std::cerr << fatalErrorMessage.str() << std::endl;
    }
    void onFatalException(std::shared_ptr<std::exception> exceptionPtr, std::string callStack) override {
        std::ostringstream fatalExceptionMessage;
        fatalExceptionMessage << "onFatalException: exception=[" << exceptionPtr->what() << "] callstack=\n" << callStack;

        NES_FATAL_ERROR(fatalExceptionMessage.str());
        std::cerr << fatalExceptionMessage.str() << std::endl;
    }
};

int main(int argc, const char* argv[]) {


    std::cout << logo << std::endl;

    // Activating and installing error listener
    auto runner = std::make_shared<BenchmarkRunner>();
    NES::Exceptions::installGlobalErrorListener(runner);

    if (argc > 2 || argc == 0) {
        std::cerr << "Error: Only --configPath= is allowed as a command line argument!\nExiting now..." << std::endl;
        return -1;
    }
    auto configPathArg = std::string(argv[1]);
    if (configPathArg.find("--configPath") == std::string::npos) {
        std::cerr << "Error: --configPath could not been found in " << configPathArg << "!" << std::endl;
        return -1;
    }

    // Reading and parsing the config yaml file
    auto configPath = configPathArg.substr(configPathArg.find("=") + 1, configPathArg.length() - 1);
    std::cout << "parsed yaml config: " << configPath << std::endl;
    if (configPath.empty() || !std::filesystem::exists(configPath)) {
        std::cerr << "No yaml file provided or the file does not exist!" << std::endl;
        return -1;
    }


    NES::Logger::setupLogging("e2eRunnerStartup.log", NES::Benchmark::E2EBenchmarkConfig::getLogLevel(configPath));
    NES::Benchmark::E2EBenchmarkConfig e2EBenchmarkConfig;

    try {
        e2EBenchmarkConfig = NES::Benchmark::E2EBenchmarkConfig::createBenchmarks(configPath);
        NES_INFO("E2ERunner: Created the following experiments: " << e2EBenchmarkConfig.toString());
    } catch (std::exception& e) {
        NES_ERROR("E2ERunner: Error while creating benchmarks!");
        return -1;
    }

    // Creates the data providers
    NES::Runtime::BufferManagerPtr bufferManager = std::make_shared<NES::Runtime::BufferManager>();
    auto dataGenerator = NES::DataGeneration::DataGenerator::createGeneratorByName("Default", bufferManager);


    std::stringstream outputCsvStream;
    outputCsvStream << "BM_NAME,NES_VERSION,SchemaSizeInB";
    outputCsvStream << ",Time,ProcessedTasks,ProcessedBuffers,ProcessedTuples,LatencySum,QueueSizeSum";
    outputCsvStream << ",ThroughputInTupsPerSec,ThroughputInTasksPerSec,ThroughputInBuffersPerSec,ThroughputInMiBPerSec";
    outputCsvStream << ",WorkerThreads,SourceCnt,BufferSizeInB,InputType,DataProviderMode,Query";
    outputCsvStream << "\n";

    int portOffset = 0;
    auto configOverAllRuns = e2EBenchmarkConfig.getConfigOverAllRuns();
    for (auto& configPerRun : e2EBenchmarkConfig.getAllConfigPerRuns()) {
        auto coordinatorConf = NES::Configurations::CoordinatorConfiguration::create();
        auto workerConf = NES::Configurations::WorkerConfiguration::create();

        portOffset += 23;

        // Coordinator configurations
        coordinatorConf->rpcPort = (5000 + portOffset);
        coordinatorConf->restPort = (9082 + portOffset);
        coordinatorConf->enableMonitoring = false;

        // Worker configurations
        coordinatorConf->worker.numWorkerThreads = configPerRun.numWorkerThreads->getValue();
        coordinatorConf->worker.bufferSizeInBytes = configPerRun.bufferSizeInBytes->getValue();
        coordinatorConf->worker.rpcPort = coordinatorConf->rpcPort.getValue() + 1;
        coordinatorConf->worker.dataPort = coordinatorConf->rpcPort.getValue() + 2;
        coordinatorConf->worker.coordinatorIp = coordinatorConf->coordinatorIp.getValue();
        coordinatorConf->worker.localWorkerIp = coordinatorConf->coordinatorIp.getValue();
        coordinatorConf->worker.queryCompiler.windowingStrategy = NES::QueryCompilation::QueryCompilerOptions::DEFAULT;
        coordinatorConf->worker.numaAwareness = true;
        coordinatorConf->worker.queryCompiler.useCompilationCache = true;
        coordinatorConf->worker.enableMonitoring = false;

        NES_INFO("E2ERunner: Creating a data generator and a data provider...")
        auto buffers = dataGenerator->createData(configPerRun.numBuffersToProduce->getValue(),
                                                 configPerRun.bufferSizeInBytes->getValue());
        auto provider = NES::DataProviding::DataProvider::createProvider(/*source Index*/ 0,
                                                                         e2EBenchmarkConfig,
                                                                         buffers);

        size_t taskQueueId = 0;
        size_t queryIndex = 0;
        size_t generatorQueueIndex = 0;
        size_t sourceAffinity = std::numeric_limits<uint64_t>::max();
        auto schema = dataGenerator->getSchema();
        auto logicalStreamName = "input";
        auto physicalStreamName = "input1";
        auto func = [provider, generatorQueueIndex](NES::Runtime::TupleBuffer& buffer, uint64_t) {
            provider->provideNextBuffer(buffer, generatorQueueIndex);
        };
        NES::LambdaSourceTypePtr sourceConf = NES::LambdaSourceType::create(func,
                                                                            configPerRun.numBuffersToProduce->getValue(),
                                                                            0, NES::GatheringMode::INTERVAL_MODE,
                                                                            sourceAffinity,
                                                                            taskQueueId);
        NES::LogicalSourcePtr logicalSource = NES::LogicalSource::create(logicalStreamName, schema);
        auto physicalSource = NES::PhysicalSource::create(logicalStreamName, physicalStreamName, sourceConf);
        coordinatorConf->logicalSources.add(logicalSource);
        coordinatorConf->worker.physicalSources.add(physicalSource);

        NES_INFO("E2ERunner: Starting nesCoordinator...");
        auto coordinator = std::make_shared<NES::NesCoordinator>(coordinatorConf);
        auto rpcPort = coordinator->startCoordinator(/**blocking**/ false);
        NES_INFO("E2ERunner: Started nesCoordinator at port " << rpcPort << "!");

        auto queryService = coordinator->getQueryService();
        auto queryCatalog = coordinator->getQueryCatalogService();


        auto queryId = queryService->validateAndQueueAddQueryRequest(configOverAllRuns.query->getValue(), "BottomUp");
        bool queryResult = NES::Benchmark::Utils::waitForQueryToStart(queryId, queryCatalog);
        if (!queryResult) {
            NES_ERROR("E2ERunner: Query id=" << queryId << " did not start!");
            return -1;
        }
        NES_INFO("E2ERunner: Started query with id=" << queryId);

        NES_INFO("E2ERunner: Starting the data provider...");
        provider->start();
        NES_INFO("E2ERunner: Started the data povider!");

        // Wait for the system to come to a steady state
        NES_INFO("E2ERunner: Now waiting for " << configOverAllRuns.startupSleepIntervalInSeconds->getValue()
                 << "s to let the system come to a steady state!");
        sleep(configOverAllRuns.startupSleepIntervalInSeconds->getValue());


        // For now, we only support one way of collecting the measurements
        NES_INFO("E2ERunner: Starting to collect measurements...")
        auto nodeEngine = coordinator->getNodeEngine();
        bool found = false;

        while (!found) {
            auto stats = coordinator->getNodeEngine()->getQueryStatistics(queryId);
            for (auto iter : stats) {
                while (iter->getProcessedBuffers() < 2) {
                    NES_INFO("E2ERunner: Query  with id " << queryId << " not ready with value= " << iter->getProcessedBuffers()
                                               << ". Sleeping for a second now...");
                    sleep(1);
                }
                NES_INFO("E2ERunner: Query  with id " << queryId << " not ready with value= " << iter->getProcessedBuffers());
                found = true;
            }
        }

        NES::Measurements::Measurements measurements;
        // We have to measure once more than the required numMeasurementsToCollect as we build deltas
        for (uint64_t cnt = 0; cnt <= e2EBenchmarkConfig.getConfigOverAllRuns().numMeasurementsToCollect->getValue(); ++cnt) {
            int64_t nextPeriodStartTime = e2EBenchmarkConfig.getConfigOverAllRuns().experimentMeasureIntervalInSeconds->getValue() * 1000;
            nextPeriodStartTime += std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

            auto statisticsCoordinator = coordinator->getNodeEngine()->getQueryStatistics(queryId);
            uint64_t timeStamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
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
                    availGlobalBufferSum += (subPlanStatistics->getAvailableGlobalBufferSum() / subPlanStatistics->getProcessedBuffers());
                    availFixedBufferSum += (subPlanStatistics->getAvailableFixedBufferSum() / subPlanStatistics->getProcessedBuffers());
                }

                measurements.addNewMeasurement(processedTasks, processedBuffers, processedTuples,
                                               latencySum, queueSizeSum, availGlobalBufferSum,
                                               availFixedBufferSum, timeStamp);
            }


            // Calculating the time to sleep
            auto curTime = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
            auto sleepTime = nextPeriodStartTime - curTime;
            std::this_thread::sleep_for(std::chrono::milliseconds(sleepTime));
        }

        // Shutting everything down
        NES_INFO("E2ERunner: Shutting everything down...");
        buffers.clear();

        // Removing query from the system
        NES_INFO("E2ERunner: Removing query from the system...");
        NES_ASSERT(queryService->validateAndQueueStopQueryRequest(queryId), "No valid stop request!");
        provider->stop();
        coordinator->stopCoordinator(true);


        coordinator.reset();
        queryService.reset();
        queryCatalog.reset();
        provider.reset();
        buffers.clear();




        auto schemaSizeInB = dataGenerator->getSchema()->getSchemaSizeInBytes();
        for (auto measurementsCsv : measurements.getMeasurementsAsCSV(schemaSizeInB)) {
            outputCsvStream << e2EBenchmarkConfig.getConfigOverAllRuns().benchmarkName->getValue();
            outputCsvStream << "," << NES_VERSION << "," << schemaSizeInB;
            outputCsvStream << "," << measurementsCsv;
            outputCsvStream << "," << configPerRun.numWorkerThreads->getValue();
            outputCsvStream << "," << configPerRun.numSources->getValue();
            outputCsvStream << "," << configPerRun.bufferSizeInBytes->getValue();
            outputCsvStream << "," << configOverAllRuns.inputType->getValue();
            outputCsvStream << "," << configOverAllRuns.dataProviderMode->getValue();
            outputCsvStream << "," << configOverAllRuns.query->getValue();
            outputCsvStream << "\n";
        }


        std::ofstream ofs;
        ofs.open(e2EBenchmarkConfig.getConfigOverAllRuns().outputFile->getValue(),
                 std::ofstream::out | std::ofstream::app);
        ofs << outputCsvStream.str();
        ofs.close();

        outputCsvStream.clear();
    }

}