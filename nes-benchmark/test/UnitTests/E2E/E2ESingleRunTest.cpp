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

#include <API/Schema.hpp>
#include <BaseIntegrationTest.hpp>
#include <Configurations/Coordinator/LogicalSourceType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/PhysicalSourceType.hpp>
#include <DataGeneration/DefaultDataGenerator.hpp>
#include <DataGeneration/ZipfianDataGenerator.hpp>
#include <E2E/Configurations/E2EBenchmarkConfig.hpp>
#include <E2E/E2ERunner.hpp>
#include <E2E/E2ESingleRun.hpp>
#include <Util/Logger/Logger.hpp>
#include <Version/version.hpp>
#include <fstream>
#include <gtest/gtest.h>
#include <string>

namespace NES::Benchmark {
class E2ESingleRunTest : public Testing::BaseIntegrationTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("E2ESingleRunTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup E2ESingleRunTest test class.");
    }
};

TEST_F(E2ESingleRunTest, createE2ESingleRun) {
    E2EBenchmarkConfigPerRun configPerRun;
    E2EBenchmarkConfigOverAllRuns configOverAllRuns;

    ASSERT_NO_THROW(E2ESingleRun singleRun(configPerRun, configOverAllRuns, *rpcCoordinatorPort, *restPort));
}

/**
 * @brief Testing if E2ESingleRun::setupCoordinatorConfig() is correct by comparing with a hardcoded truth
 **/
TEST_F(E2ESingleRunTest, setUpCoordinatorAndWorkerConfig) {
    E2EBenchmarkConfigOverAllRuns configOverAllRuns;
    E2EBenchmarkConfigPerRun configPerRun;

    configPerRun.numberOfWorkerThreads->setValue(20);
    configPerRun.bufferSizeInBytes->setValue(10 * 1024);
    configPerRun.numberOfBuffersInGlobalBufferManager->setValue(42);
    configPerRun.numberOfBuffersInSourceLocalBufferPool->setValue(43);

    uint16_t rpcCoordinatorPortSaved = *rpcCoordinatorPort;
    uint16_t restPortSaved = *restPort;
    E2ESingleRun singleRun(configPerRun, configOverAllRuns, rpcCoordinatorPortSaved, restPortSaved);
    singleRun.setupCoordinatorConfig();

    auto coordinatorConf = singleRun.getCoordinatorConf();
    ASSERT_EQ(coordinatorConf->rpcPort.getValue(), rpcCoordinatorPortSaved);
    ASSERT_EQ(coordinatorConf->restPort.getValue(), restPortSaved);
    ASSERT_EQ(coordinatorConf->worker.numWorkerThreads.getValue(), configPerRun.numberOfWorkerThreads->getValue());
    ASSERT_EQ(coordinatorConf->worker.bufferSizeInBytes.getValue(), configPerRun.bufferSizeInBytes->getValue());
    ASSERT_EQ(coordinatorConf->worker.numberOfBuffersInGlobalBufferManager.getValue(),
              configPerRun.numberOfBuffersInGlobalBufferManager->getValue());
    ASSERT_EQ(coordinatorConf->worker.numberOfBuffersInSourceLocalBufferPool.getValue(),
              configPerRun.numberOfBuffersInSourceLocalBufferPool->getValue());
}

/**
 * @brief Testing if E2ESingleRun::createSources() is correct by creating several configPerRuns and then comparing versus a
 * hardcoded truth
 **/
TEST_F(E2ESingleRunTest, createSources) {
    E2EBenchmarkConfigOverAllRuns configOverAllRuns;
    std::vector<E2EBenchmarkConfigPerRun> allConfigPerRuns;

    auto defaultDataGenerator = std::make_unique<DataGeneration::DefaultDataGenerator>(0, 1000);
    auto zipfianDataGenerator = std::make_unique<DataGeneration::ZipfianDataGenerator>(0.8, 0, 1000);
    const auto defaultDataGeneratorName = defaultDataGenerator->getName();
    const auto zipfianDataGeneratorName = zipfianDataGenerator->getName();

    configOverAllRuns.sourceNameToDataGenerator.clear();
    configOverAllRuns.sourceNameToDataGenerator[defaultDataGeneratorName] = std::move(defaultDataGenerator);
    configOverAllRuns.sourceNameToDataGenerator[zipfianDataGeneratorName] = std::move(zipfianDataGenerator);
    configOverAllRuns.numberOfPreAllocatedBuffer->setValue(10);

    for (auto i = 0; i < 3; ++i) {
        E2EBenchmarkConfigPerRun configPerRun;
        configPerRun.logicalSrcToNoPhysicalSrc = {{defaultDataGeneratorName, i + 1}, {zipfianDataGeneratorName, i + 2}};
        configPerRun.bufferSizeInBytes->setValue(512);
        allConfigPerRuns.emplace_back(configPerRun);
    }

    for (auto cnt = 0UL; cnt < allConfigPerRuns.size(); ++cnt) {
        E2ESingleRun singleRun(allConfigPerRuns[cnt], configOverAllRuns, *rpcCoordinatorPort, *restPort);
        singleRun.setupCoordinatorConfig();
        singleRun.createSources();

        auto coordinatorConf = singleRun.getCoordinatorConf();
        ASSERT_EQ(coordinatorConf->logicalSourceTypes.size(), 2);

        ASSERT_EQ(coordinatorConf->logicalSourceTypes[0].getValue()->getLogicalSourceName(), defaultDataGeneratorName);
        ASSERT_EQ(coordinatorConf->logicalSourceTypes[1].getValue()->getLogicalSourceName(), zipfianDataGeneratorName);
        ASSERT_EQ(coordinatorConf->worker.physicalSourceTypes.size(), cnt + 1 + cnt + 2);

        std::map<std::string, uint64_t> tmpMap{{defaultDataGeneratorName, 0}, {zipfianDataGeneratorName, 0}};
        for (auto i = 0UL; i < coordinatorConf->worker.physicalSourceTypes.size(); ++i) {
            auto physicalSource = coordinatorConf->worker.physicalSourceTypes[i];
            tmpMap[physicalSource.getValue()->getLogicalSourceName()] += 1;
        }

        ASSERT_EQ(tmpMap[defaultDataGeneratorName], cnt + 1);
        ASSERT_EQ(tmpMap[zipfianDataGeneratorName], cnt + 2);
    }
}

/**
 * @brief Testing if E2ESingleRun::getStringLogicalSourceToNumberOfPhysicalSources() is correct by comparing versus
 * a hardcoded truth
 **/
TEST_F(E2ESingleRunTest, getNumberOfPhysicalSources) {
    auto defaultDataGenerator = std::make_unique<DataGeneration::DefaultDataGenerator>(0, 1000);
    auto zipfianDataGenerator = std::make_unique<DataGeneration::ZipfianDataGenerator>(0.8, 0, 1000);
    E2EBenchmarkConfigPerRun configPerRun;
    configPerRun.logicalSrcToNoPhysicalSrc = {{defaultDataGenerator->getName(), 123}, {zipfianDataGenerator->getName(), 456}};

    E2EBenchmarkConfigOverAllRuns configOverAllRuns;
    E2ESingleRun singleRun(configPerRun, configOverAllRuns, *rpcCoordinatorPort, *restPort);

    std::stringstream assertedString;
    assertedString << defaultDataGenerator->getName() << ": " << 123 << ", " << zipfianDataGenerator->getName() << ": " << 456;

    ASSERT_EQ(assertedString.str(), configPerRun.getStringLogicalSourceToNumberOfPhysicalSources());
}

/**
 * @brief Testing if E2ESingleRun::writeHeaderToCsvFile() is correct by creating multiple different measurements and
 * then compare the written csv file to a hardcoded truth
 **/
TEST_F(E2ESingleRunTest, writeMeasurementsToCSV) {
    auto bmName = "Some awesome BM Name", inputType = "Auto", dataProviderMode = "ZeroCopy";
    auto queryString = "Query::from(source)", csvFile = "tmp.csv";
    auto numberOfWorkerThreads = 12, numberOfQueriesToDeploy = 123, bufferSizeInBytes = 8 * 1024;
    auto defaultDataGenerator = std::make_unique<DataGeneration::DefaultDataGenerator>(0, 1000);
    auto zipfianDataGenerator = std::make_unique<DataGeneration::ZipfianDataGenerator>(0.8, 0, 1000);
    const auto defaultDataGeneratorName = defaultDataGenerator->getName();
    const auto zipfianDataGeneratorName = zipfianDataGenerator->getName();

    auto schemaSizeInB =
        defaultDataGenerator->getSchema()->getSchemaSizeInBytes() + zipfianDataGenerator->getSchema()->getSchemaSizeInBytes();

    E2EBenchmarkConfigPerRun configPerRun;
    configPerRun.logicalSrcToNoPhysicalSrc = {{defaultDataGeneratorName, 2}, {zipfianDataGeneratorName, 23}};
    configPerRun.numberOfWorkerThreads->setValue(numberOfWorkerThreads);
    configPerRun.numberOfQueriesToDeploy->setValue(numberOfQueriesToDeploy);
    configPerRun.bufferSizeInBytes->setValue(bufferSizeInBytes);

    E2EBenchmarkConfigOverAllRuns configOverAllRuns;
    configOverAllRuns.benchmarkName->setValue(bmName);
    configOverAllRuns.sourceNameToDataGenerator.clear();
    configOverAllRuns.sourceNameToDataGenerator[defaultDataGeneratorName] = std::move(defaultDataGenerator);
    configOverAllRuns.sourceNameToDataGenerator[zipfianDataGeneratorName] = std::move(zipfianDataGenerator);
    configOverAllRuns.inputType->setValue(inputType);
    configOverAllRuns.dataProviderMode->setValue(dataProviderMode);
    configOverAllRuns.queries[0] = {queryString, 0};
    configOverAllRuns.outputFile->setValue(csvFile);
    std::filesystem::remove(csvFile);

    E2ESingleRun singleRun(configPerRun, configOverAllRuns, *rpcCoordinatorPort, *restPort);
    singleRun.setupCoordinatorConfig();
    singleRun.createSources();
    auto& measurements = singleRun.getMeasurements();

    auto processedTasksStart = 42;
    auto processedBuffersStart = 84;
    auto processedTuplesStart = 126;
    auto latencySumStart = 168;
    auto queueSizeSumStart = 210;
    auto availGlobalBufferSumStart = 252;
    auto availFixedBufferSumStart = 294;
    auto MAX_TIMESTAMP = 10;

    std::stringstream assertedCsvFile;
    assertedCsvFile
        << "BenchmarkName,NES_VERSION,SchemaSize,timestamp,processedTasks,processedBuffers,processedTuples,latencySum,"
           "queueSizeSum,availGlobalBufferSum,availFixedBufferSum,"
           "tuplesPerSecond,tasksPerSecond,bufferPerSecond,mebiBPerSecond,"
           "numberOfWorkerOfThreads,numberOfDeployedQueries,numberOfSources,bufferSizeInBytes,inputType,dataProviderMode,"
           "queryString"
        << std::endl;

    for (auto i = 0; i < MAX_TIMESTAMP; ++i) {
        auto timeStamp = i;
        measurements.addNewTimestamp(timeStamp);
        measurements.addNewMeasurement(processedTasksStart + i,
                                       processedBuffersStart + i,
                                       processedTuplesStart + i,
                                       latencySumStart + i,
                                       queueSizeSumStart + i,
                                       availGlobalBufferSumStart + i,
                                       availFixedBufferSumStart + i,
                                       timeStamp);

        if (i < MAX_TIMESTAMP - 1) {
            assertedCsvFile << "\"" << bmName << "\""
                            << "," << NES_VERSION << "," << schemaSizeInB << "," << timeStamp << "," << (processedTasksStart + i)
                            << "," << (processedBuffersStart + i) << "," << (processedTuplesStart + i) << ","
                            << (latencySumStart + i) << "," << (queueSizeSumStart + i) / numberOfQueriesToDeploy << ","
                            << (availGlobalBufferSumStart + i) / numberOfQueriesToDeploy << ","
                            << (availFixedBufferSumStart + i) / numberOfQueriesToDeploy
                            // tuplesPerSecond, tasksPerSecond, bufferPerSecond, mebiPerSecond
                            << "," << 1 << "," << 1 << "," << 1 << "," << (1.0 * schemaSizeInB) / (1024.0 * 1024.0) << ","
                            << numberOfWorkerThreads << "," << numberOfQueriesToDeploy << ","
                            << "\"" << configPerRun.getStringLogicalSourceToNumberOfPhysicalSources() << "\""
                            << "," << bufferSizeInBytes << "," << inputType << "," << dataProviderMode << ","
                            << "\"" << queryString << "\"" << std::endl;
        }
    }

    Benchmark::writeHeaderToCsvFile(configOverAllRuns);
    singleRun.writeMeasurementsToCsv();

    std::ifstream ifs(csvFile);
    std::string writtenCsvFile((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());
    ASSERT_EQ(writtenCsvFile, assertedCsvFile.str());
}
}// namespace NES::Benchmark
