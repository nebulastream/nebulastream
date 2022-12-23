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

#include <gtest/gtest.h>
#include <Util/Logger/Logger.hpp>
#include <E2E/Configurations/E2EBenchmarkConfig.hpp>
#include <E2E/E2ESingleRun.hpp>
#include <NesBaseTest.hpp>
#include <Version/version.hpp>
#include <DataGeneration/DefaultDataGenerator.hpp>
#include <DataGeneration/ZipfianDataGenerator.hpp>
#include <fstream>
#include <string>


namespace NES::Benchmark {
    class E2ESingleRunTest : public Testing::NESBaseTest {
    public:
        /* Will be called before any test in this class are executed. */
        static void SetUpTestCase() {
            NES::Logger::setupLogging("E2ESingleRunTest.log", NES::LogLevel::LOG_DEBUG);
            NES_INFO("Setup E2ESingleRunTest test class.");
        }

        /* Will be called before a test is executed. */
        void SetUp() override {
            Testing::NESBaseTest::SetUp();
            NES_INFO("Setup E2ESingleRunTest test case.");

        }

        /* Will be called before a test is executed. */
        void TearDown() override {
            NES_INFO("Tear down E2ESingleRunTest test case.");
            Testing::NESBaseTest::TearDown();
        }

        /* Will be called after all tests in this class are finished. */
        static void TearDownTestCase() { NES_INFO("Tear down E2ESingleRunTest test class."); }
    };


    TEST_F(E2ESingleRunTest, createE2ESingleRun) {
        E2EBenchmarkConfigPerRun configPerRun;
        E2EBenchmarkConfigOverAllRuns configOverAllRuns;

        EXPECT_NO_THROW(E2ESingleRun singleRun(configPerRun, configOverAllRuns, *rpcCoordinatorPort, *restPort));
    }

    TEST_F(E2ESingleRunTest, setUpCoordinatorAndWorkerConfig) {
        E2EBenchmarkConfigOverAllRuns configOverAllRuns;
        E2EBenchmarkConfigPerRun configPerRun;

        configPerRun.numberOfWorkerThreads->setValue(20);
        configPerRun.bufferSizeInBytes->setValue(10 * 1024);
        configPerRun.numberOfBuffersInGlobalBufferManager->setValue(42);
        configPerRun.numberOfBuffersInSourceLocalBufferPool->setValue(43);

        auto rpcCoordinatorPortSaved = *rpcCoordinatorPort;
        auto restPortSaved = *restPort;
        E2ESingleRun singleRun(configPerRun, configOverAllRuns, rpcCoordinatorPortSaved, restPortSaved);
        singleRun.setupCoordinatorConfig();

        auto coordinatorConf = singleRun.getCoordinatorConf();
        EXPECT_EQ(coordinatorConf->rpcPort.getValue(), rpcCoordinatorPortSaved);
        EXPECT_EQ(coordinatorConf->restPort.getValue(), restPortSaved);
        EXPECT_EQ(coordinatorConf->worker.numWorkerThreads.getValue(), configPerRun.numberOfWorkerThreads->getValue());
        EXPECT_EQ(coordinatorConf->worker.bufferSizeInBytes.getValue(), configPerRun.bufferSizeInBytes->getValue());
        EXPECT_EQ(coordinatorConf->worker.numberOfBuffersInGlobalBufferManager.getValue(),
                  configPerRun.numberOfBuffersInGlobalBufferManager->getValue());
        EXPECT_EQ(coordinatorConf->worker.numberOfBuffersInSourceLocalBufferPool.getValue(),
                  configPerRun.numberOfBuffersInSourceLocalBufferPool->getValue());
        EXPECT_EQ(coordinatorConf->worker.rpcPort.getValue(), rpcCoordinatorPortSaved + 1);
        EXPECT_EQ(coordinatorConf->worker.dataPort.getValue(), rpcCoordinatorPortSaved + 2);
    }

    TEST_F(E2ESingleRunTest, createSources) {
        E2EBenchmarkConfigOverAllRuns configOverAllRuns;
        std::vector<E2EBenchmarkConfigPerRun> allConfigPerRuns;

        auto defaultDataGenerator = std::make_shared<DataGeneration::DefaultDataGenerator>(0, 1000);
        auto zipfianDataGenerator = std::make_shared<DataGeneration::ZipfianDataGenerator>(0.8, 0, 1000);
        configOverAllRuns.srcNameToDataGenerator = {{defaultDataGenerator->getName(), defaultDataGenerator},
                                                    {zipfianDataGenerator->getName(), zipfianDataGenerator}};

        for (auto i = 0; i < 3; ++i) {
            E2EBenchmarkConfigPerRun configPerRun;
            configPerRun.logicalSrcToNoPhysicalSrc = {{defaultDataGenerator->getName(), i + 1},
                                                      {zipfianDataGenerator->getName(), i + 2}};
            allConfigPerRuns.emplace_back(configPerRun);
        }

        for (auto cnt = 0UL; cnt < allConfigPerRuns.size(); ++cnt) {
            E2ESingleRun singleRun(allConfigPerRuns[cnt], configOverAllRuns, *rpcCoordinatorPort, *restPort);
            singleRun.createSources();

            auto coordinatorConf = singleRun.getCoordinatorConf();
            EXPECT_EQ(coordinatorConf->logicalSources.size(), 2);

            EXPECT_EQ(coordinatorConf->logicalSources[0].getName(), defaultDataGenerator->getName());
            EXPECT_EQ(coordinatorConf->logicalSources[1].getName(), defaultDataGenerator->getName());
            EXPECT_EQ(coordinatorConf->worker.physicalSources.size(), cnt + 1 + cnt + 2);

            std::map<std::string, uint64_t> tmpMap{{defaultDataGenerator->getName(), 0},
                                                   {zipfianDataGenerator->getName(), 0}};
            for (auto i = 0UL; i < coordinatorConf->worker.physicalSources.size(); ++i) {
                auto physicalSource = coordinatorConf->worker.physicalSources[i];
                tmpMap[physicalSource.getName()] += 1;
            }

            EXPECT_EQ(tmpMap[defaultDataGenerator->getName()], cnt + 1);
            EXPECT_EQ(tmpMap[zipfianDataGenerator->getName()], cnt + 2);
        }
    }

    TEST_F(E2ESingleRunTest, getNumberOfPhysicalSources) {
        auto defaultDataGenerator = std::make_shared<DataGeneration::DefaultDataGenerator>(0, 1000);
        auto zipfianDataGenerator = std::make_shared<DataGeneration::ZipfianDataGenerator>(0.8, 0, 1000);
        E2EBenchmarkConfigPerRun configPerRun;
        configPerRun.logicalSrcToNoPhysicalSrc = {{defaultDataGenerator->getName(), 123},
                                                  {zipfianDataGenerator->getName(), 456}};

        E2EBenchmarkConfigOverAllRuns configOverAllRuns;
        E2ESingleRun singleRun(configPerRun, configOverAllRuns, *rpcCoordinatorPort, *restPort);

        std::stringstream expected;
        expected << defaultDataGenerator->getName() << ": " << 123 << ", "
                 << zipfianDataGenerator->getName() << ": " << 456;

        EXPECT_EQ(expected.str(), configPerRun.getStrLogicalSrcToNumberOfPhysicalSrc());
    }


    TEST_F(E2ESingleRunTest, writeMeasurementsToCSV) {
        auto bmName = "Some awesome BM Name", inputType = "Auto", dataProviderMode = "ZeroCopy";
        auto queryString = "Query::from(source)", csvFile = "tmp.csv";
        auto numberOfWorkerThreads = 12, numberOfQueriesToDeploy = 123, bufferSizeInBytes = 8* 1024;
        auto defaultDataGenerator = std::make_shared<DataGeneration::DefaultDataGenerator>(0, 1000);
        auto zipfianDataGenerator = std::make_shared<DataGeneration::ZipfianDataGenerator>(0.8, 0, 1000);

        auto schemaSizeInB = defaultDataGenerator->getSchema()->getSchemaSizeInBytes() +
                             zipfianDataGenerator->getSchema()->getSchemaSizeInBytes();

        E2EBenchmarkConfigPerRun configPerRun;
        configPerRun.logicalSrcToNoPhysicalSrc = {{defaultDataGenerator->getName(), 2},
                                                  {zipfianDataGenerator->getName(), 23}};
        configPerRun.numberOfWorkerThreads->setValue(numberOfWorkerThreads);
        configPerRun.numberOfQueriesToDeploy->setValue(numberOfQueriesToDeploy);
        configPerRun.bufferSizeInBytes->setValue(bufferSizeInBytes);

        E2EBenchmarkConfigOverAllRuns configOverAllRuns;
        configOverAllRuns.benchmarkName->setValue(bmName);
        configOverAllRuns.srcNameToDataGenerator = {{defaultDataGenerator->getName(), defaultDataGenerator}};
        configOverAllRuns.inputType->setValue(inputType);
        configOverAllRuns.dataProviderMode->setValue(dataProviderMode);
        configOverAllRuns.query->setValue(queryString);
        configOverAllRuns.outputFile->setValue(csvFile);

        E2ESingleRun singleRun(configPerRun, configOverAllRuns, *rpcCoordinatorPort, *restPort);
        singleRun.createSources();
        auto measurements = singleRun.getMeasurements();

        auto processedTasksStart = 42;
        auto processedBuffersStart = 84;
        auto processedTuplesStart = 126;
        auto latencySumStart = 168;
        auto queueSizeSumStart = 210;
        auto availGlobalBufferSumStart = 252;
        auto availFixedBufferSumStart = 294;

        std::stringstream expectedCsvFile;
        expectedCsvFile << "BenchmarkName,NES_VERSION,SchemaSize,timestamp,processedTasks,processedBuffers,processedTuples,latencySum,"
                           "queueSizeSum,availGlobalBufferSum,availFixedBufferSum,"
                           "tuplesPerSecond,tasksPerSecond,bufferPerSecond,mebiBPerSecond,"
                           "numberOfWorkerOfThreads,numberOfDeployedQueries,numberOfSources,bufferSizeInBytes,inputType,dataProviderMode,queryString"
                        << std::endl;

        for (auto i = 0; i < 10; ++i) {
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

            expectedCsvFile << bmName
                            << "," << NES_VERSION << "," << schemaSizeInB
                            << "," << timeStamp << "," << processedTasksStart
                            << "," << (processedBuffersStart + i) << "," << (processedTuplesStart + i)
                            << "," << (latencySumStart + i) << "," << (queueSizeSumStart + i)
                            << "," << (availGlobalBufferSumStart + i) << "," << (availFixedBufferSumStart + i)
                            // tuplesPerSecond, tasksPerSecond, bufferPerSecond, mebiPerSecond
                            << "," << 1 << "," << 1 << "," << 1 << "," << 1
                            << "," << numberOfWorkerThreads << "," << numberOfQueriesToDeploy
                            << "," << "\"" << configPerRun.getStrLogicalSrcToNumberOfPhysicalSrc() << "\""
                            << "," << bufferSizeInBytes << "," << inputType
                            << "," << dataProviderMode << "\"" << queryString << "\"" << "\n";
        }


        std::ifstream ifs(csvFile);
        std::string writtenCsvFile((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());
        EXPECT_EQ(writtenCsvFile, expectedCsvFile.str());
    }



}