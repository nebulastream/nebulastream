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
#include <DataGeneration/DefaultDataGenerator.hpp>
#include <DataGeneration/ZipfianDataGenerator.hpp>


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
        // we test here, if we can set the coordinator and worker config from a parsed yaml file
        // might need multiple tests for this

        E2EBenchmarkConfigOverAllRuns configOverAllRuns;
        E2EBenchmarkConfigPerRun configPerRun;

        configPerRun.numberOfWorkerThreads->setValue(20);
        configPerRun.bufferSizeInBytes->setValue(10 * 1024);
        configPerRun.numberOfBuffersInGlobalBufferManager->setValue(42);
        configPerRun.numberOfBuffersInSourceLocalBufferPool->setValue(43);

        auto rpcCoordinatorPortSaved = *rpcCoordinatorPort;
        auto restPortSaved = *restPort;
        E2ESingleRun singleRun(configPerRun, configOverAllRuns, rpcCoordinatorPortSaved, restPortSaved);
        singleRun.createSources();

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
        // test here if we can create different logical sources in terms of data generators
        E2EBenchmarkConfigOverAllRuns configOverAllRuns;
        std::vector<E2EBenchmarkConfigPerRun> allConfigPerRuns;

        auto defaultDataGenerator = std::make_shared<DataGeneration::DefaultDataGenerator>(0, 1000);
        auto zipfianDataGenerator = std::make_shared<DataGeneration::ZipfianDataGenerator>(0.8, 0, 1000);
        configOverAllRuns.mapLogicalSrcNameToDataGenerator = {{defaultDataGenerator->getName(), defaultDataGenerator},
                                                              {zipfianDataGenerator->getName(), zipfianDataGenerator}};

        for (auto i = 0; i < 3; ++i) {
            E2EBenchmarkConfigPerRun configPerRun;
            configPerRun.mapLogicalSrcToNumberOfPhysSrc{{defaultDataGenerator->getName(), }};
            Hier weiter machen mit dem Erstellen der Create Sources Test
        }



    }


    TEST_F(E2ESingleRunTest, createSourcesDifferentPhysicalSourcesPerRun) {
        // we test here, if we can create all logical and physical sources
        // might need multiple tests for having different test cases, such as having different



        // Create here different configsPerRun with different number of physical sources for each run
        for (auto& configPerRun : allConfigPerRuns) {

        }
    }


    TEST_F(E2ESingleRunTest, writeMeasurementsToCSV) {
        // we test here, if we can write the measurements to a csv file by writing measurements to a csv file and then
        // comparing the created csv file with an expected output

    }



}