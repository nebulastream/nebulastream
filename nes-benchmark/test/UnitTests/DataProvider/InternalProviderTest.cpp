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

#include <DataProvider/InternalProvider.hpp>
#include <DataGeneration/DataGenerator.hpp>
#include <E2E/Configurations/E2EBenchmarkConfigOverAllRuns.hpp>
#include <E2E/Configurations/E2EBenchmarkConfigPerRun.hpp>
#include <gtest/gtest.h>
#include <Util/Logger/Logger.hpp>

namespace NES::Benchmark::DataProviding {
    class InternalProviderTest : public testing::Test {
      public:
        /* Will be called before any test in this class are executed. */
        static void SetUpTestCase() {
            NES::Logger::setupLogging("InternalProviderTest.log", NES::LogLevel::LOG_DEBUG);
            std::cout << "Setup InternalProviderTest test class." << std::endl;
        }

        /* Will be called before a test is executed. */
        void SetUp() override { std::cout << "Setup InternalProviderTest test case." << std::endl; }

        /* Will be called before a test is executed. */
        void TearDown() override { std::cout << "Tear down InternalProviderTest test case." << std::endl; }

        /* Will be called after all tests in this class are finished. */
        static void TearDownTestCase() { std::cout << "Tear down InternalProviderTest test class." << std::endl; }
    };

    TEST_F(InternalProviderTest, readNextBufferTest) {
        E2EBenchmarkConfigOverAllRuns configOverAllRuns;
        E2EBenchmarkConfigPerRun configPerRun;
        size_t sourceId = 0;

        auto dataGenerator = std::make_shared<DataGeneration::DataGenerator>();
        auto bufferManager =  std::make_shared<Runtime::BufferManager>();
        dataGenerator->setBufferManager(bufferManager);
        auto createdBuffers = dataGenerator->createData(configOverAllRuns.numberOfPreAllocatedBuffer->getValue(),
                                                        configPerRun.bufferSizeInBytes->getValue());

        auto internalProviderDefault = DataProvider::createProvider(sourceId, configOverAllRuns, createdBuffers);
        auto nextBufferDefault = internalProviderDefault->readNextBuffer(sourceId);
    }

    // the following tests are not necessary, functions are empty
    /*TEST_F(InternalProviderTest, recyclePooledBufferTest) {

    }

    TEST_F(InternalProviderTest, recycleUnpooledBufferTest) {

    }

    TEST_F(InternalProviderTest, startTest) {

    }*/

    TEST_F(InternalProviderTest, stopTest) {

    }
}//namespace NES::Benchmark::DataGeneration