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

//#include <API/Schema.hpp>
#include <DataProvider/InternalProvider.hpp>
//#include <DataGeneration/DataGenerator.hpp>
//#include <E2E/Configurations/E2EBenchmarkConfigOverAllRuns.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
//#include <Runtime/BufferRecycler.hpp>
#include <Runtime/TupleBuffer.hpp>
//#include <Runtime/detail/TupleBufferImpl.hpp>
//#include <vector>
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
        uint64_t currentlyEmittedBuffer = 0;
        size_t sourceId = 0;
        size_t numberOfBuffers = 2;

        auto bufferManager =  std::make_shared<Runtime::BufferManager>();

        std::vector<Runtime::TupleBuffer> createdBuffers;
        createdBuffers.reserve(numberOfBuffers);

        // TODO: test for column layout
        // is it necessary to create a schema or is there some other way to set memoryLayout?
        auto schemaDefault = Schema::create(Schema::ROW_LAYOUT);
        auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schemaDefault, bufferManager->getBufferSize());

        for (uint64_t curBuffer = 0; curBuffer < numberOfBuffers; ++curBuffer) {
            Runtime::TupleBuffer bufferRef = bufferManager->getBufferBlocking();
            auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, bufferRef);

            for (uint64_t curRecord = 0; curRecord < dynamicBuffer.getCapacity(); ++curRecord) {
                dynamicBuffer[curRecord]["id"].write<uint64_t>(curRecord);
                dynamicBuffer[curRecord]["value"].write<uint64_t>(curRecord);
                dynamicBuffer[curRecord]["payload"].write<uint64_t>(curRecord);
                dynamicBuffer[curRecord]["timestamp"].write<uint64_t>(curRecord);
            }

            dynamicBuffer.setNumberOfTuples(dynamicBuffer.getCapacity());
            createdBuffers.emplace_back(bufferRef);
        }

        auto internalProviderDefault = DataProvider::createProvider(sourceId, configOverAllRuns, createdBuffers);
        auto nextBufferDefault = internalProviderDefault->readNextBuffer(sourceId);

        ASSERT_FALSE(createdBuffers.empty());
        
        auto buffer = createdBuffers[currentlyEmittedBuffer % createdBuffers.size()];
        //++currentlyEmittedBuffer;

        // why is there no matching function for wrapMemory?
        auto expectedNextBuffer = Runtime::TupleBuffer::wrapMemory(buffer.getBuffer(), buffer.getBufferSize(), internalProviderDefault);
        auto currentTime = std::chrono::high_resolution_clock::now().time_since_epoch();
        auto timeStamp = std::chrono::duration_cast<std::chrono::milliseconds>(currentTime).count();

        expectedNextBuffer.setCreationTimestamp(timeStamp);
        expectedNextBuffer.setNumberOfTuples(buffer.getNumberOfTuples());

        ASSERT_EQ(expectedNextBuffer, nextBufferDefault);
    }

    // the following tests are not necessary, functions are not currently implemented
    /*TEST_F(InternalProviderTest, recyclePooledBufferTest) {

    }

    TEST_F(InternalProviderTest, recycleUnpooledBufferTest) {

    }

    TEST_F(InternalProviderTest, startTest) {

    }*/

    TEST_F(InternalProviderTest, stopTest) {
        E2EBenchmarkConfigOverAllRuns configOverAllRuns;
        size_t sourceId = 0;
        size_t numberOfBuffers = 2;

        auto bufferManager =  std::make_shared<Runtime::BufferManager>();

        std::vector<Runtime::TupleBuffer> createdBuffers;
        createdBuffers.reserve(numberOfBuffers);

        // TODO: test for column layout
        // is it necessary to create a schema or is there some other way to set memoryLayout?
        auto schemaDefault = Schema::create(Schema::ROW_LAYOUT);
        auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schemaDefault, bufferManager->getBufferSize());

        for (uint64_t curBuffer = 0; curBuffer < numberOfBuffers; ++curBuffer) {
            Runtime::TupleBuffer bufferRef = bufferManager->getBufferBlocking();
            auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, bufferRef);

            for (uint64_t curRecord = 0; curRecord < dynamicBuffer.getCapacity(); ++curRecord) {
                dynamicBuffer[curRecord]["id"].write<uint64_t>(curRecord);
                dynamicBuffer[curRecord]["value"].write<uint64_t>(curRecord);
                dynamicBuffer[curRecord]["payload"].write<uint64_t>(curRecord);
                dynamicBuffer[curRecord]["timestamp"].write<uint64_t>(curRecord);
            }

            dynamicBuffer.setNumberOfTuples(dynamicBuffer.getCapacity());
            createdBuffers.emplace_back(bufferRef);
        }

        auto internalProviderDefault = DataProvider::createProvider(sourceId, configOverAllRuns, createdBuffers);
        internalProviderDefault->stop();

        ASSERT_TRUE(createdBuffers.empty());
    }
}//namespace NES::Benchmark::DataGeneration