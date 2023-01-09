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

#include <NesBaseTest.hpp>
#include <DataProvider/InternalProvider.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>

namespace NES::Benchmark::DataProviding {
    class InternalProviderTest : public Testing::NESBaseTest {
      public:
        /* Will be called before any test in this class are executed. */
        static void SetUpTestCase() {
            NES::Logger::setupLogging("InternalProviderTest.log", NES::LogLevel::LOG_DEBUG);
            NES_INFO("Setup InternalProviderTest test class.");
        }

        /* Will be called before a test is executed. */
        void SetUp() override {
            Testing::NESBaseTest::SetUp();
            bufferManager =  std::make_shared<Runtime::BufferManager>();
            NES_INFO("Setup InternalProviderTest test case.");
        }

        /* Will be called before a test is executed. */
        void TearDown() override {
            NES_INFO("Tear down InternalProviderTest test case.");
            Testing::NESBaseTest::TearDown();
        }

        /* Will be called after all tests in this class are finished. */
        static void TearDownTestCase() { NES_INFO("Tear down InternalProviderTest test class."); }

        std::shared_ptr<Runtime::BufferManager> bufferManager;
    };

    TEST_F(InternalProviderTest, readNextBufferRowLayoutTest) {
        E2EBenchmarkConfigOverAllRuns configOverAllRuns;
        uint64_t currentlyEmittedBuffer = 0;
        size_t sourceId = 0;
        size_t numberOfBuffers = 2;

        std::vector<Runtime::TupleBuffer> createdBuffers;
        createdBuffers.reserve(numberOfBuffers);

        auto schemaDefault = Schema::create(Schema::ROW_LAYOUT)
             ->addField(createField("id", NES::UINT64))
             ->addField(createField("value", NES::UINT64))
             ->addField(createField("payload", NES::UINT64))
             ->addField(createField("timestamp", NES::UINT64));
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

        auto internalProviderDefault = std::dynamic_pointer_cast<InternalProvider>(DataProvider::createProvider(sourceId, configOverAllRuns, createdBuffers));
        internalProviderDefault->start();
        auto nextBufferDefault = internalProviderDefault->readNextBuffer(sourceId);

        ASSERT_FALSE(createdBuffers.empty());

        auto buffer = createdBuffers[currentlyEmittedBuffer % createdBuffers.size()];
        auto expectedNextBuffer = Runtime::TupleBuffer::wrapMemory(buffer.getBuffer(), buffer.getBufferSize(), internalProviderDefault.get());
        expectedNextBuffer.setNumberOfTuples(buffer.getNumberOfTuples());

        ASSERT_EQ(nextBufferDefault->getBufferSize(), expectedNextBuffer.getBufferSize());

        auto dataBuffer = nextBufferDefault->getBuffer();
        auto expectedBuffer = expectedNextBuffer.getBuffer();

        ASSERT_TRUE(memcmp(dataBuffer, expectedBuffer, nextBufferDefault->getBufferSize()) == 0);
    }

    TEST_F(InternalProviderTest, readNextBufferColumnarLayoutTest) {
        E2EBenchmarkConfigOverAllRuns configOverAllRuns;
        uint64_t currentlyEmittedBuffer = 0;
        size_t sourceId = 0;
        size_t numberOfBuffers = 2;

        std::vector<Runtime::TupleBuffer> createdBuffers;
        createdBuffers.reserve(numberOfBuffers);

        auto schemaDefault = Schema::create(Schema::COLUMNAR_LAYOUT)
             ->addField(createField("id", NES::UINT64))
             ->addField(createField("value", NES::UINT64))
             ->addField(createField("payload", NES::UINT64))
             ->addField(createField("timestamp", NES::UINT64));
        auto memoryLayout = Runtime::MemoryLayouts::ColumnLayout::create(schemaDefault, bufferManager->getBufferSize());

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

        auto internalProviderDefault = std::dynamic_pointer_cast<InternalProvider>(DataProvider::createProvider(sourceId, configOverAllRuns, createdBuffers));
        internalProviderDefault->start();
        auto nextBufferDefault = internalProviderDefault->readNextBuffer(sourceId);

        ASSERT_FALSE(createdBuffers.empty());

        auto buffer = createdBuffers[currentlyEmittedBuffer % createdBuffers.size()];
        auto expectedNextBuffer = Runtime::TupleBuffer::wrapMemory(buffer.getBuffer(), buffer.getBufferSize(), internalProviderDefault.get());
        expectedNextBuffer.setNumberOfTuples(buffer.getNumberOfTuples());

        ASSERT_EQ(nextBufferDefault->getBufferSize(), expectedNextBuffer.getBufferSize());

        auto dataBuffer = nextBufferDefault->getBuffer();
        auto expectedBuffer = expectedNextBuffer.getBuffer();

        ASSERT_TRUE(memcmp(dataBuffer, expectedBuffer, nextBufferDefault->getBufferSize()) == 0);
    }

    TEST_F(InternalProviderTest, stopRowLayoutTest) {
        E2EBenchmarkConfigOverAllRuns configOverAllRuns;
        size_t sourceId = 0;
        size_t numberOfBuffers = 2;

        std::vector<Runtime::TupleBuffer> createdBuffers;
        createdBuffers.reserve(numberOfBuffers);

        auto schemaDefault = Schema::create(Schema::ROW_LAYOUT)
             ->addField(createField("id", NES::UINT64))
             ->addField(createField("value", NES::UINT64))
             ->addField(createField("payload", NES::UINT64))
             ->addField(createField("timestamp", NES::UINT64));
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

        auto internalProviderDefault = std::dynamic_pointer_cast<InternalProvider>(DataProvider::createProvider(sourceId, configOverAllRuns, createdBuffers));
        internalProviderDefault->stop();

        auto preAllocatedBuffers = internalProviderDefault->getPreAllocatedBuffers();

        ASSERT_EQ(preAllocatedBuffers.size(), 0);
    }

    TEST_F(InternalProviderTest, stopColumnarLayoutTest) {
        E2EBenchmarkConfigOverAllRuns configOverAllRuns;
        size_t sourceId = 0;
        size_t numberOfBuffers = 2;

        std::vector<Runtime::TupleBuffer> createdBuffers;
        createdBuffers.reserve(numberOfBuffers);

        auto schemaDefault = Schema::create(Schema::COLUMNAR_LAYOUT)
             ->addField(createField("id", NES::UINT64))
             ->addField(createField("value", NES::UINT64))
             ->addField(createField("payload", NES::UINT64))
             ->addField(createField("timestamp", NES::UINT64));
        auto memoryLayout = Runtime::MemoryLayouts::ColumnLayout::create(schemaDefault, bufferManager->getBufferSize());

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

        auto internalProviderDefault = std::dynamic_pointer_cast<InternalProvider>(DataProvider::createProvider(sourceId, configOverAllRuns, createdBuffers));
        internalProviderDefault->stop();

        auto preAllocatedBuffers = internalProviderDefault->getPreAllocatedBuffers();

        ASSERT_EQ(preAllocatedBuffers.size(), 0);
    }
}//namespace NES::Benchmark::DataProviding