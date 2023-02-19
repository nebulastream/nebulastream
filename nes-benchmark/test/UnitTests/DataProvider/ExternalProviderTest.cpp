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

#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <DataProvider/ExternalProvider.hpp>
#include <NesBaseTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>

namespace NES::Benchmark::DataProvision {
    class ExternalProviderTest : public Testing::NESBaseTest {
      public:
        /* Will be called before any test in this class are executed. */
        static void SetUpTestCase() {
            NES::Logger::setupLogging("ExternalProviderTest.log", NES::LogLevel::LOG_DEBUG);
            NES_INFO("Setup ExternalProviderTest test class.");
        }

        /* Will be called before a test is executed. */
        void SetUp() override {
            Testing::NESBaseTest::SetUp();
            bufferManager =  std::make_shared<Runtime::BufferManager>();
            NES_INFO("Setup ExternalProviderTest test case.");
        }

        /* Will be called before a test is executed. */
        void TearDown() override {
            NES_INFO("Tear down ExternalProviderTest test case.");
            Testing::NESBaseTest::TearDown();
        }

        /* Will be called after all tests in this class are finished. */
        static void TearDownTestCase() { NES_INFO("Tear down ExternalProviderTest test class."); }

        std::shared_ptr<Runtime::BufferManager> bufferManager;
    };

    TEST_F(ExternalProviderTest, readNextBufferRowLayoutTest) {
        E2EBenchmarkConfigOverAllRuns configOverAllRuns;
        configOverAllRuns.dataProvider->setValue("External");
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

        ASSERT_FALSE(createdBuffers.empty());

        auto externalProviderDefault = std::dynamic_pointer_cast<ExternalProvider>(DataProvider::createProvider(sourceId, configOverAllRuns, createdBuffers));
        externalProviderDefault->start();
        // we wait for the provider to startup
        sleep(1);

        auto nextBufferDefault = externalProviderDefault->readNextBuffer(sourceId);

        auto& bufferQueue = externalProviderDefault->getBufferQueue();
        TupleBufferHolder bufferHolder;
        bufferQueue.read(bufferHolder);
        auto expectedNextBuffer = bufferHolder.bufferToHold;

        ASSERT_EQ(nextBufferDefault->getBufferSize(), expectedNextBuffer.getBufferSize());

        auto defaultBuffer = nextBufferDefault->getBuffer();
        auto expectedBuffer = expectedNextBuffer.getBuffer();
        ASSERT_TRUE(memcmp(defaultBuffer, expectedBuffer, nextBufferDefault->getBufferSize()) == 0);
    }

    TEST_F(ExternalProviderTest, readNextBufferColumnarLayoutTest) {
        E2EBenchmarkConfigOverAllRuns configOverAllRuns;
        configOverAllRuns.dataProvider->setValue("External");
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

        ASSERT_FALSE(createdBuffers.empty());

        auto externalProviderDefault = std::dynamic_pointer_cast<ExternalProvider>(DataProvider::createProvider(sourceId, configOverAllRuns, createdBuffers));
        externalProviderDefault->start();
        // we wait for the provider to startup
        sleep(1);

        auto nextBufferDefault = externalProviderDefault->readNextBuffer(sourceId);

        auto& bufferQueue = externalProviderDefault->getBufferQueue();
        TupleBufferHolder bufferHolder;
        bufferQueue.read(bufferHolder);
        auto expectedNextBuffer = bufferHolder.bufferToHold;

        ASSERT_EQ(nextBufferDefault->getBufferSize(), expectedNextBuffer.getBufferSize());

        auto defaultBuffer = nextBufferDefault->getBuffer();
        auto expectedBuffer = expectedNextBuffer.getBuffer();
        ASSERT_TRUE(memcmp(defaultBuffer, expectedBuffer, nextBufferDefault->getBufferSize()) == 0);
    }

    TEST_F(ExternalProviderTest, startRowLayoutTest) {
        E2EBenchmarkConfigOverAllRuns configOverAllRuns;
        configOverAllRuns.dataProvider->setValue("External");
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

        auto externalProviderDefault = std::dynamic_pointer_cast<ExternalProvider>(DataProvider::createProvider(sourceId, configOverAllRuns, createdBuffers));
        externalProviderDefault->start();
        // we wait for the provider to startup
        sleep(1);

        auto& generatorThread = externalProviderDefault->getGeneratorThread();
        ASSERT_TRUE(generatorThread.joinable());

        auto preAllocatedBuffers = externalProviderDefault->getPreAllocatedBuffers();
        ASSERT_FALSE(preAllocatedBuffers.empty());
    }

    TEST_F(ExternalProviderTest, startColumnarLayoutTest) {
        E2EBenchmarkConfigOverAllRuns configOverAllRuns;
        configOverAllRuns.dataProvider->setValue("External");
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

        auto externalProviderDefault = std::dynamic_pointer_cast<ExternalProvider>(DataProvider::createProvider(sourceId, configOverAllRuns, createdBuffers));
        externalProviderDefault->start();
        // we wait for the provider to startup
        sleep(1);

        auto& generatorThread = externalProviderDefault->getGeneratorThread();
        ASSERT_TRUE(generatorThread.joinable());

        auto preAllocatedBuffers = externalProviderDefault->getPreAllocatedBuffers();
        ASSERT_FALSE(preAllocatedBuffers.empty());
    }

    TEST_F(ExternalProviderTest, stopRowLayoutTest) {
        E2EBenchmarkConfigOverAllRuns configOverAllRuns;
        configOverAllRuns.dataProvider->setValue("External");
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

        auto externalProviderDefault = std::dynamic_pointer_cast<ExternalProvider>(DataProvider::createProvider(sourceId, configOverAllRuns, createdBuffers));
        externalProviderDefault->stop();

        auto& generatorThread = externalProviderDefault->getGeneratorThread();
        ASSERT_TRUE(!generatorThread.joinable());

        auto preAllocatedBuffers = externalProviderDefault->getPreAllocatedBuffers();
        ASSERT_TRUE(preAllocatedBuffers.empty());
    }

    TEST_F(ExternalProviderTest, stopColumnarLayoutTest) {
        E2EBenchmarkConfigOverAllRuns configOverAllRuns;
        configOverAllRuns.dataProvider->setValue("External");
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

        auto externalProviderDefault = std::dynamic_pointer_cast<ExternalProvider>(DataProvider::createProvider(sourceId, configOverAllRuns, createdBuffers));
        externalProviderDefault->stop();

        auto& generatorThread = externalProviderDefault->getGeneratorThread();
        ASSERT_TRUE(!generatorThread.joinable());

        auto preAllocatedBuffers = externalProviderDefault->getPreAllocatedBuffers();
        ASSERT_TRUE(preAllocatedBuffers.empty());
    }
}//namespace NES::Benchmark::DataProvision
