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
#include <DataProvider/ExternalProvider.hpp>
#include <Runtime/MemoryLayout/ColumnLayout.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <gtest/gtest.h>
#include <API/TestSchemas.hpp>

namespace NES::Benchmark::DataProvision {
    class ExternalProviderTest : public Testing::BaseIntegrationTest {
      public:
        /* Will be called before any test in this class are executed. */
        static void SetUpTestCase() {
            NES::Logger::setupLogging("ExternalProviderTest.log", NES::LogLevel::LOG_DEBUG);
            NES_INFO("Setup ExternalProviderTest test class.");
        }

        /* Will be called before a test is executed. */
        void SetUp() override {
            Testing::BaseIntegrationTest::SetUp();
            bufferManager =  std::make_shared<Runtime::BufferManager>();
            NES_INFO("Setup ExternalProviderTest test case.");
        }

        /* Will be called before a test is executed. */
        void TearDown() override {
            NES_INFO("Tear down ExternalProviderTest test case.");
            Testing::BaseIntegrationTest::TearDown();
        }

        /* Will be called after all tests in this class are finished. */
        static void TearDownTestCase() { NES_INFO("Tear down ExternalProviderTest test class."); }

        std::shared_ptr<Runtime::BufferManager> bufferManager;
    };

    /**
     * @brief busy waiting until the ExternalProvider has started
     */
    void waitForExternalProviderStartup(ExternalProvider& externalProvider) {
        NES_DEBUG("Waiting until ExternalProvider has started...");
        externalProvider.waitUntilStarted();
    }

    /**
     * @brief This test should not be run on the CI, as here we use a sleep to generate x amount of buffers and then
     * compare to an expected. This might fail randomly as the CI is not fast enough to produce large amounts of buffers
     */
    // Enable when fixing #4625
    TEST_F(ExternalProviderTest, DISABLED_uniformIngestionRateTest) {
        E2EBenchmarkConfigOverAllRuns configOverAllRuns;
        configOverAllRuns.dataProvider->setValue("External");
        size_t sourceId = 0;
        size_t numberOfBuffers = 2;

        std::vector<Runtime::TupleBuffer> createdBuffers;
        createdBuffers.reserve(numberOfBuffers);

        auto schemaDefault = TestSchemas::getSchemaTemplate("id_val_time_u64")->addField("payload", BasicType::UINT64);
        auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schemaDefault, bufferManager->getBufferSize());

        for (uint64_t curBuffer = 0; curBuffer < numberOfBuffers; ++curBuffer) {
            Runtime::TupleBuffer bufferRef = bufferManager->getBufferBlocking();
            auto testBuffer = Runtime::MemoryLayouts::TestTupleBuffer(memoryLayout, bufferRef);

            for (uint64_t curRecord = 0; curRecord < testBuffer.getCapacity(); ++curRecord) {
                testBuffer[curRecord]["id"].write<uint64_t>(curRecord);
                testBuffer[curRecord]["value"].write<uint64_t>(curRecord);
                testBuffer[curRecord]["payload"].write<uint64_t>(curRecord);
                testBuffer[curRecord]["timestamp"].write<uint64_t>(curRecord);
            }

            testBuffer.setNumberOfTuples(testBuffer.getCapacity());
            createdBuffers.emplace_back(bufferRef);
        }

        auto externalProviderDefault = std::dynamic_pointer_cast<ExternalProvider>(DataProvider::createProvider(sourceId, configOverAllRuns, createdBuffers));
        externalProviderDefault->start();
        // we wait for buffers to be ingested into the buffer queue
        sleep(1);

        auto& bufferQueue = externalProviderDefault->getBufferQueue();
        auto queueSize = bufferQueue.size();

        // we expect queueSize to not exactly match the theoretical value of 50000 as we cannot ensure that exactly 1 second has
        // passed between starting the provider and getting the queue size
        NES_INFO("{}", queueSize);
        ASSERT_TRUE(50000 <= queueSize && queueSize <= 50500);
    }

    /**
     * @brief Testing if ExternalProvider::readNextBuffer() works by creating tupleBuffers and then checks if the buffers can be
     * accessed by readNextBuffer() for a row layout
     */
    TEST_F(ExternalProviderTest, readNextBufferRowLayoutTest) {
        E2EBenchmarkConfigOverAllRuns configOverAllRuns;
        configOverAllRuns.dataProvider->setValue("External");
        size_t sourceId = 0;
        size_t numberOfBuffers = 2;

        std::vector<Runtime::TupleBuffer> createdBuffers;
        createdBuffers.reserve(numberOfBuffers);

        auto schemaDefault = TestSchemas::getSchemaTemplate("id_val_time_u64")->addField("payload", BasicType::UINT64);
        auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schemaDefault, bufferManager->getBufferSize());

        for (uint64_t curBuffer = 0; curBuffer < numberOfBuffers; ++curBuffer) {
            Runtime::TupleBuffer bufferRef = bufferManager->getBufferBlocking();
            auto testBuffer = Runtime::MemoryLayouts::TestTupleBuffer(memoryLayout, bufferRef);

            for (uint64_t curRecord = 0; curRecord < testBuffer.getCapacity(); ++curRecord) {
                testBuffer[curRecord]["id"].write<uint64_t>(curRecord);
                testBuffer[curRecord]["value"].write<uint64_t>(curRecord);
                testBuffer[curRecord]["payload"].write<uint64_t>(curRecord);
                testBuffer[curRecord]["timestamp"].write<uint64_t>(curRecord);
            }

            testBuffer.setNumberOfTuples(testBuffer.getCapacity());
            createdBuffers.emplace_back(bufferRef);
        }

        ASSERT_FALSE(createdBuffers.empty());

        auto externalProviderDefault = std::dynamic_pointer_cast<ExternalProvider>(DataProvider::createProvider(sourceId, configOverAllRuns, createdBuffers));
        /* We do not want to throw an exception in our test environment regarding timing, because this could induce randomly
         * failing tests due to the CI being overwhelmed
         */
        externalProviderDefault->setThrowException(/* throwException */ false);
        externalProviderDefault->start();
        waitForExternalProviderStartup(*externalProviderDefault);

        auto nextBufferDefault = externalProviderDefault->readNextBuffer(sourceId);

        auto& bufferQueue = externalProviderDefault->getBufferQueue();
        auto expectedNextBuffer = createdBuffers[0];

        ASSERT_EQ(nextBufferDefault->getBufferSize(), expectedNextBuffer.getBufferSize());

        auto defaultBuffer = nextBufferDefault->getBuffer();
        auto expectedBuffer = expectedNextBuffer.getBuffer();
        ASSERT_TRUE(memcmp(defaultBuffer, expectedBuffer, nextBufferDefault->getBufferSize()) == 0);
    }

    /**
     * @brief Testing if ExternalProvider::readNextBuffer() works by creating tupleBuffers and then checks if the buffers can be
     * accessed by readNextBuffer() for a column layout
     */
    TEST_F(ExternalProviderTest, readNextBufferColumnarLayoutTest) {
        E2EBenchmarkConfigOverAllRuns configOverAllRuns;
        configOverAllRuns.dataProvider->setValue("External");
        size_t sourceId = 0;
        size_t numberOfBuffers = 2;

        std::vector<Runtime::TupleBuffer> createdBuffers;
        createdBuffers.reserve(numberOfBuffers);

        auto schemaDefault = Schema::create(Schema::MemoryLayoutType::COLUMNAR_LAYOUT)
                                 ->addField(createField("id", BasicType::UINT64))
                                 ->addField(createField("value", BasicType::UINT64))
                                 ->addField(createField("payload", BasicType::UINT64))
                                 ->addField(createField("timestamp", BasicType::UINT64));
        auto memoryLayout = Runtime::MemoryLayouts::ColumnLayout::create(schemaDefault, bufferManager->getBufferSize());

        for (uint64_t curBuffer = 0; curBuffer < numberOfBuffers; ++curBuffer) {
            Runtime::TupleBuffer bufferRef = bufferManager->getBufferBlocking();
            auto testBuffer = Runtime::MemoryLayouts::TestTupleBuffer(memoryLayout, bufferRef);

            for (uint64_t curRecord = 0; curRecord < testBuffer.getCapacity(); ++curRecord) {
                testBuffer[curRecord]["id"].write<uint64_t>(curRecord);
                testBuffer[curRecord]["value"].write<uint64_t>(curRecord);
                testBuffer[curRecord]["payload"].write<uint64_t>(curRecord);
                testBuffer[curRecord]["timestamp"].write<uint64_t>(curRecord);
            }

            testBuffer.setNumberOfTuples(testBuffer.getCapacity());
            createdBuffers.emplace_back(bufferRef);
        }

        ASSERT_FALSE(createdBuffers.empty());

        auto externalProviderDefault = std::dynamic_pointer_cast<ExternalProvider>(DataProvider::createProvider(sourceId, configOverAllRuns, createdBuffers));
        /* We do not want to throw an exception in our test environment regarding timing, because this could induce randomly
         * failing tests due to the CI being overwhelmed
         */
        externalProviderDefault->setThrowException(/* throwException */ false);
        externalProviderDefault->start();
        waitForExternalProviderStartup(*externalProviderDefault);

        auto nextBufferDefault = externalProviderDefault->readNextBuffer(sourceId);

        auto& bufferQueue = externalProviderDefault->getBufferQueue();
        auto expectedNextBuffer = createdBuffers[0];

        ASSERT_EQ(nextBufferDefault->getBufferSize(), expectedNextBuffer.getBufferSize());

        auto defaultBuffer = nextBufferDefault->getBuffer();
        auto expectedBuffer = expectedNextBuffer.getBuffer();
        ASSERT_TRUE(memcmp(defaultBuffer, expectedBuffer, nextBufferDefault->getBufferSize()) == 0);
    }

    /**
     * @brief Testing if ExternalProvider::start() works by creating tupleBuffers and then checks if the thread can be joined
     * for row layout
     */
    TEST_F(ExternalProviderTest, startRowLayoutTest) {
        E2EBenchmarkConfigOverAllRuns configOverAllRuns;
        configOverAllRuns.dataProvider->setValue("External");
        size_t sourceId = 0;
        size_t numberOfBuffers = 2;

        std::vector<Runtime::TupleBuffer> createdBuffers;
        createdBuffers.reserve(numberOfBuffers);

        auto schemaDefault = TestSchemas::getSchemaTemplate("id_val_time_u64")->addField("payload", BasicType::UINT64);
        auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schemaDefault, bufferManager->getBufferSize());

        for (uint64_t curBuffer = 0; curBuffer < numberOfBuffers; ++curBuffer) {
            Runtime::TupleBuffer bufferRef = bufferManager->getBufferBlocking();
            auto testBuffer = Runtime::MemoryLayouts::TestTupleBuffer(memoryLayout, bufferRef);

            for (uint64_t curRecord = 0; curRecord < testBuffer.getCapacity(); ++curRecord) {
                testBuffer[curRecord]["id"].write<uint64_t>(curRecord);
                testBuffer[curRecord]["value"].write<uint64_t>(curRecord);
                testBuffer[curRecord]["payload"].write<uint64_t>(curRecord);
                testBuffer[curRecord]["timestamp"].write<uint64_t>(curRecord);
            }

            testBuffer.setNumberOfTuples(testBuffer.getCapacity());
            createdBuffers.emplace_back(bufferRef);
        }

        auto externalProviderDefault = std::dynamic_pointer_cast<ExternalProvider>(DataProvider::createProvider(sourceId, configOverAllRuns, createdBuffers));
        /* We do not want to throw an exception in our test environment regarding timing, because this could induce randomly
         * failing tests due to the CI being overwhelmed
         */
        externalProviderDefault->setThrowException(/* throwException */ false);
        externalProviderDefault->start();
        waitForExternalProviderStartup(*externalProviderDefault);

        auto& generatorThread = externalProviderDefault->getGeneratorThread();
        ASSERT_TRUE(generatorThread.joinable());

        auto preAllocatedBuffers = externalProviderDefault->getPreAllocatedBuffers();
        ASSERT_FALSE(preAllocatedBuffers.empty());
    }

    /**
     * @brief Testing if ExternalProvider::start() works by creating tupleBuffers and then checks if the thread can be joined
     * for column layout
     */
    TEST_F(ExternalProviderTest, startColumnarLayoutTest) {
        E2EBenchmarkConfigOverAllRuns configOverAllRuns;
        configOverAllRuns.dataProvider->setValue("External");
        size_t sourceId = 0;
        size_t numberOfBuffers = 2;

        std::vector<Runtime::TupleBuffer> createdBuffers;
        createdBuffers.reserve(numberOfBuffers);

        auto schemaDefault = Schema::create(Schema::MemoryLayoutType::COLUMNAR_LAYOUT)
                                 ->addField(createField("id", BasicType::UINT64))
                                 ->addField(createField("value", BasicType::UINT64))
                                 ->addField(createField("payload", BasicType::UINT64))
                                 ->addField(createField("timestamp", BasicType::UINT64));
        auto memoryLayout = Runtime::MemoryLayouts::ColumnLayout::create(schemaDefault, bufferManager->getBufferSize());

        for (uint64_t curBuffer = 0; curBuffer < numberOfBuffers; ++curBuffer) {
            Runtime::TupleBuffer bufferRef = bufferManager->getBufferBlocking();
            auto testBuffer = Runtime::MemoryLayouts::TestTupleBuffer(memoryLayout, bufferRef);

            for (uint64_t curRecord = 0; curRecord < testBuffer.getCapacity(); ++curRecord) {
                testBuffer[curRecord]["id"].write<uint64_t>(curRecord);
                testBuffer[curRecord]["value"].write<uint64_t>(curRecord);
                testBuffer[curRecord]["payload"].write<uint64_t>(curRecord);
                testBuffer[curRecord]["timestamp"].write<uint64_t>(curRecord);
            }

            testBuffer.setNumberOfTuples(testBuffer.getCapacity());
            createdBuffers.emplace_back(bufferRef);
        }

        auto externalProviderDefault = std::dynamic_pointer_cast<ExternalProvider>(DataProvider::createProvider(sourceId, configOverAllRuns, createdBuffers));
        /* We do not want to throw an exception in our test environment regarding timing, because this could induce randomly
         * failing tests due to the CI being overwhelmed
         */
        externalProviderDefault->setThrowException(/* throwException */ false);
        externalProviderDefault->start();
        waitForExternalProviderStartup(*externalProviderDefault);

        auto& generatorThread = externalProviderDefault->getGeneratorThread();
        ASSERT_TRUE(generatorThread.joinable());

        auto preAllocatedBuffers = externalProviderDefault->getPreAllocatedBuffers();
        ASSERT_FALSE(preAllocatedBuffers.empty());
    }

    /**
     * @brief Testing if ExternalProvider::stop() works by creating tupleBuffers and then checks if the ExternalProvider stops correctly
     *  row layout
     */
    TEST_F(ExternalProviderTest, stopRowLayoutTest) {
        E2EBenchmarkConfigOverAllRuns configOverAllRuns;
        configOverAllRuns.dataProvider->setValue("External");
        size_t sourceId = 0;
        size_t numberOfBuffers = 2;

        std::vector<Runtime::TupleBuffer> createdBuffers;
        createdBuffers.reserve(numberOfBuffers);

        auto schemaDefault = TestSchemas::getSchemaTemplate("id_val_time_u64")->addField("payload", BasicType::UINT64);
        auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schemaDefault, bufferManager->getBufferSize());

        for (uint64_t curBuffer = 0; curBuffer < numberOfBuffers; ++curBuffer) {
            Runtime::TupleBuffer bufferRef = bufferManager->getBufferBlocking();
            auto testBuffer = Runtime::MemoryLayouts::TestTupleBuffer(memoryLayout, bufferRef);

            for (uint64_t curRecord = 0; curRecord < testBuffer.getCapacity(); ++curRecord) {
                testBuffer[curRecord]["id"].write<uint64_t>(curRecord);
                testBuffer[curRecord]["value"].write<uint64_t>(curRecord);
                testBuffer[curRecord]["payload"].write<uint64_t>(curRecord);
                testBuffer[curRecord]["timestamp"].write<uint64_t>(curRecord);
            }

            testBuffer.setNumberOfTuples(testBuffer.getCapacity());
            createdBuffers.emplace_back(bufferRef);
        }

        auto externalProviderDefault = std::dynamic_pointer_cast<ExternalProvider>(DataProvider::createProvider(sourceId, configOverAllRuns, createdBuffers));
        /* We do not want to throw an exception in our test environment regarding timing, because this could induce randomly
         * failing tests due to the CI being overwhelmed
         */
        externalProviderDefault->setThrowException(/* throwException */ false);
        externalProviderDefault->start();
        waitForExternalProviderStartup(*externalProviderDefault);

        externalProviderDefault->stop();

        auto& generatorThread = externalProviderDefault->getGeneratorThread();
        ASSERT_TRUE(!generatorThread.joinable());

        auto preAllocatedBuffers = externalProviderDefault->getPreAllocatedBuffers();
        ASSERT_TRUE(preAllocatedBuffers.empty());
    }

    /**
     * @brief Testing if ExternalProvider::stop() works by creating tupleBuffers and then checks if the ExternalProvider stops correctly
     * column layout
     */
    TEST_F(ExternalProviderTest, stopColumnarLayoutTest) {
        E2EBenchmarkConfigOverAllRuns configOverAllRuns;
        configOverAllRuns.dataProvider->setValue("External");
        size_t sourceId = 0;
        size_t numberOfBuffers = 2;

        std::vector<Runtime::TupleBuffer> createdBuffers;
        createdBuffers.reserve(numberOfBuffers);

        auto schemaDefault = Schema::create(Schema::MemoryLayoutType::COLUMNAR_LAYOUT)
                                 ->addField(createField("id", BasicType::UINT64))
                                 ->addField(createField("value", BasicType::UINT64))
                                 ->addField(createField("payload", BasicType::UINT64))
                                 ->addField(createField("timestamp", BasicType::UINT64));
        auto memoryLayout = Runtime::MemoryLayouts::ColumnLayout::create(schemaDefault, bufferManager->getBufferSize());

        for (uint64_t curBuffer = 0; curBuffer < numberOfBuffers; ++curBuffer) {
            Runtime::TupleBuffer bufferRef = bufferManager->getBufferBlocking();
            auto testBuffer = Runtime::MemoryLayouts::TestTupleBuffer(memoryLayout, bufferRef);

            for (uint64_t curRecord = 0; curRecord < testBuffer.getCapacity(); ++curRecord) {
                testBuffer[curRecord]["id"].write<uint64_t>(curRecord);
                testBuffer[curRecord]["value"].write<uint64_t>(curRecord);
                testBuffer[curRecord]["payload"].write<uint64_t>(curRecord);
                testBuffer[curRecord]["timestamp"].write<uint64_t>(curRecord);
            }

            testBuffer.setNumberOfTuples(testBuffer.getCapacity());
            createdBuffers.emplace_back(bufferRef);
        }

        auto externalProviderDefault = std::dynamic_pointer_cast<ExternalProvider>(DataProvider::createProvider(sourceId, configOverAllRuns, createdBuffers));
        /* We do not want to throw an exception in our test environment regarding timing, because this could induce randomly
         * failing tests due to the CI being overwhelmed
         */
        externalProviderDefault->setThrowException(/* throwException */ false);
        externalProviderDefault->stop();

        auto& generatorThread = externalProviderDefault->getGeneratorThread();
        ASSERT_TRUE(!generatorThread.joinable());

        auto preAllocatedBuffers = externalProviderDefault->getPreAllocatedBuffers();
        ASSERT_TRUE(preAllocatedBuffers.empty());
    }
}//namespace NES::Benchmark::DataProvision
