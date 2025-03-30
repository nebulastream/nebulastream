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
#include <API/TestSchemas.hpp>
#include <BaseIntegrationTest.hpp>
#include <DataProvider/InternalProvider.hpp>
#include <Runtime/MemoryLayout/ColumnLayout.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <gtest/gtest.h>

namespace NES::Benchmark::DataProvision {
class InternalProviderTest : public Testing::BaseIntegrationTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("InternalProviderTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup InternalProviderTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseIntegrationTest::SetUp();
        bufferManager = std::make_shared<Runtime::BufferManager>();
        NES_INFO("Setup InternalProviderTest test case.");
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        NES_INFO("Tear down InternalProviderTest test case.");
        Testing::BaseIntegrationTest::TearDown();
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down InternalProviderTest test class."); }

    std::shared_ptr<Runtime::BufferManager> bufferManager;
};

/**
     * @brief Testing if InternalProvider::readNextBuffer() works by creating tupleBuffers and then checks if the buffers can be
     * accessed by readNextBuffer() for a row layout
     */
//TEST_F(InternalProviderTest, readNextBufferRowLayoutTest) {
//    E2EBenchmarkConfigOverAllRuns configOverAllRuns;
//    uint64_t currentlyEmittedBuffer = 0;
//    size_t sourceId = 0;
//    size_t numberOfBuffers = 2;
//
//    std::vector<Runtime::TupleBuffer> createdBuffers;
//    createdBuffers.reserve(numberOfBuffers);
//
//    auto schemaDefault = TestSchemas::getSchemaTemplate("id_val_time_u64")->addField("payload", BasicType::UINT64);
//    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schemaDefault, bufferManager->getBufferSize());
//
//    for (uint64_t curBuffer = 0; curBuffer < numberOfBuffers; ++curBuffer) {
//        Runtime::TupleBuffer bufferRef = bufferManager->getBufferBlocking();
//        auto testBuffer = Runtime::MemoryLayouts::TestTupleBuffer(memoryLayout, bufferRef);
//
//        for (uint64_t curRecord = 0; curRecord < testBuffer.getCapacity(); ++curRecord) {
//            testBuffer[curRecord]["id"].write<uint64_t>(curRecord);
//            testBuffer[curRecord]["value"].write<uint64_t>(curRecord);
//            testBuffer[curRecord]["payload"].write<uint64_t>(curRecord);
//            testBuffer[curRecord]["timestamp"].write<uint64_t>(curRecord);
//        }
//
//        testBuffer.setNumberOfTuples(testBuffer.getCapacity());
//        createdBuffers.emplace_back(bufferRef);
//    }
//
//    auto internalProviderDefault =
//        std::dynamic_pointer_cast<InternalProvider>(DataProvider::createProvider(sourceId, configOverAllRuns, createdBuffers));
//    internalProviderDefault->start();
//    auto nextBufferDefault = internalProviderDefault->readNextBuffer(sourceId);
//
//    ASSERT_FALSE(createdBuffers.empty());
//
//    auto buffer = createdBuffers[currentlyEmittedBuffer % createdBuffers.size()];
//    auto expectedNextBuffer =
//        Runtime::TupleBuffer::wrapMemory(buffer.getBuffer(), buffer.getBufferSize(), internalProviderDefault.get());
//    expectedNextBuffer.setNumberOfTuples(buffer.getNumberOfTuples());
//
//    ASSERT_EQ(nextBufferDefault->getBufferSize(), expectedNextBuffer.getBufferSize());
//
//    auto dataBuffer = nextBufferDefault->getBuffer();
//    auto expectedBuffer = expectedNextBuffer.getBuffer();
//
//    ASSERT_TRUE(memcmp(dataBuffer, expectedBuffer, nextBufferDefault->getBufferSize()) == 0);
//}
//
///**
//     * @brief Testing if InternalProvider::readNextBuffer() works by creating tupleBuffers and then checks if the buffers can be
//     * accessed by readNextBuffer() for a column layout
//     */
//TEST_F(InternalProviderTest, readNextBufferColumnarLayoutTest) {
//    E2EBenchmarkConfigOverAllRuns configOverAllRuns;
//    uint64_t currentlyEmittedBuffer = 0;
//    size_t sourceId = 0;
//    size_t numberOfBuffers = 2;
//
//    std::vector<Runtime::TupleBuffer> createdBuffers;
//    createdBuffers.reserve(numberOfBuffers);
//
//    auto schemaDefault = Schema::create(Schema::MemoryLayoutType::COLUMNAR_LAYOUT)
//                             ->addField(createField("id", BasicType::UINT64))
//                             ->addField(createField("value", BasicType::UINT64))
//                             ->addField(createField("payload", BasicType::UINT64))
//                             ->addField(createField("timestamp", BasicType::UINT64));
//    auto memoryLayout = Runtime::MemoryLayouts::ColumnLayout::create(schemaDefault, bufferManager->getBufferSize());
//
//    for (uint64_t curBuffer = 0; curBuffer < numberOfBuffers; ++curBuffer) {
//        Runtime::TupleBuffer bufferRef = bufferManager->getBufferBlocking();
//        auto testBuffer = Runtime::MemoryLayouts::TestTupleBuffer(memoryLayout, bufferRef);
//
//        for (uint64_t curRecord = 0; curRecord < testBuffer.getCapacity(); ++curRecord) {
//            testBuffer[curRecord]["id"].write<uint64_t>(curRecord);
//            testBuffer[curRecord]["value"].write<uint64_t>(curRecord);
//            testBuffer[curRecord]["payload"].write<uint64_t>(curRecord);
//            testBuffer[curRecord]["timestamp"].write<uint64_t>(curRecord);
//        }
//
//        testBuffer.setNumberOfTuples(testBuffer.getCapacity());
//        createdBuffers.emplace_back(bufferRef);
//    }
//
//    auto internalProviderDefault =
//        std::dynamic_pointer_cast<InternalProvider>(DataProvider::createProvider(sourceId, configOverAllRuns, createdBuffers));
//    internalProviderDefault->start();
//    auto nextBufferDefault = internalProviderDefault->readNextBuffer(sourceId);
//
//    ASSERT_FALSE(createdBuffers.empty());
//
//    auto buffer = createdBuffers[currentlyEmittedBuffer % createdBuffers.size()];
//    auto expectedNextBuffer =
//        Runtime::TupleBuffer::wrapMemory(buffer.getBuffer(), buffer.getBufferSize(), internalProviderDefault.get());
//    expectedNextBuffer.setNumberOfTuples(buffer.getNumberOfTuples());
//
//    ASSERT_EQ(nextBufferDefault->getBufferSize(), expectedNextBuffer.getBufferSize());
//
//    auto dataBuffer = nextBufferDefault->getBuffer();
//    auto expectedBuffer = expectedNextBuffer.getBuffer();
//
//    ASSERT_TRUE(memcmp(dataBuffer, expectedBuffer, nextBufferDefault->getBufferSize()) == 0);
//}
//
///**
//     * @brief Testing if ExternalProvider::stop() works by creating tupleBuffers and then checks if the ExternalProvider stops correctly
//     *  row layout
//     */
//TEST_F(InternalProviderTest, stopRowLayoutTest) {
//    E2EBenchmarkConfigOverAllRuns configOverAllRuns;
//    size_t sourceId = 0;
//    size_t numberOfBuffers = 2;
//
//    std::vector<Runtime::TupleBuffer> createdBuffers;
//    createdBuffers.reserve(numberOfBuffers);
//
//    auto schemaDefault = TestSchemas::getSchemaTemplate("id_val_time_u64")->addField("payload", BasicType::UINT64);
//    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schemaDefault, bufferManager->getBufferSize());
//
//    for (uint64_t curBuffer = 0; curBuffer < numberOfBuffers; ++curBuffer) {
//        Runtime::TupleBuffer bufferRef = bufferManager->getBufferBlocking();
//        auto testBuffer = Runtime::MemoryLayouts::TestTupleBuffer(memoryLayout, bufferRef);
//
//        for (uint64_t curRecord = 0; curRecord < testBuffer.getCapacity(); ++curRecord) {
//            testBuffer[curRecord]["id"].write<uint64_t>(curRecord);
//            testBuffer[curRecord]["value"].write<uint64_t>(curRecord);
//            testBuffer[curRecord]["payload"].write<uint64_t>(curRecord);
//            testBuffer[curRecord]["timestamp"].write<uint64_t>(curRecord);
//        }
//
//        testBuffer.setNumberOfTuples(testBuffer.getCapacity());
//        createdBuffers.emplace_back(bufferRef);
//    }
//
//    auto internalProviderDefault =
//        std::dynamic_pointer_cast<InternalProvider>(DataProvider::createProvider(sourceId, configOverAllRuns, createdBuffers));
//    internalProviderDefault->stop();
//
//    auto preAllocatedBuffers = internalProviderDefault->getPreAllocatedBuffers();
//
//    ASSERT_EQ(preAllocatedBuffers.size(), 0);
//}
//
///**
//     * @brief Testing if ExternalProvider::stop() works by creating tupleBuffers and then checks if the ExternalProvider stops correctly
//     * column layout
//     */
//TEST_F(InternalProviderTest, stopColumnarLayoutTest) {
//    E2EBenchmarkConfigOverAllRuns configOverAllRuns;
//    size_t sourceId = 0;
//    size_t numberOfBuffers = 2;
//
//    std::vector<Runtime::TupleBuffer> createdBuffers;
//    createdBuffers.reserve(numberOfBuffers);
//
//    auto schemaDefault = Schema::create(Schema::MemoryLayoutType::COLUMNAR_LAYOUT)
//                             ->addField(createField("id", BasicType::UINT64))
//                             ->addField(createField("value", BasicType::UINT64))
//                             ->addField(createField("payload", BasicType::UINT64))
//                             ->addField(createField("timestamp", BasicType::UINT64));
//    auto memoryLayout = Runtime::MemoryLayouts::ColumnLayout::create(schemaDefault, bufferManager->getBufferSize());
//
//    for (uint64_t curBuffer = 0; curBuffer < numberOfBuffers; ++curBuffer) {
//        Runtime::TupleBuffer bufferRef = bufferManager->getBufferBlocking();
//        auto testBuffer = Runtime::MemoryLayouts::TestTupleBuffer(memoryLayout, bufferRef);
//
//        for (uint64_t curRecord = 0; curRecord < testBuffer.getCapacity(); ++curRecord) {
//            testBuffer[curRecord]["id"].write<uint64_t>(curRecord);
//            testBuffer[curRecord]["value"].write<uint64_t>(curRecord);
//            testBuffer[curRecord]["payload"].write<uint64_t>(curRecord);
//            testBuffer[curRecord]["timestamp"].write<uint64_t>(curRecord);
//        }
//
//        testBuffer.setNumberOfTuples(testBuffer.getCapacity());
//        createdBuffers.emplace_back(bufferRef);
//    }
//
//    auto internalProviderDefault =
//        std::dynamic_pointer_cast<InternalProvider>(DataProvider::createProvider(sourceId, configOverAllRuns, createdBuffers));
//    internalProviderDefault->stop();
//
//    auto preAllocatedBuffers = internalProviderDefault->getPreAllocatedBuffers();
//
//    ASSERT_EQ(preAllocatedBuffers.size(), 0);
//}
}//namespace NES::Benchmark::DataProvision
