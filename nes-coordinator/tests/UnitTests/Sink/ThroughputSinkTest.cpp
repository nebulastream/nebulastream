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
#include <BaseIntegrationTest.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sinks/Mediums/ThroughputSink.hpp>
#include <Sinks/SinkCreator.hpp>
#include <barrier>

#include <Configurations/Worker/WorkerConfiguration.hpp>

#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>

#include <Configurations/Worker/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <gtest/gtest.h>
#include <string>

#ifndef OPERATORID
#define OPERATORID 1
#define COUNTER_NAME "throughput"
#endif

#ifndef NUMSOURCELOCALBUFFERS
#define NUMSOURCELOCALBUFFERS 12
#endif

const std::string KAFKA_BROKER = "localhost:9092";

namespace NES {

/**
* NOTE: this test requires a running kafka instance
*/
class ThroughputSinkTest : public Testing::BaseUnitTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ThroughputSinkTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("ThroughputSinkTest::SetUpTestCase()");
    }

    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        NES_DEBUG("Setup ThroughputSinkTest test case.");
        auto sourceType = DefaultSourceType::create("x", "x1");
        auto workerConfiguration = WorkerConfiguration::create();
        testSchema = Schema::create()->addField("KEY", BasicType::UINT32)->addField("VALUE", BasicType::UINT32);
    }

    /* Will be called after a test is executed. */
    void TearDown() override {
        NES_DEBUG("ThroughputSinkTest::TearDown() Tear down ThroughputSinkTest");
        Testing::BaseUnitTest::TearDown();
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() {
        NES_DEBUG("ThroughputSinkTest::TearDownTestCases() Tear down ThroughputSinkTest test class.");
    }

    static NES::Runtime::TupleBuffer createSimpleBuffer(uint64_t bufferSize,
                                                        const std::shared_ptr<Runtime::BufferManager>& buffMgr) {
        auto buffer = buffMgr->getBufferBlocking();
        for (uint32_t j = 0; j < bufferSize / sizeof(uint32_t); ++j) {
            buffer.getBuffer<uint32_t>()[j] = 2;
        }
        buffer.setNumberOfTuples(bufferSize / sizeof(uint64_t));

        return buffer;
    }

    SchemaPtr testSchema;
    uint64_t buffer_size{};
    Runtime::BufferManagerPtr bufferManager = std::make_shared<Runtime::BufferManager>();
};

/**
* Tests basic set up of Throughput sink
*/
TEST_F(ThroughputSinkTest, ThroughputSinkInit) {
    auto sink = createThroughputSink(COUNTER_NAME, 10000, testSchema, OPERATORID, OPERATORID, nullptr, 2);
}

/**
* Test if schema, Throughput server address, clientId, user, and topic are the same
*/
TEST_F(ThroughputSinkTest, ThroughputSourcePrint) {
    auto sink = createThroughputSink(COUNTER_NAME, 10000, testSchema, OPERATORID, OPERATORID, nullptr, 2);

    std::string expected = "THROUGHPUT_SINK(" COUNTER_NAME ")";

    EXPECT_EQ(sink->toString(), expected);
}

/**
 * Tests if obtained value is valid.
 */
TEST_F(ThroughputSinkTest, ThroughputSinkWriteBuffer) {

    auto test_schema = Schema::create()->addField("var", BasicType::UINT32);
    auto testBuffer = createSimpleBuffer(bufferManager->getBufferSize(), bufferManager);

    size_t number_of_workers = 20;
    std::vector<std::jthread> threads(number_of_workers);

    std::barrier sync_point(threads.size());
    auto bufferSize = bufferManager->getBufferSize();
    auto bm = bufferManager;
    for (auto& t : threads) {
        t = std::jthread([number_of_workers, test_schema, &sync_point, bufferSize, &bm](std::stop_token) {
            auto sink = std::dynamic_pointer_cast<ThroughputSink>(
                createThroughputSink(COUNTER_NAME, 10000, test_schema, OPERATORID, OPERATORID, nullptr, 2));
            sink->setup();
            Runtime::WorkerContext workerContext(Runtime::NesThread::getId(), bm, 1);
            auto inputBuffer = createSimpleBuffer(bufferSize, bm);
            sync_point.arrive_and_wait();
            for (size_t i = 0; i < 10000; i++) {
                sink->writeData(inputBuffer, workerContext);
                std::this_thread::sleep_for(std::chrono::nanoseconds(100));
            }
            sync_point.arrive_and_wait();
            ASSERT_EQ(sink->getCurrentCounter(), 10000 * number_of_workers * inputBuffer.getNumberOfTuples());
        });
    }

    for (auto& j : threads) {
        j.join();
    }
}
}// namespace NES