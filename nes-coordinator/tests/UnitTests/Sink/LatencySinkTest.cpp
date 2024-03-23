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
#include <Sinks/Mediums/LatencySink.hpp>
#include <Sinks/SinkCreator.hpp>
#include <barrier>

#include <Configurations/Worker/WorkerConfiguration.hpp>

#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>

#include <Configurations/Worker/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <gtest/gtest.h>
#include <string>

#ifndef OPERATORID
#define OPERATORID 1
#endif

#ifndef NUMSOURCELOCALBUFFERS
#define NUMSOURCELOCALBUFFERS 12
#endif

namespace NES {

class LatencySinkTest : public Testing::BaseUnitTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("LatencySinkTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("LatencySinkTest::SetUpTestCase()");
    }

    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        NES_DEBUG("Setup LatencySinkTest test case.");
        auto sourceType = DefaultSourceType::create("x", "x1");
        auto workerConfiguration = WorkerConfiguration::create();
        testSchema = Schema::create()->addField("KEY", BasicType::UINT64);
    }

    /* Will be called after a test is executed. */
    void TearDown() override {
        NES_DEBUG("LatencySinkTest::TearDown() Tear down LatencySinkTest");
        Testing::BaseUnitTest::TearDown();
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_DEBUG("LatencySinkTest::TearDownTestCases() Tear down LatencySinkTest test class."); }

    static NES::Runtime::TupleBuffer createSimpleBuffer(const std::shared_ptr<Runtime::BufferManager>& buffMgr) {
        auto buffer = buffMgr->getBufferBlocking();
        buffer.getBuffer<uint64_t>()[0] =
            std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now().time_since_epoch())
                .count();
        buffer.setNumberOfTuples(1);

        return buffer;
    }

    SchemaPtr testSchema;
    uint64_t buffer_size{};
    Runtime::BufferManagerPtr bufferManager = std::make_shared<Runtime::BufferManager>();
};

/**
* Tests basic set up of Latency sink
*/
TEST_F(LatencySinkTest, LatencySinkInit) { auto sink = createLatencySink(1, testSchema, OPERATORID, OPERATORID, nullptr, 2); }

/**
* Test if schema, Latency server address, clientId, user, and topic are the same
*/
TEST_F(LatencySinkTest, LatencySourcePrint) {
    auto sink = createLatencySink(1, testSchema, OPERATORID, OPERATORID, nullptr, 2);

    std::string expected = "LATENCY_SINK(1)";

    EXPECT_EQ(sink->toString(), expected);
}

/**
 * Tests if obtained value is valid.
 */
TEST_F(LatencySinkTest, LatencySinkWriteBuffer) {

    auto test_schema = Schema::create()->addField("var", BasicType::UINT64);
    auto sink = createLatencySink(1, testSchema, OPERATORID, OPERATORID, nullptr, 1);
    auto workerContext = Runtime::WorkerContext(1, bufferManager, 10);
    auto inputBuffer = createSimpleBuffer(bufferManager);
    for (size_t i = 0; i < 100; i++) {
        std::this_thread::sleep_for(std::chrono::microseconds(100));
        sink->writeData(inputBuffer, workerContext);
    }
}
}// namespace NES