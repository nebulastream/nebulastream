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

#include "Util/TestUtils.hpp"
#include <API/QueryAPI.hpp>
#include <API/TestSchemas.hpp>
#include <BaseIntegrationTest.hpp>
#include <gtest/gtest.h>

#include <Catalogs/Source/PhysicalSource.hpp>
#include <Components/NesCoordinator.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Services/RequestHandlerService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <gmock/gmock-matchers.h>
#include <iostream>

using namespace std;

namespace NES::Runtime::Execution {

class MultipleJoinsTest : public Testing::BaseIntegrationTest,
                          public ::testing::WithParamInterface<
                              std::tuple<QueryCompilation::StreamJoinStrategy, QueryCompilation::WindowingStrategy>> {
  public:
    Runtime::BufferManagerPtr bufferManager;
    QueryCompilation::StreamJoinStrategy joinStrategy;
    QueryCompilation::WindowingStrategy windowingStrategy;
    static void SetUpTestCase() {
        NES::Logger::setupLogging("MultipleJoinsTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup MultipleJoinsTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        NES_INFO("QueryExecutionTest: Setup MultipleJoinsTest test class.");
        BaseIntegrationTest::SetUp();

        joinStrategy = std::get<0>(NES::Runtime::Execution::MultipleJoinsTest::GetParam());
        windowingStrategy = std::get<1>(NES::Runtime::Execution::MultipleJoinsTest::GetParam());
        bufferManager = std::make_shared<Runtime::BufferManager>();
    }

    void runJoinQuery(const Query& query,
                      const TestUtils::CsvFileParams& csvFileParams,
                      const TestUtils::JoinParams& joinParams,
                      const std::string& expectedOutputCSV) {

        auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                               .setJoinStrategy(joinStrategy)
                               .setWindowingStrategy(windowingStrategy);

        for (auto i = 0_u64; i < joinParams.inputSchemas.size(); ++i) {
            std::string logicalSourceName = "window" + std::to_string(i + 1);
            std::string physicalSourceName = "windowPhysical" + std::to_string(i + 1);
            auto sourceConfig =
                TestUtils::createSourceTypeCSV({logicalSourceName, physicalSourceName, csvFileParams.inputCsvFiles[i]});
            auto csvSourceType =
                TestUtils::createSourceTypeCSV({logicalSourceName, physicalSourceName, csvFileParams.inputCsvFiles[i]});
            testHarness.addLogicalSource(logicalSourceName, joinParams.inputSchemas[i])
                .attachWorkerWithCSVSourceToCoordinator(csvSourceType);
        }

        // Run the query and get the actual dynamic buffers
        auto actualBuffers =
            testHarness.validate().setupTopology().runQuery(NES::Util::countLines(expectedOutputCSV)).getOutput();

        // Comparing equality
        const auto outputSchema = testHarness.getOutputSchema();
        auto tmpBuffers =
            TestUtils::createExpectedBufferFromCSVString(expectedOutputCSV, outputSchema, testHarness.getBufferManager());
        auto expectedBuffers = TestUtils::createTestTupleBuffers(tmpBuffers, outputSchema);
        EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
    }
};

TEST_P(MultipleJoinsTest, testJoins2WithDifferentSourceTumblingWindowOnCoodinator) {
    if (joinStrategy == QueryCompilation::StreamJoinStrategy::HASH_JOIN_VAR_SIZED
        && windowingStrategy == QueryCompilation::WindowingStrategy::BUCKETING) {
        GTEST_SKIP();
    }

    const auto windowSchema = TestSchemas::getSchemaTemplate("id_val_time_u64")->updateSourceName("window1");
    const auto window2Schema = TestSchemas::getSchemaTemplate("id2_val2_time_u64")->updateSourceName("window2");
    const auto window3Schema = TestSchemas::getSchemaTemplate("id3_val3_time_u64")->updateSourceName("window3");

    TestUtils::JoinParams joinParams({windowSchema, window2Schema, window3Schema});
    TestUtils::CsvFileParams csvFileParams({"window.csv", "window2.csv", "window4.csv"}, "");

    // Expected output
    const auto expectedOutput = "1000, 2000, 1000, 2000, 4, 1, 1002, 4, 3, 1112, 4, 4, 1001\n"
                                "1000, 2000, 1000, 2000, 4, 1, 1002, 4, 3, 1102, 4, 4, 1001\n"
                                "1000, 2000, 1000, 2000, 12, 1, 1001, 12, 5, 1011, 12, 1, 1300\n"
                                "3000, 4000, 3000, 4000, 11, 3, 3001, 11, 3, 3001, 11, 9, 3000\n"
                                "12000, 13000, 12000, 13000, 1, 12, 12000, 1, 12, 12000, 1, 12, 12000\n"
                                "13000, 14000, 13000, 14000, 1, 13, 13000, 1, 13, 13000, 1, 13, 13000\n"
                                "14000, 15000, 14000, 15000, 1, 14, 14000, 1, 14, 14000, 1, 14, 14000\n"
                                "15000, 16000, 15000, 16000, 1, 15, 15000, 1, 15, 15000, 1, 15, 15000\n";

    const auto query = Query::from("window1")
                           .joinWith(Query::from("window2"))
                           .where(Attribute("id") == Attribute("id2"))
                           .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
                           .joinWith(Query::from("window3"))
                           .where(Attribute("id") == Attribute("id3"))
                           .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)));
    runJoinQuery(query, csvFileParams, joinParams, expectedOutput);
}

TEST_P(MultipleJoinsTest, testJoin3WithDifferentSourceTumblingWindowOnCoodinatorSequential) {
    if (joinStrategy == QueryCompilation::StreamJoinStrategy::HASH_JOIN_VAR_SIZED
        && windowingStrategy == QueryCompilation::WindowingStrategy::BUCKETING) {
        GTEST_SKIP();
    }

    const auto windowSchema = TestSchemas::getSchemaTemplate("id_val_time_u64")->updateSourceName("window1");
    const auto window2Schema = TestSchemas::getSchemaTemplate("id2_val2_time_u64")->updateSourceName("window2");
    const auto window3Schema = TestSchemas::getSchemaTemplate("id3_val3_time_u64")->updateSourceName("window3");
    const auto window4Schema = TestSchemas::getSchemaTemplate("id4_val4_time_u64")->updateSourceName("window4");

    TestUtils::JoinParams joinParams({windowSchema, window2Schema, window3Schema, window4Schema});
    TestUtils::CsvFileParams csvFileParams({"window.csv", "window2.csv", "window4.csv", "window4.csv"});

    // Expected output
    const auto expectedOutput =
        "12000, 13000, 12000, 13000, 12000, 13000, 1, 12, 12000, 1, 12, 12000, 1, 12, 12000, 1, 12, 12000\n"
        "13000, 14000, 13000, 14000, 13000, 14000, 1, 13, 13000, 1, 13, 13000, 1, 13, 13000, 1, 13, 13000\n"
        "14000, 15000, 14000, 15000, 14000, 15000, 1, 14, 14000, 1, 14, 14000, 1, 14, 14000, 1, 14, 14000\n"
        "15000, 16000, 15000, 16000, 15000, 16000, 1, 15, 15000, 1, 15, 15000, 1, 15, 15000, 1, 15, 15000\n"
        "3000, 4000, 3000, 4000, 3000, 4000, 11, 3, 3001, 11, 3, 3001, 11, 9, 3000, 11, 9, 3000\n"
        "1000, 2000, 1000, 2000, 1000, 2000, 4,  1, 1002, 4, 3, 1102, 4, 4, 1001, 4, 4, 1001\n"
        "1000, 2000, 1000, 2000, 1000, 2000, 4, 1, 1002, 4, 3, 1112, 4, 4, 1001, 4, 4, 1001\n"
        "1000, 2000, 1000, 2000, 1000, 2000, 12, 1, 1001, 12, 5, 1011, 12, 1, 1300, 12, 1, 1300\n";

    const auto query = Query::from("window1")
                           .joinWith(Query::from("window2"))
                           .where(Attribute("id") == Attribute("id2"))
                           .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
                           .joinWith(Query::from("window3"))
                           .where(Attribute("id") == Attribute("id3"))
                           .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
                           .joinWith(Query::from("window4"))
                           .where(Attribute("id") == Attribute("id4"))
                           .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)));
    runJoinQuery(query, csvFileParams, joinParams, expectedOutput);
}

TEST_P(MultipleJoinsTest, testJoin3WithDifferentSourceTumblingWindowOnCoodinatorNested) {
    if (joinStrategy == QueryCompilation::StreamJoinStrategy::HASH_JOIN_VAR_SIZED
        && windowingStrategy == QueryCompilation::WindowingStrategy::BUCKETING) {
        GTEST_SKIP();
    }

    const auto windowSchema = TestSchemas::getSchemaTemplate("id_val_time_u64")->updateSourceName("window1");
    const auto window2Schema = TestSchemas::getSchemaTemplate("id2_val2_time_u64")->updateSourceName("window2");
    const auto window3Schema = TestSchemas::getSchemaTemplate("id3_val3_time_u64")->updateSourceName("window3");
    const auto window4Schema = TestSchemas::getSchemaTemplate("id4_val4_time_u64")->updateSourceName("window4");

    TestUtils::JoinParams joinParams({windowSchema, window2Schema, window3Schema, window4Schema});
    TestUtils::CsvFileParams csvFileParams({"window.csv", "window2.csv", "window4.csv", "window4.csv"});

    // Expected output
    const auto expectedOutput =
        "1000, 2000, 1000, 2000, 12, 1, 1001, 12, 5, 1011, 1000, 2000, 12, 1, 1300, 12, 1, 1300\n"
        "1000, 2000, 1000, 2000, 4, 1, 1002, 4, 3, 1102, 1000, 2000, 4,4, 1001, 4, 4, 1001\n"
        "1000, 2000, 1000, 2000, 4, 1, 1002, 4, 3, 1112, 1000, 2000, 4, 4, 1001, 4, 4, 1001\n"
        "3000, 4000, 3000, 4000,  11, 3, 3001, 11, 3, 3001, 3000, 4000, 11, 9, 3000, 11, 9, 3000\n"
        "12000, 13000, 12000, 13000, 1, 12, 12000, 1, 12, 12000, 12000, 13000, 1,12, 12000, 1, 12, 12000\n"
        "13000, 14000, 13000, 14000, 1, 13, 13000, 1, 13, 13000, 13000, 14000, 1, 13, 13000, 1, 13, 13000\n"
        "14000, 15000, 14000, 15000, 1, 14, 14000, 1, 14, 14000, 14000, 15000, 1, 14, 14000, 1, 14, 14000\n"
        "15000, 16000, 15000, 16000, 1, 15, 15000, 1, 15, 15000, 15000, 16000, 1, 15, 15000, 1, 15, 15000\n";

    const auto query = Query::from("window1")
                           .joinWith(Query::from("window2"))
                           .where(Attribute("id") == Attribute("id2"))
                           .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
                           .joinWith((Query::from("window3"))
                                         .joinWith(Query::from("window4"))
                                         .where(Attribute("id3") == Attribute("id4"))
                                         .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000))))
                           .where(Attribute("id") == Attribute("id4"))
                           .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)));

    runJoinQuery(query, csvFileParams, joinParams, expectedOutput);
}

/**
 * Sliding window joins
 *
 */
TEST_P(MultipleJoinsTest, testJoins2WithDifferentSourceSlidingWindowOnCoodinator) {
    if (joinStrategy == QueryCompilation::StreamJoinStrategy::HASH_JOIN_VAR_SIZED
        && windowingStrategy == QueryCompilation::WindowingStrategy::BUCKETING) {
        GTEST_SKIP();
    }

    const auto windowSchema = TestSchemas::getSchemaTemplate("id_val_time_u64")->updateSourceName("window1");
    const auto window2Schema = TestSchemas::getSchemaTemplate("id2_val2_time_u64")->updateSourceName("window2");
    const auto window3Schema = TestSchemas::getSchemaTemplate("id3_val3_time_u64")->updateSourceName("window3");

    TestUtils::JoinParams joinParams({windowSchema, window2Schema, window3Schema});
    TestUtils::CsvFileParams csvFileParams({"window.csv", "window2.csv", "window4.csv"}, "");

    // Expected output
    const auto expectedOutput = "500, 1500, 500, 1500, 12, 1, 1001, 12, 5, 1011, 12, 1, 1300\n"
                                "500, 1500, 500, 1500, 4, 1, 1002, 4, 3, 1102, 4, 4, 1001\n"
                                "500, 1500, 500, 1500, 4, 1, 1002, 4, 3, 1112, 4, 4, 1001\n"
                                "500, 1500, 1000, 2000, 12, 1, 1001, 12, 5, 1011, 12, 1, 1300\n"
                                "500, 1500, 1000, 2000, 4, 1, 1002, 4, 3, 1102, 4, 4, 1001\n"
                                "500, 1500, 1000, 2000, 4, 1, 1002, 4, 3, 1112, 4, 4, 1001\n"
                                "1000, 2000, 500, 1500, 12, 1, 1001, 12, 5, 1011, 12, 1, 1300\n"
                                "1000, 2000, 500, 1500, 4, 1, 1002, 4, 3, 1102, 4, 4, 1001\n"
                                "1000, 2000, 500, 1500, 4, 1, 1002, 4, 3, 1112, 4, 4, 1001\n"
                                "1000, 2000, 1000, 2000, 12, 1, 1001, 12, 5, 1011, 12, 1, 1300\n"
                                "1000, 2000, 1000, 2000, 4, 1, 1002, 4, 3, 1102, 4, 4, 1001\n"
                                "1000, 2000, 1000, 2000, 4, 1, 1002, 4, 3, 1112, 4, 4, 1001\n"
                                "2500, 3500, 2500, 3500, 11, 3, 3001, 11, 3, 3001, 11, 9, 3000\n"
                                "2500, 3500, 3000, 4000, 11, 3, 3001, 11, 3, 3001, 11, 9, 3000\n"
                                "3000, 4000, 2500, 3500, 11, 3, 3001, 11, 3, 3001, 11, 9, 3000\n"
                                "3000, 4000, 3000, 4000, 11, 3, 3001, 11, 3, 3001, 11, 9, 3000\n"
                                "11500, 12500, 11500, 12500, 1, 12, 12000, 1, 12, 12000, 1, 12, 12000\n"
                                "11500, 12500, 12000, 13000, 1, 12, 12000, 1, 12, 12000, 1, 12, 12000\n"
                                "12000, 13000, 11500, 12500, 1, 12, 12000, 1, 12, 12000, 1, 12, 12000\n"
                                "12000, 13000, 12000, 13000, 1, 12, 12000, 1, 12, 12000, 1, 12, 12000\n"
                                "12500, 13500, 12500, 13500, 1, 13, 13000, 1, 13, 13000, 1, 13, 13000\n"
                                "12500, 13500, 13000, 14000, 1, 13, 13000, 1, 13, 13000, 1, 13, 13000\n"
                                "13000, 14000, 12500, 13500, 1, 13, 13000, 1, 13, 13000, 1, 13, 13000\n"
                                "13000, 14000, 13000, 14000, 1, 13, 13000, 1, 13, 13000, 1, 13, 13000\n"
                                "13500, 14500, 13500, 14500, 1, 14, 14000, 1, 14, 14000, 1, 14, 14000\n"
                                "13500, 14500, 14000, 15000, 1, 14, 14000, 1, 14, 14000, 1, 14, 14000\n"
                                "14000, 15000, 13500, 14500, 1, 14, 14000, 1, 14, 14000, 1, 14, 14000\n"
                                "14000, 15000, 14000, 15000, 1, 14, 14000, 1, 14, 14000, 1, 14, 14000\n"
                                "14500, 15500, 14500, 15500, 1, 15, 15000, 1, 15, 15000, 1, 15, 15000\n"
                                "14500, 15500, 15000, 16000, 1, 15, 15000, 1, 15, 15000, 1, 15, 15000\n"
                                "15000, 16000, 14500, 15500, 1, 15, 15000, 1, 15, 15000, 1, 15, 15000\n"
                                "15000, 16000, 15000, 16000, 1, 15, 15000, 1, 15, 15000, 1, 15, 15000\n";

    const auto query = Query::from("window1")
                           .joinWith(Query::from("window2"))
                           .where(Attribute("id") == Attribute("id2"))
                           .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(1), Milliseconds(500)))
                           .joinWith(Query::from("window3"))
                           .where(Attribute("id") == Attribute("id3"))
                           .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(1), Milliseconds(500)));
    runJoinQuery(query, csvFileParams, joinParams, expectedOutput);
}

TEST_P(MultipleJoinsTest, testJoin3WithDifferentSourceSlidingWindowOnCoodinatorSequential) {
    if (joinStrategy == QueryCompilation::StreamJoinStrategy::HASH_JOIN_VAR_SIZED
        && windowingStrategy == QueryCompilation::WindowingStrategy::BUCKETING) {
        GTEST_SKIP();
    }

    const auto windowSchema = TestSchemas::getSchemaTemplate("id_val_time_u64")->updateSourceName("window1");
    const auto window2Schema = TestSchemas::getSchemaTemplate("id2_val2_time_u64")->updateSourceName("window2");
    const auto window3Schema = TestSchemas::getSchemaTemplate("id3_val3_time_u64")->updateSourceName("window3");
    const auto window4Schema = TestSchemas::getSchemaTemplate("id4_val4_time_u64")->updateSourceName("window4");

    TestUtils::JoinParams joinParams({windowSchema, window2Schema, window3Schema, window4Schema});
    TestUtils::CsvFileParams csvFileParams({"window.csv", "window2.csv", "window4.csv", "window4.csv"});

    // Expected output
    const auto expectedOutput =
        "500, 1500, 500, 1500,  500, 1500, 12, 1, 1001, 12, 5, 1011, 12, 1, 1300, 12, 1, 1300\n"
        "500, 1500, 500, 1500,  500, 1500,  4, 1, 1002, 4, 3, 1102, 4, 4, 1001, 4, 4, 1001\n"
        "500, 1500, 500, 1500,  500, 1500, 4, 1, 1002, 4, 3, 1112, 4, 4, 1001, 4, 4, 1001\n"
        "500, 1500, 500, 1500,  1000, 2000, 12, 1, 1001, 12, 5, 1011, 12, 1, 1300, 12, 1, 1300\n"
        "500, 1500, 500, 1500,  1000, 2000, 4, 1, 1002, 4, 3, 1102, 4, 4, 1001, 4, 4, 1001\n"
        "500, 1500, 500, 1500,  1000, 2000, 4, 1, 1002, 4, 3, 1112, 4, 4, 1001, 4, 4, 1001\n"
        "500, 1500, 1000, 2000,  500, 1500, 12, 1, 1001, 12, 5, 1011, 12, 1, 1300, 12, 1, 1300\n"
        "500, 1500, 1000, 2000,  500, 1500, 4, 1, 1002, 4, 3, 1102, 4, 4, 1001, 4, 4, 1001\n"
        "500, 1500, 1000, 2000,  500, 1500, 4, 1, 1002, 4, 3, 1112, 4, 4, 1001, 4, 4, 1001\n"
        "500, 1500, 1000, 2000,  1000, 2000, 12, 1, 1001, 12, 5, 1011, 12, 1, 1300, 12, 1, 1300\n"
        "500, 1500, 1000, 2000,  1000, 2000, 4, 1, 1002, 4, 3, 1102, 4, 4, 1001, 4, 4, 1001\n"
        "500, 1500, 1000, 2000,  1000, 2000, 4, 1, 1002, 4, 3, 1112, 4, 4, 1001, 4, 4, 1001\n"
        "1000, 2000, 500, 1500,  500, 1500, 12, 1, 1001, 12, 5, 1011, 12, 1, 1300, 12, 1, 1300\n"
        "1000, 2000, 500, 1500,  500, 1500, 4, 1, 1002, 4, 3, 1102, 4, 4, 1001, 4, 4, 1001\n"
        "1000, 2000, 500, 1500,  500, 1500, 4, 1, 1002, 4, 3, 1112, 4, 4, 1001, 4, 4, 1001\n"
        "1000, 2000, 500, 1500,  1000, 2000, 12, 1, 1001, 12, 5, 1011, 12, 1, 1300, 12, 1, 1300\n"
        "1000, 2000, 500, 1500,  1000, 2000, 4, 1, 1002, 4, 3, 1102, 4, 4, 1001, 4, 4, 1001\n"
        "1000, 2000, 500, 1500,  1000, 2000, 4, 1, 1002, 4, 3, 1112, 4, 4, 1001, 4, 4, 1001\n"
        "1000, 2000,1000, 2000,  500, 1500,  12, 1, 1001, 12, 5, 1011, 12, 1, 1300, 12, 1, 1300\n"
        "1000, 2000,1000, 2000,  500, 1500,  4, 1, 1002, 4, 3, 1102, 4, 4, 1001, 4, 4, 1001\n"
        "1000, 2000,1000, 2000, 500, 1500, 4, 1, 1002, 4, 3, 1112, 4, 4, 1001, 4, 4, 1001\n"
        "1000, 2000,1000, 2000, 1000, 2000,  12, 1, 1001, 12, 5, 1011, 12, 1, 1300, 12, 1, 1300\n"
        "1000, 2000,1000, 2000, 1000, 2000,  4, 1, 1002, 4, 3, 1102, 4, 4, 1001, 4, 4, 1001\n"
        "1000, 2000,1000, 2000,  1000, 2000, 4, 1, 1002, 4, 3, 1112, 4, 4, 1001, 4, 4, 1001\n"
        "2500, 3500,2500, 3500, 2500, 3500,  11, 3, 3001, 11, 3, 3001, 11, 9, 3000, 11, 9, 3000\n"
        "2500, 3500, 2500, 3500, 3000, 4000, 11, 3, 3001, 11, 3, 3001, 11, 9, 3000, 11, 9, 3000\n"
        "2500, 3500, 3000, 4000, 2500, 3500, 11, 3, 3001, 11, 3, 3001, 11, 9, 3000, 11, 9, 3000\n"
        "2500, 3500, 3000, 4000, 3000, 4000, 11, 3, 3001, 11, 3, 3001, 11, 9, 3000, 11, 9, 3000\n"
        "3000, 4000, 2500, 3500, 2500, 3500, 11, 3, 3001, 11, 3, 3001, 11, 9, 3000, 11, 9, 3000\n"
        "3000, 4000, 2500, 3500, 3000, 4000, 11, 3, 3001, 11, 3, 3001, 11, 9, 3000, 11, 9, 3000\n"
        "3000, 4000, 3000, 4000, 2500, 3500, 11, 3, 3001, 11, 3, 3001, 11, 9, 3000, 11, 9, 3000\n"
        "3000, 4000, 3000, 4000, 3000, 4000, 11, 3, 3001, 11, 3, 3001, 11, 9, 3000, 11, 9, 3000\n"
        "11500, 12500, 11500, 12500, 11500, 12500, 1, 12, 12000, 1, 12, 12000, 1, 12, 12000, 1, 12, 12000\n"
        "11500, 12500, 11500, 12500, 12000, 13000,  1, 12, 12000, 1, 12, 12000, 1, 12, 12000, 1, 12, 12000\n"
        "11500, 12500, 12000, 13000, 11500, 12500,  1, 12, 12000, 1, 12, 12000, 1, 12, 12000, 1, 12, 12000\n"
        "11500, 12500, 12000, 13000, 12000, 13000,  1, 12, 12000, 1, 12, 12000, 1, 12, 12000, 1, 12, 12000\n"
        "12000, 13000, 11500, 12500, 11500, 12500,  1, 12, 12000, 1, 12, 12000, 1, 12, 12000, 1, 12, 12000\n"
        "12000, 13000, 11500, 12500, 12000, 13000,  1, 12, 12000, 1, 12, 12000, 1, 12, 12000, 1, 12, 12000\n"
        "12000, 13000, 12000, 13000, 11500, 12500,  1, 12, 12000, 1, 12, 12000, 1, 12, 12000, 1, 12, 12000\n"
        "12000, 13000, 12000, 13000, 12000, 13000,  1, 12, 12000, 1, 12, 12000, 1, 12, 12000, 1, 12, 12000\n"
        "12500, 13500, 12500, 13500, 12500, 13500,  1, 13, 13000, 1, 13, 13000, 1, 13, 13000, 1, 13, 13000\n"
        "12500, 13500, 12500, 13500, 13000, 14000,  1, 13, 13000, 1, 13, 13000, 1, 13, 13000, 1, 13, 13000\n"
        "12500, 13500, 13000, 14000, 12500, 13500,  1, 13, 13000, 1, 13, 13000, 1, 13, 13000, 1, 13, 13000\n"
        "12500, 13500, 13000, 14000, 13000, 14000,  1, 13, 13000, 1, 13, 13000, 1, 13, 13000, 1, 13, 13000\n"
        "13000, 14000, 12500, 13500, 12500, 13500,  1, 13, 13000, 1, 13, 13000, 1, 13, 13000, 1, 13, 13000\n"
        "13000, 14000, 12500, 13500, 13000, 14000,  1, 13, 13000, 1, 13, 13000, 1, 13, 13000, 1, 13, 13000\n"
        "13000, 14000, 13000, 14000, 12500, 13500,  1, 13, 13000, 1, 13, 13000, 1, 13, 13000, 1, 13, 13000\n"
        "13000, 14000, 13000, 14000, 13000, 14000,  1, 13, 13000, 1, 13, 13000, 1, 13, 13000, 1, 13, 13000\n"
        "13500, 14500, 13500, 14500, 13500, 14500,  1, 14, 14000, 1, 14, 14000, 1, 14, 14000, 1, 14, 14000\n"
        "13500, 14500, 13500, 14500, 14000, 15000,  1, 14, 14000, 1, 14, 14000, 1, 14, 14000, 1, 14, 14000\n"
        "13500, 14500, 14000, 15000, 13500, 14500,  1, 14, 14000, 1, 14, 14000, 1, 14, 14000, 1, 14, 14000\n"
        "13500, 14500, 14000, 15000, 14000, 15000,  1, 14, 14000, 1, 14, 14000, 1, 14, 14000, 1, 14, 14000\n"
        "14000, 15000, 13500, 14500, 13500, 14500,  1, 14, 14000, 1, 14, 14000, 1, 14, 14000, 1, 14, 14000\n"
        "14000, 15000, 13500, 14500, 14000, 15000,  1, 14, 14000, 1, 14, 14000, 1, 14, 14000, 1, 14, 14000\n"
        "14000, 15000, 14000, 15000, 13500, 14500,  1, 14, 14000, 1, 14, 14000, 1, 14, 14000, 1, 14, 14000\n"
        "14000, 15000, 14000, 15000, 14000, 15000,  1, 14, 14000, 1, 14, 14000, 1, 14, 14000, 1, 14, 14000\n"
        "14500, 15500, 14500, 15500, 14500, 15500,  1, 15, 15000, 1, 15, 15000, 1, 15, 15000, 1, 15, 15000\n"
        "14500, 15500, 14500, 15500, 15000, 16000,  1, 15, 15000, 1, 15, 15000, 1, 15, 15000, 1, 15, 15000\n"
        "14500, 15500, 15000, 16000, 14500, 15500,  1, 15, 15000, 1, 15, 15000, 1, 15, 15000, 1, 15, 15000\n"
        "14500, 15500, 15000, 16000, 15000, 16000,  1, 15, 15000, 1, 15, 15000, 1, 15, 15000, 1, 15, 15000\n"
        "15000, 16000, 14500, 15500, 14500, 15500,  1, 15, 15000, 1, 15, 15000, 1, 15, 15000, 1, 15, 15000\n"
        "15000, 16000, 14500, 15500, 15000, 16000,  1, 15, 15000, 1, 15, 15000, 1, 15, 15000, 1, 15, 15000\n"
        "15000, 16000, 15000, 16000, 14500, 15500,  1, 15, 15000, 1, 15, 15000, 1, 15, 15000, 1, 15, 15000\n"
        "15000, 16000, 15000, 16000, 15000, 16000,  1, 15, 15000, 1, 15, 15000, 1, 15, 15000, 1, 15, 15000\n";

    const auto query = Query::from("window1")
                           .joinWith(Query::from("window2"))
                           .where(Attribute("id") == Attribute("id2"))
                           .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(1), Milliseconds(500)))
                           .joinWith(Query::from("window3"))
                           .where(Attribute("id") == Attribute("id3"))
                           .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(1), Milliseconds(500)))
                           .joinWith(Query::from("window4"))
                           .where(Attribute("id") == Attribute("id4"))
                           .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(1), Milliseconds(500)));
    runJoinQuery(query, csvFileParams, joinParams, expectedOutput);
}

TEST_P(MultipleJoinsTest, testJoin3WithDifferentSourceSlidingWindowOnCoodinatorNested) {
    if (joinStrategy == QueryCompilation::StreamJoinStrategy::HASH_JOIN_VAR_SIZED
        && windowingStrategy == QueryCompilation::WindowingStrategy::BUCKETING) {
        GTEST_SKIP();
    }

    const auto windowSchema = TestSchemas::getSchemaTemplate("id_val_time_u64")->updateSourceName("window1");
    const auto window2Schema = TestSchemas::getSchemaTemplate("id2_val2_time_u64")->updateSourceName("window2");
    const auto window3Schema = TestSchemas::getSchemaTemplate("id3_val3_time_u64")->updateSourceName("window3");
    const auto window4Schema = TestSchemas::getSchemaTemplate("id4_val4_time_u64")->updateSourceName("window4");

    TestUtils::JoinParams joinParams({windowSchema, window2Schema, window3Schema, window4Schema});
    TestUtils::CsvFileParams csvFileParams({"window.csv", "window2.csv", "window4.csv", "window4.csv"});

    // Expected output
    const auto expectedOutput =
        "500, 1500, 500, 1500, 12, 1, 1001, 12, 5, 1011, 500, 1500, 12, 1, 1300, 12, 1, 1300\n"
        "500, 1500, 500, 1500, 12, 1, 1001, 12, 5, 1011, 1000, 2000, 12, 1, 1300, 12, 1, 1300\n"
        "500, 1500, 500, 1500, 4, 1, 1002, 4, 3, 1102, 500, 1500, 4, 4, 1001, 4, 4, 1001\n"
        "500, 1500, 500, 1500, 4, 1, 1002, 4, 3, 1102, 1000, 2000, 4, 4, 1001, 4, 4, 1001\n"
        "500, 1500, 500, 1500, 4, 1, 1002, 4, 3, 1112, 500, 1500, 4, 4, 1001, 4, 4, 1001\n"
        "500, 1500, 500, 1500, 4, 1, 1002, 4, 3, 1112, 1000, 2000, 4, 4, 1001, 4, 4, 1001\n"
        "500, 1500, 1000, 2000, 12, 1, 1001, 12, 5, 1011, 500, 1500,  12, 1, 1300, 12, 1, 1300\n"
        "500, 1500, 1000, 2000, 12, 1, 1001, 12, 5, 1011, 1000, 2000, 12, 1, 1300, 12, 1, 1300\n"
        "500, 1500, 1000, 2000, 4, 1, 1002, 4, 3, 1102, 500, 1500, 4, 4, 1001, 4, 4, 1001\n"
        "500, 1500, 1000, 2000, 4, 1, 1002, 4, 3, 1102, 1000, 2000, 4, 4, 1001, 4, 4, 1001\n"
        "500, 1500, 1000, 2000, 4, 1, 1002, 4, 3, 1112, 500, 1500, 4, 4, 1001, 4, 4, 1001\n"
        "500, 1500, 1000, 2000, 4, 1, 1002, 4, 3, 1112, 1000, 2000, 4, 4, 1001, 4, 4, 1001\n"
        "1000, 2000, 500, 1500, 12, 1, 1001, 12, 5, 1011, 500, 1500, 12, 1, 1300, 12, 1, 1300\n"
        "1000, 2000, 500, 1500, 12, 1, 1001, 12, 5, 1011, 1000, 2000, 12,1, 1300, 12, 1, 1300\n"
        "1000, 2000, 500, 1500, 4, 1, 1002, 4, 3, 1102, 500, 1500, 4, 4, 1001, 4, 4, 1001\n"
        "1000, 2000, 500, 1500, 4, 1, 1002, 4, 3, 1102, 1000, 2000, 4, 4, 1001, 4, 4, 1001\n"
        "1000, 2000, 500, 1500, 4, 1, 1002, 4, 3, 1112, 500, 1500, 4, 4, 1001, 4, 4, 1001\n"
        "1000, 2000, 500, 1500, 4, 1, 1002, 4, 3, 1112, 1000, 2000, 4, 4, 1001, 4, 4, 1001\n"
        "1000, 2000, 1000, 2000, 12, 1, 1001, 12, 5, 1011, 500, 1500, 12, 1, 1300, 12, 1, 1300\n"
        "1000, 2000, 1000, 2000, 12, 1, 1001, 12, 5, 1011, 1000, 2000, 12, 1, 1300, 12, 1, 1300\n"
        "1000, 2000, 1000, 2000, 4, 1, 1002, 4, 3, 1102, 500, 1500, 4, 4, 1001, 4, 4, 1001\n"
        "1000, 2000, 1000, 2000, 4, 1, 1002, 4, 3, 1102, 1000, 2000, 4, 4, 1001, 4, 4, 1001\n"
        "1000, 2000, 1000, 2000, 4, 1, 1002, 4, 3, 1112, 500, 1500, 4, 4, 1001, 4, 4, 1001\n"
        "1000, 2000, 1000, 2000, 4, 1, 1002, 4, 3, 1112, 1000, 2000, 4, 4, 1001, 4, 4, 1001\n"
        "2500, 3500, 2500, 3500, 11, 3, 3001, 11, 3, 3001, 2500, 3500, 11, 9, 3000, 11, 9, 3000\n"
        "2500, 3500, 2500, 3500, 11, 3, 3001, 11, 3, 3001, 3000, 4000, 11, 9, 3000, 11, 9, 3000\n"
        "2500, 3500, 3000, 4000, 11, 3, 3001, 11, 3, 3001, 2500, 3500,  11, 9, 3000, 11, 9, 3000\n"
        "2500, 3500, 3000, 4000, 11, 3, 3001, 11, 3, 3001, 3000, 4000, 11, 9, 3000, 11, 9, 3000\n"
        "3000, 4000, 2500, 3500, 11, 3, 3001, 11, 3, 3001, 2500, 3500,  11, 9, 3000, 11, 9, 3000\n"
        "3000, 4000, 2500, 3500, 11, 3, 3001, 11, 3, 3001, 3000, 4000, 11, 9, 3000, 11, 9, 3000\n"
        "3000, 4000, 3000, 4000, 11, 3, 3001, 11, 3, 3001, 2500, 3500,  11, 9, 3000, 11, 9, 3000\n"
        "3000, 4000, 3000, 4000, 11, 3, 3001, 11, 3, 3001, 3000, 4000,  11, 9, 3000, 11, 9, 3000\n"
        "11500, 12500, 11500, 12500, 1, 12, 12000, 1, 12, 12000, 11500, 12500,  1, 12, 12000, 1, 12, 12000\n"
        "11500, 12500, 11500, 12500, 1, 12, 12000, 1, 12, 12000, 12000, 13000,  1, 12, 12000, 1, 12, 12000\n"
        "11500, 12500, 12000, 13000, 1, 12, 12000, 1, 12, 12000, 11500, 12500,  1, 12, 12000, 1, 12, 12000\n"
        "11500, 12500, 12000, 13000, 1, 12, 12000, 1, 12, 12000, 12000, 13000,  1, 12, 12000, 1, 12, 12000\n"
        "12000, 13000, 11500, 12500, 1, 12, 12000, 1, 12, 12000, 11500, 12500,  1, 12, 12000, 1, 12, 12000\n"
        "12000, 13000, 11500, 12500, 1, 12, 12000, 1, 12, 12000, 12000, 13000,  1, 12, 12000, 1, 12, 12000\n"
        "12000, 13000, 12000, 13000, 1, 12, 12000, 1, 12, 12000, 11500, 12500,  1, 12, 12000, 1, 12, 12000\n"
        "12000, 13000, 12000, 13000, 1, 12, 12000, 1, 12, 12000, 12000, 13000,  1, 12, 12000, 1, 12, 12000\n"
        "12500, 13500, 12500, 13500, 1, 13, 13000, 1, 13, 13000, 12500, 13500,  1, 13, 13000, 1, 13, 13000\n"
        "12500, 13500, 12500, 13500, 1, 13, 13000, 1, 13, 13000, 13000, 14000,  1, 13, 13000, 1, 13, 13000\n"
        "12500, 13500, 13000, 14000, 1, 13, 13000, 1, 13, 13000, 12500, 13500,  1, 13, 13000, 1, 13, 13000\n"
        "12500, 13500, 13000, 14000, 1, 13, 13000, 1, 13, 13000, 13000, 14000,  1, 13, 13000, 1, 13, 13000\n"
        "13000, 14000, 12500, 13500, 1, 13, 13000, 1, 13, 13000, 12500, 13500,  1, 13, 13000, 1, 13, 13000\n"
        "13000, 14000, 12500, 13500, 1, 13, 13000, 1, 13, 13000, 13000, 14000,  1, 13, 13000, 1, 13, 13000\n"
        "13000, 14000, 13000, 14000, 1, 13, 13000, 1, 13, 13000, 12500, 13500,  1, 13, 13000, 1, 13, 13000\n"
        "13000, 14000, 13000, 14000, 1, 13, 13000, 1, 13, 13000, 13000, 14000,  1, 13, 13000, 1, 13, 13000\n"
        "13500, 14500, 13500, 14500, 1, 14, 14000, 1, 14, 14000, 13500, 14500,  1, 14, 14000, 1, 14, 14000\n"
        "13500, 14500, 13500, 14500, 1, 14, 14000, 1, 14, 14000, 14000, 15000,  1, 14, 14000, 1, 14, 14000\n"
        "13500, 14500, 14000, 15000, 1, 14, 14000, 1, 14, 14000, 13500, 14500,  1, 14, 14000, 1, 14, 14000\n"
        "13500, 14500, 14000, 15000, 1, 14, 14000, 1, 14, 14000, 14000, 15000,  1, 14, 14000, 1, 14, 14000\n"
        "14000, 15000, 13500, 14500, 1, 14, 14000, 1, 14, 14000, 13500, 14500,  1, 14, 14000, 1, 14, 14000\n"
        "14000, 15000, 13500, 14500, 1, 14, 14000, 1, 14, 14000, 14000, 15000, 1, 14, 14000, 1, 14, 14000\n"
        "14000, 15000, 14000, 15000, 1, 14, 14000, 1, 14, 14000, 13500, 14500, 1, 14, 14000, 1, 14, 14000\n"
        "14000, 15000, 14000, 15000, 1, 14, 14000, 1, 14, 14000, 14000, 15000, 1, 14, 14000, 1, 14, 14000\n"
        "14500, 15500, 14500, 15500, 1, 15, 15000, 1, 15, 15000, 14500, 15500, 1, 15, 15000, 1, 15, 15000\n"
        "14500, 15500, 14500, 15500, 1, 15, 15000, 1, 15, 15000, 15000, 16000, 1, 15, 15000, 1, 15, 15000\n"
        "14500, 15500, 15000, 16000, 1, 15, 15000, 1, 15, 15000, 14500, 15500, 1, 15, 15000, 1, 15, 15000\n"
        "14500, 15500, 15000, 16000, 1, 15, 15000, 1, 15, 15000, 15000, 16000, 1, 15, 15000, 1, 15, 15000\n"
        "15000, 16000, 14500, 15500, 1, 15, 15000, 1, 15, 15000, 14500, 15500, 1, 15, 15000, 1, 15, 15000\n"
        "15000, 16000, 14500, 15500, 1, 15, 15000, 1, 15, 15000, 15000, 16000, 1, 15, 15000, 1, 15, 15000\n"
        "15000, 16000, 15000, 16000, 1, 15, 15000, 1, 15, 15000, 14500, 15500, 1, 15, 15000, 1, 15, 15000\n"
        "15000, 16000, 15000, 16000, 1, 15, 15000, 1, 15, 15000, 15000, 16000, 1, 15, 15000, 1, 15, 15000\n";

    const auto query =
        Query::from("window1")
            .joinWith(Query::from("window2"))
            .where(Attribute("id") == Attribute("id2"))
            .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(1), Milliseconds(500)))
            .joinWith((Query::from("window3"))
                          .joinWith(Query::from("window4"))
                          .where(Attribute("id3") == Attribute("id4"))
                          .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(1), Milliseconds(500))))
            .where(Attribute("id") == Attribute("id4"))
            .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(1), Milliseconds(500)));
    runJoinQuery(query, csvFileParams, joinParams, expectedOutput);
}

/*
 * Three-way join with data IDs as join-key for tumbling windows.
 */
TEST_P(MultipleJoinsTest, testMapNotKeyWithDifferentSourceTumblingWindow) {
    if (joinStrategy == QueryCompilation::StreamJoinStrategy::HASH_JOIN_VAR_SIZED
        && windowingStrategy == QueryCompilation::WindowingStrategy::BUCKETING) {
        GTEST_SKIP();
    }

    const auto windowSchema = TestSchemas::getSchemaTemplate("id_val_time_u64")->updateSourceName("window1");
    const auto window2Schema = TestSchemas::getSchemaTemplate("id2_val2_time_u64")->updateSourceName("window2");
    const auto window3Schema = TestSchemas::getSchemaTemplate("id3_val3_time_u64")->updateSourceName("window3");

    TestUtils::JoinParams joinParams({windowSchema, window2Schema, window3Schema});
    TestUtils::CsvFileParams csvFileParams({"window.csv", "window2.csv", "window4.csv"}, "");

    // Expected output
    const auto expectedOutput = "1000, 2000, 1000, 2000, 12, 1, 1001, 1, 12, 5, 1011, 1, 12, 1, 1300, 1\n";

    const auto query = Query::from("window1")
                           .filter(Attribute("id") == 12)
                           .map(Attribute("key1") = 1)
                           .joinWith(Query::from("window2"))
                           .where(Attribute("id") == Attribute("id2"))
                           .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
                           .map(Attribute("key2") = 1)
                           .joinWith(Query::from("window3"))
                           .where(Attribute("id") == Attribute("id3"))
                           .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
                           .map(Attribute("key3") = 1);

    runJoinQuery(query, csvFileParams, joinParams, expectedOutput);
}

/*
 * Three-way join with map as join-key for tumbling windows.
 */
TEST_P(MultipleJoinsTest, testMapsAsKeysWithDifferentSourceTumblingWindow) {
    if (joinStrategy == QueryCompilation::StreamJoinStrategy::HASH_JOIN_VAR_SIZED
        && windowingStrategy == QueryCompilation::WindowingStrategy::BUCKETING) {
        GTEST_SKIP();
    }

    const auto windowSchema = TestSchemas::getSchemaTemplate("id_val_time_u64")->updateSourceName("window1");
    const auto window2Schema = TestSchemas::getSchemaTemplate("id2_val2_time_u64")->updateSourceName("window2");
    const auto window3Schema = TestSchemas::getSchemaTemplate("id3_val3_time_u64")->updateSourceName("window3");

    TestUtils::JoinParams joinParams({windowSchema, window2Schema, window3Schema});
    TestUtils::CsvFileParams csvFileParams({"window.csv", "window2.csv", "window4.csv"}, "");

    // Expected output
    const auto expectedOutput = "1000, 2000, 1000, 2000, 12, 1, 1001, 1, 12, 5, 1011, 1, 12, 1, 1300, 1\n";
    // format of output: 123start; 123end; 123key; 12start; 12end; 12key; win1; id1; 1timestamp; key1; win2; id2; 2timestamp; key2; win3; id3; 3timestamp; key3

    const auto query = Query::from("window1")
                           .filter(Attribute("id") == 12)
                           .map(Attribute("key1") = 1)
                           .joinWith(Query::from("window2").map(Attribute("key2") = 1))
                           .where(Attribute("key1") == Attribute("key2"))
                           .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
                           .filter(Attribute("id2") == 12)
                           .joinWith(Query::from("window3").map(Attribute("key3") = 1).filter(Attribute("id3") == 12))
                           .where(Attribute("key1") == Attribute("key3"))
                           .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)));

    runJoinQuery(query, csvFileParams, joinParams, expectedOutput);
}

/*
* Three-way join on three keys with same name for tumbling windows.
 */
TEST_P(MultipleJoinsTest, testMapsThreeSameKeynamesTumblingWindow) {
    if (joinStrategy == QueryCompilation::StreamJoinStrategy::HASH_JOIN_VAR_SIZED
        && windowingStrategy == QueryCompilation::WindowingStrategy::BUCKETING) {
        GTEST_SKIP();
    }

    const auto windowSchema = TestSchemas::getSchemaTemplate("id_val_time_u64")->updateSourceName("window1");
    const auto window2Schema = TestSchemas::getSchemaTemplate("id2_val2_time_u64")->updateSourceName("window2");
    const auto window3Schema = Schema::create()
                                   ->addField(createField("id3", BasicType::UINT64))
                                   ->addField(createField("value3", BasicType::UINT64))
                                   ->addField(createField("timestamp", BasicType::UINT64))
                                   ->updateSourceName("window3");
    TestUtils::JoinParams joinParams({windowSchema, window2Schema, window3Schema});
    TestUtils::CsvFileParams csvFileParams({"window.csv", "window2.csv", "window4.csv"}, "");

    // Expected output
    const auto expectedOutput = "1000, 2000, 1000, 2000, 12, 1, 1001, 1, 12, 5, 1011, 1, 12, 1, 1300, 1\n";
    // format of output: 123start; 123end; 12start; 12end; win1; id1; 1timestamp; key1; win2; id2; 2timestamp; key2; win3; id3; 3timestamp; key3

    const auto query = Query::from("window1")
                           .filter(Attribute("id") == 12)
                           .map(Attribute("key1") = 1)
                           .joinWith(Query::from("window2").map(Attribute("key1") = 1))
                           .where(Attribute("key1") == Attribute("key1"))
                           .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
                           .filter(Attribute("id2") == 12)
                           .joinWith(Query::from("window3").map(Attribute("key1") = 1).filter(Attribute("id3") == 12))
                           .where(Attribute("key1") == Attribute("key1"))
                           .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)));

    runJoinQuery(query, csvFileParams, joinParams, expectedOutput);
}

/*
 * Three-way join with data IDs as keys for sliding windows.
 */
TEST_P(MultipleJoinsTest, testMapNotKeyWithDifferentSourceSlidingWindow) {
    if (joinStrategy == QueryCompilation::StreamJoinStrategy::HASH_JOIN_VAR_SIZED
        && windowingStrategy == QueryCompilation::WindowingStrategy::BUCKETING) {
        GTEST_SKIP();
    }

    const auto windowSchema = TestSchemas::getSchemaTemplate("id_val_time_u64")->updateSourceName("window1");
    const auto window2Schema = TestSchemas::getSchemaTemplate("id2_val2_time_u64")->updateSourceName("window2");
    const auto window3Schema = TestSchemas::getSchemaTemplate("id3_val3_time_u64")->updateSourceName("window3");

    TestUtils::JoinParams joinParams({windowSchema, window2Schema, window3Schema});
    TestUtils::CsvFileParams csvFileParams({"window.csv", "window2.csv", "window4.csv"}, "");

    // Expected output
    const auto expectedOutput = "1000, 2000, 1000, 2000, 12, 1, 1001, 1, 12, 5, 1011, 1, 12, 1, 1300, 1\n";

    const auto query = Query::from("window1")
                           .filter(Attribute("id") == 12)
                           .map(Attribute("key1") = 1)
                           .joinWith(Query::from("window2"))
                           .where(Attribute("id") == Attribute("id2"))
                           .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000), Milliseconds(1000)))
                           .map(Attribute("key2") = 1)
                           .joinWith(Query::from("window3"))
                           .where(Attribute("id") == Attribute("id3"))
                           .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000), Milliseconds(1000)))
                           .map(Attribute("key3") = 1);

    runJoinQuery(query, csvFileParams, joinParams, expectedOutput);
}

/*
 * Three-way join with map as keys for sliding windows.
 */
TEST_P(MultipleJoinsTest, testMapAsKeyWithDifferentSourceSlidingWindow) {
    if (joinStrategy == QueryCompilation::StreamJoinStrategy::HASH_JOIN_VAR_SIZED
        && windowingStrategy == QueryCompilation::WindowingStrategy::BUCKETING) {
        GTEST_SKIP();
    }

    const auto windowSchema = TestSchemas::getSchemaTemplate("id_val_time_u64")->updateSourceName("window1");
    const auto window2Schema = TestSchemas::getSchemaTemplate("id2_val2_time_u64")->updateSourceName("window2");
    const auto window3Schema = TestSchemas::getSchemaTemplate("id3_val3_time_u64")->updateSourceName("window3");

    TestUtils::JoinParams joinParams({windowSchema, window2Schema, window3Schema});
    TestUtils::CsvFileParams csvFileParams({"window.csv", "window2.csv", "window4.csv"}, "");

    // Expected output
    const auto expectedOutput = "1000, 2000, 1000, 2000, 12, 1, 1001, 1, 12, 5, 1011, 1, 12, 1, 1300, 1\n";

    const auto query = Query::from("window1")
                           .filter(Attribute("id") == 12)
                           .map(Attribute("key1") = 1)
                           .joinWith(Query::from("window2").map(Attribute("key2") = 1))
                           .where(Attribute("key1") == Attribute("key2"))
                           .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000), Milliseconds(1000)))
                           .filter(Attribute("id2") == 12)
                           .joinWith(Query::from("window3").map(Attribute("key3") = 1).filter(Attribute("id3") == 12))
                           .where(Attribute("key1") == Attribute("key3"))
                           .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000), Milliseconds(1000)));

    runJoinQuery(query, csvFileParams, joinParams, expectedOutput);
}

INSTANTIATE_TEST_CASE_P(testJoinQueries,
                        MultipleJoinsTest,
                        JOIN_STRATEGIES_WINDOW_STRATEGIES,
                        [](const testing::TestParamInfo<MultipleJoinsTest::ParamType>& info) {
                            return std::string(magic_enum::enum_name(std::get<0>(info.param))) + "_"
                                + std::string(magic_enum::enum_name(std::get<1>(info.param)));
                        });
}// namespace NES::Runtime::Execution
