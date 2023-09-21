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

#include <API/QueryAPI.hpp>
#include <BaseIntegrationTest.hpp>
#include <gtest/gtest.h>

#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Components/NesCoordinator.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <gmock/gmock-matchers.h>
#include <iostream>

using namespace std;

namespace NES::Runtime::Execution {


class MultipleJoinsTest : public Testing::BaseIntegrationTest,
                          public ::testing::WithParamInterface<std::tuple<QueryCompilation::StreamJoinStrategy,
                                                                          QueryCompilation::WindowingStrategy>>{
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

    template<typename ResultRecord>
    std::vector<ResultRecord> runJoinQuery(const Query& query,
                                           const TestUtils::CsvFileParams& csvFileParams,
                                           const TestUtils::JoinParams& joinParams,
                                           const std::vector<ResultRecord>& expectedRecords) {
        EXPECT_EQ(sizeof(ResultRecord), joinParams.outputSchema->getSchemaSizeInBytes());
        TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                      .enableNautilus()
                                      .setJoinStrategy(joinStrategy)
                                      .setWindowingStrategy(windowingStrategy);

        for (auto i = 0_u64; i < joinParams.inputSchemas.size(); ++i) {
            auto sourceConfig = TestUtils::createSourceConfig(csvFileParams.inputCsvFiles[i]);
            std::string logicalSourceName = "window" + std::to_string(i+1);
            testHarness.addLogicalSource(logicalSourceName, joinParams.inputSchemas[i])
                       .attachWorkerWithCSVSourceToCoordinator(logicalSourceName, sourceConfig);
        }

        auto actualResult = testHarness.validate().setupTopology().getOutput<ResultRecord>(expectedRecords.size());
        EXPECT_EQ(actualResult.size(), expectedRecords.size());
        EXPECT_THAT(actualResult, ::testing::UnorderedElementsAreArray(expectedRecords));

        // We return the actual results so that we can print them or do something else with them
        return actualResult;
    }
};

TEST_P(MultipleJoinsTest, testJoins2WithDifferentSourceTumblingWindowOnCoodinator) {
    struct __attribute__((packed)) ResultRecord {
        uint64_t window1window2window3start;
        uint64_t window1window2window3end;
        uint64_t window1window2window3key;
        uint64_t window1window2start;
        uint64_t window1window2end;
        uint64_t window1window2key;
        uint64_t window1win1;
        uint64_t window1id1;
        uint64_t window1timestamp;
        uint64_t window2win2;
        uint64_t window2id2;
        uint64_t window2timestamp;
        uint64_t window3win3;
        uint64_t window3id3;
        uint64_t window3timestamp;

        bool operator==(const ResultRecord& rhs) const {
            return window1window2window3start == rhs.window1window2window3start
                && window1window2window3end == rhs.window1window2window3end
                && window1window2window3key == rhs.window1window2window3key && window1window2start == rhs.window1window2start
                && window1window2end == rhs.window1window2end && window1window2key == rhs.window1window2key
                && window1win1 == rhs.window1win1 && window1id1 == rhs.window1id1 && window1timestamp == rhs.window1timestamp
                && window2win2 == rhs.window2win2 && window2id2 == rhs.window2id2 && window2timestamp == rhs.window2timestamp
                && window3win3 == rhs.window3win3 && window3id3 == rhs.window3id3 && window3timestamp == rhs.window3timestamp;
        }
    };

    const auto windowSchema = Schema::create()
                                  ->addField(createField("win1", BasicType::UINT64))
                                  ->addField(createField("id1", BasicType::UINT64))
                                  ->addField(createField("timestamp", BasicType::UINT64));
    const auto window2Schema = Schema::create()
                                   ->addField(createField("win2", BasicType::UINT64))
                                   ->addField(createField("id2", BasicType::UINT64))
                                   ->addField(createField("timestamp", BasicType::UINT64));
    const auto window3Schema = Schema::create()
                                   ->addField(createField("win3", BasicType::UINT64))
                                   ->addField(createField("id3", BasicType::UINT64))
                                   ->addField(createField("timestamp", BasicType::UINT64));
    TestUtils::JoinParams joinParams({windowSchema, window2Schema, window3Schema}, {"id1", "id2", "id3"});
    TestUtils::CsvFileParams csvFileParams({"window.csv", "window2.csv", "window4.csv"}, "");
    const std::vector<ResultRecord> expectedTuples = {
        {1000, 2000, 4, 1000, 2000, 4, 1, 4, 1002, 3, 4, 1112, 4, 4, 1001},
        {1000, 2000, 4, 1000, 2000, 4, 1, 4, 1002, 3, 4, 1102, 4, 4, 1001},
        {1000, 2000, 12, 1000, 2000, 12, 1, 12, 1001, 5, 12, 1011, 1, 12, 1300},
        {3000, 4000, 11, 3000, 4000, 11, 3, 11, 3001, 3, 11, 3001, 9, 11, 3000},
        {12000, 13000, 1, 12000, 13000, 1, 12, 1, 12000, 12, 1, 12000, 12, 1, 12000},
        {13000, 14000, 1, 13000, 14000, 1, 13, 1, 13000, 13, 1, 13000, 13, 1, 13000},
        {14000, 15000, 1, 14000, 15000, 1, 14, 1, 14000, 14, 1, 14000, 14, 1, 14000},
        {15000, 16000, 1, 15000, 16000, 1, 15, 1, 15000, 15, 1, 15000, 15, 1, 15000}
    };

    const auto query = Query::from("window1")
                           .joinWith(Query::from("window2"))
                               .where(Attribute("id1"))
                               .equalsTo(Attribute("id2"))
                               .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
                           .joinWith(Query::from("window3"))
                               .where(Attribute("id1"))
                               .equalsTo(Attribute("id3"))
                               .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)));
    runJoinQuery<ResultRecord>(query, csvFileParams, joinParams, expectedTuples);
}

TEST_P(MultipleJoinsTest, testJoin3WithDifferentSourceTumblingWindowOnCoodinatorSequential) {
    struct ResultRecord {
        uint64_t window1window2window3window4start;
        uint64_t window1window2window3window4end;
        uint64_t window1window2window3window4key;
        uint64_t window1window2window3start;
        uint64_t window1window2window3end;
        uint64_t window1window2window3key;
        uint64_t window1window2start;
        uint64_t window1window2end;
        uint64_t window1window2key;
        uint64_t window1win1;
        uint64_t window1id1;
        uint64_t window1timestamp;
        uint64_t window2win2;
        uint64_t window2id2;
        uint64_t window2timestamp;
        uint64_t window3win3;
        uint64_t window3id3;
        uint64_t window3timestamp;
        uint64_t window4win4;
        uint64_t window4id4;
        uint64_t window4timestamp;

        bool operator==(const ResultRecord& rhs) const {
            return window1window2window3window4start == rhs.window1window2window3window4start
                && window1window2window3window4end == rhs.window1window2window3window4end
                && window1window2window3window4key == rhs.window1window2window3window4key
                && window1window2window3start == rhs.window1window2window3start
                && window1window2window3end == rhs.window1window2window3end
                && window1window2window3key == rhs.window1window2window3key && window1window2start == rhs.window1window2start
                && window1window2end == rhs.window1window2end && window1window2key == rhs.window1window2key
                && window1win1 == rhs.window1win1 && window1id1 == rhs.window1id1 && window1timestamp == rhs.window1timestamp
                && window2win2 == rhs.window2win2 && window2id2 == rhs.window2id2 && window2timestamp == rhs.window2timestamp
                && window3win3 == rhs.window3win3 && window3id3 == rhs.window3id3 && window3timestamp == rhs.window3timestamp
                && window4win4 == rhs.window4win4 && window4id4 == rhs.window4id4 && window4timestamp == rhs.window4timestamp;
        }
    };

    const auto windowSchema = Schema::create()
                                  ->addField(createField("win1", BasicType::UINT64))
                                  ->addField(createField("id1", BasicType::UINT64))
                                  ->addField(createField("timestamp", BasicType::UINT64));
    const auto window2Schema = Schema::create()
                                   ->addField(createField("win2", BasicType::UINT64))
                                   ->addField(createField("id2", BasicType::UINT64))
                                   ->addField(createField("timestamp", BasicType::UINT64));
    const auto window3Schema = Schema::create()
                                   ->addField(createField("win3", BasicType::UINT64))
                                   ->addField(createField("id3", BasicType::UINT64))
                                   ->addField(createField("timestamp", BasicType::UINT64));
    const auto window4Schema = Schema::create()
                                   ->addField(createField("win4", BasicType::UINT64))
                                   ->addField(createField("id4", BasicType::UINT64))
                                   ->addField(createField("timestamp", BasicType::UINT64));
    TestUtils::JoinParams joinParams({windowSchema, window2Schema, window3Schema, window4Schema}, {"id1", "id2", "id3", "id4"});
    TestUtils::CsvFileParams csvFileParams({"window.csv", "window2.csv", "window4.csv", "window4.csv"});
    const std::vector<ResultRecord> expectedTuples = {
        {12000, 13000, 1, 12000, 13000, 1, 12000, 13000, 1, 12, 1, 12000, 12, 1, 12000, 12, 1, 12000, 12, 1, 12000},
        {13000, 14000, 1, 13000, 14000, 1, 13000, 14000, 1, 13, 1, 13000, 13, 1, 13000, 13, 1, 13000, 13, 1, 13000},
        {14000, 15000, 1, 14000, 15000, 1, 14000, 15000, 1, 14, 1, 14000, 14, 1, 14000, 14, 1, 14000, 14, 1, 14000},
        {15000, 16000, 1, 15000, 16000, 1, 15000, 16000, 1, 15, 1, 15000, 15, 1, 15000, 15, 1, 15000, 15, 1, 15000},
        {3000, 4000, 11, 3000, 4000, 11, 3000, 4000, 11, 3, 11, 3001, 3, 11, 3001, 9, 11, 3000, 9, 11, 3000},
        {1000, 2000, 4, 1000, 2000, 4, 1000, 2000, 4, 1, 4, 1002, 3, 4, 1102, 4, 4, 1001, 4, 4, 1001},
        {1000, 2000, 4, 1000, 2000, 4, 1000, 2000, 4, 1, 4, 1002, 3, 4, 1112, 4, 4, 1001, 4, 4, 1001},
        {1000, 2000, 12, 1000, 2000, 12, 1000, 2000, 12, 1, 12, 1001, 5, 12, 1011, 1, 12, 1300, 1, 12, 1300}
    };

    const auto query = Query::from("window1")
                           .joinWith(Query::from("window2"))
                               .where(Attribute("id1"))
                               .equalsTo(Attribute("id2"))
                               .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
                           .joinWith(Query::from("window3"))
                               .where(Attribute("id1"))
                               .equalsTo(Attribute("id3"))
                               .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
                           .joinWith(Query::from("window4"))
                               .where(Attribute("id1"))
                               .equalsTo(Attribute("id4"))
                               .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)));
    runJoinQuery<ResultRecord>(query, csvFileParams, joinParams, expectedTuples);
}

TEST_P(MultipleJoinsTest, testJoin3WithDifferentSourceTumblingWindowOnCoodinatorNested) {
    struct ResultRecord {
        uint64_t window1window2window3window4start;
        uint64_t window1window2window3window4end;
        uint64_t window1window2window3window4key;
        uint64_t window1window2window3start;
        uint64_t window1window2window3end;
        uint64_t window1window2window3key;
        uint64_t window1window2start;
        uint64_t window1window2end;
        uint64_t window1window2key;
        uint64_t window1win1;
        uint64_t window1id1;
        uint64_t window1timestamp;
        uint64_t window2win2;
        uint64_t window2id2;
        uint64_t window2timestamp;
        uint64_t window3win3;
        uint64_t window3id3;
        uint64_t window3timestamp;
        uint64_t window4win4;
        uint64_t window4id4;
        uint64_t window4timestamp;

        bool operator==(const ResultRecord& rhs) const {
            return window1window2window3window4start == rhs.window1window2window3window4start
                && window1window2window3window4end == rhs.window1window2window3window4end
                && window1window2window3window4key == rhs.window1window2window3window4key
                && window1window2window3start == rhs.window1window2window3start
                && window1window2window3end == rhs.window1window2window3end
                && window1window2window3key == rhs.window1window2window3key && window1window2start == rhs.window1window2start
                && window1window2end == rhs.window1window2end && window1window2key == rhs.window1window2key
                && window1win1 == rhs.window1win1 && window1id1 == rhs.window1id1 && window1timestamp == rhs.window1timestamp
                && window2win2 == rhs.window2win2 && window2id2 == rhs.window2id2 && window2timestamp == rhs.window2timestamp
                && window3win3 == rhs.window3win3 && window3id3 == rhs.window3id3 && window3timestamp == rhs.window3timestamp
                && window4win4 == rhs.window4win4 && window4id4 == rhs.window4id4 && window4timestamp == rhs.window4timestamp;
        }
    };

    const auto windowSchema = Schema::create()
                                  ->addField(createField("win1", BasicType::UINT64))
                                  ->addField(createField("id1", BasicType::UINT64))
                                  ->addField(createField("timestamp", BasicType::UINT64));
    const auto window2Schema = Schema::create()
                                   ->addField(createField("win2", BasicType::UINT64))
                                   ->addField(createField("id2", BasicType::UINT64))
                                   ->addField(createField("timestamp", BasicType::UINT64));
    const auto window3Schema = Schema::create()
                                   ->addField(createField("win3", BasicType::UINT64))
                                   ->addField(createField("id3", BasicType::UINT64))
                                   ->addField(createField("timestamp", BasicType::UINT64));
    const auto window4Schema = Schema::create()
                                   ->addField(createField("win4", BasicType::UINT64))
                                   ->addField(createField("id4", BasicType::UINT64))
                                   ->addField(createField("timestamp", BasicType::UINT64));
    TestUtils::JoinParams joinParams({windowSchema, window2Schema, window3Schema, window4Schema}, {"id1", "id2", "id3", "id4"});
    TestUtils::CsvFileParams csvFileParams({"window.csv", "window2.csv", "window4.csv", "window4.csv"});
    const std::vector<ResultRecord> expectedTuples = {
        {1000, 2000, 12, 1000, 2000, 12, 1, 12, 1001, 5, 12, 1011, 1000, 2000, 12, 1, 12, 1300, 1, 12, 1300},
        {1000, 2000, 4, 1000, 2000, 4, 1, 4, 1002, 3, 4, 1102, 1000, 2000, 4, 4, 4, 1001, 4, 4, 1001},
        {1000, 2000, 4, 1000, 2000, 4, 1, 4, 1002, 3, 4, 1112, 1000, 2000, 4, 4, 4, 1001, 4, 4, 1001},
        {3000, 4000, 11, 3000, 4000, 11, 3, 11, 3001, 3, 11, 3001, 3000, 4000, 11, 9, 11, 3000, 9, 11, 3000},
        {12000, 13000, 1, 12000, 13000, 1, 12, 1, 12000, 12, 1, 12000, 12000, 13000, 1, 12, 1, 12000, 12, 1, 12000},
        {13000, 14000, 1, 13000, 14000, 1, 13, 1, 13000, 13, 1, 13000, 13000, 14000, 1, 13, 1, 13000, 13, 1, 13000},
        {14000, 15000, 1, 14000, 15000, 1, 14, 1, 14000, 14, 1, 14000, 14000, 15000, 1, 14, 1, 14000, 14, 1, 14000},
        {15000, 16000, 1, 15000, 16000, 1, 15, 1, 15000, 15, 1, 15000, 15000, 16000, 1, 15, 1, 15000, 15, 1, 15000}
    };

    const auto query = Query::from("window1")
                           .joinWith(Query::from("window2"))
                           .where(Attribute("id1"))
                           .equalsTo(Attribute("id2"))
                           .window(TumblingWindow::of(EventTime(Attribute("timestamp")),Milliseconds(1000)))
                           .joinWith((Query::from("window3"))
                                         .joinWith(Query::from("window4"))
                                         .where(Attribute("id3"))
                                         .equalsTo(Attribute("id4"))
                                         .window(TumblingWindow::of(EventTime(Attribute("timestamp")),Milliseconds(1000))))
                           .where(Attribute("id1"))
                           .equalsTo(Attribute("id4"))
                           .window(TumblingWindow::of(EventTime(Attribute("timestamp")),Milliseconds(1000)));

    runJoinQuery<ResultRecord>(query, csvFileParams, joinParams, expectedTuples);
}

/**
 * Sliding window joins
 *
 */
TEST_P(MultipleJoinsTest, testJoins2WithDifferentSourceSlidingWindowOnCoodinator) {
    struct __attribute__((packed)) ResultRecord {
        uint64_t window1window2window3start;
        uint64_t window1window2window3end;
        uint64_t window1window2window3key;
        uint64_t window1window2start;
        uint64_t window1window2end;
        uint64_t window1window2key;
        uint64_t window1win1;
        uint64_t window1id1;
        uint64_t window1timestamp;
        uint64_t window2win2;
        uint64_t window2id2;
        uint64_t window2timestamp;
        uint64_t window3win3;
        uint64_t window3id3;
        uint64_t window3timestamp;

        bool operator==(const ResultRecord& rhs) const {
            return window1window2window3start == rhs.window1window2window3start
                && window1window2window3end == rhs.window1window2window3end
                && window1window2window3key == rhs.window1window2window3key && window1window2start == rhs.window1window2start
                && window1window2end == rhs.window1window2end && window1window2key == rhs.window1window2key
                && window1win1 == rhs.window1win1 && window1id1 == rhs.window1id1 && window1timestamp == rhs.window1timestamp
                && window2win2 == rhs.window2win2 && window2id2 == rhs.window2id2 && window2timestamp == rhs.window2timestamp
                && window3win3 == rhs.window3win3 && window3id3 == rhs.window3id3 && window3timestamp == rhs.window3timestamp;
        }
    };

    const auto windowSchema = Schema::create()
                                  ->addField(createField("win1", BasicType::UINT64))
                                  ->addField(createField("id1", BasicType::UINT64))
                                  ->addField(createField("timestamp", BasicType::UINT64));
    const auto window2Schema = Schema::create()
                                   ->addField(createField("win2", BasicType::UINT64))
                                   ->addField(createField("id2", BasicType::UINT64))
                                   ->addField(createField("timestamp", BasicType::UINT64));
    const auto window3Schema = Schema::create()
                                   ->addField(createField("win3", BasicType::UINT64))
                                   ->addField(createField("id3", BasicType::UINT64))
                                   ->addField(createField("timestamp", BasicType::UINT64));
    TestUtils::JoinParams joinParams({windowSchema, window2Schema, window3Schema}, {"id1", "id2", "id3"});
    TestUtils::CsvFileParams csvFileParams({"window.csv", "window2.csv", "window4.csv"}, "");
    const std::vector<ResultRecord> expectedTuples = {
        {500,1500,12,500,1500,12,1,12,1001,5,12,1011,1,12,1300},
        {500,1500,4,500,1500,4,1,4,1002,3,4,1102,4,4,1001},
        {500,1500,4,500,1500,4,1,4,1002,3,4,1112,4,4,1001},
        {500,1500,12,1000,2000,12,1,12,1001,5,12,1011,1,12,1300},
        {500,1500,4,1000,2000,4,1,4,1002,3,4,1102,4,4,1001},
        {500,1500,4,1000,2000,4,1,4,1002,3,4,1112,4,4,1001},
        {1000,2000,12,500,1500,12,1,12,1001,5,12,1011,1,12,1300},
        {1000,2000,4,500,1500,4,1,4,1002,3,4,1102,4,4,1001},
        {1000,2000,4,500,1500,4,1,4,1002,3,4,1112,4,4,1001},
        {1000,2000,12,1000,2000,12,1,12,1001,5,12,1011,1,12,1300},
        {1000,2000,4,1000,2000,4,1,4,1002,3,4,1102,4,4,1001},
        {1000,2000,4,1000,2000,4,1,4,1002,3,4,1112,4,4,1001},
        {2500,3500,11,2500,3500,11,3,11,3001,3,11,3001,9,11,3000},
        {2500,3500,11,3000,4000,11,3,11,3001,3,11,3001,9,11,3000},
        {3000,4000,11,2500,3500,11,3,11,3001,3,11,3001,9,11,3000},
        {3000,4000,11,3000,4000,11,3,11,3001,3,11,3001,9,11,3000},
        {11500,12500,1,11500,12500,1,12,1,12000,12,1,12000,12,1,12000},
        {11500,12500,1,12000,13000,1,12,1,12000,12,1,12000,12,1,12000},
        {12000,13000,1,11500,12500,1,12,1,12000,12,1,12000,12,1,12000},
        {12000,13000,1,12000,13000,1,12,1,12000,12,1,12000,12,1,12000},
        {12500,13500,1,12500,13500,1,13,1,13000,13,1,13000,13,1,13000},
        {12500,13500,1,13000,14000,1,13,1,13000,13,1,13000,13,1,13000},
        {13000,14000,1,12500,13500,1,13,1,13000,13,1,13000,13,1,13000},
        {13000,14000,1,13000,14000,1,13,1,13000,13,1,13000,13,1,13000},
        {13500,14500,1,13500,14500,1,14,1,14000,14,1,14000,14,1,14000},
        {13500,14500,1,14000,15000,1,14,1,14000,14,1,14000,14,1,14000},
        {14000,15000,1,13500,14500,1,14,1,14000,14,1,14000,14,1,14000},
        {14000,15000,1,14000,15000,1,14,1,14000,14,1,14000,14,1,14000},
        {14500,15500,1,14500,15500,1,15,1,15000,15,1,15000,15,1,15000},
        {14500,15500,1,15000,16000,1,15,1,15000,15,1,15000,15,1,15000},
        {15000,16000,1,14500,15500,1,15,1,15000,15,1,15000,15,1,15000},
        {15000,16000,1,15000,16000,1,15,1,15000,15,1,15000,15,1,15000}
    };

    const auto query = Query::from("window1")
                           .joinWith(Query::from("window2"))
                               .where(Attribute("id1"))
                               .equalsTo(Attribute("id2"))
                               .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(1), Milliseconds(500)))
                           .joinWith(Query::from("window3"))
                               .where(Attribute("id1"))
                               .equalsTo(Attribute("id3"))
                               .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(1), Milliseconds(500)));
    runJoinQuery<ResultRecord>(query, csvFileParams, joinParams, expectedTuples);
}

TEST_P(MultipleJoinsTest, testJoin3WithDifferentSourceSlidingWindowOnCoodinatorSequential) {
    struct ResultRecord {
        uint64_t window1window2window3window4start;
        uint64_t window1window2window3window4end;
        uint64_t window1window2window3window4key;
        uint64_t window1window2window3start;
        uint64_t window1window2window3end;
        uint64_t window1window2window3key;
        uint64_t window1window2start;
        uint64_t window1window2end;
        uint64_t window1window2key;
        uint64_t window1win1;
        uint64_t window1id1;
        uint64_t window1timestamp;
        uint64_t window2win2;
        uint64_t window2id2;
        uint64_t window2timestamp;
        uint64_t window3win3;
        uint64_t window3id3;
        uint64_t window3timestamp;
        uint64_t window4win4;
        uint64_t window4id4;
        uint64_t window4timestamp;

        bool operator==(const ResultRecord& rhs) const {
            return window1window2window3window4start == rhs.window1window2window3window4start
                && window1window2window3window4end == rhs.window1window2window3window4end
                && window1window2window3window4key == rhs.window1window2window3window4key
                && window1window2window3start == rhs.window1window2window3start
                && window1window2window3end == rhs.window1window2window3end
                && window1window2window3key == rhs.window1window2window3key && window1window2start == rhs.window1window2start
                && window1window2end == rhs.window1window2end && window1window2key == rhs.window1window2key
                && window1win1 == rhs.window1win1 && window1id1 == rhs.window1id1 && window1timestamp == rhs.window1timestamp
                && window2win2 == rhs.window2win2 && window2id2 == rhs.window2id2 && window2timestamp == rhs.window2timestamp
                && window3win3 == rhs.window3win3 && window3id3 == rhs.window3id3 && window3timestamp == rhs.window3timestamp
                && window4win4 == rhs.window4win4 && window4id4 == rhs.window4id4 && window4timestamp == rhs.window4timestamp;
        }
    };

    const auto windowSchema = Schema::create()
                                  ->addField(createField("win1", BasicType::UINT64))
                                  ->addField(createField("id1", BasicType::UINT64))
                                  ->addField(createField("timestamp", BasicType::UINT64));
    const auto window2Schema = Schema::create()
                                   ->addField(createField("win2", BasicType::UINT64))
                                   ->addField(createField("id2", BasicType::UINT64))
                                   ->addField(createField("timestamp", BasicType::UINT64));
    const auto window3Schema = Schema::create()
                                   ->addField(createField("win3", BasicType::UINT64))
                                   ->addField(createField("id3", BasicType::UINT64))
                                   ->addField(createField("timestamp", BasicType::UINT64));
    const auto window4Schema = Schema::create()
                                   ->addField(createField("win4", BasicType::UINT64))
                                   ->addField(createField("id4", BasicType::UINT64))
                                   ->addField(createField("timestamp", BasicType::UINT64));
    TestUtils::JoinParams joinParams({windowSchema, window2Schema, window3Schema, window4Schema}, {"id1", "id2", "id3", "id4"});
    TestUtils::CsvFileParams csvFileParams({"window.csv", "window2.csv", "window4.csv", "window4.csv"});
    const std::vector<ResultRecord> expectedTuples = {
        {500,1500,12,500,1500,12,500,1500,12,1,12,1001,5,12,1011,1,12,1300,1,12,1300},
        {500,1500,4,500,1500,4,500,1500,4,1,4,1002,3,4,1102,4,4,1001,4,4,1001},
        {500,1500,4,500,1500,4,500,1500,4,1,4,1002,3,4,1112,4,4,1001,4,4,1001},
        {500,1500,12,500,1500,12,1000,2000,12,1,12,1001,5,12,1011,1,12,1300,1,12,1300},
        {500,1500,4,500,1500,4,1000,2000,4,1,4,1002,3,4,1102,4,4,1001,4,4,1001},
        {500,1500,4,500,1500,4,1000,2000,4,1,4,1002,3,4,1112,4,4,1001,4,4,1001},
        {500,1500,12,1000,2000,12,500,1500,12,1,12,1001,5,12,1011,1,12,1300,1,12,1300},
        {500,1500,4,1000,2000,4,500,1500,4,1,4,1002,3,4,1102,4,4,1001,4,4,1001},
        {500,1500,4,1000,2000,4,500,1500,4,1,4,1002,3,4,1112,4,4,1001,4,4,1001},
        {500,1500,12,1000,2000,12,1000,2000,12,1,12,1001,5,12,1011,1,12,1300,1,12,1300},
        {500,1500,4,1000,2000,4,1000,2000,4,1,4,1002,3,4,1102,4,4,1001,4,4,1001},
        {500,1500,4,1000,2000,4,1000,2000,4,1,4,1002,3,4,1112,4,4,1001,4,4,1001},
        {1000,2000,12,500,1500,12,500,1500,12,1,12,1001,5,12,1011,1,12,1300,1,12,1300},
        {1000,2000,4,500,1500,4,500,1500,4,1,4,1002,3,4,1102,4,4,1001,4,4,1001},
        {1000,2000,4,500,1500,4,500,1500,4,1,4,1002,3,4,1112,4,4,1001,4,4,1001},
        {1000,2000,12,500,1500,12,1000,2000,12,1,12,1001,5,12,1011,1,12,1300,1,12,1300},
        {1000,2000,4,500,1500,4,1000,2000,4,1,4,1002,3,4,1102,4,4,1001,4,4,1001},
        {1000,2000,4,500,1500,4,1000,2000,4,1,4,1002,3,4,1112,4,4,1001,4,4,1001},
        {1000,2000,12,1000,2000,12,500,1500,12,1,12,1001,5,12,1011,1,12,1300,1,12,1300},
        {1000,2000,4,1000,2000,4,500,1500,4,1,4,1002,3,4,1102,4,4,1001,4,4,1001},
        {1000,2000,4,1000,2000,4,500,1500,4,1,4,1002,3,4,1112,4,4,1001,4,4,1001},
        {1000,2000,12,1000,2000,12,1000,2000,12,1,12,1001,5,12,1011,1,12,1300,1,12,1300},
        {1000,2000,4,1000,2000,4,1000,2000,4,1,4,1002,3,4,1102,4,4,1001,4,4,1001},
        {1000,2000,4,1000,2000,4,1000,2000,4,1,4,1002,3,4,1112,4,4,1001,4,4,1001},
        {2500,3500,11,2500,3500,11,2500,3500,11,3,11,3001,3,11,3001,9,11,3000,9,11,3000},
        {2500,3500,11,2500,3500,11,3000,4000,11,3,11,3001,3,11,3001,9,11,3000,9,11,3000},
        {2500,3500,11,3000,4000,11,2500,3500,11,3,11,3001,3,11,3001,9,11,3000,9,11,3000},
        {2500,3500,11,3000,4000,11,3000,4000,11,3,11,3001,3,11,3001,9,11,3000,9,11,3000},
        {3000,4000,11,2500,3500,11,2500,3500,11,3,11,3001,3,11,3001,9,11,3000,9,11,3000},
        {3000,4000,11,2500,3500,11,3000,4000,11,3,11,3001,3,11,3001,9,11,3000,9,11,3000},
        {3000,4000,11,3000,4000,11,2500,3500,11,3,11,3001,3,11,3001,9,11,3000,9,11,3000},
        {3000,4000,11,3000,4000,11,3000,4000,11,3,11,3001,3,11,3001,9,11,3000,9,11,3000},
        {11500,12500,1,11500,12500,1,11500,12500,1,12,1,12000,12,1,12000,12,1,12000,12,1,12000},
        {11500,12500,1,11500,12500,1,12000,13000,1,12,1,12000,12,1,12000,12,1,12000,12,1,12000},
        {11500,12500,1,12000,13000,1,11500,12500,1,12,1,12000,12,1,12000,12,1,12000,12,1,12000},
        {11500,12500,1,12000,13000,1,12000,13000,1,12,1,12000,12,1,12000,12,1,12000,12,1,12000},
        {12000,13000,1,11500,12500,1,11500,12500,1,12,1,12000,12,1,12000,12,1,12000,12,1,12000},
        {12000,13000,1,11500,12500,1,12000,13000,1,12,1,12000,12,1,12000,12,1,12000,12,1,12000},
        {12000,13000,1,12000,13000,1,11500,12500,1,12,1,12000,12,1,12000,12,1,12000,12,1,12000},
        {12000,13000,1,12000,13000,1,12000,13000,1,12,1,12000,12,1,12000,12,1,12000,12,1,12000},
        {12500,13500,1,12500,13500,1,12500,13500,1,13,1,13000,13,1,13000,13,1,13000,13,1,13000},
        {12500,13500,1,12500,13500,1,13000,14000,1,13,1,13000,13,1,13000,13,1,13000,13,1,13000},
        {12500,13500,1,13000,14000,1,12500,13500,1,13,1,13000,13,1,13000,13,1,13000,13,1,13000},
        {12500,13500,1,13000,14000,1,13000,14000,1,13,1,13000,13,1,13000,13,1,13000,13,1,13000},
        {13000,14000,1,12500,13500,1,12500,13500,1,13,1,13000,13,1,13000,13,1,13000,13,1,13000},
        {13000,14000,1,12500,13500,1,13000,14000,1,13,1,13000,13,1,13000,13,1,13000,13,1,13000},
        {13000,14000,1,13000,14000,1,12500,13500,1,13,1,13000,13,1,13000,13,1,13000,13,1,13000},
        {13000,14000,1,13000,14000,1,13000,14000,1,13,1,13000,13,1,13000,13,1,13000,13,1,13000},
        {13500,14500,1,13500,14500,1,13500,14500,1,14,1,14000,14,1,14000,14,1,14000,14,1,14000},
        {13500,14500,1,13500,14500,1,14000,15000,1,14,1,14000,14,1,14000,14,1,14000,14,1,14000},
        {13500,14500,1,14000,15000,1,13500,14500,1,14,1,14000,14,1,14000,14,1,14000,14,1,14000},
        {13500,14500,1,14000,15000,1,14000,15000,1,14,1,14000,14,1,14000,14,1,14000,14,1,14000},
        {14000,15000,1,13500,14500,1,13500,14500,1,14,1,14000,14,1,14000,14,1,14000,14,1,14000},
        {14000,15000,1,13500,14500,1,14000,15000,1,14,1,14000,14,1,14000,14,1,14000,14,1,14000},
        {14000,15000,1,14000,15000,1,13500,14500,1,14,1,14000,14,1,14000,14,1,14000,14,1,14000},
        {14000,15000,1,14000,15000,1,14000,15000,1,14,1,14000,14,1,14000,14,1,14000,14,1,14000},
        {14500,15500,1,14500,15500,1,14500,15500,1,15,1,15000,15,1,15000,15,1,15000,15,1,15000},
        {14500,15500,1,14500,15500,1,15000,16000,1,15,1,15000,15,1,15000,15,1,15000,15,1,15000},
        {14500,15500,1,15000,16000,1,14500,15500,1,15,1,15000,15,1,15000,15,1,15000,15,1,15000},
        {14500,15500,1,15000,16000,1,15000,16000,1,15,1,15000,15,1,15000,15,1,15000,15,1,15000},
        {15000,16000,1,14500,15500,1,14500,15500,1,15,1,15000,15,1,15000,15,1,15000,15,1,15000},
        {15000,16000,1,14500,15500,1,15000,16000,1,15,1,15000,15,1,15000,15,1,15000,15,1,15000},
        {15000,16000,1,15000,16000,1,14500,15500,1,15,1,15000,15,1,15000,15,1,15000,15,1,15000},
        {15000,16000,1,15000,16000,1,15000,16000,1,15,1,15000,15,1,15000,15,1,15000,15,1,15000}
    };

    const auto query = Query::from("window1")
                           .joinWith(Query::from("window2"))
                               .where(Attribute("id1"))
                               .equalsTo(Attribute("id2"))
                               .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(1), Milliseconds(500)))
                           .joinWith(Query::from("window3"))
                               .where(Attribute("id1"))
                               .equalsTo(Attribute("id3"))
                               .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(1), Milliseconds(500)))
                           .joinWith(Query::from("window4"))
                               .where(Attribute("id1"))
                               .equalsTo(Attribute("id4"))
                               .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(1), Milliseconds(500)));
    runJoinQuery<ResultRecord>(query, csvFileParams, joinParams, expectedTuples);
}

TEST_P(MultipleJoinsTest, testJoin3WithDifferentSourceSlidingWindowOnCoodinatorNested) {
    struct ResultRecord {
        uint64_t window1window2window3window4start;
        uint64_t window1window2window3window4end;
        uint64_t window1window2window3window4key;
        uint64_t window1window2window3start;
        uint64_t window1window2window3end;
        uint64_t window1window2window3key;
        uint64_t window1window2start;
        uint64_t window1window2end;
        uint64_t window1window2key;
        uint64_t window1win1;
        uint64_t window1id1;
        uint64_t window1timestamp;
        uint64_t window2win2;
        uint64_t window2id2;
        uint64_t window2timestamp;
        uint64_t window3win3;
        uint64_t window3id3;
        uint64_t window3timestamp;
        uint64_t window4win4;
        uint64_t window4id4;
        uint64_t window4timestamp;

        bool operator==(const ResultRecord& rhs) const {
            return window1window2window3window4start == rhs.window1window2window3window4start
                && window1window2window3window4end == rhs.window1window2window3window4end
                && window1window2window3window4key == rhs.window1window2window3window4key
                && window1window2window3start == rhs.window1window2window3start
                && window1window2window3end == rhs.window1window2window3end
                && window1window2window3key == rhs.window1window2window3key && window1window2start == rhs.window1window2start
                && window1window2end == rhs.window1window2end && window1window2key == rhs.window1window2key
                && window1win1 == rhs.window1win1 && window1id1 == rhs.window1id1 && window1timestamp == rhs.window1timestamp
                && window2win2 == rhs.window2win2 && window2id2 == rhs.window2id2 && window2timestamp == rhs.window2timestamp
                && window3win3 == rhs.window3win3 && window3id3 == rhs.window3id3 && window3timestamp == rhs.window3timestamp
                && window4win4 == rhs.window4win4 && window4id4 == rhs.window4id4 && window4timestamp == rhs.window4timestamp;
        }
    };

    const auto windowSchema = Schema::create()
                                  ->addField(createField("win1", BasicType::UINT64))
                                  ->addField(createField("id1", BasicType::UINT64))
                                  ->addField(createField("timestamp", BasicType::UINT64));
    const auto window2Schema = Schema::create()
                                   ->addField(createField("win2", BasicType::UINT64))
                                   ->addField(createField("id2", BasicType::UINT64))
                                   ->addField(createField("timestamp", BasicType::UINT64));
    const auto window3Schema = Schema::create()
                                   ->addField(createField("win3", BasicType::UINT64))
                                   ->addField(createField("id3", BasicType::UINT64))
                                   ->addField(createField("timestamp", BasicType::UINT64));
    const auto window4Schema = Schema::create()
                                   ->addField(createField("win4", BasicType::UINT64))
                                   ->addField(createField("id4", BasicType::UINT64))
                                   ->addField(createField("timestamp", BasicType::UINT64));
    TestUtils::JoinParams joinParams({windowSchema, window2Schema, window3Schema, window4Schema}, {"id1", "id2", "id3", "id4"});
    TestUtils::CsvFileParams csvFileParams({"window.csv", "window2.csv", "window4.csv", "window4.csv"});
    const std::vector<ResultRecord> expectedTuples = {
        {500,1500,12,500,1500,12,1,12,1001,5,12,1011,500,1500,12,1,12,1300,1,12,1300},
        {500,1500,12,500,1500,12,1,12,1001,5,12,1011,1000,2000,12,1,12,1300,1,12,1300},
        {500,1500,4,500,1500,4,1,4,1002,3,4,1102,500,1500,4,4,4,1001,4,4,1001},
        {500,1500,4,500,1500,4,1,4,1002,3,4,1102,1000,2000,4,4,4,1001,4,4,1001},
        {500,1500,4,500,1500,4,1,4,1002,3,4,1112,500,1500,4,4,4,1001,4,4,1001},
        {500,1500,4,500,1500,4,1,4,1002,3,4,1112,1000,2000,4,4,4,1001,4,4,1001},
        {500,1500,12,1000,2000,12,1,12,1001,5,12,1011,500,1500,12,1,12,1300,1,12,1300},
        {500,1500,12,1000,2000,12,1,12,1001,5,12,1011,1000,2000,12,1,12,1300,1,12,1300},
        {500,1500,4,1000,2000,4,1,4,1002,3,4,1102,500,1500,4,4,4,1001,4,4,1001},
        {500,1500,4,1000,2000,4,1,4,1002,3,4,1102,1000,2000,4,4,4,1001,4,4,1001},
        {500,1500,4,1000,2000,4,1,4,1002,3,4,1112,500,1500,4,4,4,1001,4,4,1001},
        {500,1500,4,1000,2000,4,1,4,1002,3,4,1112,1000,2000,4,4,4,1001,4,4,1001},
        {1000,2000,12,500,1500,12,1,12,1001,5,12,1011,500,1500,12,1,12,1300,1,12,1300},
        {1000,2000,12,500,1500,12,1,12,1001,5,12,1011,1000,2000,12,1,12,1300,1,12,1300},
        {1000,2000,4,500,1500,4,1,4,1002,3,4,1102,500,1500,4,4,4,1001,4,4,1001},
        {1000,2000,4,500,1500,4,1,4,1002,3,4,1102,1000,2000,4,4,4,1001,4,4,1001},
        {1000,2000,4,500,1500,4,1,4,1002,3,4,1112,500,1500,4,4,4,1001,4,4,1001},
        {1000,2000,4,500,1500,4,1,4,1002,3,4,1112,1000,2000,4,4,4,1001,4,4,1001},
        {1000,2000,12,1000,2000,12,1,12,1001,5,12,1011,500,1500,12,1,12,1300,1,12,1300},
        {1000,2000,12,1000,2000,12,1,12,1001,5,12,1011,1000,2000,12,1,12,1300,1,12,1300},
        {1000,2000,4,1000,2000,4,1,4,1002,3,4,1102,500,1500,4,4,4,1001,4,4,1001},
        {1000,2000,4,1000,2000,4,1,4,1002,3,4,1102,1000,2000,4,4,4,1001,4,4,1001},
        {1000,2000,4,1000,2000,4,1,4,1002,3,4,1112,500,1500,4,4,4,1001,4,4,1001},
        {1000,2000,4,1000,2000,4,1,4,1002,3,4,1112,1000,2000,4,4,4,1001,4,4,1001},
        {2500,3500,11,2500,3500,11,3,11,3001,3,11,3001,2500,3500,11,9,11,3000,9,11,3000},
        {2500,3500,11,2500,3500,11,3,11,3001,3,11,3001,3000,4000,11,9,11,3000,9,11,3000},
        {2500,3500,11,3000,4000,11,3,11,3001,3,11,3001,2500,3500,11,9,11,3000,9,11,3000},
        {2500,3500,11,3000,4000,11,3,11,3001,3,11,3001,3000,4000,11,9,11,3000,9,11,3000},
        {3000,4000,11,2500,3500,11,3,11,3001,3,11,3001,2500,3500,11,9,11,3000,9,11,3000},
        {3000,4000,11,2500,3500,11,3,11,3001,3,11,3001,3000,4000,11,9,11,3000,9,11,3000},
        {3000,4000,11,3000,4000,11,3,11,3001,3,11,3001,2500,3500,11,9,11,3000,9,11,3000},
        {3000,4000,11,3000,4000,11,3,11,3001,3,11,3001,3000,4000,11,9,11,3000,9,11,3000},
        {11500,12500,1,11500,12500,1,12,1,12000,12,1,12000,11500,12500,1,12,1,12000,12,1,12000},
        {11500,12500,1,11500,12500,1,12,1,12000,12,1,12000,12000,13000,1,12,1,12000,12,1,12000},
        {11500,12500,1,12000,13000,1,12,1,12000,12,1,12000,11500,12500,1,12,1,12000,12,1,12000},
        {11500,12500,1,12000,13000,1,12,1,12000,12,1,12000,12000,13000,1,12,1,12000,12,1,12000},
        {12000,13000,1,11500,12500,1,12,1,12000,12,1,12000,11500,12500,1,12,1,12000,12,1,12000},
        {12000,13000,1,11500,12500,1,12,1,12000,12,1,12000,12000,13000,1,12,1,12000,12,1,12000},
        {12000,13000,1,12000,13000,1,12,1,12000,12,1,12000,11500,12500,1,12,1,12000,12,1,12000},
        {12000,13000,1,12000,13000,1,12,1,12000,12,1,12000,12000,13000,1,12,1,12000,12,1,12000},
        {12500,13500,1,12500,13500,1,13,1,13000,13,1,13000,12500,13500,1,13,1,13000,13,1,13000},
        {12500,13500,1,12500,13500,1,13,1,13000,13,1,13000,13000,14000,1,13,1,13000,13,1,13000},
        {12500,13500,1,13000,14000,1,13,1,13000,13,1,13000,12500,13500,1,13,1,13000,13,1,13000},
        {12500,13500,1,13000,14000,1,13,1,13000,13,1,13000,13000,14000,1,13,1,13000,13,1,13000},
        {13000,14000,1,12500,13500,1,13,1,13000,13,1,13000,12500,13500,1,13,1,13000,13,1,13000},
        {13000,14000,1,12500,13500,1,13,1,13000,13,1,13000,13000,14000,1,13,1,13000,13,1,13000},
        {13000,14000,1,13000,14000,1,13,1,13000,13,1,13000,12500,13500,1,13,1,13000,13,1,13000},
        {13000,14000,1,13000,14000,1,13,1,13000,13,1,13000,13000,14000,1,13,1,13000,13,1,13000},
        {13500,14500,1,13500,14500,1,14,1,14000,14,1,14000,13500,14500,1,14,1,14000,14,1,14000},
        {13500,14500,1,13500,14500,1,14,1,14000,14,1,14000,14000,15000,1,14,1,14000,14,1,14000},
        {13500,14500,1,14000,15000,1,14,1,14000,14,1,14000,13500,14500,1,14,1,14000,14,1,14000},
        {13500,14500,1,14000,15000,1,14,1,14000,14,1,14000,14000,15000,1,14,1,14000,14,1,14000},
        {14000,15000,1,13500,14500,1,14,1,14000,14,1,14000,13500,14500,1,14,1,14000,14,1,14000},
        {14000,15000,1,13500,14500,1,14,1,14000,14,1,14000,14000,15000,1,14,1,14000,14,1,14000},
        {14000,15000,1,14000,15000,1,14,1,14000,14,1,14000,13500,14500,1,14,1,14000,14,1,14000},
        {14000,15000,1,14000,15000,1,14,1,14000,14,1,14000,14000,15000,1,14,1,14000,14,1,14000},
        {14500,15500,1,14500,15500,1,15,1,15000,15,1,15000,14500,15500,1,15,1,15000,15,1,15000},
        {14500,15500,1,14500,15500,1,15,1,15000,15,1,15000,15000,16000,1,15,1,15000,15,1,15000},
        {14500,15500,1,15000,16000,1,15,1,15000,15,1,15000,14500,15500,1,15,1,15000,15,1,15000},
        {14500,15500,1,15000,16000,1,15,1,15000,15,1,15000,15000,16000,1,15,1,15000,15,1,15000},
        {15000,16000,1,14500,15500,1,15,1,15000,15,1,15000,14500,15500,1,15,1,15000,15,1,15000},
        {15000,16000,1,14500,15500,1,15,1,15000,15,1,15000,15000,16000,1,15,1,15000,15,1,15000},
        {15000,16000,1,15000,16000,1,15,1,15000,15,1,15000,14500,15500,1,15,1,15000,15,1,15000},
        {15000,16000,1,15000,16000,1,15,1,15000,15,1,15000,15000,16000,1,15,1,15000,15,1,15000}
    };

    const auto query = Query::from("window1")
                           .joinWith(Query::from("window2"))
                               .where(Attribute("id1"))
                               .equalsTo(Attribute("id2"))
                               .window(SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500)))
                           .joinWith((Query::from("window3"))
                                         .joinWith(Query::from("window4"))
                                         .where(Attribute("id3"))
                                         .equalsTo(Attribute("id4"))
                                         .window(SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500))))
                               .where(Attribute("id1"))
                               .equalsTo(Attribute("id4"))
                               .window(SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500)));
    runJoinQuery<ResultRecord>(query, csvFileParams, joinParams, expectedTuples);
}

INSTANTIATE_TEST_CASE_P(
    testJoinQueries,
    MultipleJoinsTest,
    JOIN_STRATEGIES_WINDOW_STRATEGIES,
    [](const testing::TestParamInfo<MultipleJoinsTest::ParamType>& info) {
        return std::string(magic_enum::enum_name(std::get<0>(info.param))) + "_" +
            std::string(magic_enum::enum_name(std::get<1>(info.param)));
    });
}// namespace NES::Runtime::Execution
