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
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Statistics/Synopses/CountMinStatistic.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Statistics/Synopses/HyperLogLogStatistic.hpp>
#include <Runtime/NesThread.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Sinks/SinkCreator.hpp>
#include <StatisticIdentifiers.hpp>
#include <Util/TestUtils.hpp>
#include <gmock/gmock-matchers.h>

namespace NES {

class StatisticSinkTest : public Testing::BaseIntegrationTest, public ::testing::WithParamInterface<int> {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("StatisticSinkTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup StatisticSinkTest class.");
    }

    /* Called before a single test. */
    void SetUp() override {
        Testing::BaseIntegrationTest::SetUp();
        auto workerConfiguration = WorkerConfiguration::create();
        this->nodeEngine = Runtime::NodeEngineBuilder::create(workerConfiguration)
                               .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                               .build();
    }

    /* Called after a single test. */
    void TearDown() override {
        ASSERT_TRUE(nodeEngine->stop());
        Testing::BaseIntegrationTest::TearDown();
    }

    /**
     * @brief Checks if the left and right vector contain the same (order-insensitive) statistics
     * @param left
     * @param right
     * @return True, if vector contain the same, false otherwise
     */
    bool sameStatisticsInVectors(std::vector<Statistic::HashStatisticPair>& left, std::vector<Statistic::StatisticPtr>& right) {
        if (left.size() != right.size()) {
            NES_ERROR("Vectors are not equal with {} and {} items!", left.size(), right.size());
            return false;
        }

        // We iterate over the left and search for the same item in the right and then remove it from right vector
        for (const auto& elem : left) {
            auto it = std::find_if(right.begin(), right.end(), [&](const Statistic::StatisticPtr& statistic) {
                return statistic->equal(*elem.second);
            });

            if (it == right.end()) {
                NES_ERROR("Can not find statistic {} in right vector!", elem.second->toString());
                return false;
            }

            // Remove the found element from the right vector
            right.erase(it);
        }

        return true;
    }

    /**
     * @brief Creates #numberOfSketches random CountMin-Sketches
     * @param numberOfSketches
     * @param schema
     * @param bufferManager
     * @return Pair<Vector of TupleBuffers, Vector of Statistics>
     */
    static std::pair<std::vector<Runtime::TupleBuffer>, std::vector<Statistic::StatisticPtr>>
    createRandomCountMinSketches(uint64_t numberOfSketches,
                                 const SchemaPtr& schema,
                                 const Runtime::BufferManagerPtr& bufferManager) {
        std::vector<Runtime::TupleBuffer> createdBuffers;
        std::vector<Statistic::StatisticPtr> expectedStatistics;
        constexpr auto windowSize = 10;

        auto buffer = bufferManager->getBufferBlocking();
        uint64_t curBufTuplePos = 0;
        const auto capacity = bufferManager->getBufferSize() / schema->getSchemaSizeInBytes();

        for (auto curCnt = 0_u64; curCnt < numberOfSketches; ++curCnt) {
            if (curBufTuplePos >= capacity) {
                buffer.setNumberOfTuples(curBufTuplePos);
                createdBuffers.emplace_back(buffer);
                buffer = bufferManager->getBufferBlocking();
                curBufTuplePos = 0;
            }

            // Creating "random" values that represent of a CountMinStatistic for a tumbling window with a size of 10
            const Windowing::TimeMeasure startTs(windowSize * curCnt);
            const Windowing::TimeMeasure endTs(windowSize * (curCnt + 1));
            const uint64_t statisticType = 42;
            const uint64_t statisticHash = curCnt;
            const uint64_t width = rand() % 10 + 5;
            const uint64_t depth = rand() % 5 + 1;
            const uint64_t numberOfBitsInKey = 64;

            // We simulate a filling of count min by randomly updating the sketch
            const auto numberOfTuplesToInsert = rand() % 100 + 100;
            auto countMin = Statistic::CountMinStatistic::createInit(startTs, endTs, width, depth, numberOfBitsInKey)
                                ->as<Statistic::CountMinStatistic>();
            for (auto i = 0; i < numberOfTuplesToInsert; ++i) {
                for (auto row = 0_u64; row < depth; ++row) {
                    auto rndCol = rand() % width;
                    countMin->update(row, rndCol);
                }
            }

            // Now using the values for writing a tuple to the tuple buffer
            auto testBuffer = Runtime::MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(buffer, schema);
            testBuffer[curBufTuplePos]["test$" + Statistic::STATISTIC_HASH_FIELD_NAME].write(statisticHash);
            testBuffer[curBufTuplePos]["test$" + Statistic::BASE_FIELD_NAME_START].write(startTs.getTime());
            testBuffer[curBufTuplePos]["test$" + Statistic::BASE_FIELD_NAME_END].write(endTs.getTime());
            testBuffer[curBufTuplePos]["test$" + Statistic::STATISTIC_TYPE_FIELD_NAME].write(statisticType);
            testBuffer[curBufTuplePos]["test$" + Statistic::OBSERVED_TUPLES_FIELD_NAME].write(countMin->getObservedTuples());
            testBuffer[curBufTuplePos]["test$" + Statistic::WIDTH_FIELD_NAME].write(width);
            testBuffer[curBufTuplePos]["test$" + Statistic::DEPTH_FIELD_NAME].write(depth);
            testBuffer[curBufTuplePos]["test$" + Statistic::NUMBER_OF_BITS_IN_KEY].write(numberOfBitsInKey);
            testBuffer[curBufTuplePos].writeVarSized("test$" + Statistic::STATISTIC_DATA_FIELD_NAME,
                                                     countMin->getCountMinDataAsString(),
                                                     bufferManager.get());
            curBufTuplePos += 1;

            // Creating now the expected CountMinStatistic
            expectedStatistics.emplace_back(countMin);
        }

        // If the current tuple buffer has tuples, but we have not emplaced it
        if (curBufTuplePos > 0) {
            buffer.setNumberOfTuples(curBufTuplePos);
            createdBuffers.emplace_back(buffer);
        }

        return {createdBuffers, expectedStatistics};
    }

    /**
     * @brief Creates #numberOfSketches random HyperLogLog-Sketches
     * @param numberOfSketches
     * @param schema
     * @param bufferManager
     * @return Pair<Vector of TupleBuffers, Vector of Statistics>
     */
    static std::pair<std::vector<Runtime::TupleBuffer>, std::vector<Statistic::StatisticPtr>>
    createRandomHyperLogLogSketches(uint64_t numberOfSketches,
                                    const SchemaPtr& schema,
                                    const Runtime::BufferManagerPtr& bufferManager) {
        std::vector<Runtime::TupleBuffer> createdBuffers;
        std::vector<Statistic::StatisticPtr> expectedStatistics;
        constexpr auto windowSize = 10;

        auto buffer = bufferManager->getBufferBlocking();
        uint64_t curBufTuplePos = 0;
        const auto capacity = bufferManager->getBufferSize() / schema->getSchemaSizeInBytes();

        for (auto curCnt = 0_u64; curCnt < numberOfSketches; ++curCnt) {
            if (curBufTuplePos >= capacity) {
                buffer.setNumberOfTuples(curBufTuplePos);
                createdBuffers.emplace_back(buffer);
                buffer = bufferManager->getBufferBlocking();
                curBufTuplePos = 0;
            }

            // Creating "random" values that represent of a HyperLogLogStatistics for a tumbling window with a size of 10
            const Windowing::TimeMeasure startTs(windowSize * curCnt);
            const Windowing::TimeMeasure endTs(windowSize * (curCnt + 1));
            const uint64_t statisticType = 42;
            const uint64_t statisticHash = curCnt;
            const uint64_t width = rand() % 10 + 5;

            // We simulate a filling of count min by randomly updating the sketch
            const auto numberOfTuplesToInsert = rand() % 100 + 100;
            auto hyperLogLog =
                Statistic::HyperLogLogStatistic::createInit(startTs, endTs, width)->as<Statistic::HyperLogLogStatistic>();
            for (auto i = 0; i < numberOfTuplesToInsert; ++i) {
                auto hash = rand();
                hyperLogLog->update(hash);
            }
            const auto estimate = hyperLogLog->getEstimate();

            // Now using the values for writing a tuple to the tuple buffer
            auto testBuffer = Runtime::MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(buffer, schema);
            testBuffer[curBufTuplePos]["test$" + Statistic::STATISTIC_HASH_FIELD_NAME].write(statisticHash);
            testBuffer[curBufTuplePos]["test$" + Statistic::BASE_FIELD_NAME_START].write(startTs.getTime());
            testBuffer[curBufTuplePos]["test$" + Statistic::BASE_FIELD_NAME_END].write(endTs.getTime());
            testBuffer[curBufTuplePos]["test$" + Statistic::STATISTIC_TYPE_FIELD_NAME].write(statisticType);
            testBuffer[curBufTuplePos]["test$" + Statistic::OBSERVED_TUPLES_FIELD_NAME].write(hyperLogLog->getObservedTuples());
            testBuffer[curBufTuplePos]["test$" + Statistic::WIDTH_FIELD_NAME].write(width);
            testBuffer[curBufTuplePos]["test$" + Statistic::ESTIMATE_FIELD_NAME].write(estimate);
            testBuffer[curBufTuplePos].writeVarSized("test$" + Statistic::STATISTIC_DATA_FIELD_NAME,
                                                     hyperLogLog->getHyperLogLogDataAsString(),
                                                     bufferManager.get());
            curBufTuplePos += 1;

            // Creating now the expected HyperLogLogStatistics
            expectedStatistics.emplace_back(hyperLogLog);
        }

        // If the current tuple buffer has tuples, but we have not emplaced it
        if (curBufTuplePos > 0) {
            buffer.setNumberOfTuples(curBufTuplePos);
            createdBuffers.emplace_back(buffer);
        }

        return {createdBuffers, expectedStatistics};
    }

    Runtime::NodeEnginePtr nodeEngine{nullptr};
};

/**
 * @brief Tests if our statistic sink can put CountMin-Sketches into the StatisticStore
 */
TEST_P(StatisticSinkTest, testCountMin) {
    const auto numberOfStatistics = StatisticSinkTest::GetParam();
    auto countMinStatisticSchema = Schema::create()
                                       ->addField(Statistic::WIDTH_FIELD_NAME, BasicType::UINT64)
                                       ->addField(Statistic::DEPTH_FIELD_NAME, BasicType::UINT64)
                                       ->addField(Statistic::NUMBER_OF_BITS_IN_KEY, BasicType::UINT64)
                                       ->addField(Statistic::STATISTIC_DATA_FIELD_NAME, BasicType::TEXT)
                                       ->addField(Statistic::BASE_FIELD_NAME_START, BasicType::UINT64)
                                       ->addField(Statistic::BASE_FIELD_NAME_END, BasicType::UINT64)
                                       ->addField(Statistic::STATISTIC_HASH_FIELD_NAME, BasicType::UINT64)
                                       ->addField(Statistic::STATISTIC_TYPE_FIELD_NAME, BasicType::UINT64)
                                       ->addField(Statistic::OBSERVED_TUPLES_FIELD_NAME, BasicType::UINT64)
                                       ->updateSourceName("test");

    // Creating the number of buffers
    auto [buffers, expectedStatistics] =
        createRandomCountMinSketches(numberOfStatistics, countMinStatisticSchema, nodeEngine->getBufferManager());
    auto statisticSink = createStatisticSink(countMinStatisticSchema,
                                             nodeEngine,
                                             1,                       // numOfProducers
                                             SharedQueryId(1),        // queryId
                                             DecomposedQueryPlanId(1),// querySubPlanId
                                             1,                       // numberOfOrigins
                                             Statistic::StatisticSinkFormatType::COUNT_MIN);
    Runtime::WorkerContext wctx(Runtime::NesThread::getId(), nodeEngine->getBufferManager(), 64);

    for (auto& buf : buffers) {
        EXPECT_TRUE(statisticSink->writeData(buf, wctx));
    }

    auto storedStatistics = nodeEngine->getStatisticStore()->getAllStatistics();
    EXPECT_TRUE(sameStatisticsInVectors(storedStatistics, expectedStatistics));
}

/**
 * @brief Tests if our statistic sink can put HyperLogLog-Sketches into the StatisticStore
 */
TEST_P(StatisticSinkTest, testHyperLogLog) {
    const auto numberOfStatistics = StatisticSinkTest::GetParam();
    auto hyperLogLogStatisticSchema = Schema::create()
                                          ->addField(Statistic::WIDTH_FIELD_NAME, BasicType::UINT64)
                                          ->addField(Statistic::STATISTIC_DATA_FIELD_NAME, BasicType::TEXT)
                                          ->addField(Statistic::BASE_FIELD_NAME_START, BasicType::UINT64)
                                          ->addField(Statistic::BASE_FIELD_NAME_END, BasicType::UINT64)
                                          ->addField(Statistic::STATISTIC_HASH_FIELD_NAME, BasicType::UINT64)
                                          ->addField(Statistic::STATISTIC_TYPE_FIELD_NAME, BasicType::UINT64)
                                          ->addField(Statistic::ESTIMATE_FIELD_NAME, BasicType::FLOAT64)
                                          ->addField(Statistic::OBSERVED_TUPLES_FIELD_NAME, BasicType::UINT64)
                                          ->updateSourceName("test");

    // Creating the number of buffers
    auto [buffers, expectedStatistics] =
        createRandomHyperLogLogSketches(numberOfStatistics, hyperLogLogStatisticSchema, nodeEngine->getBufferManager());
    auto statisticSink = createStatisticSink(hyperLogLogStatisticSchema,
                                             nodeEngine,
                                             1,// numOfProducers
                                             SharedQueryId(1),
                                             DecomposedQueryPlanId(1),
                                             1,// numberOfOrigins
                                             Statistic::StatisticSinkFormatType::HLL);
    Runtime::WorkerContext wctx(Runtime::NesThread::getId(), nodeEngine->getBufferManager(), 64);

    for (auto& buf : buffers) {
        EXPECT_TRUE(statisticSink->writeData(buf, wctx));
    }

    auto storedStatistics = nodeEngine->getStatisticStore()->getAllStatistics();
    EXPECT_TRUE(sameStatisticsInVectors(storedStatistics, expectedStatistics));
}

INSTANTIATE_TEST_CASE_P(testStatisticSink,
                        StatisticSinkTest,
                        ::testing::Values(1, 2, 10, 5000),// No. statistics
                        [](const testing::TestParamInfo<StatisticSinkTest::ParamType>& info) {
                            return std::string(std::to_string(info.param) + "_Statistics");
                        });
}// namespace NES