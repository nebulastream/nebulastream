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
#include <Runtime/NesThread.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sinks/SinkCreator.hpp>
#include <StatisticIdentifiers.hpp>
#include <Util/TestUtils.hpp>
#include <gmock/gmock-matchers.h>

namespace NES {

class StatisticSinkTest : public Testing::BaseIntegrationTest, public ::testing::WithParamInterface<int>  {
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
    bool sameStatisticsInVectors(std::vector<Statistic::StatisticPtr>& left,
                                 std::vector<Statistic::StatisticPtr>& right) {
        if (left.size() != right.size()) {
            NES_ERROR("Vectors are not equal with {} and {} items!", left.size(), right.size());
            return false;
        }

        // We iterate over the left and search for the same item in the right and then remove it from right vector
        for (const auto& elem : left) {
            auto it = std::find_if(right.begin(), right.end(), [&](const Statistic::StatisticPtr& statistic) {
                return statistic->equal(*elem);
            });

            if (it == right.end()) {
                NES_ERROR("Can not find statistic {} in right vector!", elem->toString());
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
    createRandomCountMinSketches(uint64_t numberOfSketches, const SchemaPtr& schema, const Runtime::BufferManagerPtr& bufferManager) {
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
            const uint64_t observedTuples = rand() % 100 + 100;
            const uint64_t width = rand() % 10 + 5;
            const uint64_t depth = rand() % 5 + 1;
            const std::string countMinData = "abcdef";


            // Now using the values for writing a tuple to the tuple buffer
            auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer::createDynamicTupleBuffer(buffer, schema);
            dynamicBuffer[curBufTuplePos]["test$" + Statistic::STATISTIC_HASH_FIELD_NAME].write(statisticHash);
            dynamicBuffer[curBufTuplePos]["test$" + Statistic::BASE_FIELD_NAME_START].write(startTs.getTime());
            dynamicBuffer[curBufTuplePos]["test$" + Statistic::BASE_FIELD_NAME_END].write(endTs.getTime());
            dynamicBuffer[curBufTuplePos]["test$" + Statistic::STATISTIC_TYPE_FIELD_NAME].write(statisticType);
            dynamicBuffer[curBufTuplePos]["test$" + Statistic::OBSERVED_TUPLES_FIELD_NAME].write(observedTuples);
            dynamicBuffer[curBufTuplePos]["test$" + Statistic::WIDTH_FIELD_NAME].write(width);
            dynamicBuffer[curBufTuplePos]["test$" + Statistic::DEPTH_FIELD_NAME].write(depth);
            dynamicBuffer[curBufTuplePos].writeVarSized("test$" + Statistic::STATISTIC_DATA_FIELD_NAME, countMinData,
                                                        bufferManager.get());
            curBufTuplePos += 1;

            // Creating now the expected CountMinStatistic
            expectedStatistics.emplace_back(Statistic::CountMinStatistic::create(startTs, endTs, observedTuples, width, depth, countMinData));
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
                                       ->addField(Statistic::STATISTIC_DATA_FIELD_NAME, BasicType::TEXT)
                                       ->addField(Statistic::BASE_FIELD_NAME_START, BasicType::UINT64)
                                       ->addField(Statistic::BASE_FIELD_NAME_END, BasicType::UINT64)
                                       ->addField(Statistic::STATISTIC_HASH_FIELD_NAME, BasicType::UINT64)
                                       ->addField(Statistic::STATISTIC_TYPE_FIELD_NAME, BasicType::UINT64)
                                       ->addField(Statistic::OBSERVED_TUPLES_FIELD_NAME, BasicType::UINT64)
                                       ->updateSourceName("test");

    // Creating the number of buffers
    auto [buffers, expectedStatistics] = createRandomCountMinSketches(numberOfStatistics, countMinStatisticSchema,
                                                nodeEngine->getBufferManager());
    auto statisticSink = createStatisticSink(countMinStatisticSchema,
                                             nodeEngine,
                                             1, // numOfProducers
                                             1, // queryId
                                             1, // querySubPlanId
                                             1, // numberOfOrigins
                                             Statistic::StatisticSinkFormatType::COUNT_MIN);
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
} // namespace NES