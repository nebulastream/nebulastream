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

#include <API/TestSchemas.hpp>
#include <BaseIntegrationTest.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Runtime/NesThread.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sinks/Formats/StatisticCollection/StatisticFormatFactory.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Sinks/Mediums/StatisticSink.hpp>
#include <Sinks/SinkCreator.hpp>
#include <StatisticIdentifiers.hpp>
#include <Statistics/Synopses/CountMinStatistic.hpp>
#include <Statistics/Synopses/HyperLogLogStatistic.hpp>
#include <Statistics/Synopses/ReservoirSampleStatistic.hpp>
#include <Statistics/Synopses/DDSketchStatistic.hpp>
#include <Util/TestUtils.hpp>
#include <Util/Core.hpp>
#include <Statistics/StatisticUtil.hpp>
#include <gmock/gmock-matchers.h>

namespace NES {

class StatisticSinkTest : public Testing::BaseIntegrationTest,
                          public ::testing::WithParamInterface<std::tuple<int, Statistic::StatisticDataCodec>> {
  public:
    int numberOfStatistics;
    Statistic::StatisticDataCodec statisticDataCodec;

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

        // Getting the parameter values
        std::tie(numberOfStatistics, statisticDataCodec) = GetParam();
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
    std::pair<std::vector<Runtime::TupleBuffer>, std::vector<Statistic::StatisticPtr>>
    createRandomCountMinSketches(uint64_t numberOfSketches,
                                 const SchemaPtr& schema,
                                 const Runtime::BufferManagerPtr& bufferManager) {
        std::vector<Statistic::StatisticPtr> expectedStatistics;
        constexpr auto windowSize = 10;

        for (auto curCnt = 0_u64; curCnt < numberOfSketches; ++curCnt) {
            // Creating "random" values that represent of a CountMinStatistic for a tumbling window with a size of 10
            const Windowing::TimeMeasure startTs(windowSize * curCnt);
            const Windowing::TimeMeasure endTs(windowSize * (curCnt + 1));
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

            // Creating now the expected CountMinStatistic
            expectedStatistics.emplace_back(countMin);
        }

        // Writing the expected statistics into buffers via the statistic format
        auto statisticFormat = Statistic::StatisticFormatFactory::createFromSchema(schema,
                                                                                   bufferManager->getBufferSize(),
                                                                                   Statistic::StatisticSynopsisType::COUNT_MIN,
                                                                                   statisticDataCodec);
        std::vector<Statistic::HashStatisticPair> statisticsWithHashes;
        std::transform(expectedStatistics.begin(), expectedStatistics.end(),
                       std::back_inserter(statisticsWithHashes),
                       [](const auto& statistic) {
                           static auto hash = 0;
                           return std::make_pair(++hash, statistic);
                       });
        auto createdBuffers = statisticFormat->writeStatisticsIntoBuffers(statisticsWithHashes, *bufferManager);
        return {createdBuffers, expectedStatistics};
    }

    /**
     * @brief Creates #numberOfSketches random HyperLogLog-Sketches
     * @param numberOfSketches
     * @param schema
     * @param bufferManager
     * @return Pair<Vector of TupleBuffers, Vector of Statistics>
     */
    std::pair<std::vector<Runtime::TupleBuffer>, std::vector<Statistic::StatisticPtr>>
    createRandomHyperLogLogSketches(uint64_t numberOfSketches,
                                    const SchemaPtr& schema,
                                    const Runtime::BufferManagerPtr& bufferManager) {
        std::vector<Statistic::StatisticPtr> expectedStatistics;
        constexpr auto windowSize = 10;

        for (auto curCnt = 0_u64; curCnt < numberOfSketches; ++curCnt) {
            // Creating "random" values that represent of a HyperLogLogStatistics for a tumbling window with a size of 10
            const Windowing::TimeMeasure startTs(windowSize * curCnt);
            const Windowing::TimeMeasure endTs(windowSize * (curCnt + 1));
            const uint64_t width = rand() % 10 + 5;

            // We simulate a filling of hyperloglog by randomly updating the sketch
            const auto numberOfTuplesToInsert = rand() % 100 + 100;
            auto hyperLogLog =
                Statistic::HyperLogLogStatistic::createInit(startTs, endTs, width)->as<Statistic::HyperLogLogStatistic>();
            for (auto i = 0; i < numberOfTuplesToInsert; ++i) {
                auto hash = rand();
                hyperLogLog->update(hash);
            }

            // Creating now the expected HyperLogLogStatistics
            expectedStatistics.emplace_back(hyperLogLog);
        }

        // Writing the expected statistics into buffers via the statistic format
        auto statisticFormat = Statistic::StatisticFormatFactory::createFromSchema(schema,
                                                                                   bufferManager->getBufferSize(),
                                                                                   Statistic::StatisticSynopsisType::HLL,
                                                                                   statisticDataCodec);
        std::vector<Statistic::HashStatisticPair> statisticsWithHashes;
        std::transform(expectedStatistics.begin(), expectedStatistics.end(),
                       std::back_inserter(statisticsWithHashes),
                       [](const auto& statistic) {
                           static auto hash = 0;
                           return std::make_pair(++hash, statistic);
                       });
        auto createdBuffers = statisticFormat->writeStatisticsIntoBuffers(statisticsWithHashes, *bufferManager);
        return {createdBuffers, expectedStatistics};
    }

    /**
     * @brief Creates #numberOfSketches random Reservoir Samples
     * @param numberOfSamples
     * @param schema
     * @param bufferManager
     * @return Pair<Vector of TupleBuffers, Vector of Statistics>
     */
    std::pair<std::vector<Runtime::TupleBuffer>, std::vector<Statistic::StatisticPtr>>
    createRandomReservoirSampleStatistics(uint64_t numberOfSamples,
                                          const SchemaPtr& schema,
                                          const Runtime::BufferManagerPtr& bufferManager,
                                          const SchemaPtr sampleSchema) {
        std::vector<Statistic::StatisticPtr> expectedStatistics;
        constexpr auto windowSize = 10;

        for (auto curCnt = 0_u64; curCnt < numberOfSamples; ++curCnt) {
            // Creating "random" values that represent of a Reservoir Samples for a tumbling window with a size of 10
            const Windowing::TimeMeasure startTs(windowSize * curCnt);
            const Windowing::TimeMeasure endTs(windowSize * (curCnt + 1));
            const uint64_t sampleSize = rand() % 10 + 5;
            auto sampleMemoryLayout = Util::createMemoryLayout(sampleSchema, sampleSize * sampleSchema->getSchemaSizeInBytes());

            // We simulate a filling of a sample by writing random values into the reservoir.
            const auto sample = Statistic::ReservoirSampleStatistic::createInit(startTs, endTs, sampleSize, sampleMemoryLayout->getSchema())->as<Statistic::ReservoirSampleStatistic>();
            auto* reservoirSpace = sample->getReservoirSpace();
            for (auto i = 0_u64; i < sampleSize; ++i) {
                int8_t rndVal = rand();
                std::memcpy(reservoirSpace + i * sizeof(int8_t), &rndVal, sizeof(int8_t));
            }

            // Creating now the expected ReservoirSamples
            expectedStatistics.emplace_back(sample);
        }

        // Writing the expected statistics into buffers via the statistic format
        auto statisticFormat = Statistic::StatisticFormatFactory::createFromSchema(schema,
                                                                                   bufferManager->getBufferSize(),
                                                                                   Statistic::StatisticSynopsisType::RESERVOIR_SAMPLE,
                                                                                   statisticDataCodec);
        std::vector<Statistic::HashStatisticPair> statisticsWithHashes;
        std::transform(expectedStatistics.begin(), expectedStatistics.end(),
                       std::back_inserter(statisticsWithHashes),
                       [](const auto& statistic) {
                           static auto hash = 0;
                           return std::make_pair(++hash, statistic);
                       });
        auto createdBuffers = statisticFormat->writeStatisticsIntoBuffers(statisticsWithHashes, *bufferManager);
        return {createdBuffers, expectedStatistics};
    }

    /**
     * @brief Creates #numberOfSketches random DD-Sketches
     * @param numberOfSketches
     * @param schema
     * @param bufferManager
     * @return Pair<Vector of TupleBuffers, Vector of Statistics>
     */
    std::pair<std::vector<Runtime::TupleBuffer>, std::vector<Statistic::StatisticPtr>>
    createRandomDDSketches(uint64_t numberOfSketches, const SchemaPtr& schema, const Runtime::BufferManagerPtr& bufferManager) {
        std::vector<Statistic::StatisticPtr> expectedStatistics;
        constexpr auto windowSize = 10;

        for (auto curCnt = 0_u64; curCnt < numberOfSketches; ++curCnt) {
            // Creating "random" values that represent of a Reservoir Samples for a tumbling window with a size of 10
            const Windowing::TimeMeasure startTs(windowSize * curCnt);
            const Windowing::TimeMeasure endTs(windowSize * (curCnt + 1));
            const uint64_t numberOfBuckets = rand() % 10 + 5;
            const double error = rand() % 1 + 0.01;
            const double gamma = (1 + error) / (1 - error);
            const auto numberOfUpdates = rand() % 100 + 100;

            // We simulate a filling of a sample by writing random values into the reservoir.
            const auto ddSketch = Statistic::DDSketchStatistic::createInit(startTs, endTs, numberOfBuckets, gamma)->as<Statistic::DDSketchStatistic>();
            for (auto i = 0; i < numberOfUpdates; ++i) {
                auto logFloorIndex = rand() % 10000;
                auto weight = rand() % 10000;
                ddSketch->addValue(logFloorIndex, weight);
            }

            // Creating now the expected ReservoirSamples
            expectedStatistics.emplace_back(ddSketch);
        }

        // Writing the expected statistics into buffers via the statistic format
        auto statisticFormat = Statistic::StatisticFormatFactory::createFromSchema(schema,
                                                                                   bufferManager->getBufferSize(),
                                                                                   Statistic::StatisticSynopsisType::DD_SKETCH,
                                                                                   statisticDataCodec);
        std::vector<Statistic::HashStatisticPair> statisticsWithHashes;
        std::transform(expectedStatistics.begin(), expectedStatistics.end(),
                       std::back_inserter(statisticsWithHashes),
                       [](const auto& statistic) {
                           static auto hash = 0;
                           return std::make_pair(++hash, statistic);
                       });
        auto createdBuffers = statisticFormat->writeStatisticsIntoBuffers(statisticsWithHashes, *bufferManager);
        return {createdBuffers, expectedStatistics};
    }



    Runtime::NodeEnginePtr nodeEngine{nullptr};
};

/**
 * @brief Tests if our statistic sink can put CountMin-Sketches into the StatisticStore
 */
TEST_P(StatisticSinkTest, testCountMin) {
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
                                             Statistic::StatisticSynopsisType::COUNT_MIN,
                                             statisticDataCodec);
    Runtime::WorkerContext wctx(Runtime::NesThread::getId(), nodeEngine->getBufferManager(), 64);

    for (auto& buf : buffers) {
        EXPECT_TRUE(statisticSink->writeData(buf, wctx));
    }

    auto storedStatistics = nodeEngine->getStatisticManager()->getStatisticStore()->getAllStatistics();
    EXPECT_TRUE(sameStatisticsInVectors(storedStatistics, expectedStatistics));
}

/**
 * @brief Tests if our statistic sink can put HyperLogLog-Sketches into the StatisticStore
 */
TEST_P(StatisticSinkTest, testHyperLogLog) {
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
                                             Statistic::StatisticSynopsisType::HLL,
                                             statisticDataCodec);
    Runtime::WorkerContext wctx(Runtime::NesThread::getId(), nodeEngine->getBufferManager(), 64);

    for (auto& buf : buffers) {
        EXPECT_TRUE(statisticSink->writeData(buf, wctx));
    }

    auto storedStatistics = nodeEngine->getStatisticManager()->getStatisticStore()->getAllStatistics();
    EXPECT_TRUE(sameStatisticsInVectors(storedStatistics, expectedStatistics));
}

/**
 * @brief Tests if our statistic sink can put DDSKetches into the StatisticStore
 */
TEST_P(StatisticSinkTest, testDDSketch) {
    auto ddSketchStatisticSchema = Schema::create()
                                          ->addField(Statistic::WIDTH_FIELD_NAME, BasicType::UINT64)
                                          ->addField(Statistic::STATISTIC_DATA_FIELD_NAME, BasicType::TEXT)
                                          ->addField(Statistic::BASE_FIELD_NAME_START, BasicType::UINT64)
                                          ->addField(Statistic::BASE_FIELD_NAME_END, BasicType::UINT64)
                                          ->addField(Statistic::STATISTIC_HASH_FIELD_NAME, BasicType::UINT64)
                                          ->addField(Statistic::STATISTIC_TYPE_FIELD_NAME, BasicType::UINT64)
                                          ->addField(Statistic::GAMMA_FIELD_NAME, BasicType::FLOAT64)
                                          ->addField(Statistic::OBSERVED_TUPLES_FIELD_NAME, BasicType::UINT64)
                                          ->updateSourceName("test");

    // Creating the number of buffers
    auto [buffers, expectedStatistics] =
        createRandomDDSketches(numberOfStatistics, ddSketchStatisticSchema, nodeEngine->getBufferManager());
    auto statisticSink = createStatisticSink(ddSketchStatisticSchema,
                                             nodeEngine,
                                             1,// numOfProducers
                                             SharedQueryId(1),
                                             DecomposedQueryPlanId(1),
                                             1,// numberOfOrigins
                                             Statistic::StatisticSynopsisType::DD_SKETCH,
                                             statisticDataCodec);
    Runtime::WorkerContext wctx(Runtime::NesThread::getId(), nodeEngine->getBufferManager(), 64);

    for (auto& buf : buffers) {
        EXPECT_TRUE(statisticSink->writeData(buf, wctx));
    }

    auto storedStatistics = nodeEngine->getStatisticManager()->getStatisticStore()->getAllStatistics();
    EXPECT_TRUE(sameStatisticsInVectors(storedStatistics, expectedStatistics));
}

/**
 * @brief Tests if our statistic sink can put Reservoir Samples into the StatisticStore
 */
TEST_P(StatisticSinkTest, testReservoirSample) {
    auto inputSchema = TestSchemas::getSchemaTemplate("id_val_time_u64");
    auto reservoirSampleStatisticSchema = Schema::create()
                                                  ->addField(Statistic::WIDTH_FIELD_NAME, BasicType::UINT64)
                                                  ->addField(Statistic::STATISTIC_DATA_FIELD_NAME, BasicType::TEXT)
                                                  ->addField(Statistic::BASE_FIELD_NAME_START, BasicType::UINT64)
                                                  ->addField(Statistic::BASE_FIELD_NAME_END, BasicType::UINT64)
                                                  ->addField(Statistic::STATISTIC_HASH_FIELD_NAME, BasicType::UINT64)
                                                  ->addField(Statistic::STATISTIC_TYPE_FIELD_NAME, BasicType::UINT64)
                                                  ->addField(Statistic::OBSERVED_TUPLES_FIELD_NAME, BasicType::UINT64)
                                                  ->copyFields(inputSchema)
                                                  ->updateSourceName("test");
    auto sampleSchema = Statistic::StatisticUtil::createSampleSchema(inputSchema);


    // Creating the number of buffers
    auto [buffers, expectedStatistics] =
        createRandomReservoirSampleStatistics(numberOfStatistics, reservoirSampleStatisticSchema, nodeEngine->getBufferManager(), sampleSchema);
    auto statisticSink = createStatisticSink(reservoirSampleStatisticSchema,
                                             nodeEngine,
                                             1,// numOfProducers
                                             SharedQueryId(1),
                                             DecomposedQueryPlanId(1),
                                             1,// numberOfOrigins
                                             Statistic::StatisticSynopsisType::RESERVOIR_SAMPLE,
                                             statisticDataCodec);
    Runtime::WorkerContext wctx(Runtime::NesThread::getId(), nodeEngine->getBufferManager(), 64);

    for (auto& buf : buffers) {
        EXPECT_TRUE(statisticSink->writeData(buf, wctx));
    }

    auto storedStatistics = nodeEngine->getStatisticManager()->getStatisticStore()->getAllStatistics();
    EXPECT_TRUE(sameStatisticsInVectors(storedStatistics, expectedStatistics));
}

INSTANTIATE_TEST_CASE_P(testStatisticSink,
                        StatisticSinkTest,
                        ::testing::Combine(
                            ::testing::Values(1, 2, 10, 5000), // No. statistics
                            ::testing::ValuesIn(            // All possible statistic sink datatype
                                magic_enum::enum_values<Statistic::StatisticDataCodec>())
                            ),
                        [](const testing::TestParamInfo<StatisticSinkTest::ParamType>& info) {
                            const auto param = info.param;
                            return std::to_string(std::get<0>(param)) + "_Statistics" +
                                std::string(magic_enum::enum_name(std::get<1>(param)));
                        });
}// namespace NES
