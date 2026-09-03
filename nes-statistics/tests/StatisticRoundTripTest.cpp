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

/// End-to-end cover for the statistic build and probe paths, without any transport involved.
///
/// This is where the Nautilus-traced code in ScalarStatisticAggregationPhysicalFunction,
/// StatisticStoreWriter and StatisticStoreReader is actually executed; everything else about those
/// operators is only checked at compile time.

#include <cstddef>
#include <filesystem>
#include <string>
#include <StatisticStore/StatisticStoreRegistry.hpp>
#include <Util/Logger/Logger.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <StatisticTestSupport.hpp>

namespace NES
{

using namespace StatisticTestSupport;

class StatisticRoundTripTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase() { Logger::setupLogging("StatisticRoundTripTest.log", LogLevel::LOG_DEBUG); }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        StatisticStoreRegistry::instance().clear();
    }

    void TearDown() override
    {
        StatisticStoreRegistry::instance().clear();
        BaseUnitTest::TearDown();
    }
};

/// The build half: a windowed aggregation carrying a ScalarStatistic must persist one statistic per closed
/// window. If the writer's fusion into the aggregation lowering is wrong, or its traced code does not run, the
/// store stays empty and this fails.
TEST_F(StatisticRoundTripTest, ScalarStatisticAggregationPersistsOneStatisticPerWindow)
{
    const auto inputPath = writeInput("statistic-build-input.csv");
    const auto outputPath = std::filesystem::temp_directory_path() / "statistic-build-output.csv";
    std::filesystem::remove(outputPath);

    runToCompletion(addFileSink(buildStatisticPlan(inputPath), outputPath));

    const auto store = StatisticStoreRegistry::instance().getOrCreate(std::string{StatisticStoreRegistry::DEFAULT_STORE_NAME});
    const auto statistics
        = store->getStatistics(StatisticId{STATISTIC_ID}, Windowing::TimeMeasure{0}, Windowing::TimeMeasure{WINDOW_SIZE_MS * 10});

    ASSERT_EQ(statistics.size(), 2U) << "expected one statistic per closed window";
    for (const auto& statistic : statistics)
    {
        EXPECT_EQ(statistic.getStatisticType(), StatisticType::Avg);
        EXPECT_EQ(statistic.getNumberOfSeenTuples(), 4U);
        EXPECT_EQ(statistic.getStatisticDataSize(), sizeof(double));
    }
}

/// The read half: a probe chained onto the build reads back what the writer just stored. The two averages are
/// distinct, so a value surviving the round trip cannot be a coincidence of zero-initialised memory.
TEST_F(StatisticRoundTripTest, ProbeReadsBackTheStoredAverages)
{
    const auto inputPath = writeInput("statistic-probe-input.csv");
    const auto outputPath = std::filesystem::temp_directory_path() / "statistic-probe-output.csv";
    std::filesystem::remove(outputPath);

    runToCompletion(addFileSink(addScalarProbe(buildStatisticPlan(inputPath)), outputPath));

    ASSERT_TRUE(std::filesystem::exists(outputPath)) << "probe produced no sink output";
    const auto output = readFile(outputPath);

    /// Whole rows rather than substrings: "20" occurs inside "200", so a substring check would pass on the
    /// second window alone. Column order follows the probe's output schema, which is unordered, so it is pinned
    /// by the header the sink writes.
    EXPECT_NE(
        output.find("STATISTICEND:UINT64:NOT_NULLABLE,STATISTICID:UINT64:NOT_NULLABLE,"
                    "STATISTICNUMBEROFSEENTUPLES:UINT64:NOT_NULLABLE,STATISTICSTART:UINT64:NOT_NULLABLE,"
                    "STATISTICVALUE:FLOAT64:NOT_NULLABLE"),
        std::string::npos)
        << "unexpected probe output schema:\n"
        << output;
    EXPECT_NE(output.find("1000,401,4,0,20.0"), std::string::npos) << "first window [0,1000) avg 20 missing from:\n" << output;
    EXPECT_NE(output.find("2000,401,4,1000,200.0"), std::string::npos) << "second window [1000,2000) avg 200 missing from:\n" << output;
}

}
