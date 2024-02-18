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

#include <API/Expressions/Expressions.hpp>
#include <API/QueryAPI.hpp>
#include <BaseIntegrationTest.hpp>
#include <Operators/LogicalOperators/Windows/Measures/TimeMeasure.hpp>
#include <Operators/LogicalOperators/Windows/Types/TumblingWindow.hpp>
#include <StatisticCollection/Characteristic/DataCharacteristic.hpp>
#include <StatisticCollection/Characteristic/InfrastructureCharacteristic.hpp>
#include <StatisticCollection/Characteristic/WorkloadCharacteristic.hpp>
#include <StatisticCollection/Statistic/Metric/Selectivity.hpp>
#include <StatisticCollection/Statistic/Metric/MinVal.hpp>
#include <StatisticCollection/Statistic/Metric/IngestionRate.hpp>
#include <StatisticCollection/Statistic/Metric/Cardinality.hpp>
#include <StatisticCollection/StatisticCoordinator.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <gtest/gtest.h>

namespace NES {

class StatisticTest : public Testing::BaseUnitTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("StatisticTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup StatisticTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override { NES::Testing::BaseUnitTest::SetUp(); }

    static void TearDownTestCase() {}

    Statistic::StatisticCoordinator statCoordinator;
};

TEST_F(StatisticTest, simpleTest) {
    using namespace NES::Statistic;
    using namespace Windowing;
    constexpr auto queryId = 1;
    constexpr auto operatorId = 1;
    constexpr auto nodeId = 1;

    //----------------------- Tracking
    statCoordinator.trackStatistic(DataCharacteristic::create(Selectivity::create(Attribute("f1") > 4), "car", {"car_1", "car_2", "car_3"}),
                                   SlidingWindow::of(IngestionTime(), Seconds(10), Seconds(1)));
    statCoordinator.trackStatistic(WorkloadCharacteristic::create(Selectivity::create(Attribute("f2") == 42), queryId, operatorId),
                                   SlidingWindow::of(IngestionTime(), Seconds(10), Seconds(1)));

    statCoordinator.trackStatistic(WorkloadCharacteristic::create(Cardinality::create("f2"), queryId, operatorId),
                                   TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(4)),
                                   SENDING_ADAPTIVE);

    statCoordinator.trackStatistic(WorkloadCharacteristic::create(MinVal::create("f2"), queryId, operatorId),
                                   TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(4)),
                                   SENDING_LAZY);

    statCoordinator.trackStatistic(WorkloadCharacteristic::create(Cardinality::create("f2"), queryId, operatorId),
                                   TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(4)),
                                   SENDING_ASAP);

    statCoordinator.trackStatistic(InfrastructureStatistic::create(IngestionRate::create(), nodeId),
                                   TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(4)));

    //----------------------- Probing
    const bool estimationAllowed = true;
    const bool estimationNOTAllowed = false;

    auto probeResult = statCoordinator.probeStatistic(DataCharacteristic::create(Cardinality::create("f2"), "car", {"car_2"}),
                                                      Hours(24),
                                                      Seconds(2),
                                                      estimationAllowed);

    auto anotherProbeResult =
        statCoordinator.probeStatistic(WorkloadCharacteristic::create(Selectivity::create(Attribute("f1") > 4), queryId, operatorId),
                                       Hours(1),
                                       Seconds(10),
                                       estimationAllowed);

    auto yetAnotherProbeResult = statCoordinator.probeStatistic(InfrastructureStatistic::create(IngestionRate::create(), nodeId),
                                                                Minutes(1),
                                                                Milliseconds(10),
                                                                estimationNOTAllowed,
                                                                [](const ProbeResult<>& probeResult) {
                                                                    return probeResult;
                                                                });
    ((void) probeResult);
    ((void) anotherProbeResult);
    ((void) yetAnotherProbeResult);
}

}// namespace NES