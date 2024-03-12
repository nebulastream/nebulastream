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
#include <Components/NesCoordinator.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Characteristic/DataCharacteristic.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Statistics/Metrics/Selectivity.hpp>
#include <Operators/LogicalOperators/Windows/Measures/TimeMeasure.hpp>
#include <StatisticCollection/StatisticCoordinator.hpp>
#include <StatisticCollection/StatisticStorage/DefaultStatisticStore.hpp>
#include <memory>
namespace NES {


/*
 * NOTE: Dear reviewer, once we have implemented some actual executable operators, this test suite makes more sense.
 * As we then can spawn a topology and multiple workers and check if it works for various topologies.
 * I merely wanted to start writing the integration test here to also show what the vision is
 */
class StatisticCoordinatorTest : public Testing::BaseIntegrationTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("StatisticCoordinatorTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup StatisticCoordinatorTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        NES::Testing::BaseIntegrationTest::SetUp();
        // Skipping until, we have implemented some executable statistic build operators
        GTEST_SKIP();

        // Creates a NesCoordinator, as otherwise we have to create a lot of services for our statisticCoordinator
        auto configuration = CoordinatorConfiguration::createDefault();
        configuration->rpcPort = *rpcCoordinatorPort;
        configuration->restPort = *restPort;
        configuration->worker.numWorkerThreads = 3;
        configuration->worker.physicalSourceTypes.add(DefaultSourceType::create("default_logical", "default_logical_1"));


        NES_DEBUG("Starting coordinator...");
        nesCoordinator = std::make_shared<NesCoordinator>(configuration);
        nesCoordinator->startCoordinator(false);


        // Creating the statisticCoordinator
        statisticStore = Statistic::DefaultStatisticStore::create();
        statisticCoordinator = std::make_shared<Statistic::StatisticCoordinator>(nesCoordinator->getRequestHandlerService(),
                                                                                 Statistic::DefaultStatisticQueryGenerator::create(),
                                                                                 statisticStore,
                                                                                 nesCoordinator->getQueryCatalog());
    }

    Statistic::StatisticCoordinatorPtr statisticCoordinator;
    Statistic::AbstractStatisticStorePtr statisticStore;
    std::shared_ptr<NesCoordinator> nesCoordinator;
};




/**
 * @brief Tests if the StatisticCoordinator
 */
TEST_F(StatisticCoordinatorTest, trackOneStatisticProbeOnce) {
    using namespace NES::Statistic;
    using namespace Windowing;

    auto characteristic = DataCharacteristic::create(Selectivity::create(Over("f1")), "default_logical", "default_logical_1");
    auto window = SlidingWindow::of(IngestionTime(), Seconds(10), Seconds(1));
    auto statisticKey = statisticCoordinator->trackStatistic(characteristic, window);

    // Wait some time and let the statistic build during this time and then get the statistics from [0, 10]
    auto probeResult = statisticCoordinator->probeStatistic(statisticKey[0], Seconds(0), Seconds(100), Seconds(10),
                                                            ProbeExpression(Attribute("f1") < 10),
                                                            false);

}


} // namespace NES