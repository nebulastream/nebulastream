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
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <Operators/LogicalOperators/LogicalMapOperator.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Descriptor/HyperLogLogDescriptor.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Statistics/Metrics/IngestionRate.hpp>
#include <Optimizer/Phases/StatisticIdInferencePhase.hpp>
#include <Optimizer/QueryRewrite/LogicalSourceExpansionRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <StatisticCollection/StatisticRegistry/StatisticIdsExtractor.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <gtest/gtest.h>
#include <vector>

namespace NES {

constexpr auto expandAlsoOperators = false;

class StatisticIdsExtractorTest : public Testing::BaseUnitTest, public testing::WithParamInterface<uint64_t> {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("StatisticIdsExtractorTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup StatisticIdsExtractorTest test class.");
    }
    void SetUp() override {
        BaseUnitTest::SetUp();
        window = TumblingWindow::of(IngestionTime(), Minutes(30));
        const auto metric = Statistic::IngestionRate::create();
        statisticDescriptor = Statistic::HyperLogLogDescriptor::create(metric->getField());
        metricHash = metric->hash();

        const auto numberOfSources = StatisticIdsExtractorTest::GetParam();
        sourceCatalog = setUpSourceCatalog(numberOfSources);
    }

    /**
     * @brief Creates #numberOfSources physical sources
     * @param numberOfSources
     * @return SourceCatalog
     */
    Catalogs::Source::SourceCatalogPtr setUpSourceCatalog(const int numberOfSources) {
        NES_DEBUG("Creating SourceCatalog...");
        auto sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
        std::map<std::string, std::any> properties;
        properties[NES::Worker::Properties::MAINTENANCE] = false;
        properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

        auto logicalSource = LogicalSource::create("default_logical", Schema::create());
        for (auto i = 0; i < numberOfSources; ++i) {
            auto physicalNode = TopologyNode::create(WorkerId(i), "localhost", 4000, 4002, 4, properties);
            auto csvSourceType = CSVSourceType::create("default_logical", "default_physical_" + std::to_string(i));
            auto physicalSource = PhysicalSource::create(csvSourceType);
            auto sce = Catalogs::Source::SourceCatalogEntry::create(physicalSource, logicalSource, physicalNode->getId());
            sourceCatalog->addPhysicalSource("default_logical", sce);
        }

        NES_DEBUG("Created SourceCatalog!");
        return sourceCatalog;
    }

    Windowing::WindowTypePtr window;
    Statistic::WindowStatisticDescriptorPtr statisticDescriptor;
    Statistic::MetricHash metricHash;
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
};

/**
 * @brief Tests if we extract the correct statistic ids for a query containing one operator
 */
TEST_P(StatisticIdsExtractorTest, oneOperator) {
    auto query = Query::from("default_logical")
                     .map(Attribute("f1") = Attribute("f1"))
                     .buildStatistic(window, statisticDescriptor, metricHash)
                     .sink(FileSinkDescriptor::create(""));

    auto logicalSourceExpansionRule = Optimizer::LogicalSourceExpansionRule::create(sourceCatalog, expandAlsoOperators);
    auto statisticIdInferencePhase = Optimizer::StatisticIdInferencePhase::create();

    // 1. Running the statisticIdInferencePhase, the logicalSourceExpansionRule, and StatisticIdExtractor
    auto queryPlan = statisticIdInferencePhase->execute(query.getQueryPlan());
    queryPlan = logicalSourceExpansionRule->apply(query.getQueryPlan());
    auto newStatisticIds = Statistic::StatisticIdsExtractor::extractStatisticIdsFromQueryPlan(*queryPlan);

    // 2. Checking if the newStatisticIds contain the statisticId of the map operator
    auto mapOperators = queryPlan->getOperatorByType<LogicalMapOperator>();
    std::vector<StatisticId> expectedStatisticIds;
    std::transform(mapOperators.begin(),
                   mapOperators.end(),
                   std::back_inserter(expectedStatisticIds),
                   [](const LogicalMapOperatorPtr& op) {
                       return op->getStatisticId();
                   });

    EXPECT_THAT(newStatisticIds, ::testing::UnorderedElementsAreArray(expectedStatisticIds));
}

/**
 * @brief Tests if we extract the correct statistic ids for a query containing two physical sources and no operator.
 *  With this test case, we simulate a data or workload characteristic
 */
TEST_P(StatisticIdsExtractorTest, noOperators) {
    auto query = Query::from("default_logical")
                     .buildStatistic(window, statisticDescriptor, metricHash)
                     .sink(FileSinkDescriptor::create(""));

    auto logicalSourceExpansionRule = Optimizer::LogicalSourceExpansionRule::create(sourceCatalog, expandAlsoOperators);
    auto statisticIdInferencePhase = Optimizer::StatisticIdInferencePhase::create();

    // 1. Running the statisticIdInferencePhase, the logicalSourceExpansionRule, and StatisticIdExtractor
    auto queryPlan = statisticIdInferencePhase->execute(query.getQueryPlan());
    queryPlan = logicalSourceExpansionRule->apply(query.getQueryPlan());
    auto newStatisticIds = Statistic::StatisticIdsExtractor::extractStatisticIdsFromQueryPlan(*queryPlan);

    // 2. Checking if the newStatisticIds contain the statisticId of the physical source
    auto allPhysicalSources = sourceCatalog->getPhysicalSources("default_logical");
    std::vector<StatisticId> expectedStatisticIds;
    std::transform(allPhysicalSources.begin(),
                   allPhysicalSources.end(),
                   std::back_inserter(expectedStatisticIds),
                   [](const Catalogs::Source::SourceCatalogEntryPtr& sourceCatalogEntry) {
                       return sourceCatalogEntry->getPhysicalSource()->getStatisticId();
                   });

    EXPECT_THAT(newStatisticIds, ::testing::UnorderedElementsAreArray(expectedStatisticIds));
}

/**
 * @brief Tests if we extract the correct statistic ids for a query containing two operators
 */
TEST_P(StatisticIdsExtractorTest, twoOperators) {
    auto query = Query::from("default_logical")
                     .filter(Attribute("f1") < 10)
                     .map(Attribute("f1") = Attribute("f1"))
                     .buildStatistic(window, statisticDescriptor, metricHash)
                     .sink(FileSinkDescriptor::create(""));

    auto logicalSourceExpansionRule = Optimizer::LogicalSourceExpansionRule::create(sourceCatalog, expandAlsoOperators);
    auto statisticIdInferencePhase = Optimizer::StatisticIdInferencePhase::create();

    // 1. Running the statisticIdInferencePhase, the logicalSourceExpansionRule, and StatisticIdExtractor
    auto queryPlan = statisticIdInferencePhase->execute(query.getQueryPlan());
    queryPlan = logicalSourceExpansionRule->apply(query.getQueryPlan());
    auto newStatisticIds = Statistic::StatisticIdsExtractor::extractStatisticIdsFromQueryPlan(*queryPlan);

    // 2. Checking if the newStatisticIds contain the statisticId of the map operator
    auto mapOperators = queryPlan->getOperatorByType<LogicalMapOperator>();
    std::vector<StatisticId> expectedStatisticIds;
    std::transform(mapOperators.begin(),
                   mapOperators.end(),
                   std::back_inserter(expectedStatisticIds),
                   [](const LogicalMapOperatorPtr& op) {
                       return op->getStatisticId();
                   });

    EXPECT_THAT(newStatisticIds, ::testing::UnorderedElementsAreArray(expectedStatisticIds));
}

INSTANTIATE_TEST_CASE_P(TestInputs,
                        StatisticIdsExtractorTest,
                        ::testing::Values(1, 2, 3, 4, 8),
                        [](const testing::TestParamInfo<StatisticIdsExtractorTest::ParamType>& info) {
                            return std::to_string(info.param) + "_PhysicalSources";
                        });

}// namespace NES