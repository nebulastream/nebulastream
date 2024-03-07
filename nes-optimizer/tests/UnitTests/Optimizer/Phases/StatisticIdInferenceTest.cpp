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
// clang-format off
#include <gtest/gtest.h>
#include <BaseIntegrationTest.hpp>
#include <BaseUnitTest.hpp>
// clang-format on
#include <API/QueryAPI.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Optimizer/Phases/StatisticIdInferencePhase.hpp>
#include <Optimizer/QueryRewrite/LogicalSourceExpansionRule.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Util/Logger/Logger.hpp>
namespace NES {

constexpr auto expandAlsoOperators = false;

class StatisticIdInferenceTest : public Testing::BaseUnitTest {

  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("StatisticIdInferenceTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup StatisticIdInferenceTest test class.");
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
            auto physicalNode = TopologyNode::create(i, "localhost", 4000, 4002, 4, properties);
            auto csvSourceType = CSVSourceType::create("default_logical", "default_physical_" + std::to_string(i));
            auto physicalSource = PhysicalSource::create(csvSourceType);
            auto sce = Catalogs::Source::SourceCatalogEntry::create(physicalSource, logicalSource, physicalNode->getId());
            sourceCatalog->addPhysicalSource("default_logical", sce);
        }

        NES_DEBUG("Created SourceCatalog!");
        return sourceCatalog;
    }

};

/**
 * @brief Tests if we can infer the statistic id of a query with one physical source and one sink
 */
TEST_F(StatisticIdInferenceTest, oneSourceOneQuery) {

    auto query = Query::from("default_logical").sink(FileSinkDescriptor::create(""));

    constexpr auto numberOfSources = 1;
    auto sourceCatalog = setUpSourceCatalog(numberOfSources);
    auto logicalSourceExpansionRule = Optimizer::LogicalSourceExpansionRule::create(sourceCatalog, expandAlsoOperators);
    auto statisticIdInferencePhase = Optimizer::StatisticIdInferencePhase::create();

    // 1. Running the statisticIdInferencePhase and the logicalSourceExpansionRule
    auto queryPlan = statisticIdInferencePhase->execute(query.getQueryPlan());
    queryPlan = logicalSourceExpansionRule->apply(query.getQueryPlan());

    // 2. Checking if all operators have a unique statistic id, by iterating over all operators and get the statisticId
    // of each operator. We check for uniqueness by comparing the size of both sets (allOperators, allStatisticIds).
    auto allOperators = queryPlan->getAllOperators();
    std::set<StatisticId> allStatisticIds;
    std::transform(allOperators.begin(), allOperators.end(), std::inserter(allStatisticIds, allStatisticIds.end()),
                   [](const OperatorPtr& op) {
                       auto statisticId = op->getStatisticId();
                       EXPECT_NE(statisticId, INVALID_STATISTIC_ID);
                       return statisticId;
                   });
    EXPECT_EQ(allStatisticIds.size(), allOperators.size());
}

/**
 * @brief Tests if we can infer the statistic id of a query with two physical sources and one sink
 */
TEST_F(StatisticIdInferenceTest, twoSourcesOneQuery) {
    auto query = Query::from("default_logical").sink(FileSinkDescriptor::create(""));

    constexpr auto numberOfSources = 2;
    auto sourceCatalog = setUpSourceCatalog(numberOfSources);
    auto logicalSourceExpansionRule = Optimizer::LogicalSourceExpansionRule::create(sourceCatalog, expandAlsoOperators);
    auto statisticIdInferencePhase = Optimizer::StatisticIdInferencePhase::create();

    // 1. Running the statisticIdInferencePhase and the logicalSourceExpansionRule
    auto queryPlan = statisticIdInferencePhase->execute(query.getQueryPlan());
    queryPlan = logicalSourceExpansionRule->apply(query.getQueryPlan());

    // 2. Checking if all operators have a unique statistic id, by iterating over all operators and get the statisticId
    // of each operator. We check for uniqueness by comparing the size of both sets (allOperators, allStatisticIds).
    auto allOperators = queryPlan->getAllOperators();
    std::set<StatisticId> allStatisticIds;
    std::transform(allOperators.begin(), allOperators.end(), std::inserter(allStatisticIds, allStatisticIds.end()),
                   [](const OperatorPtr& op) { return op->getStatisticId(); });
    EXPECT_EQ(allStatisticIds.size(), allOperators.size());
}

/**
 * @brief Tests if we can infer the statistic id of two queries with two physical sources and a sink
 */
TEST_F(StatisticIdInferenceTest, twoSourcesTwoQueries) {
    auto query1 = Query::from("default_logical").sink(FileSinkDescriptor::create(""));
    auto query2 = Query::from("default_logical").sink(FileSinkDescriptor::create(""));

    constexpr auto numberOfSources = 2;
    auto sourceCatalog = setUpSourceCatalog(numberOfSources);
    auto logicalSourceExpansionRule = Optimizer::LogicalSourceExpansionRule::create(sourceCatalog, expandAlsoOperators);
    auto statisticIdInferencePhase = Optimizer::StatisticIdInferencePhase::create();

    // 1. Running the statisticIdInferencePhase and the logicalSourceExpansionRule for both queries
    auto queryPlan1 = statisticIdInferencePhase->execute(query1.getQueryPlan());
    queryPlan1 = logicalSourceExpansionRule->apply(query1.getQueryPlan());
    auto queryPlan2 = statisticIdInferencePhase->execute(query2.getQueryPlan());
    queryPlan2 = logicalSourceExpansionRule->apply(query2.getQueryPlan());

    // 2. Checking if all operators have a unique statistic id, by iterating over all operators and get the statisticId
    // of each operator. We check for uniqueness by comparing the size of both sets (allOperators, allStatisticIds).
    std::set<StatisticId> allStatisticIds;
    auto allOperators1 = queryPlan1->getAllOperators();
    std::transform(allOperators1.begin(), allOperators1.end(), std::inserter(allStatisticIds, allStatisticIds.end()),
                   [](const OperatorPtr& op) { return op->getStatisticId(); });
    auto allOperators2 = queryPlan2->getAllOperators();
    std::transform(allOperators2.begin(), allOperators2.end(), std::inserter(allStatisticIds, allStatisticIds.end()),
                   [](const OperatorPtr& op) { return op->getStatisticId(); });

    // We have to subtract here one, as both queries use the same two sources and a source always has the same statisticId
    // across multiple queries.
    EXPECT_EQ(allStatisticIds.size(), allOperators1.size() + allOperators2.size() - 2);
}


/**
 * @brief Tests if we can infer the statistic id of a query with one physical source, one sink, and one filter and a map
 */
TEST_F(StatisticIdInferenceTest, oneSourceOneQueryWithFilterAndMap) {
    auto query = Query::from("default_logical")
                     .map(Attribute("f4") = Attribute("f5") * 42)
                     .filter(Attribute("f4") < Attribute("f2"))
                     .sink(FileSinkDescriptor::create(""));

    constexpr auto numberOfSources = 1;
    auto sourceCatalog = setUpSourceCatalog(numberOfSources);
    auto logicalSourceExpansionRule = Optimizer::LogicalSourceExpansionRule::create(sourceCatalog, expandAlsoOperators);
    auto statisticIdInferencePhase = Optimizer::StatisticIdInferencePhase::create();

    // 1. Running the statisticIdInferencePhase and the logicalSourceExpansionRule
    auto queryPlan = statisticIdInferencePhase->execute(query.getQueryPlan());
    queryPlan = logicalSourceExpansionRule->apply(query.getQueryPlan());

    // 2. Checking if all operators have a unique statistic id, by iterating over all operators and get the statisticId
    // of each operator. We check for uniqueness by comparing the size of both sets (allOperators, allStatisticIds).
    auto allOperators = queryPlan->getAllOperators();
    std::set<StatisticId> allStatisticIds;
    std::transform(allOperators.begin(), allOperators.end(), std::inserter(allStatisticIds, allStatisticIds.end()),
                   [](const OperatorPtr& op) {
                       auto statisticId = op->getStatisticId();
                       EXPECT_NE(statisticId, INVALID_STATISTIC_ID);
                       return statisticId;
                   });
    EXPECT_EQ(allStatisticIds.size(), allOperators.size());

}

/**
 * @brief Tests if we can infer the statistic id of a query with two physical sources, one sink, and one filter and a map
 */
TEST_F(StatisticIdInferenceTest, twoSourcesOneQueryWithFilterAndMap) {
    auto query = Query::from("default_logical")
                     .map(Attribute("f4") = Attribute("f5") * 42)
                     .filter(Attribute("f4") < Attribute("f2"))
                     .sink(FileSinkDescriptor::create(""));

    constexpr auto numberOfSources = 2;
    auto sourceCatalog = setUpSourceCatalog(numberOfSources);
    auto logicalSourceExpansionRule = Optimizer::LogicalSourceExpansionRule::create(sourceCatalog, expandAlsoOperators);
    auto statisticIdInferencePhase = Optimizer::StatisticIdInferencePhase::create();

    // 1. Running the statisticIdInferencePhase and the logicalSourceExpansionRule
    auto queryPlan = statisticIdInferencePhase->execute(query.getQueryPlan());
    queryPlan = logicalSourceExpansionRule->apply(query.getQueryPlan());

    // 2. Checking if all operators have a unique statistic id, by iterating over all operators and get the statisticId
    // of each operator. We check for uniqueness by comparing the size of both sets (allOperators, allStatisticIds).
    auto allOperators = queryPlan->getAllOperators();
    std::set<StatisticId> allStatisticIds;
    std::transform(allOperators.begin(), allOperators.end(), std::inserter(allStatisticIds, allStatisticIds.end()),
                   [](const OperatorPtr& op) {
                       auto statisticId = op->getStatisticId();
                       NES_INFO("StatisticId {} for op {}", statisticId, op->toString());
                       EXPECT_NE(statisticId, INVALID_STATISTIC_ID);
                       return statisticId;
                   });
    EXPECT_EQ(allStatisticIds.size(), allOperators.size());
}

/**
 * @brief Tests if we can infer the statistic id of two queries with two physical sources, a sink, and one filter and a map.
 */
TEST_F(StatisticIdInferenceTest, twoSourcesTwoQueriesWithFilterAndMap) {
    auto query1 = Query::from("default_logical")
                     .map(Attribute("f4") = Attribute("f5") * 42)
                     .filter(Attribute("f4") < Attribute("f2"))
                     .sink(FileSinkDescriptor::create(""));

    auto query2 = Query::from("default_logical")
                     .map(Attribute("f3") = Attribute("f7") * 42)
                     .filter(Attribute("f1") < Attribute("f2"))
                     .sink(FileSinkDescriptor::create(""));

    constexpr auto numberOfSources = 1;
    auto sourceCatalog = setUpSourceCatalog(numberOfSources);
    auto logicalSourceExpansionRule = Optimizer::LogicalSourceExpansionRule::create(sourceCatalog, expandAlsoOperators);
    auto statisticIdInferencePhase = Optimizer::StatisticIdInferencePhase::create();

    // 1. Running the statisticIdInferencePhase and the logicalSourceExpansionRule for both queries
    auto queryPlan1 = statisticIdInferencePhase->execute(query1.getQueryPlan());
    queryPlan1 = logicalSourceExpansionRule->apply(query1.getQueryPlan());
    auto queryPlan2 = statisticIdInferencePhase->execute(query2.getQueryPlan());
    queryPlan2 = logicalSourceExpansionRule->apply(query2.getQueryPlan());

    // 2. Checking if all operators have a unique statistic id, by iterating over all operators and get the statisticId
    // of each operator. We check for uniqueness by comparing the size of both sets (allOperators, allStatisticIds).
    std::set<StatisticId> allStatisticIds;
    auto allOperators1 = queryPlan1->getAllOperators();
    std::transform(allOperators1.begin(), allOperators1.end(), std::inserter(allStatisticIds, allStatisticIds.end()),
                   [](const OperatorPtr& op) {
                       auto statisticId = op->getStatisticId();
                       EXPECT_NE(statisticId, INVALID_STATISTIC_ID);
                       return statisticId;
                   });
    auto allOperators2 = queryPlan2->getAllOperators();
    std::transform(allOperators2.begin(), allOperators2.end(), std::inserter(allStatisticIds, allStatisticIds.end()),
                   [](const OperatorPtr& op) {
                       auto statisticId = op->getStatisticId();
                       EXPECT_NE(statisticId, INVALID_STATISTIC_ID);
                       return statisticId;
                   });

    // We have to subtract here one, as both queries use the same source and a source always has the same statisticId
    // across multiple queries.
    EXPECT_EQ(allStatisticIds.size(), allOperators1.size() + allOperators2.size() - 1);
}


} // namespace NES