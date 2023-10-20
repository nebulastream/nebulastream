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
// clang-format on
#include <API/QueryAPI.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <Util/DumpHandler/ConsoleDumpHandler.hpp>
#include <Nodes/Iterators/DepthFirstNodeIterator.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Operators/LogicalOperators/UnionLogicalOperatorNode.hpp>
#include <Operators/OperatorNode.hpp>
#include <Optimizer/QueryRewrite/LogicalSourceExpansionRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Util/Mobility/SpatialType.hpp>
#include <Util/Logger/Logger.hpp>
#include <iostream>

using namespace NES;
using namespace Configurations;

class LogicalSourceExpansionRuleTest : public Testing::BaseUnitTest {

  public:
    SchemaPtr schema;

    /* Will be called before all tests in this class are started. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("LogicalSourceExpansionRuleTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup LogicalSourceExpansionRuleTest test case.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
    }

    void setupSensorNodeAndSourceCatalog(const Catalogs::Source::SourceCatalogPtr& sourceCatalog) {
        NES_INFO("Setup LogicalSourceExpansionRuleTest test case.");
        std::map<std::string, std::any> properties;
        properties[NES::Worker::Properties::MAINTENANCE] = false;
        properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

        TopologyNodePtr physicalNode1 = TopologyNode::create(1, "localhost", 4000, 4002, 4, properties);
        TopologyNodePtr physicalNode2 = TopologyNode::create(2, "localhost", 4000, 4002, 4, properties);

        auto csvSourceType = CSVSourceType::create("default_logical", "test_stream");
        PhysicalSourcePtr physicalSource = PhysicalSource::create(csvSourceType);
        LogicalSourcePtr logicalSource = LogicalSource::create("default_logical", Schema::create());
        Catalogs::Source::SourceCatalogEntryPtr sce1 =
            std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, physicalNode1);
        Catalogs::Source::SourceCatalogEntryPtr sce2 =
            std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, physicalNode2);

        sourceCatalog->addPhysicalSource("default_logical", sce1);
        sourceCatalog->addPhysicalSource("default_logical", sce2);
    }
};

TEST_F(LogicalSourceExpansionRuleTest, testLogicalSourceExpansionRuleForQueryWithJustSource) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>();
    setupSensorNodeAndSourceCatalog(sourceCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    const std::string logicalSourceName = "default_logical";
    Query query = Query::from(logicalSourceName).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan = query.getQueryPlan();

    // Execute
    auto logicalSourceExpansionRule = Optimizer::LogicalSourceExpansionRule::create(sourceCatalog, false);
    const QueryPlanPtr updatedPlan = logicalSourceExpansionRule->apply(queryPlan);

    // Validate
    std::vector<TopologyNodePtr> sourceTopologyNodes = sourceCatalog->getSourceNodesForLogicalSource(logicalSourceName);
    EXPECT_EQ(updatedPlan->getSourceOperators().size(), sourceTopologyNodes.size());
    std::vector<OperatorNodePtr> rootOperators = updatedPlan->getRootOperators();
    EXPECT_EQ(rootOperators.size(), 1u);
    EXPECT_EQ(rootOperators[0]->getChildren().size(), 2u);
}

TEST_F(LogicalSourceExpansionRuleTest, testLogicalSourceExpansionRuleForQueryWithMultipleSinksAndJustSource) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>();
    setupSensorNodeAndSourceCatalog(sourceCatalog);
    const std::string logicalSourceName = "default_logical";

    // Prepare
    auto sourceOperator = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create(logicalSourceName));

    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    auto sinkOperator1 = LogicalOperatorFactory::createSinkOperator(printSinkDescriptor);

    auto sinkOperator2 = LogicalOperatorFactory::createSinkOperator(printSinkDescriptor);

    sinkOperator1->addChild(sourceOperator);
    sinkOperator2->addChild(sourceOperator);

    QueryPlanPtr queryPlan = QueryPlan::create();
    queryPlan->addRootOperator(sinkOperator1);
    queryPlan->addRootOperator(sinkOperator2);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    queryPlan->setQueryId(queryId);

    // Execute
    auto logicalSourceExpansionRule = Optimizer::LogicalSourceExpansionRule::create(sourceCatalog, false);
    const QueryPlanPtr updatedPlan = logicalSourceExpansionRule->apply(queryPlan);

    // Validate
    std::vector<TopologyNodePtr> sourceTopologyNodes = sourceCatalog->getSourceNodesForLogicalSource(logicalSourceName);
    EXPECT_EQ(updatedPlan->getSourceOperators().size(), sourceTopologyNodes.size());
    std::vector<OperatorNodePtr> rootOperators = updatedPlan->getRootOperators();
    EXPECT_EQ(rootOperators.size(), 2U);
    EXPECT_EQ(rootOperators[0]->getChildren().size(), 2U);
}

TEST_F(LogicalSourceExpansionRuleTest, testLogicalSourceExpansionRuleForQueryWithMultipleSinks) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>();
    setupSensorNodeAndSourceCatalog(sourceCatalog);
    const std::string logicalSourceName = "default_logical";

    // Prepare
    auto sourceOperator = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create(logicalSourceName));

    auto filterOperator = LogicalOperatorFactory::createFilterOperator(Attribute("id") < 45);
    filterOperator->addChild(sourceOperator);

    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    auto sinkOperator1 = LogicalOperatorFactory::createSinkOperator(printSinkDescriptor);

    auto sinkOperator2 = LogicalOperatorFactory::createSinkOperator(printSinkDescriptor);

    sinkOperator1->addChild(filterOperator);
    sinkOperator2->addChild(filterOperator);

    QueryPlanPtr queryPlan = QueryPlan::create();
    queryPlan->addRootOperator(sinkOperator1);
    queryPlan->addRootOperator(sinkOperator2);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    queryPlan->setQueryId(queryId);

    // Execute
    auto logicalSourceExpansionRule = Optimizer::LogicalSourceExpansionRule::create(sourceCatalog, false);
    const QueryPlanPtr updatedPlan = logicalSourceExpansionRule->apply(queryPlan);

    // Validate
    std::vector<TopologyNodePtr> sourceTopologyNodes = sourceCatalog->getSourceNodesForLogicalSource(logicalSourceName);
    EXPECT_EQ(updatedPlan->getSourceOperators().size(), sourceTopologyNodes.size());
    std::vector<OperatorNodePtr> rootOperators = updatedPlan->getRootOperators();
    EXPECT_EQ(rootOperators.size(), 2U);
    EXPECT_EQ(rootOperators[0]->getChildren().size(), 2U);
}

TEST_F(LogicalSourceExpansionRuleTest, testLogicalSourceExpansionRuleForQuery) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>();
    setupSensorNodeAndSourceCatalog(sourceCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    const std::string logicalSourceName = "default_logical";
    Query query =
        Query::from(logicalSourceName).map(Attribute("value") = 40).filter(Attribute("id") < 45).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan = query.getQueryPlan();

    // Execute
    auto logicalSourceExpansionRule = Optimizer::LogicalSourceExpansionRule::create(sourceCatalog, false);
    const QueryPlanPtr updatedPlan = logicalSourceExpansionRule->apply(queryPlan);

    // Validate
    std::vector<TopologyNodePtr> sourceTopologyNodes = sourceCatalog->getSourceNodesForLogicalSource(logicalSourceName);
    EXPECT_EQ(updatedPlan->getSourceOperators().size(), sourceTopologyNodes.size());
    std::vector<OperatorNodePtr> rootOperators = updatedPlan->getRootOperators();
    EXPECT_EQ(rootOperators.size(), 1U);
    EXPECT_EQ(rootOperators[0]->getChildren().size(), 2U);
}

TEST_F(LogicalSourceExpansionRuleTest, testLogicalSourceExpansionRuleForQueryWithMergeOperator) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>();
    setupSensorNodeAndSourceCatalog(sourceCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    const std::string logicalSourceName = "default_logical";
    Query subQuery = Query::from(logicalSourceName).map(Attribute("value") = 50);

    Query query = Query::from(logicalSourceName)
                      .map(Attribute("value") = 40)
                      .unionWith(subQuery)
                      .filter(Attribute("id") < 45)
                      .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan = query.getQueryPlan();

    // Execute
    auto logicalSourceExpansionRule = Optimizer::LogicalSourceExpansionRule::create(sourceCatalog, false);
    const QueryPlanPtr updatedPlan = logicalSourceExpansionRule->apply(queryPlan);

    // Validate
    std::vector<TopologyNodePtr> sourceTopologyNodes = sourceCatalog->getSourceNodesForLogicalSource(logicalSourceName);
    EXPECT_EQ(updatedPlan->getSourceOperators().size(), sourceTopologyNodes.size() * 2);
    std::vector<OperatorNodePtr> rootOperators = updatedPlan->getRootOperators();
    EXPECT_EQ(rootOperators.size(), 1U);
    EXPECT_EQ(rootOperators[0]->getChildren().size(), 1U);
    auto mergeOperators = queryPlan->getOperatorByType<UnionLogicalOperatorNode>();
    EXPECT_EQ(mergeOperators.size(), 1U);
    EXPECT_EQ(mergeOperators[0]->getChildren().size(), 4U);
}
