/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
// clang-format on
#include <API/QueryAPI.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Nodes/Util/ConsoleDumpHandler.hpp>
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Operators/LogicalOperators/UnionLogicalOperatorNode.hpp>
#include <Operators/OperatorNode.hpp>
#include <Optimizer/QueryRewrite/LogicalSourceExpansionRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <iostream>

using namespace NES;
using namespace Configurations;

class LogicalSourceExpansionRuleTest : public testing::Test {

  public:
    SchemaPtr schema;

    /* Will be called before a test is executed. */
    void SetUp() override {
        NES::setupLogging("LogicalSourceExpansionRuleTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup LogicalSourceExpansionRuleTest test case.");
        schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
    }

    /* Will be called before a test is executed. */
    void TearDown() override { NES_INFO("Setup LogicalSourceExpansionRuleTest test case."); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down LogicalSourceExpansionRuleTest test class."); }
};

void setupSensorNodeAndStreamCatalog(const SourceCatalogPtr& streamCatalog) {
    NES_INFO("Setup LogicalSourceExpansionRuleTest test case.");
    TopologyNodePtr physicalNode1 = TopologyNode::create(1, "localhost", 4000, 4002, 4);
    TopologyNodePtr physicalNode2 = TopologyNode::create(2, "localhost", 4000, 4002, 4);

    auto csvSourceType = CSVSourceType::create();
    PhysicalSourcePtr physicalSource = PhysicalSource::create("default_logical", "test_stream", csvSourceType);
    LogicalSourcePtr logicalSource = LogicalSource::create("default_logical", Schema::create());
    SourceCatalogEntryPtr sce1 = std::make_shared<SourceCatalogEntry>(physicalSource, logicalSource, physicalNode1);
    SourceCatalogEntryPtr sce2 = std::make_shared<SourceCatalogEntry>(physicalSource, logicalSource, physicalNode2);

    streamCatalog->addPhysicalSource("default_logical", sce1);
    streamCatalog->addPhysicalSource("default_logical", sce2);
}

TEST_F(LogicalSourceExpansionRuleTest, testLogicalSourceExpansionRuleForQueryWithJustSource) {
    SourceCatalogPtr streamCatalog = std::make_shared<SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndStreamCatalog(streamCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    const std::string logicalStreamName = "default_logical";
    Query query = Query::from(logicalStreamName).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan = query.getQueryPlan();

    // Execute
    auto logicalSourceExpansionRule = Optimizer::LogicalSourceExpansionRule::create(streamCatalog, false);
    const QueryPlanPtr updatedPlan = logicalSourceExpansionRule->apply(queryPlan);

    // Validate
    std::vector<TopologyNodePtr> sourceTopologyNodes = streamCatalog->getSourceNodesForLogicalStream(logicalStreamName);
    EXPECT_EQ(updatedPlan->getSourceOperators().size(), sourceTopologyNodes.size());
    std::vector<OperatorNodePtr> rootOperators = updatedPlan->getRootOperators();
    EXPECT_EQ(rootOperators.size(), 1u);
    EXPECT_EQ(rootOperators[0]->getChildren().size(), 2u);
}

TEST_F(LogicalSourceExpansionRuleTest, testLogicalSourceExpansionRuleForQueryWithMultipleSinksAndJustSource) {
    SourceCatalogPtr streamCatalog = std::make_shared<SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndStreamCatalog(streamCatalog);
    const std::string logicalStreamName = "default_logical";

    // Prepare
    auto sourceOperator = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create(logicalStreamName));

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
    auto logicalSourceExpansionRule = Optimizer::LogicalSourceExpansionRule::create(streamCatalog, false);
    const QueryPlanPtr updatedPlan = logicalSourceExpansionRule->apply(queryPlan);

    // Validate
    std::vector<TopologyNodePtr> sourceTopologyNodes = streamCatalog->getSourceNodesForLogicalStream(logicalStreamName);
    EXPECT_EQ(updatedPlan->getSourceOperators().size(), sourceTopologyNodes.size());
    std::vector<OperatorNodePtr> rootOperators = updatedPlan->getRootOperators();
    EXPECT_EQ(rootOperators.size(), 2U);
    EXPECT_EQ(rootOperators[0]->getChildren().size(), 2U);
}

TEST_F(LogicalSourceExpansionRuleTest, testLogicalSourceExpansionRuleForQueryWithMultipleSinks) {
    SourceCatalogPtr streamCatalog = std::make_shared<SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndStreamCatalog(streamCatalog);
    const std::string logicalStreamName = "default_logical";

    // Prepare
    auto sourceOperator = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create(logicalStreamName));

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
    auto logicalSourceExpansionRule = Optimizer::LogicalSourceExpansionRule::create(streamCatalog, false);
    const QueryPlanPtr updatedPlan = logicalSourceExpansionRule->apply(queryPlan);

    // Validate
    std::vector<TopologyNodePtr> sourceTopologyNodes = streamCatalog->getSourceNodesForLogicalStream(logicalStreamName);
    EXPECT_EQ(updatedPlan->getSourceOperators().size(), sourceTopologyNodes.size());
    std::vector<OperatorNodePtr> rootOperators = updatedPlan->getRootOperators();
    EXPECT_EQ(rootOperators.size(), 2U);
    EXPECT_EQ(rootOperators[0]->getChildren().size(), 2U);
}

TEST_F(LogicalSourceExpansionRuleTest, testLogicalSourceExpansionRuleForQuery) {
    SourceCatalogPtr streamCatalog = std::make_shared<SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndStreamCatalog(streamCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    const std::string logicalStreamName = "default_logical";
    Query query =
        Query::from(logicalStreamName).map(Attribute("value") = 40).filter(Attribute("id") < 45).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan = query.getQueryPlan();

    // Execute
    auto logicalSourceExpansionRule = Optimizer::LogicalSourceExpansionRule::create(streamCatalog, false);
    const QueryPlanPtr updatedPlan = logicalSourceExpansionRule->apply(queryPlan);

    // Validate
    std::vector<TopologyNodePtr> sourceTopologyNodes = streamCatalog->getSourceNodesForLogicalStream(logicalStreamName);
    EXPECT_EQ(updatedPlan->getSourceOperators().size(), sourceTopologyNodes.size());
    std::vector<OperatorNodePtr> rootOperators = updatedPlan->getRootOperators();
    EXPECT_EQ(rootOperators.size(), 1U);
    EXPECT_EQ(rootOperators[0]->getChildren().size(), 2U);
}

TEST_F(LogicalSourceExpansionRuleTest, testLogicalSourceExpansionRuleForQueryWithMergeOperator) {
    SourceCatalogPtr streamCatalog = std::make_shared<SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndStreamCatalog(streamCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    const std::string logicalStreamName = "default_logical";
    Query subQuery = Query::from(logicalStreamName).map(Attribute("value") = 50);

    Query query = Query::from(logicalStreamName)
                      .map(Attribute("value") = 40)
                      .unionWith(subQuery)
                      .filter(Attribute("id") < 45)
                      .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan = query.getQueryPlan();

    // Execute
    auto logicalSourceExpansionRule = Optimizer::LogicalSourceExpansionRule::create(streamCatalog, false);
    const QueryPlanPtr updatedPlan = logicalSourceExpansionRule->apply(queryPlan);

    // Validate
    std::vector<TopologyNodePtr> sourceTopologyNodes = streamCatalog->getSourceNodesForLogicalStream(logicalStreamName);
    EXPECT_EQ(updatedPlan->getSourceOperators().size(), sourceTopologyNodes.size() * 2);
    std::vector<OperatorNodePtr> rootOperators = updatedPlan->getRootOperators();
    EXPECT_EQ(rootOperators.size(), 1U);
    EXPECT_EQ(rootOperators[0]->getChildren().size(), 1U);
    auto mergeOperators = queryPlan->getOperatorByType<UnionLogicalOperatorNode>();
    EXPECT_EQ(mergeOperators.size(), 1U);
    EXPECT_EQ(mergeOperators[0]->getChildren().size(), 4U);
}
