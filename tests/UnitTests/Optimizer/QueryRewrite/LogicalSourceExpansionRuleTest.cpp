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
#include <API/Query.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/MergeLogicalOperatorNode.hpp>
#include <Operators/OperatorNode.hpp>
#include <Nodes/Util/ConsoleDumpHandler.hpp>
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>
#include <Optimizer/QueryRewrite/LogicalSourceExpansionRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/TopologyNode.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Util/Logger.hpp>
#include <iostream>
#include <Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>

using namespace NES;

class LogicalSourceExpansionRuleTest : public testing::Test {

  public:
    SchemaPtr schema;

    /* Will be called before a test is executed. */
    void SetUp() {
        NES::setupLogging("LogicalSourceExpansionRuleTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup LogicalSourceExpansionRuleTest test case.");
        schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
    }

    /* Will be called before a test is executed. */
    void TearDown() {
        NES_INFO("Setup LogicalSourceExpansionRuleTest test case.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() {
        NES_INFO("Tear down LogicalSourceExpansionRuleTest test class.");
    }
};

void setupSensorNodeAndStreamCatalog(StreamCatalogPtr streamCatalog) {
    NES_INFO("Setup LogicalSourceExpansionRuleTest test case.");
    TopologyNodePtr physicalNode1 = TopologyNode::create(1, "localhost", 4000, 4002, 4);
    TopologyNodePtr physicalNode2 = TopologyNode::create(2, "localhost", 4000, 4002, 4);

    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create(/**Source Type**/ "DefaultSource", /**Source Config**/ "",
                                                                      /**Source Frequence**/ 1, /**Number Of Tuples To Produce Per Buffer**/ 0,
                                                                      /**Number of Buffers To Produce**/ 3, /**Physical Stream Name**/ "test2",
                                                                      /**Logical Stream Name**/ "test_stream");

    StreamCatalogEntryPtr sce1 = std::make_shared<StreamCatalogEntry>(streamConf, physicalNode1);
    StreamCatalogEntryPtr sce2 = std::make_shared<StreamCatalogEntry>(streamConf, physicalNode2);
    streamCatalog->addPhysicalStream("default_logical", sce1);
    streamCatalog->addPhysicalStream("default_logical", sce2);
}

TEST_F(LogicalSourceExpansionRuleTest, testLogicalSourceExpansionRuleForQueryWithJustSource) {
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    setupSensorNodeAndStreamCatalog(streamCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    const std::string logicalStreamName = "default_logical";
    Query query = Query::from(logicalStreamName)
                      .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan = query.getQueryPlan();

    // Execute
    LogicalSourceExpansionRulePtr logicalSourceExpansionRule = LogicalSourceExpansionRule::create(streamCatalog);
    const QueryPlanPtr updatedPlan = logicalSourceExpansionRule->apply(queryPlan);

    // Validate
    std::vector<TopologyNodePtr> sourceTopologyNodes = streamCatalog->getSourceNodesForLogicalStream(logicalStreamName);
    EXPECT_EQ(updatedPlan->getSourceOperators().size(), sourceTopologyNodes.size());
    std::vector<OperatorNodePtr> rootOperators = updatedPlan->getRootOperators();
    EXPECT_EQ(rootOperators.size(), 1);
    EXPECT_EQ(rootOperators[0]->getChildren().size(), 2);
}

TEST_F(LogicalSourceExpansionRuleTest, testLogicalSourceExpansionRuleForQueryWithMultipleSinksAndJustSource) {
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    setupSensorNodeAndStreamCatalog(streamCatalog);
    const std::string logicalStreamName = "default_logical";

    // Prepare
    auto sourceOperator = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create(logicalStreamName));

    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    auto sinkOperator1 = LogicalOperatorFactory::createSinkOperator(printSinkDescriptor);

    auto sinkOperator2 = LogicalOperatorFactory::createSinkOperator(printSinkDescriptor);

    sinkOperator1->addChild(sourceOperator);
    sinkOperator2->addChild(sourceOperator);

    QueryPlanPtr queryPlan =  QueryPlan::create();
    queryPlan->addRootOperator(sinkOperator1);
    queryPlan->addRootOperator(sinkOperator2);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    queryPlan->setQueryId(queryId);

    // Execute
    LogicalSourceExpansionRulePtr logicalSourceExpansionRule = LogicalSourceExpansionRule::create(streamCatalog);
    const QueryPlanPtr updatedPlan = logicalSourceExpansionRule->apply(queryPlan);

    // Validate
    std::vector<TopologyNodePtr> sourceTopologyNodes = streamCatalog->getSourceNodesForLogicalStream(logicalStreamName);
    EXPECT_EQ(updatedPlan->getSourceOperators().size(), sourceTopologyNodes.size());
    std::vector<OperatorNodePtr> rootOperators = updatedPlan->getRootOperators();
    EXPECT_EQ(rootOperators.size(), 2);
    EXPECT_EQ(rootOperators[0]->getChildren().size(), 2);
}

TEST_F(LogicalSourceExpansionRuleTest, testLogicalSourceExpansionRuleForQueryWithMultipleSinks) {
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
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

    QueryPlanPtr queryPlan =  QueryPlan::create();
    queryPlan->addRootOperator(sinkOperator1);
    queryPlan->addRootOperator(sinkOperator2);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    queryPlan->setQueryId(queryId);

    // Execute
    LogicalSourceExpansionRulePtr logicalSourceExpansionRule = LogicalSourceExpansionRule::create(streamCatalog);
    const QueryPlanPtr updatedPlan = logicalSourceExpansionRule->apply(queryPlan);

    // Validate
    std::vector<TopologyNodePtr> sourceTopologyNodes = streamCatalog->getSourceNodesForLogicalStream(logicalStreamName);
    EXPECT_EQ(updatedPlan->getSourceOperators().size(), sourceTopologyNodes.size());
    std::vector<OperatorNodePtr> rootOperators = updatedPlan->getRootOperators();
    EXPECT_EQ(rootOperators.size(), 2);
    EXPECT_EQ(rootOperators[0]->getChildren().size(), 2);
}

TEST_F(LogicalSourceExpansionRuleTest, testLogicalSourceExpansionRuleForQuery) {
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    setupSensorNodeAndStreamCatalog(streamCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    const std::string logicalStreamName = "default_logical";
    Query query = Query::from(logicalStreamName)
                      .map(Attribute("value") = 40)
                      .filter(Attribute("id") < 45)
                      .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan = query.getQueryPlan();

    // Execute
    LogicalSourceExpansionRulePtr logicalSourceExpansionRule = LogicalSourceExpansionRule::create(streamCatalog);
    const QueryPlanPtr updatedPlan = logicalSourceExpansionRule->apply(queryPlan);

    // Validate
    std::vector<TopologyNodePtr> sourceTopologyNodes = streamCatalog->getSourceNodesForLogicalStream(logicalStreamName);
    EXPECT_EQ(updatedPlan->getSourceOperators().size(), sourceTopologyNodes.size());
    std::vector<OperatorNodePtr> rootOperators = updatedPlan->getRootOperators();
    EXPECT_EQ(rootOperators.size(), 1);
    EXPECT_EQ(rootOperators[0]->getChildren().size(), 2);
}

TEST_F(LogicalSourceExpansionRuleTest, testLogicalSourceExpansionRuleForQueryWithMergeOperator) {
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    setupSensorNodeAndStreamCatalog(streamCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    const std::string logicalStreamName = "default_logical";
    Query subQuery = Query::from(logicalStreamName)
                         .map(Attribute("value") = 50);

    Query query = Query::from(logicalStreamName)
                      .map(Attribute("value") = 40)
                      .merge(&subQuery)
                      .filter(Attribute("id") < 45)
                      .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan = query.getQueryPlan();

    // Execute
    LogicalSourceExpansionRulePtr logicalSourceExpansionRule = LogicalSourceExpansionRule::create(streamCatalog);
    const QueryPlanPtr updatedPlan = logicalSourceExpansionRule->apply(queryPlan);

    // Validate
    std::vector<TopologyNodePtr> sourceTopologyNodes = streamCatalog->getSourceNodesForLogicalStream(logicalStreamName);
    EXPECT_EQ(updatedPlan->getSourceOperators().size(), sourceTopologyNodes.size() * 2);
    std::vector<OperatorNodePtr> rootOperators = updatedPlan->getRootOperators();
    EXPECT_EQ(rootOperators.size(), 1);
    EXPECT_EQ(rootOperators[0]->getChildren().size(), 1);
    auto mergeOperators = queryPlan->getOperatorByType<MergeLogicalOperatorNode>();
    EXPECT_EQ(mergeOperators.size(), 1);
    EXPECT_EQ(mergeOperators[0]->getChildren().size(), 4);
}
