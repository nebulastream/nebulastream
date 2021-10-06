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
#include <Catalogs/StreamCatalog.hpp>
#include <Configurations/ConfigOptions/SourceConfig.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Windowing/CentralWindowOperator.hpp>
#include <Operators/LogicalOperators/Windowing/SliceCreationOperator.hpp>
#include <Operators/LogicalOperators/Windowing/WindowComputationOperator.hpp>
#include <Operators/OperatorNode.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryRewrite/DistributeWindowRule.hpp>
#include <Optimizer/QueryRewrite/LogicalSourceExpansionRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/WindowAggregations/SumAggregationDescriptor.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
#include <Windowing/WindowTypes/TumblingWindow.hpp>
#include <Windowing/WindowTypes/WindowType.hpp>
#include <iostream>
using namespace NES;

class DistributeWindowRuleTest : public testing::Test {

  public:
    SchemaPtr schema;

    /* Will be called before a test is executed. */
    void SetUp() override {
        NES::setupLogging("DistributeWindowRuleTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup DistributeWindowRuleTest test case.");
        schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
    }

    /* Will be called before a test is executed. */
    void TearDown() override { NES_INFO("Setup DistributeWindowRuleTest test case."); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down DistributeWindowRuleTest test class."); }
};

void setupSensorNodeAndStreamCatalogTwoNodes(const StreamCatalogPtr& streamCatalog) {
    NES_INFO("Setup LogicalSourceExpansionRuleTest test case.");
    TopologyNodePtr physicalNode1 = TopologyNode::create(1, "localhost", 4000, 4002, 4);
    TopologyNodePtr physicalNode2 = TopologyNode::create(2, "localhost", 4000, 4002, 4);

    SourceConfigPtr sourceConfig = SourceConfig::create();
    sourceConfig->setSourceConfig("");
    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig->setNumberOfBuffersToProduce(3);
    sourceConfig->setPhysicalStreamName("test2");
    sourceConfig->setLogicalStreamName("test_stream");

    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create(sourceConfig);

    StreamCatalogEntryPtr sce1 = std::make_shared<StreamCatalogEntry>(streamConf, physicalNode1);
    StreamCatalogEntryPtr sce2 = std::make_shared<StreamCatalogEntry>(streamConf, physicalNode2);
    streamCatalog->addPhysicalStream("default_logical", sce1);
    streamCatalog->addPhysicalStream("default_logical", sce2);
}

void setupSensorNodeAndStreamCatalogFiveNodes(const StreamCatalogPtr& streamCatalog) {
    NES_INFO("Setup LogicalSourceExpansionRuleTest test case.");
    TopologyPtr topology = Topology::create();

    TopologyNodePtr physicalNode1 = TopologyNode::create(1, "localhost", 4000, 4002, 4);
    TopologyNodePtr physicalNode2 = TopologyNode::create(2, "localhost", 4000, 4002, 4);
    TopologyNodePtr physicalNode3 = TopologyNode::create(3, "localhost", 4000, 4002, 4);
    TopologyNodePtr physicalNode4 = TopologyNode::create(4, "localhost", 4000, 4002, 4);
    TopologyNodePtr physicalNode5 = TopologyNode::create(5, "localhost", 4000, 4002, 4);

    std::cout << "topo=" << topology->toString() << std::endl;

    SourceConfigPtr sourceConfig = SourceConfig::create();
    sourceConfig->setSourceConfig("");
    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig->setNumberOfBuffersToProduce(3);
    sourceConfig->setPhysicalStreamName("test2");
    sourceConfig->setLogicalStreamName("test_stream");

    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create(sourceConfig);

    StreamCatalogEntryPtr sce1 = std::make_shared<StreamCatalogEntry>(streamConf, physicalNode1);
    StreamCatalogEntryPtr sce2 = std::make_shared<StreamCatalogEntry>(streamConf, physicalNode2);
    StreamCatalogEntryPtr sce3 = std::make_shared<StreamCatalogEntry>(streamConf, physicalNode3);
    StreamCatalogEntryPtr sce4 = std::make_shared<StreamCatalogEntry>(streamConf, physicalNode4);
    StreamCatalogEntryPtr sce5 = std::make_shared<StreamCatalogEntry>(streamConf, physicalNode5);

    streamCatalog->addPhysicalStream("default_logical", sce1);
    streamCatalog->addPhysicalStream("default_logical", sce2);
    streamCatalog->addPhysicalStream("default_logical", sce3);
    streamCatalog->addPhysicalStream("default_logical", sce4);
    streamCatalog->addPhysicalStream("default_logical", sce5);
}

void setupSensorNodeAndStreamCatalog(const StreamCatalogPtr& streamCatalog) {
    NES_INFO("Setup DistributeWindowRuleTest test case.");
    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4);

    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();

    StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(streamConf, physicalNode);
    streamCatalog->addPhysicalStream("default_logical", sce);
}

TEST_F(DistributeWindowRuleTest, testRuleForCentralWindow) {
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndStreamCatalog(streamCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("default_logical")
                      .window(NES::Windowing::TumblingWindow::of(NES::Windowing::TimeCharacteristic::createIngestionTime(), API::Seconds(10)))
                      .byKey(Attribute("id"))
                      .apply(API::Sum(Attribute("value")))
                      .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    std::cout << " plan before=" << queryPlan->toString() << std::endl;
    // Execute
    auto distributeWindowRule = Optimizer::DistributeWindowRule::create();
    const QueryPlanPtr updatedPlan = distributeWindowRule->apply(queryPlan);

    std::cout << " plan after=" << queryPlan->toString() << std::endl;
    auto windowOps = queryPlan->getOperatorByType<CentralWindowOperator>();
    ASSERT_EQ(windowOps.size(), 1u);
}

TEST_F(DistributeWindowRuleTest, testRuleForDistributedWindow) {
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndStreamCatalogTwoNodes(streamCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("default_logical")
                      .filter(Attribute("id") < 45)
                      .window(NES::Windowing::TumblingWindow::of(NES::Windowing::TimeCharacteristic::createIngestionTime(), API::Seconds(10)))
                      .byKey(Attribute("id"))
                      .apply(API::Sum(Attribute("value")))
                      .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan = Optimizer::TypeInferencePhase::create(streamCatalog)->execute(queryPlan);
    std::cout << " plan before log expand=" << queryPlan->toString() << std::endl;
    auto logicalSourceExpansionRule = Optimizer::LogicalSourceExpansionRule::create(streamCatalog);
    QueryPlanPtr updatedPlan = logicalSourceExpansionRule->apply(queryPlan);
    std::cout << " plan after log expand=" << queryPlan->toString() << std::endl;

    std::cout << " plan before window distr=" << queryPlan->toString() << std::endl;
    auto distributeWindowRule = Optimizer::DistributeWindowRule::create();
    updatedPlan = distributeWindowRule->apply(queryPlan);
    std::cout << " plan after window distr=" << queryPlan->toString() << std::endl;

    auto compOps = queryPlan->getOperatorByType<WindowComputationOperator>();
    ASSERT_EQ(compOps.size(), 1u);

    auto sliceOps = queryPlan->getOperatorByType<SliceCreationOperator>();
    ASSERT_EQ(sliceOps.size(), 2u);
}

TEST_F(DistributeWindowRuleTest, testRuleForDistributedWindowWithMerger) {
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndStreamCatalogFiveNodes(streamCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("default_logical")
                      .filter(Attribute("id") < 45)
                      .window(NES::Windowing::TumblingWindow::of(NES::Windowing::TimeCharacteristic::createIngestionTime(), API::Seconds(10)))
                      .byKey(Attribute("id"))
                      .apply(API::Sum(Attribute("value")))
                      .sink(printSinkDescriptor);

    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan = Optimizer::TypeInferencePhase::create(streamCatalog)->execute(queryPlan);
    std::cout << " plan before log expand=" << queryPlan->toString() << std::endl;
    auto logicalSourceExpansionRule = Optimizer::LogicalSourceExpansionRule::create(streamCatalog);
    QueryPlanPtr updatedPlan = logicalSourceExpansionRule->apply(queryPlan);
    std::cout << " plan after log expand=" << queryPlan->toString() << std::endl;

    std::cout << " plan before window distr=" << queryPlan->toString() << std::endl;
    auto distributeWindowRule = Optimizer::DistributeWindowRule::create();
    updatedPlan = distributeWindowRule->apply(queryPlan);
    std::cout << " plan after window distr=" << queryPlan->toString() << std::endl;

    auto compOps = queryPlan->getOperatorByType<WindowComputationOperator>();
    ASSERT_EQ(compOps.size(), 1u);

    auto sliceOps = queryPlan->getOperatorByType<SliceCreationOperator>();
    ASSERT_EQ(sliceOps.size(), 5u);
}