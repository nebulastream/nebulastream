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
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/OperatorNode.hpp>
#include <Optimizer/QueryRewrite/DistributeWindowRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/TopologyNode.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Phases/TypeInferencePhase.hpp>
#include <Util/Logger.hpp>
#include <iostream>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
#include <Windowing/WindowTypes/WindowType.hpp>
#include <Windowing/WindowTypes/TumblingWindow.hpp>
#include <Windowing/WindowAggregations/SumAggregationDescriptor.hpp>
#include <Operators/LogicalOperators/Windowing/CentralWindowOperator.hpp>
#include <Optimizer/QueryRewrite/LogicalSourceExpansionRule.hpp>
#include <Operators/LogicalOperators/Windowing/SliceCreationOperator.hpp>
#include <Operators/LogicalOperators/Windowing/WindowComputationOperator.hpp>

using namespace NES;

class DistributeWindowRuleTest : public testing::Test {

  public:
    SchemaPtr schema;

    /* Will be called before a test is executed. */
    void SetUp() {
        NES::setupLogging("DistributeWindowRuleTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup DistributeWindowRuleTest test case.");
        schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
    }

    /* Will be called before a test is executed. */
    void TearDown() { NES_INFO("Setup DistributeWindowRuleTest test case."); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down DistributeWindowRuleTest test class."); }
};

void setupSensorNodeAndStreamCatalogTwoNodes(StreamCatalogPtr streamCatalog) {
    NES_INFO("Setup LogicalSourceExpansionRuleTest test case.");
    TopologyNodePtr physicalNode1 = TopologyNode::create(1, "localhost", 4000, 4002, 4);
    TopologyNodePtr physicalNode2 = TopologyNode::create(2, "localhost", 4000, 4002, 4);

    PhysicalStreamConfigPtr streamConf =
        PhysicalStreamConfig::create(/**Source Type**/ "DefaultSource", /**Source Config**/ "",
                                     /**Source Frequence**/ 1, /**Number Of Tuples To Produce Per Buffer**/ 0,
                                     /**Number of Buffers To Produce**/ 3, /**Physical Stream Name**/ "test2",
                                     /**Logical Stream Name**/ "test_stream");

    StreamCatalogEntryPtr sce1 = std::make_shared<StreamCatalogEntry>(streamConf, physicalNode1);
    StreamCatalogEntryPtr sce2 = std::make_shared<StreamCatalogEntry>(streamConf, physicalNode2);
    streamCatalog->addPhysicalStream("default_logical", sce1);
    streamCatalog->addPhysicalStream("default_logical", sce2);
}

void setupSensorNodeAndStreamCatalog(StreamCatalogPtr streamCatalog) {
    NES_INFO("Setup DistributeWindowRuleTest test case.");
    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4);

    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();

    StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(streamConf, physicalNode);
    streamCatalog->addPhysicalStream("default_logical", sce);
}

TEST_F(DistributeWindowRuleTest, testRuleForCentralWindow) {
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    setupSensorNodeAndStreamCatalog(streamCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("default_logical")
                      .windowByKey(Attribute("id"), TumblingWindow::of(TimeCharacteristic::createProcessingTime(), Seconds(10)),
                                   Sum(Attribute("value")))
                      .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    std::cout << " plan before=" << queryPlan->toString() << std::endl;
    // Execute
    DistributeWindowRulePtr distributeWindowRule = DistributeWindowRule::create();
    const QueryPlanPtr updatedPlan = distributeWindowRule->apply(queryPlan);

    std::cout << " plan after=" << queryPlan->toString() << std::endl;
    auto windowOps = queryPlan->getOperatorByType<CentralWindowOperator>();
    ASSERT_EQ(windowOps.size(), 1);
}

TEST_F(DistributeWindowRuleTest, testRuleForDistributedWindow) {
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    setupSensorNodeAndStreamCatalogTwoNodes(streamCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("default_logical")
                      .filter(Attribute("id") < 45)
                      .windowByKey(Attribute("id"), TumblingWindow::of(TimeCharacteristic::createProcessingTime(), Seconds(10)),
                                   Sum(Attribute("value")))
                      .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan = TypeInferencePhase::create(streamCatalog)->execute(queryPlan);
    std::cout << " plan before log expand=" << queryPlan->toString() << std::endl;
    LogicalSourceExpansionRulePtr logicalSourceExpansionRule = LogicalSourceExpansionRule::create(streamCatalog);
    QueryPlanPtr updatedPlan = logicalSourceExpansionRule->apply(queryPlan);
    std::cout << " plan after log expand=" << queryPlan->toString() << std::endl;

    std::cout << " plan before window distr=" << queryPlan->toString() << std::endl;
    DistributeWindowRulePtr distributeWindowRule = DistributeWindowRule::create();
    updatedPlan = distributeWindowRule->apply(queryPlan);
    std::cout << " plan after window distr=" << queryPlan->toString() << std::endl;

    auto compOps = queryPlan->getOperatorByType<WindowComputationOperator>();
    ASSERT_EQ(compOps.size(), 1);

    auto sliceOps = queryPlan->getOperatorByType<SliceCreationOperator>();
    ASSERT_EQ(sliceOps.size(), 2);
}