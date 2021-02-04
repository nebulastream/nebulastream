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
#include <Operators/LogicalOperators/ProjectionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/OperatorNode.hpp>
#include <Nodes/Util/ConsoleDumpHandler.hpp>
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>
#include <Optimizer/QueryRewrite/FilterPushDownRule.hpp>
#include <Optimizer/QueryRewrite/ProjectionPushDownRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/TopologyNode.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Util/Logger.hpp>
#include <iostream>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MergeLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/ProjectionLogicalOperatorNode.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>

using namespace NES;

class ProjectionPushDownRuleTest : public testing::Test {

  public:
    SchemaPtr schema;

    /* Will be called before a test is executed. */
    void SetUp() {
        NES::setupLogging("ProjectionPushDownTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup ProjectionPushDownTest test case.");
        schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
    }

    /* Will be called before a test is executed. */
    void TearDown() { NES_INFO("Setup ProjectionPushDownTest test case."); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down ProjectionPushDownTest test class."); }
};

void setupSensorNodeAndStreamCatalog(StreamCatalogPtr streamCatalog) {
    NES_INFO("Setup ProjectionPushDownTest test case.");
    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4);

    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();

    StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(streamConf, physicalNode);
    streamCatalog->addPhysicalStream("default_logical", sce);
}

TEST_F(ProjectionPushDownRuleTest, testPushingOneProjectionBelowMap) {
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    setupSensorNodeAndStreamCatalog(streamCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query =
        Query::from("default_logical").map(Attribute("value") = 40).filter(Attribute("id") < 45).project(Attribute("id")).sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    DepthFirstNodeIterator queryPlanNodeIterator(queryPlan->getRootOperators()[0]);
    auto itr = queryPlanNodeIterator.begin();

    const NodePtr sinkOperator = (*itr);
    ++itr;
    const NodePtr projectOperator = (*itr);
    ++itr;
    const NodePtr filterOperator = (*itr);
    ++itr;
    const NodePtr mapOperator = (*itr);
    ++itr;
    const NodePtr srcOperator = (*itr);

    // Execute
    ProjectionPushDownRulePtr projectPushDownRule = ProjectionPushDownRule::create();

    NES_DEBUG("Input Query Plan: " + (queryPlan)->toString());
    const QueryPlanPtr updatedPlan = projectPushDownRule->apply(queryPlan);
    NES_DEBUG("Updated Query Plan: " + (updatedPlan)->toString());

    // Validate
    DepthFirstNodeIterator updatedQueryPlanNodeIterator(updatedPlan->getRootOperators()[0]);
    itr = queryPlanNodeIterator.begin();
    EXPECT_TRUE(sinkOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(projectOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(filterOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(projectOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(mapOperator->equal((*itr)));
    ++itr;
    EXPECT_TRUE(srcOperator->equal((*itr)));
}