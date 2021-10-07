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
#include <Nodes/Util/ConsoleDumpHandler.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/ProjectionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/RenameStreamOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryRewrite/AttributeSortRule.hpp>
#include <Optimizer/QueryRewrite/RenameStreamToProjectOperatorRule.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <iostream>

using namespace NES;

class RenameStreamToProjectOperatorRuleTest : public testing::Test {

  public:
    SchemaPtr schema;
    StreamCatalogPtr streamCatalog;

    /* Will be called before all tests in this class are started. */
    static void SetUpTestCase() {
        NES::setupLogging("RenameStreamToProjectOperatorRuleTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup RenameStreamToProjectOperatorRuleTest test case.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override { schema = Schema::create()->addField("a", BasicType::UINT32)->addField("b", BasicType::UINT32); }

    /* Will be called before a test is executed. */
    void TearDown() override { NES_INFO("Setup RenameStreamToProjectOperatorRuleTest test case."); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down RenameStreamToProjectOperatorRuleTest test class."); }

    void setupSensorNodeAndStreamCatalog(const StreamCatalogPtr& streamCatalog) const {
        NES_INFO("Setup FilterPushDownTest test case.");
        TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4);
        PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();
        StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(streamConf, physicalNode);
        streamCatalog->addPhysicalStream("src", sce);
        streamCatalog->addLogicalStream("src", schema);
    }
};

TEST_F(RenameStreamToProjectOperatorRuleTest, testAddingSingleStreamRenameOperator) {

    // Prepare
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndStreamCatalog(streamCatalog);
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("src").map(Attribute("b") = Attribute("b") + Attribute("a")).as("x").sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto renameStreamOperators = queryPlan->getOperatorByType<RenameStreamOperatorNode>();
    EXPECT_TRUE(!renameStreamOperators.empty());

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);
    typeInferencePhase->execute(queryPlan);

    auto renameStreamToProjectOperatorRule = Optimizer::RenameStreamToProjectOperatorRule::create();
    auto updatedQueryPlan = renameStreamToProjectOperatorRule->apply(queryPlan);

    typeInferencePhase->execute(updatedQueryPlan);

    renameStreamOperators = updatedQueryPlan->getOperatorByType<RenameStreamOperatorNode>();
    EXPECT_TRUE(renameStreamOperators.empty());

    auto projectOperators = updatedQueryPlan->getOperatorByType<ProjectionLogicalOperatorNode>();
    EXPECT_TRUE(projectOperators.size() == 1);
}

TEST_F(RenameStreamToProjectOperatorRuleTest, testAddingMultipleStreamRenameOperator) {

    // Prepare
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndStreamCatalog(streamCatalog);
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query =
        Query::from("src").as("y").map(Attribute("b") = Attribute("b") + Attribute("a")).as("x").sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto renameStreamOperators = queryPlan->getOperatorByType<RenameStreamOperatorNode>();
    EXPECT_TRUE(!renameStreamOperators.empty());

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);
    typeInferencePhase->execute(queryPlan);

    auto renameStreamToProjectOperatorRule = Optimizer::RenameStreamToProjectOperatorRule::create();
    auto updatedQueryPlan = renameStreamToProjectOperatorRule->apply(queryPlan);

    typeInferencePhase->execute(updatedQueryPlan);

    renameStreamOperators = updatedQueryPlan->getOperatorByType<RenameStreamOperatorNode>();
    EXPECT_TRUE(renameStreamOperators.empty());

    auto projectOperators = updatedQueryPlan->getOperatorByType<ProjectionLogicalOperatorNode>();
    EXPECT_TRUE(projectOperators.size() == 2);
}

TEST_F(RenameStreamToProjectOperatorRuleTest, testAddingStreamRenameOperatorWithProject) {

    // Prepare
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndStreamCatalog(streamCatalog);
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("src")
                      .project(Attribute("b"), Attribute("a"))
                      .map(Attribute("b") = Attribute("b") + Attribute("a"))
                      .as("x")
                      .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto renameStreamOperators = queryPlan->getOperatorByType<RenameStreamOperatorNode>();
    EXPECT_TRUE(!renameStreamOperators.empty());

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);
    typeInferencePhase->execute(queryPlan);

    auto renameStreamToProjectOperatorRule = Optimizer::RenameStreamToProjectOperatorRule::create();
    auto updatedQueryPlan = renameStreamToProjectOperatorRule->apply(queryPlan);

    typeInferencePhase->execute(updatedQueryPlan);

    renameStreamOperators = updatedQueryPlan->getOperatorByType<RenameStreamOperatorNode>();
    EXPECT_TRUE(renameStreamOperators.empty());

    auto projectOperators = updatedQueryPlan->getOperatorByType<ProjectionLogicalOperatorNode>();
    EXPECT_TRUE(projectOperators.size() == 2);
}
