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

#include "z3++.h"
#include <API/QueryAPI.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Catalogs/StreamCatalogEntry.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Configurations/ConfigOptions/SourceConfig.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/ProjectionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/UnionLogicalOperatorNode.hpp>
#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <Optimizer/Phases/TopologySpecificQueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <Optimizer/QueryPlacement/ILPStrategy.hpp>
#include <Optimizer/QueryPlacement/ManualPlacementStrategy.hpp>
#include <Optimizer/QueryPlacement/PlacementStrategyFactory.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <Services/QueryParsingService.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <gtest/gtest.h>
#include <utility>

using namespace NES;
using namespace z3;

class ILPPlacementTest : public testing::Test {

  protected:
    z3::ContextPtr z3Context;

  public:
    std::shared_ptr<QueryParsingService> queryParsingService;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() { std::cout << "Setup ILPPlacementTest test class." << std::endl; }

    /* Will be called before a test is executed. */
    void SetUp() override {
        NES::setupLogging("ILPPlacementTest.log", NES::LOG_DEBUG);
        std::cout << "Setup ILPPlacementTest test case." << std::endl;
        auto cppCompiler = Compiler::CPPCompiler::create();
        auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        queryParsingService = QueryParsingService::create(jitCompiler);

        z3::config cfg;
        cfg.set("timeout", 50000);
        cfg.set("model", false);
        cfg.set("type_check", false);
        z3Context = std::make_shared<z3::context>(cfg);
    }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Setup ILPPlacementTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down ILPPlacementTest test class." << std::endl; }

    void setupTopologyAndStreamCatalogForILP() {

        topologyForILP = Topology::create();

        TopologyNodePtr rootNode = TopologyNode::create(3, "localhost", 123, 124, 100);
        rootNode->addNodeProperty("slots", 100);
        topologyForILP->setAsRoot(rootNode);

        TopologyNodePtr middleNode = TopologyNode::create(2, "localhost", 123, 124, 10);
        middleNode->addNodeProperty("slots", 10);
        topologyForILP->addNewPhysicalNodeAsChild(rootNode, middleNode);

        TopologyNodePtr sourceNode = TopologyNode::create(1, "localhost", 123, 124, 1);
        sourceNode->addNodeProperty("slots", 1);
        topologyForILP->addNewPhysicalNodeAsChild(middleNode, sourceNode);

        LinkPropertyPtr linkProperty = std::make_shared<LinkProperty>(LinkProperty(512, 100));

        sourceNode->addLinkProperty(middleNode, linkProperty);
        middleNode->addLinkProperty(sourceNode, linkProperty);
        middleNode->addLinkProperty(rootNode, linkProperty);
        rootNode->addLinkProperty(middleNode, linkProperty);

        std::string schema = "Schema::create()->addField(\"id\", BasicType::UINT32)"
                             "->addField(\"value\", BasicType::UINT64);";
        const std::string streamName = "car";

        streamCatalogForILP = std::make_shared<StreamCatalog>(queryParsingService);
        streamCatalogForILP->addLogicalStream(streamName, schema);

        SourceConfigPtr sourceConfig = SourceConfig::create();
        sourceConfig->setSourceFrequency(0);
        sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
        sourceConfig->setPhysicalStreamName("test3");
        sourceConfig->setLogicalStreamName("car");

        PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(sourceConfig);

        StreamCatalogEntryPtr streamCatalogEntry1 = std::make_shared<StreamCatalogEntry>(conf, sourceNode);

        streamCatalogForILP->addPhysicalStream("car", streamCatalogEntry1);
    }

    void assignOperatorPropertiesRecursive(LogicalOperatorNodePtr operatorNode) {
        int cost = 1;
        double dmf = 1;
        double input = 0;

        for (const auto& child : operatorNode->getChildren()) {
            LogicalOperatorNodePtr op = child->as<LogicalOperatorNode>();
            assignOperatorPropertiesRecursive(op);
            std::any output = op->getProperty("output");
            input += std::any_cast<double>(output);
        }

        NodePtr nodePtr = operatorNode->as<Node>();
        if (operatorNode->instanceOf<SinkLogicalOperatorNode>()) {
            dmf = 0;
            cost = 0;
        } else if (operatorNode->instanceOf<FilterLogicalOperatorNode>()) {
            dmf = 0.5;
            cost = 1;
        } else if (operatorNode->instanceOf<MapLogicalOperatorNode>()) {
            dmf = 2;
            cost = 2;
        } else if (operatorNode->instanceOf<JoinLogicalOperatorNode>()) {
            cost = 2;
        } else if (operatorNode->instanceOf<UnionLogicalOperatorNode>()) {
            cost = 2;
        } else if (operatorNode->instanceOf<ProjectionLogicalOperatorNode>()) {
            cost = 1;
        } else if (operatorNode->instanceOf<SourceLogicalOperatorNode>()) {
            cost = 0;
            input = 100;
        }

        double output = input * dmf;
        operatorNode->addProperty("output", output);
        operatorNode->addProperty("cost", cost);
    }

    StreamCatalogPtr streamCatalogForILP;
    TopologyPtr topologyForILP;
};

/* First test of formulas with Z3 solver */
TEST_F(ILPPlacementTest, Z3Test) {
    context c;
    optimize opt(c);

    // node1 -> node2 -> node3
    int M1 = 2;
    int M2 = 1;
    int M3 = 0;

    // src -> operator -> sink
    double dmf = 2;
    int out1 = 100;
    int out2 = out1 * dmf;

    // Binary assignment
    expr P11 = c.int_const("P11");
    expr P12 = c.int_const("P12");
    expr P13 = c.int_const("P13");

    expr P21 = c.int_const("P21");
    expr P22 = c.int_const("P22");
    expr P23 = c.int_const("P23");

    expr P31 = c.int_const("P31");
    expr P32 = c.int_const("P32");
    expr P33 = c.int_const("P33");

    // Distance
    expr D1 = M1 * P11 + M2 * P12 + M3 * P13 - M1 * P21 - M2 * P22 - M3 * P23;
    expr D2 = M1 * P21 + M2 * P22 + M3 * P23 - M1 * P31 - M2 * P32 - M3 * P33;

    // Cost
    expr cost = out1 * D1 + out2 * D2;

    // Constraints
    opt.add(D1 >= 0);
    opt.add(D2 >= 0);

    opt.add(P11 + P12 + P13 == 1);
    opt.add(P21 + P22 + P23 == 1);
    opt.add(P31 + P32 + P33 == 1);

    opt.add(P11 == 1);
    opt.add(P33 == 1);

    opt.add(P11 == 0 || P11 == 1);
    opt.add(P12 == 0 || P12 == 1);
    opt.add(P13 == 0 || P13 == 1);
    opt.add(P21 == 0 || P21 == 1);
    opt.add(P22 == 0 || P22 == 1);
    opt.add(P23 == 0 || P23 == 1);
    opt.add(P31 == 0 || P31 == 1);
    opt.add(P32 == 0 || P32 == 1);
    opt.add(P33 == 0 || P33 == 1);

    // goal
    opt.minimize(cost);

    //optimize::handle h2 = opt.maximize(y);
    while (true) {
        if (sat == opt.check()) {
            model m = opt.get_model();
            std::cout << m << std::endl;
            std::cout << "-------------------------------" << std::endl;
            if (m.eval(P21).get_numeral_int() == 1) {
                std::cout << "Operator on Node 1" << std::endl;
            } else if (m.eval(P22).get_numeral_int() == 1) {
                std::cout << "Operator on Node 2" << std::endl;
            } else if (m.eval(P23).get_numeral_int() == 1) {
                std::cout << "Operator on Node 3" << std::endl;
            }
            std::cout << "-------------------------------" << std::endl;
            break;
        } else {
            break;
        }
    }
}

/* Test query placement with ILP strategy - simple filter query */
TEST_F(ILPPlacementTest, testPlacingFilterQueryWithILPStrategy) {
    setupTopologyAndStreamCatalogForILP();

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalogForILP);
    auto placementStrategy = Optimizer::PlacementStrategyFactory::getStrategy("ILP",
                                                                              globalExecutionPlan,
                                                                              topologyForILP,
                                                                              typeInferencePhase,
                                                                              streamCatalogForILP,
                                                                              z3Context);

    Query query = Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());

    QueryPlanPtr queryPlan = query.getQueryPlan();
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    queryPlan->setQueryId(queryId);

    for (const auto& sink : queryPlan->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperatorNode>());
    }

    auto topologySpecificQueryRewrite = Optimizer::TopologySpecificQueryRewritePhase::create(streamCatalogForILP);
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    placementStrategy->updateGlobalExecutionPlan(queryPlan);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    //Assertion
    ASSERT_EQ(executionNodes.size(), 3U);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1) {
            // place filter on source node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            EXPECT_TRUE(actualRootOperator->getChildWithOperatorId(1)->instanceOf<SourceLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildWithOperatorId(2)->instanceOf<FilterLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == 3) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getId(), 3U);
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
        }
    }
}

/* Test query placement with ILP strategy - simple map query */
TEST_F(ILPPlacementTest, testPlacingMapQueryWithILPStrategy) {

    setupTopologyAndStreamCatalogForILP();

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalogForILP);
    auto placementStrategy = Optimizer::PlacementStrategyFactory::getStrategy("ILP",
                                                                              globalExecutionPlan,
                                                                              topologyForILP,
                                                                              typeInferencePhase,
                                                                              streamCatalogForILP,
                                                                              z3Context);

    Query query = Query::from("car")
                      .map(Attribute("c") = Attribute("value") + 2)
                      .map(Attribute("d") = Attribute("value") * 2)
                      .sink(PrintSinkDescriptor::create());

    QueryPlanPtr queryPlan = query.getQueryPlan();
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    queryPlan->setQueryId(queryId);

    for (const auto& sink : queryPlan->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperatorNode>());
    }

    auto topologySpecificQueryRewrite = Optimizer::TopologySpecificQueryRewritePhase::create(streamCatalogForILP);
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    placementStrategy->updateGlobalExecutionPlan(queryPlan);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    //Assertion
    ASSERT_EQ(executionNodes.size(), 3U);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == 3) {
            // both map operators should be placed on cloud node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<MapLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<MapLogicalOperatorNode>());
        }
    }
}

/* Test query placement with ILP strategy - simple query of source - filter - map - sink */
TEST_F(ILPPlacementTest, testPlacingQueryWithILPStrategy) {

    setupTopologyAndStreamCatalogForILP();

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalogForILP);
    auto placementStrategy = Optimizer::PlacementStrategyFactory::getStrategy("ILP",
                                                                              globalExecutionPlan,
                                                                              topologyForILP,
                                                                              typeInferencePhase,
                                                                              streamCatalogForILP,
                                                                              z3Context);

    Query query = Query::from("car")
                      .filter(Attribute("id") < 45)
                      .map(Attribute("c") = Attribute("value") * 2)
                      .sink(PrintSinkDescriptor::create());

    QueryPlanPtr queryPlan = query.getQueryPlan();
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    queryPlan->setQueryId(queryId);

    for (const auto& sink : queryPlan->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperatorNode>());
    }

    auto topologySpecificQueryRewrite = Optimizer::TopologySpecificQueryRewritePhase::create(streamCatalogForILP);
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    placementStrategy->updateGlobalExecutionPlan(queryPlan);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    //Assertion
    ASSERT_EQ(executionNodes.size(), 3U);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1) {
            // filter should be placed on source node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == 3) {
            // map should be placed on cloud node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            //ASSERT_EQ(actualRootOperator->getId(), 4);
            ASSERT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<MapLogicalOperatorNode>());
        }
    }
}