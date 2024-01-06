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
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/ProjectionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/UnionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkAssignerLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windows/Joins/JoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windows/WindowOperatorNode.hpp>
#include <Optimizer/Phases/QueryMergerPhase.hpp>
#include <Optimizer/Phases/QueryPlacementAmendmentPhase.hpp>
#include <Optimizer/Phases/SignatureInferencePhase.hpp>
#include <Optimizer/Phases/TopologySpecificQueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryPlacementAddition/BasePlacementAdditionStrategy.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Mobility/SpatialType.hpp>
#include <gtest/gtest.h>
#include <z3++.h>

using namespace NES;
using namespace z3;
using namespace Configurations;

class ILPPlacementTest : public Testing::BaseUnitTest {

  protected:
    z3::ContextPtr z3Context;

  public:
    Catalogs::UDF::UDFCatalogPtr udfCatalog;
    TopologyPtr topology;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ILPPlacementTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup ILPPlacementTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        NES_DEBUG("Setup ILPPlacementTest test case.");
        udfCatalog = Catalogs::UDF::UDFCatalog::create();
        z3::config cfg;
        cfg.set("timeout", 50000);
        cfg.set("model", false);
        cfg.set("type_check", false);
        z3Context = std::make_shared<z3::context>(cfg);
    }

    void setupTopologyAndSourceCatalogForILP() {

        topologyForILP = Topology::create();

        std::map<std::string, std::any> properties;
        properties[NES::Worker::Properties::MAINTENANCE] = false;
        properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;
        properties["slots"] = 100;

        WorkerId rootNodeId = 1;
        topologyForILP->registerTopologyNode(rootNodeId, "localhost", 123, 124, 100, properties);
        topologyForILP->setRootTopologyNodeId(rootNodeId);

        WorkerId middleNodeId = 2;
        properties["slots"] = 10;
        topologyForILP->registerTopologyNode(middleNodeId, "localhost", 123, 124, 10, properties);
        topologyForILP->addTopologyNodeAsChild(rootNodeId, middleNodeId);

        WorkerId srcNodeId = 3;
        properties["slots"] = 1;
        topologyForILP->registerTopologyNode(srcNodeId, "localhost", 123, 124, 1, properties);
        topologyForILP->addTopologyNodeAsChild(middleNodeId, srcNodeId);

        topologyForILP->addLinkProperty(middleNodeId, srcNodeId, 512, 100);
        topologyForILP->addLinkProperty(rootNodeId, middleNodeId, 512, 100);

        auto schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
        const std::string sourceName = "car";

        sourceCatalogForILP = std::make_shared<Catalogs::Source::SourceCatalog>();
        sourceCatalogForILP->addLogicalSource(sourceName, schema);
        auto logicalSource = sourceCatalogForILP->getLogicalSource(sourceName);
        CSVSourceTypePtr csvSourceType = CSVSourceType::create(sourceName, "test2");
        csvSourceType->setGatheringInterval(0);
        csvSourceType->setNumberOfTuplesToProducePerBuffer(0);
        auto physicalSource = PhysicalSource::create(csvSourceType);
        auto sourceCatalogEntry1 =
            Catalogs::Source::SourceCatalogEntry::create(physicalSource, logicalSource, srcNodeId);
        sourceCatalogForILP->addPhysicalSource(sourceName, sourceCatalogEntry1);
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

    Catalogs::Source::SourceCatalogPtr sourceCatalogForILP;
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
            std::stringstream mString;
            mString << m;
            NES_DEBUG("{}", mString.str());
            NES_DEBUG("-------------------------------");
            if (m.eval(P21).get_numeral_int() == 1) {
                NES_DEBUG("Operator on Node 1");
            } else if (m.eval(P22).get_numeral_int() == 1) {
                NES_DEBUG("Operator on Node 2");
            } else if (m.eval(P23).get_numeral_int() == 1) {
                NES_DEBUG("Operator on Node 3");
            }
            NES_DEBUG("-------------------------------");
            break;
        } else {
            break;
        }
    }
}

//TODO: Enable with #4453
/* Test query placement with ILP strategy - simple filter query */
TEST_F(ILPPlacementTest, DISABLED_testPlacingFilterQueryWithILPStrategy) {
    setupTopologyAndSourceCatalogForILP();

    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalogForILP, udfCatalog);
    auto queryPlacementPhase =
        Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan, topologyForILP, typeInferencePhase, coordinatorConfiguration);

    //Prepare query plan
    Query query = Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan->setQueryId(PlanIdGenerator::getNextQueryId());
    for (const auto& sink : queryPlan->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperatorNode>());
    }
    queryPlan->setPlacementStrategy(Optimizer::PlacementStrategy::ILP);
    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topologyForILP,
                                                             sourceCatalogForILP,
                                                             Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getId();

    //Perform placement
    queryPlacementPhase->execute(sharedQueryPlan);

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
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
        }
    }
}

//TODO: Enable with #4453
/* Test query placement with ILP strategy - window query */
TEST_F(ILPPlacementTest, DISABLED_testPlacingWindowQueryWithILPStrategy) {
    setupTopologyAndSourceCatalogForILP();

    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalogForILP, udfCatalog);
    auto queryPlacementPhase =
        Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan, topologyForILP, typeInferencePhase, coordinatorConfiguration);

    //Prepare query plan
    Query query = Query::from("car")
                      .window(TumblingWindow::of(EventTime(Attribute("id")), Seconds(1)))
                      .byKey(Attribute("id"))
                      .apply(Sum(Attribute("value")))
                      .sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan->setQueryId(PlanIdGenerator::getNextQueryId());
    for (const auto& sink : queryPlan->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperatorNode>());
    }
    queryPlan->setPlacementStrategy(Optimizer::PlacementStrategy::ILP);
    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topologyForILP,
                                                             sourceCatalogForILP,
                                                             Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);
    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getId();

    //Perform placement
    queryPlacementPhase->execute(sharedQueryPlan);
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
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == 2) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            // assertion for window and watermark operator being placed after source
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<WindowOperatorNode>());
            EXPECT_TRUE(
                actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<WatermarkAssignerLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]
                            ->getChildren()[0]
                            ->getChildren()[0]
                            ->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == 3) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
        }
    }
}

//TODO: Enable with #4453
/* Test query placement with ILP strategy - window query with sliding window */
TEST_F(ILPPlacementTest, DISABLED_testPlacingSlidingWindowQueryWithILPStrategy) {
    setupTopologyAndSourceCatalogForILP();

    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalogForILP, udfCatalog);
    auto queryPlacementPhase =
        Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan, topologyForILP, typeInferencePhase, coordinatorConfiguration);

    //Prepare query plan
    Query query = Query::from("car")
                      .window(SlidingWindow::of(EventTime(Attribute("id")), Seconds(1), Milliseconds(100)))
                      .byKey(Attribute("id"))
                      .apply(Sum(Attribute("value")))
                      .sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan->setQueryId(PlanIdGenerator::getNextQueryId());
    for (const auto& sink : queryPlan->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperatorNode>());
    }
    queryPlan->setPlacementStrategy(Optimizer::PlacementStrategy::ILP);
    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topologyForILP,
                                                             sourceCatalogForILP,
                                                             Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);
    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getId();

    //Perform placement
    queryPlacementPhase->execute(sharedQueryPlan);
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
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == 2) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            // assertion for window and watermark operator being placed after source
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<WindowOperatorNode>());
            EXPECT_TRUE(
                actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<WatermarkAssignerLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]
                            ->getChildren()[0]
                            ->getChildren()[0]
                            ->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == 3) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
        }
    }
}

//TODO: Enable with #4453
/* Test query placement with ILP strategy - simple map query */
TEST_F(ILPPlacementTest, DISABLED_testPlacingMapQueryWithILPStrategy) {

    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    setupTopologyAndSourceCatalogForILP();

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalogForILP, udfCatalog);
    auto queryPlacementPhase =
        Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan, topologyForILP, typeInferencePhase, coordinatorConfiguration);

    //Prepare query to place
    Query query = Query::from("car")
                      .map(Attribute("c") = Attribute("value") + 2)
                      .map(Attribute("d") = Attribute("value") * 2)
                      .sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan->setQueryId(PlanIdGenerator::getNextQueryId());
    queryPlan->setPlacementStrategy(Optimizer::PlacementStrategy::ILP);

    for (const auto& sink : queryPlan->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperatorNode>());
    }
    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topologyForILP,
                                                             sourceCatalogForILP,
                                                             Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getId();

    //Perform placement
    queryPlacementPhase->execute(sharedQueryPlan);

    //Assertion
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);
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
            EXPECT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<MapLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<MapLogicalOperatorNode>());
        }
    }
}

//TODO: Enable with #4453
/* Test query placement with ILP strategy - simple query of source - filter - map - sink */
TEST_F(ILPPlacementTest, DISABLED_testPlacingQueryWithILPStrategy) {

    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    setupTopologyAndSourceCatalogForILP();

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalogForILP, udfCatalog);
    auto queryPlacementPhase =
        Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan, topologyForILP, typeInferencePhase, coordinatorConfiguration);

    Query query = Query::from("car")
                      .filter(Attribute("id") < 45)
                      .map(Attribute("c") = Attribute("value") * 2)
                      .sink(PrintSinkDescriptor::create());

    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan->setQueryId(PlanIdGenerator::getNextQueryId());
    queryPlan->setPlacementStrategy(Optimizer::PlacementStrategy::ILP);

    for (const auto& sink : queryPlan->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperatorNode>());
    }
    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topologyForILP,
                                                             sourceCatalogForILP,
                                                             Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getId();

    queryPlacementPhase->execute(sharedQueryPlan);
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
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<MapLogicalOperatorNode>());
        }
    }
}

//TODO: Enable with #4453
/* Test incremental query placement with ILP strategy - simple query of source - filter - map - sink and then added filter - sink to filter operator*/
TEST_F(ILPPlacementTest, DISABLED_testPlacingUpdatedSharedQueryPlanWithILPStrategy) {

    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    coordinatorConfiguration->enableQueryReconfiguration = true;

    // Setup topology and source catalog
    setupTopologyAndSourceCatalogForILP();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalogForILP, udfCatalog);

    // Create queries
    Query query1 = Query::from("car")
                       .filter(Attribute("id") < 45)
                       .map(Attribute("c") = Attribute("value") * 2)
                       .sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    queryPlan1->setQueryId(PlanIdGenerator::getNextQueryId());
    queryPlan1->setPlacementStrategy(Optimizer::PlacementStrategy::ILP);
    for (const auto& sink : queryPlan1->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperatorNode>());
    }

    Query query2 = Query::from("car")
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") > Attribute("value") * 2)
                       .sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    queryPlan2->setQueryId(PlanIdGenerator::getNextQueryId());
    queryPlan2->setPlacementStrategy(Optimizer::PlacementStrategy::ILP);
    for (const auto& sink : queryPlan2->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperatorNode>());
    }

    // Perform signature computation
    z3::ContextPtr context = std::make_shared<z3::context>();
    auto signatureInferencePhase =
        Optimizer::SignatureInferencePhase::create(context,
                                                   Optimizer::QueryMergerRule::HashSignatureBasedCompleteQueryMergerRule);
    signatureInferencePhase->execute(queryPlan1);
    signatureInferencePhase->execute(queryPlan2);

    // Apply topology specific rewrite rules
    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topologyForILP,
                                                             sourceCatalogForILP,
                                                             Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan1);
    topologySpecificQueryRewrite->execute(queryPlan2);
    typeInferencePhase->execute(queryPlan2);

    //Add query plan to global query plan
    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);

    coordinatorConfiguration->optimizer.queryMergerRule = Optimizer::QueryMergerRule::HashSignatureBasedPartialQueryMergerRule;

    //Merge queries together
    auto queryMergerPhase = Optimizer::QueryMergerPhase::create(this->z3Context, coordinatorConfiguration->optimizer);
    queryMergerPhase->execute(globalQueryPlan);

    //Fetch the share query plan to place
    auto sharedQueryPlansToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();

    //Assertion to check correct amount of shared query plans to deploy are extracted.
    ASSERT_EQ(sharedQueryPlansToDeploy.size(), 1l);

    //Place the shared query plan
    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto queryPlacementPhase =
        Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan, topologyForILP, typeInferencePhase, coordinatorConfiguration);
    queryPlacementPhase->execute(sharedQueryPlansToDeploy[0]);
    SharedQueryId sharedQueryPlanId = sharedQueryPlansToDeploy[0]->getId();
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(sharedQueryPlanId);

    //Assertions to check correct placement
    ASSERT_EQ(executionNodes.size(), 3U);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1) {
            // filter should be placed on source node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(sharedQueryPlanId);
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
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(sharedQueryPlanId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<MapLogicalOperatorNode>());
        }
    }

    //Here we add two partially equivalent queries to build a shared query plan to generate change logs
    // Add the new operators to the query plan
    globalQueryPlan->addQueryPlan(queryPlan2);

    //Merge queries together
    queryMergerPhase->execute(globalQueryPlan);

    //Fetch the share query plan to place
    sharedQueryPlansToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();

    //Assertion to check correct amount of shared query plans to deploy are extracted.
    ASSERT_EQ(sharedQueryPlansToDeploy.size(), 1l);

    NES_INFO("{}", sharedQueryPlansToDeploy[0]->getQueryPlan()->toString());
    queryPlacementPhase->execute(sharedQueryPlansToDeploy[0]);
    sharedQueryPlanId = sharedQueryPlansToDeploy[0]->getId();
    executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(sharedQueryPlanId);

    //Assertions to check correct placement
    ASSERT_EQ(executionNodes.size(), 3U);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1) {
            // filter should be placed on source node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(sharedQueryPlanId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 2U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == 2) {
            // map should be placed on cloud node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(sharedQueryPlanId);
            ASSERT_EQ(querySubPlans.size(), 2U);
            //Assertion for first subquery plan
            auto querySubPlan1 = querySubPlans[0U];
            std::vector<OperatorNodePtr> rootOperatorsForPlan1 = querySubPlan1->getRootOperators();
            ASSERT_EQ(rootOperatorsForPlan1.size(), 1U);
            OperatorNodePtr rootOperator1 = rootOperatorsForPlan1[0];
            EXPECT_TRUE(rootOperator1->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(rootOperator1->getChildren().size(), 1U);
            EXPECT_TRUE(rootOperator1->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());

            //Assertion for second subquery plan
            auto querySubPlan2 = querySubPlans[1U];
            std::vector<OperatorNodePtr> rootOperatorsForPlan2 = querySubPlan2->getRootOperators();
            ASSERT_EQ(rootOperatorsForPlan2.size(), 1U);
            OperatorNodePtr rootOperator2 = rootOperatorsForPlan2[0];
            EXPECT_TRUE(rootOperator2->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(rootOperator2->getChildren().size(), 1U);
            EXPECT_TRUE(rootOperator2->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
        } else if (executionNode->getId() == 3) {
            // map should be placed on cloud node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(sharedQueryPlanId);
            ASSERT_EQ(querySubPlans.size(), 2U);
            //Assertion for first subquery plan
            auto querySubPlan1 = querySubPlans[0U];
            std::vector<OperatorNodePtr> rootOperatorsForPlan1 = querySubPlan1->getRootOperators();
            ASSERT_EQ(rootOperatorsForPlan1.size(), 1U);
            OperatorNodePtr rootOperator1 = rootOperatorsForPlan1[0];
            ASSERT_EQ(rootOperator1->getId(), queryPlan1->getRootOperators()[0]->getId());
            EXPECT_TRUE(rootOperator1->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(rootOperator1->getChildren().size(), 1U);
            EXPECT_TRUE(rootOperator1->getChildren()[0]->instanceOf<MapLogicalOperatorNode>());

            //Assertion for second subquery plan
            auto querySubPlan2 = querySubPlans[1U];
            std::vector<OperatorNodePtr> rootOperatorsForPlan2 = querySubPlan2->getRootOperators();
            ASSERT_EQ(rootOperatorsForPlan2.size(), 1U);
            OperatorNodePtr rootOperator2 = rootOperatorsForPlan2[0];
            ASSERT_EQ(rootOperator2->getId(), queryPlan2->getRootOperators()[0]->getId());
            EXPECT_TRUE(rootOperator2->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(rootOperator2->getChildren().size(), 1U);
            EXPECT_TRUE(rootOperator2->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
        }
    }
}

//TODO: Enable with #4453
/* Test incremental query placement with ILP strategy - simple query of source - filter - map - sink and then added map - sink and filter - sink to the filter operator*/
TEST_F(ILPPlacementTest, DISABLED_testPlacingMulitpleUpdatesOnASharedQueryPlanWithILPStrategy) {

    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    coordinatorConfiguration->enableQueryReconfiguration = true;

    // Setup topology and source catalog
    setupTopologyAndSourceCatalogForILP();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalogForILP, udfCatalog);

    // Create queries
    Query query1 = Query::from("car")
                       .filter(Attribute("id") < 45)
                       .map(Attribute("c") = Attribute("value") * 2)
                       .sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    queryPlan1->setQueryId(PlanIdGenerator::getNextQueryId());
    queryPlan1->setPlacementStrategy(Optimizer::PlacementStrategy::ILP);
    for (const auto& sink : queryPlan1->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperatorNode>());
    }

    Query query2 = Query::from("car")
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") > Attribute("value") * 2)
                       .sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    queryPlan2->setQueryId(PlanIdGenerator::getNextQueryId());
    queryPlan2->setPlacementStrategy(Optimizer::PlacementStrategy::ILP);
    for (const auto& sink : queryPlan2->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperatorNode>());
    }

    Query query3 = Query::from("car")
                       .filter(Attribute("id") < 45)
                       .map(Attribute("b") = Attribute("value") * 2)
                       .sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan3 = query3.getQueryPlan();
    queryPlan3->setQueryId(PlanIdGenerator::getNextQueryId());
    queryPlan3->setPlacementStrategy(Optimizer::PlacementStrategy::ILP);
    for (const auto& sink : queryPlan3->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperatorNode>());
    }

    // Perform signature computation
    z3::ContextPtr context = std::make_shared<z3::context>();
    auto signatureInferencePhase =
        Optimizer::SignatureInferencePhase::create(context,
                                                   Optimizer::QueryMergerRule::HashSignatureBasedCompleteQueryMergerRule);
    signatureInferencePhase->execute(queryPlan1);
    signatureInferencePhase->execute(queryPlan2);
    signatureInferencePhase->execute(queryPlan3);

    // Apply topology specific rewrite rules
    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topologyForILP,
                                                             sourceCatalogForILP,
                                                             Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan1);
    topologySpecificQueryRewrite->execute(queryPlan2);
    typeInferencePhase->execute(queryPlan2);
    topologySpecificQueryRewrite->execute(queryPlan3);
    typeInferencePhase->execute(queryPlan3);

    //Add query plan to global query plan
    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);

    coordinatorConfiguration->optimizer.queryMergerRule = Optimizer::QueryMergerRule::HashSignatureBasedPartialQueryMergerRule;

    //Merge queries together
    auto queryMergerPhase = Optimizer::QueryMergerPhase::create(this->z3Context, coordinatorConfiguration->optimizer);
    queryMergerPhase->execute(globalQueryPlan);

    //Fetch the share query plan to place
    auto sharedQueryPlansToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();

    //Assertion to check correct amount of shared query plans to deploy are extracted.
    ASSERT_EQ(sharedQueryPlansToDeploy.size(), 1l);

    //Place the shared query plan
    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto queryPlacementPhase =
        Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan, topologyForILP, typeInferencePhase, coordinatorConfiguration);
    queryPlacementPhase->execute(sharedQueryPlansToDeploy[0]);
    SharedQueryId sharedQueryPlanId = sharedQueryPlansToDeploy[0]->getId();
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(sharedQueryPlanId);

    //Assertions to check correct placement
    ASSERT_EQ(executionNodes.size(), 3U);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1) {
            // filter should be placed on source node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(sharedQueryPlanId);
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
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(sharedQueryPlanId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<MapLogicalOperatorNode>());
        }
    }

    //Here we add two partially equivalent queries to build a shared query plan to generate change logs
    // Add the new query to the global query plan
    globalQueryPlan->addQueryPlan(queryPlan2);
    globalQueryPlan->addQueryPlan(queryPlan3);

    //Merge queries together
    queryMergerPhase->execute(globalQueryPlan);

    //Fetch the share query plan to place
    sharedQueryPlansToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();

    //Assertion to check correct amount of shared query plans to deploy are extracted.
    ASSERT_EQ(sharedQueryPlansToDeploy.size(), 1l);

    NES_INFO("{}", sharedQueryPlansToDeploy[0]->getQueryPlan()->toString());
    queryPlacementPhase->execute(sharedQueryPlansToDeploy[0]);
    sharedQueryPlanId = sharedQueryPlansToDeploy[0]->getId();
    executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(sharedQueryPlanId);

    NES_INFO("{}", globalExecutionPlan->getAsString());

    //Assertions to check correct placement
    ASSERT_EQ(executionNodes.size(), 3U);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1) {
            // filter should be placed on source node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(sharedQueryPlanId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 3U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == 2) {
            // map should be placed on cloud node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(sharedQueryPlanId);
            ASSERT_EQ(querySubPlans.size(), 3U);
            //Assertion for first subquery plan
            auto querySubPlan1 = querySubPlans[0U];
            std::vector<OperatorNodePtr> rootOperatorsForPlan1 = querySubPlan1->getRootOperators();
            ASSERT_EQ(rootOperatorsForPlan1.size(), 1U);
            OperatorNodePtr rootOperator1 = rootOperatorsForPlan1[0];
            EXPECT_TRUE(rootOperator1->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(rootOperator1->getChildren().size(), 1U);
            EXPECT_TRUE(rootOperator1->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());

            //Assertion for second subquery plan
            auto querySubPlan2 = querySubPlans[1U];
            std::vector<OperatorNodePtr> rootOperatorsForPlan2 = querySubPlan2->getRootOperators();
            ASSERT_EQ(rootOperatorsForPlan2.size(), 1U);
            OperatorNodePtr rootOperator2 = rootOperatorsForPlan2[0];
            EXPECT_TRUE(rootOperator2->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(rootOperator2->getChildren().size(), 1U);
            EXPECT_TRUE(rootOperator2->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
        } else if (executionNode->getId() == 3) {
            // map should be placed on cloud node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(sharedQueryPlanId);
            ASSERT_EQ(querySubPlans.size(), 3U);

            for (const auto& querySubPlan : querySubPlans) {

                //Assertion for all subquery plans
                std::vector<OperatorNodePtr> rootOperatorsForPlan = querySubPlan->getRootOperators();
                ASSERT_EQ(rootOperatorsForPlan.size(), 1U);
                OperatorNodePtr rootOperator = rootOperatorsForPlan[0];

                if (rootOperator->getId() == queryPlan1->getRootOperators()[0]->getId()) {
                    EXPECT_TRUE(rootOperator->instanceOf<SinkLogicalOperatorNode>());
                    ASSERT_EQ(rootOperator->getChildren().size(), 1U);
                    EXPECT_TRUE(rootOperator->getChildren()[0]->instanceOf<MapLogicalOperatorNode>());
                } else if (rootOperator->getId() == queryPlan2->getRootOperators()[0]->getId()) {
                    EXPECT_TRUE(rootOperator->instanceOf<SinkLogicalOperatorNode>());
                    ASSERT_EQ(rootOperator->getChildren().size(), 1U);
                    EXPECT_TRUE(rootOperator->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
                } else if (rootOperator->getId() == queryPlan3->getRootOperators()[0]->getId()) {
                    EXPECT_TRUE(rootOperator->instanceOf<SinkLogicalOperatorNode>());
                    ASSERT_EQ(rootOperator->getChildren().size(), 1U);
                    EXPECT_TRUE(rootOperator->getChildren()[0]->instanceOf<MapLogicalOperatorNode>());
                } else {
                    //Sub query plan should contain for one of the three sink operator as its root
                    FAIL();
                }
            }
        }
    }
}

//TODO: as part of issue number #2939
/* Test query placement with ILP strategy for query: source - filter - map - sink and then added map - sink and filter - sink to the filter operator*/
TEST_F(ILPPlacementTest, DISABLED_testPlacingMultipleSinkSharedQueryPlanWithILPStrategy) {

    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    coordinatorConfiguration->enableQueryReconfiguration = true;

    // Setup topology and source catalog
    setupTopologyAndSourceCatalogForILP();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalogForILP, udfCatalog);

    // Create queries
    Query query1 = Query::from("car")
                       .filter(Attribute("id") < 45)
                       .map(Attribute("c") = Attribute("value") * 2)
                       .sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    queryPlan1->setQueryId(PlanIdGenerator::getNextQueryId());
    queryPlan1->setPlacementStrategy(Optimizer::PlacementStrategy::ILP);
    for (const auto& sink : queryPlan1->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperatorNode>());
    }

    Query query2 = Query::from("car")
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") > Attribute("value") * 2)
                       .sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    queryPlan2->setQueryId(PlanIdGenerator::getNextQueryId());
    queryPlan2->setPlacementStrategy(Optimizer::PlacementStrategy::ILP);
    for (const auto& sink : queryPlan2->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperatorNode>());
    }

    Query query3 = Query::from("car")
                       .filter(Attribute("id") < 45)
                       .map(Attribute("b") = Attribute("value") * 2)
                       .sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan3 = query3.getQueryPlan();
    queryPlan3->setQueryId(PlanIdGenerator::getNextQueryId());
    for (const auto& sink : queryPlan3->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperatorNode>());
    }

    // Perform signature computation
    z3::ContextPtr context = std::make_shared<z3::context>();
    auto signatureInferencePhase =
        Optimizer::SignatureInferencePhase::create(context,
                                                   Optimizer::QueryMergerRule::HashSignatureBasedCompleteQueryMergerRule);
    signatureInferencePhase->execute(queryPlan1);
    signatureInferencePhase->execute(queryPlan2);
    signatureInferencePhase->execute(queryPlan3);

    // Apply topology specific rewrite rules
    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topologyForILP,
                                                             sourceCatalogForILP,
                                                             Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan1);
    topologySpecificQueryRewrite->execute(queryPlan2);
    typeInferencePhase->execute(queryPlan2);
    topologySpecificQueryRewrite->execute(queryPlan3);
    typeInferencePhase->execute(queryPlan3);

    //Add query plan to global query plan
    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);
    globalQueryPlan->addQueryPlan(queryPlan3);

    coordinatorConfiguration->optimizer.queryMergerRule = Optimizer::QueryMergerRule::HashSignatureBasedPartialQueryMergerRule;

    //Merge queries together
    auto queryMergerPhase = Optimizer::QueryMergerPhase::create(this->z3Context, coordinatorConfiguration->optimizer);
    queryMergerPhase->execute(globalQueryPlan);

    //Fetch the share query plan to place
    auto sharedQueryPlansToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();

    //Assertion to check correct amount of shared query plans to deploy are extracted.
    ASSERT_EQ(sharedQueryPlansToDeploy.size(), 1l);

    //Place the shared query plan
    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto queryPlacementPhase =
        Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan, topologyForILP, typeInferencePhase, coordinatorConfiguration);
    queryPlacementPhase->execute(sharedQueryPlansToDeploy[0]);
    SharedQueryId sharedQueryPlanId = sharedQueryPlansToDeploy[0]->getId();
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(sharedQueryPlanId);

    NES_INFO("{}", globalExecutionPlan->getAsString());

    //Assertions to check correct placement
    ASSERT_EQ(executionNodes.size(), 3U);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1) {
            // filter should be placed on source node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(sharedQueryPlanId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 3U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == 2) {
            // map should be placed on cloud node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(sharedQueryPlanId);
            ASSERT_EQ(querySubPlans.size(), 3U);
            //Assertion for first subquery plan
            auto querySubPlan1 = querySubPlans[0U];
            std::vector<OperatorNodePtr> rootOperatorsForPlan1 = querySubPlan1->getRootOperators();
            ASSERT_EQ(rootOperatorsForPlan1.size(), 1U);
            OperatorNodePtr rootOperator1 = rootOperatorsForPlan1[0];
            EXPECT_TRUE(rootOperator1->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(rootOperator1->getChildren().size(), 1U);
            EXPECT_TRUE(rootOperator1->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());

            //Assertion for second subquery plan
            auto querySubPlan2 = querySubPlans[1U];
            std::vector<OperatorNodePtr> rootOperatorsForPlan2 = querySubPlan2->getRootOperators();
            ASSERT_EQ(rootOperatorsForPlan2.size(), 1U);
            OperatorNodePtr rootOperator2 = rootOperatorsForPlan2[0];
            EXPECT_TRUE(rootOperator2->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(rootOperator2->getChildren().size(), 1U);
            EXPECT_TRUE(rootOperator2->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
        } else if (executionNode->getId() == 3) {
            // map should be placed on cloud node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(sharedQueryPlanId);
            ASSERT_EQ(querySubPlans.size(), 3U);
            //Assertion for first subquery plan
            auto querySubPlan1 = querySubPlans[0U];
            std::vector<OperatorNodePtr> rootOperatorsForPlan1 = querySubPlan1->getRootOperators();
            ASSERT_EQ(rootOperatorsForPlan1.size(), 1U);
            OperatorNodePtr rootOperator1 = rootOperatorsForPlan1[0];
            ASSERT_EQ(rootOperator1->getId(), queryPlan1->getRootOperators()[0]->getId());
            EXPECT_TRUE(rootOperator1->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(rootOperator1->getChildren().size(), 1U);
            EXPECT_TRUE(rootOperator1->getChildren()[0]->instanceOf<MapLogicalOperatorNode>());

            //Assertion for second subquery plan
            auto querySubPlan2 = querySubPlans[1U];
            std::vector<OperatorNodePtr> rootOperatorsForPlan2 = querySubPlan2->getRootOperators();
            ASSERT_EQ(rootOperatorsForPlan2.size(), 1U);
            OperatorNodePtr rootOperator2 = rootOperatorsForPlan2[0];
            ASSERT_EQ(rootOperator2->getId(), queryPlan2->getRootOperators()[0]->getId());
            EXPECT_TRUE(rootOperator2->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(rootOperator2->getChildren().size(), 1U);
            EXPECT_TRUE(rootOperator2->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());

            //Assertion for second subquery plan
            auto querySubPlan3 = querySubPlans[2U];
            std::vector<OperatorNodePtr> rootOperatorsForPlan3 = querySubPlan3->getRootOperators();
            ASSERT_EQ(rootOperatorsForPlan3.size(), 1U);
            OperatorNodePtr rootOperator3 = rootOperatorsForPlan3[0];
            ASSERT_EQ(rootOperator3->getId(), queryPlan3->getRootOperators()[0]->getId());
            EXPECT_TRUE(rootOperator3->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(rootOperator3->getChildren().size(), 1U);
            EXPECT_TRUE(rootOperator3->getChildren()[0]->instanceOf<MapLogicalOperatorNode>());
        }
    }
}