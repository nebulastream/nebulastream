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
#include <Operators/LogicalOperators/LogicalFilterOperator.hpp>
#include <Operators/LogicalOperators/LogicalMapOperator.hpp>
#include <Operators/LogicalOperators/LogicalProjectionOperator.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Operators/LogicalOperators/LogicalUnionOperator.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkAssignerLogicalOperator.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinOperator.hpp>
#include <Operators/LogicalOperators/Windows/WindowOperator.hpp>
#include <Optimizer/Phases/QueryMergerPhase.hpp>
#include <Optimizer/Phases/QueryPlacementAmendmentPhase.hpp>
#include <Optimizer/Phases/SignatureInferencePhase.hpp>
#include <Optimizer/Phases/TopologySpecificQueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryPlacementAddition/BasePlacementAdditionStrategy.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
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

        auto rootNodeId = WorkerId(1);
        topologyForILP->registerWorker(rootNodeId, "localhost", 123, 124, 100, properties, 0,0);
        topologyForILP->addAsRootWorkerId(rootNodeId);

        auto middleNodeId = WorkerId(2);
        topologyForILP->registerWorker(middleNodeId, "localhost", 123, 124, 10, properties, 0,0);
        topologyForILP->addTopologyNodeAsChild(rootNodeId, middleNodeId);

        auto srcNodeId = WorkerId(3);
        topologyForILP->registerWorker(srcNodeId, "localhost", 123, 124, 3, properties, 0,0);
        topologyForILP->removeTopologyNodeAsChild(rootNodeId, srcNodeId);
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
        auto sourceCatalogEntry1 = Catalogs::Source::SourceCatalogEntry::create(physicalSource, logicalSource, srcNodeId);
        sourceCatalogForILP->addPhysicalSource(sourceName, sourceCatalogEntry1);
    }

    void assignOperatorPropertiesRecursive(LogicalOperatorPtr operatorNode) {
        int cost = 1;
        double dmf = 1;
        double input = 0;

        for (const auto& child : operatorNode->getChildren()) {
            LogicalOperatorPtr op = child->as<LogicalOperator>();
            assignOperatorPropertiesRecursive(op);
            std::any output = op->getProperty("output");
            input += std::any_cast<double>(output);
        }

        NodePtr nodePtr = operatorNode->as<Node>();
        if (operatorNode->instanceOf<SinkLogicalOperator>()) {
            dmf = 0;
            cost = 0;
        } else if (operatorNode->instanceOf<LogicalFilterOperator>()) {
            dmf = 0.5;
            cost = 1;
        } else if (operatorNode->instanceOf<LogicalMapOperator>()) {
            dmf = 2;
            cost = 2;
        } else if (operatorNode->instanceOf<LogicalJoinOperator>()) {
            cost = 2;
        } else if (operatorNode->instanceOf<LogicalUnionOperator>()) {
            cost = 2;
        } else if (operatorNode->instanceOf<LogicalProjectionOperator>()) {
            cost = 1;
        } else if (operatorNode->instanceOf<SourceLogicalOperator>()) {
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

/* Test query placement with ILP strategy - simple filter query */
TEST_F(ILPPlacementTest, testPlacingFilterQueryWithILPStrategy) {
    setupTopologyAndSourceCatalogForILP();

    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();

    auto globalExecutionPlan = Optimizer::GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalogForILP, udfCatalog);
    auto queryPlacementAmendmentPhase = Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan,
                                                                                        topologyForILP,
                                                                                        typeInferencePhase,
                                                                                        coordinatorConfiguration);

    //Prepare query plan
    Query query = Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan->setQueryId(PlanIdGenerator::getNextQueryId());
    for (const auto& sink : queryPlan->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperator>());
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
    queryPlacementAmendmentPhase->execute(sharedQueryPlan);

    auto lockedExecutionNodes = globalExecutionPlan->getLockedExecutionNodesHostingSharedQueryId(queryId);

    //Assertion
    ASSERT_EQ(lockedExecutionNodes.size(), 3U);
    for (const auto& executionNode : lockedExecutionNodes) {
        if (executionNode->operator*()->getId() == WorkerId(3)) {
            // place filter on source node
            auto decomposedQueryPlans = executionNode->operator*()->getAllDecomposedQueryPlans(queryId);
            ASSERT_EQ(decomposedQueryPlans.size(), 1U);
            auto decomposedQueryPlan = decomposedQueryPlans[0U];
            std::vector<OperatorPtr> actualRootOperators = decomposedQueryPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorPtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<LogicalFilterOperator>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<SourceLogicalOperator>());
        } else if (executionNode->operator*()->getId() == WorkerId(1)) {
            auto decomposedQueryPlans = executionNode->operator*()->getAllDecomposedQueryPlans(queryId);
            ASSERT_EQ(decomposedQueryPlans.size(), 1U);
            auto decomposedQueryPlan = decomposedQueryPlans[0U];
            std::vector<OperatorPtr> actualRootOperators = decomposedQueryPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorPtr actualRootOperator = actualRootOperators[0];
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperator>());
        }
    }
}

/* Test query placement with ILP strategy with a window query */
TEST_F(ILPPlacementTest, testPlacingWindowQueryWithILPStrategy) {
    setupTopologyAndSourceCatalogForILP();

    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();

    auto globalExecutionPlan = Optimizer::GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalogForILP, udfCatalog);
    auto queryPlacementAmendmentPhase = Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan,
                                                                                        topologyForILP,
                                                                                        typeInferencePhase,
                                                                                        coordinatorConfiguration);

    //Prepare query plan
    Query query = Query::from("car")
                      .window(TumblingWindow::of(EventTime(Attribute("id")), Seconds(1)))
                      .byKey(Attribute("id"))
                      .apply(Sum(Attribute("value")))
                      .sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan->setQueryId(PlanIdGenerator::getNextQueryId());
    for (const auto& sink : queryPlan->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperator>());
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
    queryPlacementAmendmentPhase->execute(sharedQueryPlan);
    auto lockedExecutionNodes = globalExecutionPlan->getLockedExecutionNodesHostingSharedQueryId(queryId);

    //Assertion
    ASSERT_EQ(lockedExecutionNodes.size(), 3U);
    for (const auto& executionNode : lockedExecutionNodes) {
        if (executionNode->operator*()->getId() == WorkerId(1)) {
            auto decomposedQueryPlans = executionNode->operator*()->getAllDecomposedQueryPlans(queryId);
            ASSERT_EQ(decomposedQueryPlans.size(), 1U);
            auto decomposedQueryPlan = decomposedQueryPlans[0U];
            std::vector<OperatorPtr> actualRootOperators = decomposedQueryPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorPtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperator>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<SourceLogicalOperator>());
        } else if (executionNode->operator*()->getId() == WorkerId(2)) {
            auto decomposedQueryPlans = executionNode->operator*()->getAllDecomposedQueryPlans(queryId);
            ASSERT_EQ(decomposedQueryPlans.size(), 1U);
            auto decomposedQueryPlan = decomposedQueryPlans[0U];
            std::vector<OperatorPtr> actualRootOperators = decomposedQueryPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorPtr actualRootOperator = actualRootOperators[0];
            // assertion for window and watermark operator being placed after source
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<WindowOperator>());
            EXPECT_TRUE(
                actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<WatermarkAssignerLogicalOperator>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]
                            ->getChildren()[0]
                            ->getChildren()[0]
                            ->instanceOf<SourceLogicalOperator>());
        } else if (executionNode->operator*()->getId() == WorkerId(3)) {
            auto decomposedQueryPlans = executionNode->operator*()->getAllDecomposedQueryPlans(queryId);
            ASSERT_EQ(decomposedQueryPlans.size(), 1U);
            auto decomposedQueryPlan = decomposedQueryPlans[0U];
            std::vector<OperatorPtr> actualRootOperators = decomposedQueryPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorPtr actualRootOperator = actualRootOperators[0];
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperator>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<SourceLogicalOperator>());
        }
    }
}

/* Test query placement with ILP strategy with a window query with default parameters*/
TEST_F(ILPPlacementTest, testPlacingWindowQueryWithILPStrategyWithDefaultParameters) {
    setupTopologyAndSourceCatalogForILP();

    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();

    auto globalExecutionPlan = Optimizer::GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalogForILP, udfCatalog);
    auto queryPlacementAmendmentPhase = Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan,
                                                                                        topologyForILP,
                                                                                        typeInferencePhase,
                                                                                        coordinatorConfiguration);

    //Prepare query plan
    Query query = Query::from("car")
                      .window(TumblingWindow::of(EventTime(Attribute("id")), Seconds(1)))
                      .byKey(Attribute("id"))
                      .apply(Sum(Attribute("value")))
                      .sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan->setQueryId(PlanIdGenerator::getNextQueryId());

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
    queryPlacementAmendmentPhase->execute(sharedQueryPlan);
    auto lockedExecutionNodes = globalExecutionPlan->getLockedExecutionNodesHostingSharedQueryId(queryId);

    //Assertion
    ASSERT_EQ(lockedExecutionNodes.size(), 3U);
    for (const auto& executionNode : lockedExecutionNodes) {
        if (executionNode->operator*()->getId() == WorkerId(1)) {
            auto decomposedQueryPlans = executionNode->operator*()->getAllDecomposedQueryPlans(queryId);
            ASSERT_EQ(decomposedQueryPlans.size(), 1U);
            auto decomposedQueryPlan = decomposedQueryPlans[0U];
            std::vector<OperatorPtr> actualRootOperators = decomposedQueryPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorPtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperator>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<SourceLogicalOperator>());
        } else if (executionNode->operator*()->getId() == WorkerId(2)) {
            auto decomposedQueryPlans = executionNode->operator*()->getAllDecomposedQueryPlans(queryId);
            ASSERT_EQ(decomposedQueryPlans.size(), 1U);
            auto decomposedQueryPlan = decomposedQueryPlans[0U];
            std::vector<OperatorPtr> actualRootOperators = decomposedQueryPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorPtr actualRootOperator = actualRootOperators[0];
            // assertion for window and watermark operator being placed after source
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<WindowOperator>());
            EXPECT_TRUE(
                actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<WatermarkAssignerLogicalOperator>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]
                            ->getChildren()[0]
                            ->getChildren()[0]
                            ->instanceOf<SourceLogicalOperator>());
        } else if (executionNode->operator*()->getId() == WorkerId(2)) {
            auto decomposedQueryPlans = executionNode->operator*()->getAllDecomposedQueryPlans(queryId);
            ASSERT_EQ(decomposedQueryPlans.size(), 1U);
            auto decomposedQueryPlan = decomposedQueryPlans[0U];
            std::vector<OperatorPtr> actualRootOperators = decomposedQueryPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorPtr actualRootOperator = actualRootOperators[0];
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperator>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<SourceLogicalOperator>());
        }
    }
}

TEST_F(ILPPlacementTest, testPlacingSlidingWindowQueryWithILPStrategy) {
    setupTopologyAndSourceCatalogForILP();

    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();

    auto globalExecutionPlan = Optimizer::GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalogForILP, udfCatalog);
    auto queryPlacementAmendmentPhase = Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan,
                                                                                        topologyForILP,
                                                                                        typeInferencePhase,
                                                                                        coordinatorConfiguration);

    //Prepare query plan
    Query query = Query::from("car")
                      .window(SlidingWindow::of(EventTime(Attribute("id")), Seconds(1), Milliseconds(100)))
                      .byKey(Attribute("id"))
                      .apply(Sum(Attribute("value")))
                      .sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan->setQueryId(PlanIdGenerator::getNextQueryId());
    for (const auto& sink : queryPlan->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperator>());
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
    queryPlacementAmendmentPhase->execute(sharedQueryPlan);
    auto executionNodes = globalExecutionPlan->getLockedExecutionNodesHostingSharedQueryId(queryId);

    //Assertion
    ASSERT_EQ(executionNodes.size(), 3U);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->operator*()->getId() == WorkerId(1)) {
            auto decomposedQueryPlans = executionNode->operator*()->getAllDecomposedQueryPlans(queryId);
            ASSERT_EQ(decomposedQueryPlans.size(), 1U);
            auto decomposedQueryPlan = decomposedQueryPlans[0U];
            std::vector<OperatorPtr> actualRootOperators = decomposedQueryPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorPtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperator>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<SourceLogicalOperator>());
        } else if (executionNode->operator*()->getId() == WorkerId(2)) {
            auto decomposedQueryPlans = executionNode->operator*()->getAllDecomposedQueryPlans(queryId);
            ASSERT_EQ(decomposedQueryPlans.size(), 1U);
            auto decomposedQueryPlan = decomposedQueryPlans[0U];
            std::vector<OperatorPtr> actualRootOperators = decomposedQueryPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorPtr actualRootOperator = actualRootOperators[0];
            // assertion for window and watermark operator being placed after source
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<WindowOperator>());
            EXPECT_TRUE(
                actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<WatermarkAssignerLogicalOperator>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]
                            ->getChildren()[0]
                            ->getChildren()[0]
                            ->instanceOf<SourceLogicalOperator>());
        } else if (executionNode->operator*()->getId() == WorkerId(3)) {
            auto decomposedQueryPlans = executionNode->operator*()->getAllDecomposedQueryPlans(queryId);
            ASSERT_EQ(decomposedQueryPlans.size(), 1U);
            auto decomposedQueryPlan = decomposedQueryPlans[0U];
            std::vector<OperatorPtr> actualRootOperators = decomposedQueryPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorPtr actualRootOperator = actualRootOperators[0];
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperator>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<SourceLogicalOperator>());
        }
    }
}


/* Test query placement with ILP strategy - simple map query */
TEST_F(ILPPlacementTest, testPlacingMapQueryWithILPStrategy) {

    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    setupTopologyAndSourceCatalogForILP();

    auto globalExecutionPlan = Optimizer::GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalogForILP, udfCatalog);
    auto queryPlacementAmendmentPhase = Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan,
                                                                                        topologyForILP,
                                                                                        typeInferencePhase,
                                                                                        coordinatorConfiguration);

    //Prepare query to place
    Query query = Query::from("car")
                      .map(Attribute("c") = Attribute("value") + 2)
                      .map(Attribute("d") = Attribute("value") * 2)
                      .sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan->setQueryId(PlanIdGenerator::getNextQueryId());
    queryPlan->setPlacementStrategy(Optimizer::PlacementStrategy::ILP);

    for (const auto& sink : queryPlan->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperator>());
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
    queryPlacementAmendmentPhase->execute(sharedQueryPlan);

    //Assertion
    auto executionNodes = globalExecutionPlan->getLockedExecutionNodesHostingSharedQueryId(queryId);
    ASSERT_EQ(executionNodes.size(), 3U);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->operator*()->getId() == WorkerId(3)) {
            auto decomposedQueryPlans = executionNode->operator*()->getAllDecomposedQueryPlans(queryId);
            ASSERT_EQ(decomposedQueryPlans.size(), 1U);
            auto decomposedQueryPlan = decomposedQueryPlans[0U];
            std::vector<OperatorPtr> actualRootOperators = decomposedQueryPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorPtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<SourceLogicalOperator>());
        } else if (executionNode->operator*()->getId() == WorkerId(1)) {
            // both map operators should be placed on cloud node
            auto decomposedQueryPlans = executionNode->operator*()->getAllDecomposedQueryPlans(queryId);
            ASSERT_EQ(decomposedQueryPlans.size(), 1U);
            auto decomposedQueryPlan = decomposedQueryPlans[0];
            std::vector<OperatorPtr> actualRootOperators = decomposedQueryPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorPtr actualRootOperator = actualRootOperators[0];
            EXPECT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperator>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<LogicalMapOperator>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<LogicalMapOperator>());
        }
    }
}


/* Test query placement with ILP strategy - simple query of source - filter - map - sink */
TEST_F(ILPPlacementTest, testPlacingQueryWithILPStrategy) {

    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    setupTopologyAndSourceCatalogForILP();

    auto globalExecutionPlan = Optimizer::GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalogForILP, udfCatalog);
    auto queryPlacementAmendmentPhase = Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan,
                                                                                        topologyForILP,
                                                                                        typeInferencePhase,
                                                                                        coordinatorConfiguration);

    Query query = Query::from("car")
                      .filter(Attribute("id") < 45)
                      .map(Attribute("c") = Attribute("value") * 2)
                      .sink(PrintSinkDescriptor::create());

    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan->setQueryId(PlanIdGenerator::getNextQueryId());
    queryPlan->setPlacementStrategy(Optimizer::PlacementStrategy::ILP);

    for (const auto& sink : queryPlan->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperator>());
    }
    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topologyForILP,
                                                             sourceCatalogForILP,
                                                             Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getId();

    queryPlacementAmendmentPhase->execute(sharedQueryPlan);
    auto executionNodes = globalExecutionPlan->getLockedExecutionNodesHostingSharedQueryId(queryId);

    //Assertion
    ASSERT_EQ(executionNodes.size(), 3U);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->operator*()->getId() == WorkerId(3)) {
            // filter should be placed on source node
            auto decomposedQueryPlans = executionNode->operator*()->getAllDecomposedQueryPlans(queryId);
            ASSERT_EQ(decomposedQueryPlans.size(), 1U);
            auto decomposedQueryPlan = decomposedQueryPlans[0U];
            std::vector<OperatorPtr> actualRootOperators = decomposedQueryPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorPtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<LogicalFilterOperator>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<SourceLogicalOperator>());
        } else if (executionNode->operator*()->getId() == WorkerId(1)) {
            // map should be placed on cloud node
            auto decomposedQueryPlans = executionNode->operator*()->getAllDecomposedQueryPlans(queryId);
            ASSERT_EQ(decomposedQueryPlans.size(), 1U);
            auto decomposedQueryPlan = decomposedQueryPlans[0U];
            std::vector<OperatorPtr> actualRootOperators = decomposedQueryPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorPtr actualRootOperator = actualRootOperators[0];
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperator>());
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<LogicalMapOperator>());
        }
    }
}

/* Test incremental query placement with ILP strategy - simple query of source - filter - map - sink and then added filter - sink to filter operator*/
TEST_F(ILPPlacementTest, testPlacingUpdatedSharedQueryPlanWithILPStrategy) {

    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    coordinatorConfiguration->optimizer.enableIncrementalPlacement = true;

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
        assignOperatorPropertiesRecursive(sink->as<LogicalOperator>());
    }

    Query query2 = Query::from("car")
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") > Attribute("value") * 2)
                       .sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    queryPlan2->setQueryId(PlanIdGenerator::getNextQueryId());
    queryPlan2->setPlacementStrategy(Optimizer::PlacementStrategy::ILP);
    for (const auto& sink : queryPlan2->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperator>());
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
    auto globalExecutionPlan = Optimizer::GlobalExecutionPlan::create();
    auto queryPlacementAmendmentPhase = Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan,
                                                                                        topologyForILP,
                                                                                        typeInferencePhase,
                                                                                        coordinatorConfiguration);
    queryPlacementAmendmentPhase->execute(sharedQueryPlansToDeploy[0]);
    {
        SharedQueryId sharedQueryPlanId = sharedQueryPlansToDeploy[0]->getId();
        auto executionNodes = globalExecutionPlan->getLockedExecutionNodesHostingSharedQueryId(sharedQueryPlanId);

        //Assertions to check correct placement
        ASSERT_EQ(executionNodes.size(), 3U);
        for (const auto& executionNode : executionNodes) {
            if (executionNode->operator*()->getId() == WorkerId(3)) {
                // filter should be placed on source node
                auto decomposedQueryPlans = executionNode->operator*()->getAllDecomposedQueryPlans(sharedQueryPlanId);
                ASSERT_EQ(decomposedQueryPlans.size(), 1U);
                auto decomposedQueryPlan = decomposedQueryPlans[0U];
                std::vector<OperatorPtr> actualRootOperators = decomposedQueryPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1U);
                OperatorPtr actualRootOperator = actualRootOperators[0];
                ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<LogicalFilterOperator>());
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<SourceLogicalOperator>());
            } else if (executionNode->operator*()->getId() == WorkerId(1)) {
                // map should be placed on cloud node
                auto decomposedQueryPlans = executionNode->operator*()->getAllDecomposedQueryPlans(sharedQueryPlanId);
                ASSERT_EQ(decomposedQueryPlans.size(), 1U);
                auto decomposedQueryPlan = decomposedQueryPlans[0U];
                std::vector<OperatorPtr> actualRootOperators = decomposedQueryPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1U);
                OperatorPtr actualRootOperator = actualRootOperators[0];
                EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperator>());
                ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<LogicalMapOperator>());
            }
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
    queryPlacementAmendmentPhase->execute(sharedQueryPlansToDeploy[0]);
    {
        auto sharedQueryPlanId = sharedQueryPlansToDeploy[0]->getId();
        auto executionNodes = globalExecutionPlan->getLockedExecutionNodesHostingSharedQueryId(sharedQueryPlanId);

        //Assertions to check correct placement
        ASSERT_EQ(executionNodes.size(), 3U);
        for (const auto& executionNode : executionNodes) {
            if (executionNode->operator*()->getId() == WorkerId(3)) {
                // filter should be placed on source node
                auto decomposedQueryPlans = executionNode->operator*()->getAllDecomposedQueryPlans(sharedQueryPlanId);
                ASSERT_EQ(decomposedQueryPlans.size(), 1U);
                auto decomposedQueryPlan = decomposedQueryPlans[0U];
                std::vector<OperatorPtr> actualRootOperators = decomposedQueryPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 2U);
                OperatorPtr actualRootOperator = actualRootOperators[0];
                ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<LogicalFilterOperator>());
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<SourceLogicalOperator>());
            } else if (executionNode->operator*()->getId() == WorkerId(2)) {
                // map should be placed on cloud node
                auto decomposedQueryPlans = executionNode->operator*()->getAllDecomposedQueryPlans(sharedQueryPlanId);
                ASSERT_EQ(decomposedQueryPlans.size(), 2U);
                //Assertion for first subquery plan
                auto decomposedQueryPlan1 = decomposedQueryPlans[0U];
                std::vector<OperatorPtr> rootOperatorsForPlan1 = decomposedQueryPlan1->getRootOperators();
                ASSERT_EQ(rootOperatorsForPlan1.size(), 1U);
                OperatorPtr rootOperator1 = rootOperatorsForPlan1[0];
                EXPECT_TRUE(rootOperator1->instanceOf<SinkLogicalOperator>());
                ASSERT_EQ(rootOperator1->getChildren().size(), 1U);
                EXPECT_TRUE(rootOperator1->getChildren()[0]->instanceOf<SourceLogicalOperator>());

                //Assertion for second subquery plan
                auto decomposedQueryPlan2 = decomposedQueryPlans[1U];
                std::vector<OperatorPtr> rootOperatorsForPlan2 = decomposedQueryPlan2->getRootOperators();
                ASSERT_EQ(rootOperatorsForPlan2.size(), 1U);
                OperatorPtr rootOperator2 = rootOperatorsForPlan2[0];
                EXPECT_TRUE(rootOperator2->instanceOf<SinkLogicalOperator>());
                ASSERT_EQ(rootOperator2->getChildren().size(), 1U);
            } else if (executionNode->operator*()->getId() == WorkerId(1)) {
                // map should be placed on cloud node
                auto decomposedQueryPlans = executionNode->operator*()->getAllDecomposedQueryPlans(sharedQueryPlanId);
                ASSERT_EQ(decomposedQueryPlans.size(), 2U);
                //Assertion for first subquery plan
                auto decomposedQueryPlan1 = decomposedQueryPlans[0U];
                std::vector<OperatorPtr> rootOperatorsForPlan1 = decomposedQueryPlan1->getRootOperators();
                ASSERT_EQ(rootOperatorsForPlan1.size(), 1U);
                OperatorPtr rootOperator1 = rootOperatorsForPlan1[0];
                ASSERT_EQ(rootOperator1->getId(), queryPlan1->getRootOperators()[0]->getId());
                EXPECT_TRUE(rootOperator1->instanceOf<SinkLogicalOperator>());
                ASSERT_EQ(rootOperator1->getChildren().size(), 1U);
                EXPECT_TRUE(rootOperator1->getChildren()[0]->instanceOf<LogicalMapOperator>());

                //Assertion for second subquery plan
                auto decomposedQueryPlan2 = decomposedQueryPlans[1U];
                std::vector<OperatorPtr> rootOperatorsForPlan2 = decomposedQueryPlan2->getRootOperators();
                ASSERT_EQ(rootOperatorsForPlan2.size(), 1U);
                OperatorPtr rootOperator2 = rootOperatorsForPlan2[0];
                ASSERT_EQ(rootOperator2->getId(), queryPlan2->getRootOperators()[0]->getId());
                EXPECT_TRUE(rootOperator2->instanceOf<SinkLogicalOperator>());
                ASSERT_EQ(rootOperator2->getChildren().size(), 1U);
            }
        }
    }
}

/* Test incremental query placement with ILP strategy - simple query of source - filter - map - sink and then added map - sink and filter - sink to the filter operator*/
TEST_F(ILPPlacementTest, testPlacingMulitpleUpdatesOnASharedQueryPlanWithILPStrategy) {

    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    coordinatorConfiguration->optimizer.enableIncrementalPlacement = true;

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
        assignOperatorPropertiesRecursive(sink->as<LogicalOperator>());
    }

    Query query2 = Query::from("car")
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") > Attribute("value") * 2)
                       .sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    queryPlan2->setQueryId(PlanIdGenerator::getNextQueryId());
    queryPlan2->setPlacementStrategy(Optimizer::PlacementStrategy::ILP);
    for (const auto& sink : queryPlan2->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperator>());
    }

    Query query3 = Query::from("car")
                       .filter(Attribute("id") < 45)
                       .map(Attribute("b") = Attribute("value") * 2)
                       .sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan3 = query3.getQueryPlan();
    queryPlan3->setQueryId(PlanIdGenerator::getNextQueryId());
    queryPlan3->setPlacementStrategy(Optimizer::PlacementStrategy::ILP);
    for (const auto& sink : queryPlan3->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperator>());
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
    auto globalExecutionPlan = Optimizer::GlobalExecutionPlan::create();
    auto queryPlacementAmendmentPhase = Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan,
                                                                                        topologyForILP,
                                                                                        typeInferencePhase,
                                                                                        coordinatorConfiguration);
    queryPlacementAmendmentPhase->execute(sharedQueryPlansToDeploy[0]);
    {
        SharedQueryId sharedQueryPlanId = sharedQueryPlansToDeploy[0]->getId();
        auto executionNodes = globalExecutionPlan->getLockedExecutionNodesHostingSharedQueryId(sharedQueryPlanId);

        //Assertions to check correct placement
        ASSERT_EQ(executionNodes.size(), 3U);
        for (const auto& executionNode : executionNodes) {
            if (executionNode->operator*()->getId() == WorkerId(3)) {
                // filter should be placed on source node
                auto decomposedQueryPlans = executionNode->operator*()->getAllDecomposedQueryPlans(sharedQueryPlanId);
                ASSERT_EQ(decomposedQueryPlans.size(), 1U);
                auto decomposedQueryPlan = decomposedQueryPlans[0U];
                std::vector<OperatorPtr> actualRootOperators = decomposedQueryPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1U);
                OperatorPtr actualRootOperator = actualRootOperators[0];
                ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<LogicalFilterOperator>());
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<SourceLogicalOperator>());
            } else if (executionNode->operator*()->getId() == WorkerId(1)) {
                // map should be placed on cloud node
                auto decomposedQueryPlans = executionNode->operator*()->getAllDecomposedQueryPlans(sharedQueryPlanId);
                ASSERT_EQ(decomposedQueryPlans.size(), 1U);
                auto decomposedQueryPlan = decomposedQueryPlans[0U];
                std::vector<OperatorPtr> actualRootOperators = decomposedQueryPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1U);
                OperatorPtr actualRootOperator = actualRootOperators[0];
                EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperator>());
                ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<LogicalMapOperator>());
            }
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
    queryPlacementAmendmentPhase->execute(sharedQueryPlansToDeploy[0]);
    {
        auto sharedQueryPlanId = sharedQueryPlansToDeploy[0]->getId();
        auto executionNodes = globalExecutionPlan->getLockedExecutionNodesHostingSharedQueryId(sharedQueryPlanId);

        //Assertions to check correct placement
        ASSERT_EQ(executionNodes.size(), 3U);
        for (const auto& executionNode : executionNodes) {
            if (executionNode->operator*()->getId() == WorkerId(3)) {
                // filter should be placed on source node
                auto decomposedQueryPlans = executionNode->operator*()->getAllDecomposedQueryPlans(sharedQueryPlanId);
                ASSERT_EQ(decomposedQueryPlans.size(), 1U);
                auto decomposedQueryPlan = decomposedQueryPlans[0U];
                std::vector<OperatorPtr> actualRootOperators = decomposedQueryPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 3U);
                OperatorPtr actualRootOperator = actualRootOperators[0];
                ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<LogicalFilterOperator>());
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<SourceLogicalOperator>());
            } else if (executionNode->operator*()->getId() == WorkerId(2)) {
                // map should be placed on cloud node
                auto decomposedQueryPlans = executionNode->operator*()->getAllDecomposedQueryPlans(sharedQueryPlanId);
                ASSERT_EQ(decomposedQueryPlans.size(), 3U);
                //Assertion for first subquery plan
                auto decomposedQueryPlan1 = decomposedQueryPlans[0U];
                std::vector<OperatorPtr> rootOperatorsForPlan1 = decomposedQueryPlan1->getRootOperators();
                ASSERT_EQ(rootOperatorsForPlan1.size(), 1U);
                OperatorPtr rootOperator1 = rootOperatorsForPlan1[0];
                EXPECT_TRUE(rootOperator1->instanceOf<SinkLogicalOperator>());
                ASSERT_EQ(rootOperator1->getChildren().size(), 1U);
                EXPECT_TRUE(rootOperator1->getChildren()[0]->instanceOf<SourceLogicalOperator>());

                //Assertion for second subquery plan
                auto decomposedQueryPlan2 = decomposedQueryPlans[1U];
                std::vector<OperatorPtr> rootOperatorsForPlan2 = decomposedQueryPlan2->getRootOperators();
                ASSERT_EQ(rootOperatorsForPlan2.size(), 1U);
                OperatorPtr rootOperator2 = rootOperatorsForPlan2[0];
                EXPECT_TRUE(rootOperator2->instanceOf<SinkLogicalOperator>());
                ASSERT_EQ(rootOperator2->getChildren().size(), 1U);
            } else if (executionNode->operator*()->getId() == WorkerId(1)) {
                // map should be placed on cloud node
                auto decomposedQueryPlans = executionNode->operator*()->getAllDecomposedQueryPlans(sharedQueryPlanId);
                ASSERT_EQ(decomposedQueryPlans.size(), 3U);

                for (const auto& decomposedQueryPlan : decomposedQueryPlans) {

                    //Assertion for all subquery plans
                    std::vector<OperatorPtr> rootOperatorsForPlan = decomposedQueryPlan->getRootOperators();
                    ASSERT_EQ(rootOperatorsForPlan.size(), 1U);
                    OperatorPtr rootOperator = rootOperatorsForPlan[0];

                    if (rootOperator->getId() == queryPlan1->getRootOperators()[0]->getId()) {
                        EXPECT_TRUE(rootOperator->instanceOf<SinkLogicalOperator>());
                        ASSERT_EQ(rootOperator->getChildren().size(), 1U);
                        EXPECT_TRUE(rootOperator->getChildren()[0]->instanceOf<LogicalMapOperator>());
                    } else if (rootOperator->getId() == queryPlan2->getRootOperators()[0]->getId()) {
                        EXPECT_TRUE(rootOperator->instanceOf<SinkLogicalOperator>());
                        ASSERT_EQ(rootOperator->getChildren().size(), 1U);
                        EXPECT_TRUE(rootOperator->getChildren()[0]->instanceOf<LogicalFilterOperator>());
                    } else if (rootOperator->getId() == queryPlan3->getRootOperators()[0]->getId()) {
                        EXPECT_TRUE(rootOperator->instanceOf<SinkLogicalOperator>());
                        ASSERT_EQ(rootOperator->getChildren().size(), 1U);
                        EXPECT_TRUE(rootOperator->getChildren()[0]->instanceOf<LogicalMapOperator>());
                    } else {
                        //Sub query plan should contain for one of the three sink operator as its root
                        FAIL();
                    }
                }
            }
        }
    }
}


/* Test query placement with ILP strategy for query: source - filter - map - sink and then added map - sink and filter - sink to the filter operator*/
TEST_F(ILPPlacementTest, testPlacingMultipleSinkSharedQueryPlanWithILPStrategy) {

    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    coordinatorConfiguration->optimizer.enableIncrementalPlacement = true;

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
        assignOperatorPropertiesRecursive(sink->as<LogicalOperator>());
    }

    Query query2 = Query::from("car")
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") > Attribute("value") * 2)
                       .sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    queryPlan2->setQueryId(PlanIdGenerator::getNextQueryId());
    queryPlan2->setPlacementStrategy(Optimizer::PlacementStrategy::ILP);
    for (const auto& sink : queryPlan2->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperator>());
    }

    Query query3 = Query::from("car")
                       .filter(Attribute("id") < 45)
                       .map(Attribute("b") = Attribute("value") * 2)
                       .sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan3 = query3.getQueryPlan();
    queryPlan3->setQueryId(PlanIdGenerator::getNextQueryId());
    for (const auto& sink : queryPlan3->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperator>());
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
    ASSERT_EQ(sharedQueryPlansToDeploy.size(), 2l);

    //Place the shared query plan
    auto globalExecutionPlan = Optimizer::GlobalExecutionPlan::create();
    auto queryPlacementAmendmentPhase = Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan,
                                                                                        topologyForILP,
                                                                                        typeInferencePhase,
                                                                                        coordinatorConfiguration);
    queryPlacementAmendmentPhase->execute(sharedQueryPlansToDeploy[0]);
    {
        SharedQueryId sharedQueryPlanId = sharedQueryPlansToDeploy[0]->getId();
        auto executionNodes = globalExecutionPlan->getLockedExecutionNodesHostingSharedQueryId(sharedQueryPlanId);

        //Assertions to check correct placement
        ASSERT_EQ(executionNodes.size(), 3U);
        for (const auto& executionNode : executionNodes) {
            if (executionNode->operator*()->getId() == WorkerId(3)) {
                // filter should be placed on source node
                auto decomposedQueryPlans = executionNode->operator*()->getAllDecomposedQueryPlans(sharedQueryPlanId);
                ASSERT_EQ(decomposedQueryPlans.size(), 1U);
                auto decomposedQueryPlan = decomposedQueryPlans[0U];
                std::vector<OperatorPtr> actualRootOperators = decomposedQueryPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 2U);
                OperatorPtr actualRootOperator = actualRootOperators[0];
                ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<LogicalFilterOperator>());
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<SourceLogicalOperator>());
            } else if (executionNode->operator*()->getId() == WorkerId(2)) {
                // map should be placed on cloud node
                auto decomposedQueryPlans = executionNode->operator*()->getAllDecomposedQueryPlans(sharedQueryPlanId);
                ASSERT_EQ(decomposedQueryPlans.size(), 2U);
                //Assertion for first subquery plan
                auto decomposedQueryPlan1 = decomposedQueryPlans[0U];
                std::vector<OperatorPtr> rootOperatorsForPlan1 = decomposedQueryPlan1->getRootOperators();
                ASSERT_EQ(rootOperatorsForPlan1.size(), 1U);
                OperatorPtr rootOperator1 = rootOperatorsForPlan1[0];
                EXPECT_TRUE(rootOperator1->instanceOf<SinkLogicalOperator>());
                ASSERT_EQ(rootOperator1->getChildren().size(), 1U);
                EXPECT_TRUE(rootOperator1->getChildren()[0]->instanceOf<SourceLogicalOperator>());

                //Assertion for second subquery plan
                auto decomposedQueryPlan2 = decomposedQueryPlans[1U];
                std::vector<OperatorPtr> rootOperatorsForPlan2 = decomposedQueryPlan2->getRootOperators();
                ASSERT_EQ(rootOperatorsForPlan2.size(), 1U);
                OperatorPtr rootOperator2 = rootOperatorsForPlan2[0];
                EXPECT_TRUE(rootOperator2->instanceOf<SinkLogicalOperator>());
                ASSERT_EQ(rootOperator2->getChildren().size(), 1U);
                EXPECT_TRUE(rootOperator2->getChildren()[0]->instanceOf<SourceLogicalOperator>());
            } else if (executionNode->operator*()->getId() == WorkerId(1)) {
                // map should be placed on cloud node
                auto decomposedQueryPlans = executionNode->operator*()->getAllDecomposedQueryPlans(sharedQueryPlanId);
                ASSERT_EQ(decomposedQueryPlans.size(), 2U);
                //Assertion for first subquery plan
                auto decomposedQueryPlan1 = decomposedQueryPlans[0U];
                std::vector<OperatorPtr> rootOperatorsForPlan1 = decomposedQueryPlan1->getRootOperators();
                ASSERT_EQ(rootOperatorsForPlan1.size(), 1U);
                OperatorPtr rootOperator1 = rootOperatorsForPlan1[0];
                ASSERT_EQ(rootOperator1->getId(), queryPlan1->getRootOperators()[0]->getId());
                EXPECT_TRUE(rootOperator1->instanceOf<SinkLogicalOperator>());
                ASSERT_EQ(rootOperator1->getChildren().size(), 1U);
                EXPECT_TRUE(rootOperator1->getChildren()[0]->instanceOf<LogicalMapOperator>());

                //Assertion for second subquery plan
                auto decomposedQueryPlan2 = decomposedQueryPlans[1U];
                std::vector<OperatorPtr> rootOperatorsForPlan2 = decomposedQueryPlan2->getRootOperators();
                ASSERT_EQ(rootOperatorsForPlan2.size(), 1U);
                OperatorPtr rootOperator2 = rootOperatorsForPlan2[0];
                ASSERT_EQ(rootOperator2->getId(), queryPlan2->getRootOperators()[0]->getId());
                EXPECT_TRUE(rootOperator2->instanceOf<SinkLogicalOperator>());
                ASSERT_EQ(rootOperator2->getChildren().size(), 1U);
                EXPECT_TRUE(rootOperator2->getChildren()[0]->instanceOf<SourceLogicalOperator>());
            }
        }
    }
}

/* Test query placement with ILP strategy - this tests checks if ILP strategy works when operators have multiple children/parents */
TEST_F(ILPPlacementTest, testMultipleChildrenQueryWithILPStrategy) {
    setupTopologyAndSourceCatalogForILP();

    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();

    auto globalExecutionPlan = Optimizer::GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalogForILP, udfCatalog);
    auto queryPlacementAmendmentPhase = Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan,
                                                                                        topologyForILP,
                                                                                        typeInferencePhase,
                                                                                        coordinatorConfiguration);
    //Prepare query plan
    Query query = Query::from("car").unionWith(Query::from("car")).sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan->setQueryId(PlanIdGenerator::getNextQueryId());
    for (const auto& sink : queryPlan->getSinkOperators()) {
        assignOperatorPropertiesRecursive(sink->as<LogicalOperator>());
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
    queryPlacementAmendmentPhase->execute(sharedQueryPlan);

    auto lockedExecutionNodes = globalExecutionPlan->getLockedExecutionNodesHostingSharedQueryId(queryId);

    //Assertion
    ASSERT_EQ(lockedExecutionNodes.size(), 3U);
    for (const auto& executionNode : lockedExecutionNodes) {
        if (executionNode->operator*()->getId() == WorkerId(3)) {
            // place filter on source node
            auto decomposedQueryPlans = executionNode->operator*()->getAllDecomposedQueryPlans(queryId);
            ASSERT_EQ(decomposedQueryPlans.size(), 2U);
            auto decomposedQueryPlan = decomposedQueryPlans[0U];
            std::vector<OperatorPtr> actualRootOperators = decomposedQueryPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorPtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<SourceLogicalOperator>());
        } else if (executionNode->operator*()->getId() == WorkerId(2)) {
            auto decomposedQueryPlans = executionNode->operator*()->getAllDecomposedQueryPlans(queryId);
            ASSERT_EQ(decomposedQueryPlans.size(), 1U);
            auto decomposedQueryPlan = decomposedQueryPlans[0U];
            std::vector<OperatorPtr> actualRootOperators = decomposedQueryPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorPtr actualRootOperator = actualRootOperators[0];
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<LogicalUnionOperator>());
        } else if (executionNode->operator*()->getId() == WorkerId(1)) {
            auto decomposedQueryPlans = executionNode->operator*()->getAllDecomposedQueryPlans(queryId);
            ASSERT_EQ(decomposedQueryPlans.size(), 1U);
            auto decomposedQueryPlan = decomposedQueryPlans[0U];
            std::vector<OperatorPtr> actualRootOperators = decomposedQueryPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorPtr actualRootOperator = actualRootOperators[0];
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperator>());
        }

    }

}