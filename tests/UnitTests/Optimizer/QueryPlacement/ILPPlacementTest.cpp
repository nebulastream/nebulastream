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

#include <API/Query.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Catalogs/StreamCatalogEntry.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Configurations/ConfigOptions/SourceConfig.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
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
#include "z3++.h"

using namespace NES;
using namespace web;
using namespace z3;

class ILPPlacementTest : public testing::Test {
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
                                                                              streamCatalogForILP);

    Query query = Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());


    QueryPlanPtr queryPlan = query.getQueryPlan();
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    queryPlan->setQueryId(queryId);

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
        } else if (executionNode->getId() == 3){
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
                                                                              streamCatalogForILP);

    Query query = Query::from("car")
                        .map(Attribute("c") = Attribute("value") + 2)
                        .map(Attribute("d") = Attribute("value") * 2)
                        .sink(PrintSinkDescriptor::create());


    QueryPlanPtr queryPlan = query.getQueryPlan();
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    queryPlan->setQueryId(queryId);

    //auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
    //queryPlan = queryReWritePhase->execute(queryPlan);
    //typeInferencePhase->execute(queryPlan);

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
        } else if (executionNode->getId() == 3){
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
                                                                              streamCatalogForILP);

    Query query = Query::from("car")
                      .filter(Attribute("id") < 45)
                      .map(Attribute("c") = Attribute("value") * 2)
                      .sink(PrintSinkDescriptor::create());

    QueryPlanPtr queryPlan = query.getQueryPlan();
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    queryPlan->setQueryId(queryId);

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
        } else if (executionNode->getId() == 3){
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

// TODO 1979: Move this to benchmark?
class ILPPlacementBenchmark : public testing::Test {
  public:
    std::shared_ptr<QueryParsingService> queryParsingService;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() { std::cout << "Setup ILPPlacementBenchmark test class." << std::endl; }

    /* Will be called before a test is executed. */
    void SetUp() override {
        NES::setupLogging("ILPPlacementBenchmark.log", NES::LOG_DEBUG);
        std::cout << "Setup ILPPlacementBenchmark test case." << std::endl;
        auto cppCompiler = Compiler::CPPCompiler::create();
        auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        queryParsingService = QueryParsingService::create(jitCompiler);
    }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Setup ILPPlacementBenchmark test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down ILPPlacementBenchmark test class." << std::endl; }

    void setupTopologyAndStreamCatalogForILP(int n, int SourcePerMiddle, int resources, bool exponentialResources) {
        int nodeID = 1;

        int resources1 = exponentialResources ? resources^0: resources;
        int resources2 = exponentialResources ? resources^1: resources;
        int resources3 = exponentialResources ? resources^2: resources;

        topologyForILP = Topology::create();
        TopologyNodePtr rootNode = TopologyNode::create(nodeID++, "localhost", 123, 124, resources1);
        rootNode->addNodeProperty("slots", resources1);
        topologyForILP->setAsRoot(rootNode);
        streamCatalogForILP = std::make_shared<StreamCatalog>(queryParsingService);

        LinkPropertyPtr linkProperty = std::make_shared<LinkProperty>(LinkProperty(512, 100));
        std::string schema = "Schema::create()->addField(\"id\", BasicType::UINT32)"
                             "->addField(\"value\", BasicType::UINT64);";

        for(int i = 0; i < n; i+=SourcePerMiddle) {
            TopologyNodePtr middleNode = TopologyNode::create(nodeID++, "localhost", 123, 124, resources2);
            middleNode->addNodeProperty("slots", resources2);
            topologyForILP->addNewPhysicalNodeAsChild(rootNode, middleNode);
            middleNode->addLinkProperty(rootNode, linkProperty);
            rootNode->addLinkProperty(middleNode, linkProperty);

            for(int j = 0; j < SourcePerMiddle; j++) {
                TopologyNodePtr sourceNode = TopologyNode::create(nodeID++, "localhost", 123, 124, resources3);
                sourceNode->addNodeProperty("slots", resources3);
                topologyForILP->addNewPhysicalNodeAsChild(middleNode, sourceNode);
                sourceNode->addLinkProperty(middleNode, linkProperty);
                middleNode->addLinkProperty(sourceNode, linkProperty);

                std::string streamName = "car" + std::to_string(i+j);
                streamCatalogForILP->addLogicalStream(streamName, schema);

                SourceConfigPtr sourceConfig = SourceConfig::create();
                sourceConfig->setSourceFrequency(0);
                sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
                sourceConfig->setPhysicalStreamName(streamName + "_source_1");
                sourceConfig->setLogicalStreamName(streamName);

                PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(sourceConfig);
                StreamCatalogEntryPtr streamCatalogEntry1 = std::make_shared<StreamCatalogEntry>(conf, sourceNode);
                streamCatalogForILP->addPhysicalStream(streamName, streamCatalogEntry1);
            }
        }
    }

    StreamCatalogPtr streamCatalogForILP;
    TopologyPtr topologyForILP;
};


/* Test query placement with ILP strategy  */
TEST_F(ILPPlacementBenchmark, increaseSources) {
    std::list<int> listOfInts( {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20});

    int repetitions = 11;
    int SourcePerMiddle = 10;
    int max_size = *std::max_element(listOfInts.begin(), listOfInts.end());
    setupTopologyAndStreamCatalogForILP(max_size, SourcePerMiddle, 10, true);

    std::map<int, std::vector<long>> counts;
    for(int n : listOfInts) {
        std::vector<long> counts_n;

        for(int j = 0; j < repetitions; j++) {
            GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
            auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalogForILP);
            auto placementStrategy = Optimizer::PlacementStrategyFactory::getStrategy("ILP",
                                                                                      globalExecutionPlan,
                                                                                      topologyForILP,
                                                                                      typeInferencePhase,
                                                                                      streamCatalogForILP);

            std::vector<Query> subqueries;
            for (int i = 0; i < n; i++) {
                Query subquery = Query::from("car" + std::to_string(i))
                    .filter(Attribute("id") < 45)
                    .map(Attribute("c") = Attribute("id") + Attribute("value"));
                subqueries.push_back(subquery);
            }

            Query query = subqueries[0];
            for (uint64_t i = 1; i < subqueries.size(); i++) {
                query.unionWith(&subqueries[i]);
            }
            query = query.sink(PrintSinkDescriptor::create());

            QueryPlanPtr queryPlan = query.getQueryPlan();
            QueryId queryId = PlanIdGenerator::getNextQueryId();
            queryPlan->setQueryId(queryId);

            int num_operators = 0;
            auto queryPlanIterator = QueryPlanIterator(queryPlan);
            for (auto node : queryPlanIterator) {
                num_operators++;
            }

            auto start = std::chrono::high_resolution_clock::now();
            ASSERT_TRUE(placementStrategy->updateGlobalExecutionPlan(queryPlan));
            auto stop = std::chrono::high_resolution_clock::now();
            long count = std::chrono::duration_cast<std::chrono::milliseconds>(stop - start).count();
            NES_INFO("Solved Placement for " << num_operators << " Operators, " << n << " Sources and " << n * 2 + 1 << " Topology nodes");
            NES_INFO("Found Solution in " << count << "ms");
            counts_n.push_back(count);
        }
        counts.insert(std::make_pair(n, counts_n));
    }

    for (auto& [n, counts_n] : counts) {
        std::sort(counts_n.begin(), counts_n.end());
        long median = counts_n[counts_n.size() / 2];
        if (counts.size() % 2 == 0) {
            median = (median + counts_n[(counts_n.size() - 1) / 2]) / 2;
        }
        std::stringstream ss;
        for (auto& count : counts_n) {
            ss << count << ", ";
        }

        NES_INFO("N: " << n << ", median: " << median << ", measures: " << ss.str());
    }
}

TEST_F(ILPPlacementBenchmark, increaseResources) {
    std::list<int> listOfInts( {1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100});

    int repetitions = 101;
    int iot_devices = 10;

    std::map<int, std::vector<long>> counts;
    for(int n : listOfInts) {
        setupTopologyAndStreamCatalogForILP(iot_devices, iot_devices, n, false);
        std::vector<long> counts_n;

        for(int j = 0; j < repetitions; j++) {
            GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
            auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalogForILP);
            auto placementStrategy = Optimizer::PlacementStrategyFactory::getStrategy("ILP",
                                                                                      globalExecutionPlan,
                                                                                      topologyForILP,
                                                                                      typeInferencePhase,
                                                                                      streamCatalogForILP);

            std::vector<Query> subqueries;
            for (int i = 0; i < iot_devices; i++) {
                Query subquery = Query::from("car" + std::to_string(i))
                    .filter(Attribute("id") < 45)
                    .map(Attribute("c") = Attribute("id") + Attribute("value"));
                subqueries.push_back(subquery);
            }

            Query query = subqueries[0];
            for (uint64_t i = 1; i < subqueries.size(); i++) {
                query.unionWith(&subqueries[i]);
            }
            query = query.sink(PrintSinkDescriptor::create());

            QueryPlanPtr queryPlan = query.getQueryPlan();
            QueryId queryId = PlanIdGenerator::getNextQueryId();
            queryPlan->setQueryId(queryId);

            int num_operators = 0;
            auto queryPlanIterator = QueryPlanIterator(queryPlan);
            for (auto node : queryPlanIterator) {
                num_operators++;
            }

            auto start = std::chrono::high_resolution_clock::now();
            ASSERT_TRUE(placementStrategy->updateGlobalExecutionPlan(queryPlan));
            auto stop = std::chrono::high_resolution_clock::now();
            long count = std::chrono::duration_cast<std::chrono::milliseconds>(stop - start).count();
            NES_INFO("Solved Placement for " << num_operators << " Operators, " << n << " Sources and " << n * 2 + 1 << " Topology nodes");
            NES_INFO("Found Solution in " << count << "ms");
            counts_n.push_back(count);
        }
        counts.insert(std::make_pair(n, counts_n));
    }

    for (auto& [n, counts_n] : counts) {
        std::sort(counts_n.begin(), counts_n.end());
        long median = counts_n[counts_n.size() / 2];
        if (counts.size() % 2 == 0) {
            median = (median + counts_n[(counts_n.size() - 1) / 2]) / 2;
        }
        std::stringstream ss;
        for (auto& count : counts_n) {
            ss << count << ", ";
        }

        NES_INFO("N: " << n << ", median: " << median << ", measures: " << ss.str());
    }
}

/* Test query placement with ILP strategy  */
TEST_F(ILPPlacementBenchmark, increaseOperators) {
    std::list<int> listOfInts( {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15});

    uint64_t repetitions = 11;
    uint64_t iot_devices = 1;
    setupTopologyAndStreamCatalogForILP(iot_devices, iot_devices, 10, true);

    std::map<int, std::vector<long>> counts;
    for(int n : listOfInts) {
        std::vector<long> counts_n;

        for(uint64_t j = 0; j < repetitions; j++) {
            GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
            auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalogForILP);
            auto placementStrategy = Optimizer::PlacementStrategyFactory::getStrategy("ILP",
                                                                                      globalExecutionPlan,
                                                                                      topologyForILP,
                                                                                      typeInferencePhase,
                                                                                      streamCatalogForILP);

            NES_INFO("N: " << n << ", Run: " << j);

            std::vector<Query> subqueries;
            for (uint64_t i = 0; i < iot_devices; i++) {
                Query subquery = Query::from("car" + std::to_string(i));
                for (int j = 0; j < n; j++) {
                    subquery = subquery.filter(Attribute("id") < 45).map(Attribute("c") = Attribute("id") + Attribute("value"));
                }
                subqueries.push_back(subquery);
            }

            Query query = subqueries[0];
            for (uint64_t i = 1; i < subqueries.size(); i++) {
                query.unionWith(&subqueries[i]);
            }
            query = query.sink(PrintSinkDescriptor::create());

            QueryPlanPtr queryPlan = query.getQueryPlan();
            QueryId queryId = PlanIdGenerator::getNextQueryId();
            queryPlan->setQueryId(queryId);

            int num_operators = 0;
            auto queryPlanIterator = QueryPlanIterator(queryPlan);
            for (auto node : queryPlanIterator) {
                num_operators++;
            }

            auto start = std::chrono::high_resolution_clock::now();
            ASSERT_TRUE(placementStrategy->updateGlobalExecutionPlan(queryPlan));
            auto stop = std::chrono::high_resolution_clock::now();
            long count = std::chrono::duration_cast<std::chrono::milliseconds>(stop - start).count();
            NES_INFO("Solved Placement for " << num_operators << " Operators, " << n << " Sources and " << n * 2 + 1 << " Topology nodes");
            NES_INFO("Found Solution in " << count << "ms");
            counts_n.push_back(count);
        }
        counts.insert(std::make_pair(n, counts_n));
    }

    for (auto& [n, counts_n] : counts) {
        std::sort(counts_n.begin(), counts_n.end());
        long median = counts_n[counts_n.size() / 2];
        if (counts.size() % 2 == 0) {
            median = (median + counts_n[(counts_n.size() - 1) / 2]) / 2;
        }
        std::stringstream ss;
        for (auto& count : counts_n) {
            ss << count << ", ";
        }

        NES_INFO("N: " << n << ", median: " << median << ", measures: " << ss.str());
    }
}