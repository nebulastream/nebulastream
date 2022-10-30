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
#include "gtest/gtest.h"
#include <NesBaseTest.hpp>

#include "z3++.h"
#include <API/QueryAPI.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Source/SourceCatalogEntry.hpp>
#include <Catalogs/UDF/UdfCatalog.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <NesBaseTest.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Phases/QueryPlacementPhase.hpp>
#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <Optimizer/Phases/SignatureInferencePhase.hpp>
#include <Optimizer/Phases/TopologySpecificQueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryMerger/Z3SignatureBasedPartialQueryMergerRule.hpp>
#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <Optimizer/QueryPlacement/ILPStrategy.hpp>
#include <Optimizer/QueryPlacement/ManualPlacementStrategy.hpp>
#include <Optimizer/QueryPlacement/PlacementStrategyFactory.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <Services/QueryParsingService.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <utility>
#include <Optimizer/Phases/TopologyReconfigurationPhase.hpp>

using namespace NES;
using namespace z3;
using namespace Configurations;

class TopologyReconfigurationPhaseTest : public Testing::TestWithErrorHandling<testing::Test> {
  public:
    z3::ContextPtr z3Context;
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    TopologyPtr topology;
    QueryParsingServicePtr queryParsingService;
    GlobalExecutionPlanPtr globalExecutionPlan;
    Optimizer::TypeInferencePhasePtr typeInferencePhase;
    std::shared_ptr<Catalogs::UDF::UdfCatalog> udfCatalog;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() { std::cout << "Setup QueryPlacementTest test class." << std::endl; }

    /* Will be called before a test is executed. */
    void SetUp() override {
        NES::Logger::setupLogging("QueryPlacementTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup QueryPlacementTest test case." << std::endl;
        z3Context = std::make_shared<z3::context>();
        auto cppCompiler = Compiler::CPPCompiler::create();
        auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        queryParsingService = QueryParsingService::create(jitCompiler);
        udfCatalog = Catalogs::UDF::UdfCatalog::create();
    }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Setup QueryPlacementTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down QueryPlacementTest test class." << std::endl; }

    void setupTopologyAndSourceCatalog(std::vector<uint16_t> resources) {

        topology = Topology::create();

        TopologyNodePtr rootNode = TopologyNode::create(1, "localhost", 123, 124, resources[0]);
        topology->setAsRoot(rootNode);

        TopologyNodePtr pathNode1 = TopologyNode::create(2, "localhost", 123, 124, resources[1]);
        topology->addNewTopologyNodeAsChild(rootNode, pathNode1);

        TopologyNodePtr pathNode2 = TopologyNode::create(3, "localhost", 123, 124, resources[2]);
        topology->addNewTopologyNodeAsChild(rootNode, pathNode2);

        TopologyNodePtr mobileNode1 = TopologyNode::create(4, "localhost", 123, 124, resources[3]);
        topology->addNewTopologyNodeAsChild(pathNode1, mobileNode1);

        TopologyNodePtr mobileNode2 = TopologyNode::create(5, "localhost", 123, 124, resources[4]);
        topology->addNewTopologyNodeAsChild(pathNode1, mobileNode2);

        std::string schema = "Schema::create()->addField(\"id\", BasicType::UINT32)"
                             "->addField(\"value\", BasicType::UINT64);";
        const std::string sourceName1 = "car";
        const std::string sourceName2 = "car2";

        sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(queryParsingService);
        sourceCatalog->addLogicalSource(sourceName1, schema);
        sourceCatalog->addLogicalSource(sourceName2, schema);
        auto logicalSource1 = sourceCatalog->getLogicalSource(sourceName1);
        auto logicalSource2 = sourceCatalog->getLogicalSource(sourceName2);

        CSVSourceTypePtr csvSourceType1 = CSVSourceType::create();
        csvSourceType1->setGatheringInterval(0);
        csvSourceType1->setNumberOfTuplesToProducePerBuffer(0);
        CSVSourceTypePtr csvSourceType2 = CSVSourceType::create();
        csvSourceType2->setGatheringInterval(0);
        csvSourceType2->setNumberOfTuplesToProducePerBuffer(0);
        auto physicalSource1 = PhysicalSource::create(sourceName1, "test2", csvSourceType1);
        auto physicalSource2 = PhysicalSource::create(sourceName2, "test2", csvSourceType2);

        /*
        Catalogs::Source::SourceCatalogEntryPtr sourceCatalogEntry1 =
            std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, sourceNode1);
        Catalogs::Source::SourceCatalogEntryPtr sourceCatalogEntry2 =
            std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, sourceNode2);
            */
        Catalogs::Source::SourceCatalogEntryPtr sourceCatalogEntry1 =
            std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource1, logicalSource1, mobileNode1);
        Catalogs::Source::SourceCatalogEntryPtr sourceCatalogEntry2 =
            std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource2, logicalSource2, mobileNode2);

        sourceCatalog->addPhysicalSource(sourceName1, sourceCatalogEntry1);
        sourceCatalog->addPhysicalSource(sourceName2, sourceCatalogEntry2);

        globalExecutionPlan = GlobalExecutionPlan::create();
        typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    }

    static void assignDataModificationFactor(QueryPlanPtr queryPlan) {
        QueryPlanIterator queryPlanIterator = QueryPlanIterator(std::move(queryPlan));

        for (auto qPlanItr = queryPlanIterator.begin(); qPlanItr != QueryPlanIterator::end(); ++qPlanItr) {
            // set data modification factor for map operator
            if ((*qPlanItr)->instanceOf<MapLogicalOperatorNode>()) {
                auto op = (*qPlanItr)->as<MapLogicalOperatorNode>();
                NES_DEBUG("input schema in bytes: " << op->getInputSchema()->getSchemaSizeInBytes());
                NES_DEBUG("output schema in bytes: " << op->getOutputSchema()->getSchemaSizeInBytes());
                double schemaSizeComparison =
                    1.0 * op->getOutputSchema()->getSchemaSizeInBytes() / op->getInputSchema()->getSchemaSizeInBytes();

                op->addProperty("DMF", schemaSizeComparison);
            }
        }
    }
};

/* Test query placement with bottom up strategy  */
TEST_F(TopologyReconfigurationPhaseTest, testPlacingQueryWithBottomUpStrategy) {

    //todo: putting 0 resources will not allow placing source, but putting one allows filter
    setupTopologyAndSourceCatalog({4, 4, 4, 1});
    topology->print();
    Query query = Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan = query.getQueryPlan();

    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
    queryPlan = queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getSharedQueryId();
    auto queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, z3Context, false);
    queryPlacementPhase->execute(NES::PlacementStrategy::BottomUp, sharedQueryPlan);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    //Assertion
    ASSERT_EQ(executionNodes.size(), 3u);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1u) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1u);
            auto querySubPlan = querySubPlans[0u];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1u);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1u);
            for (const auto& children : actualRootOperator->getChildren()) {
                EXPECT_TRUE(children->instanceOf<SourceLogicalOperatorNode>());
            }
        } else if (executionNode->getId() == 2 ){
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1u);
            auto querySubPlan = querySubPlans[0];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1u);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            for (const auto& children : actualRootOperator->getChildren()) {
                EXPECT_TRUE(children->instanceOf<FilterLogicalOperatorNode>());
            }
        } else if (executionNode->getId() == 3 ) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1u);
            auto querySubPlan = querySubPlans[0];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1u);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            for (const auto& children : actualRootOperator->getChildren()) {
                EXPECT_TRUE(children->instanceOf<FilterLogicalOperatorNode>());
            }
        } else if (executionNode->getId() == 4) {

        } else {
            FAIL();
        }
        //topology->changeParent
    }
}

TEST_F(TopologyReconfigurationPhaseTest, testMovingNodePathLengthOne) {

    //todo: putting 0 resources will not allow placing source, but putting one allows filter
    setupTopologyAndSourceCatalog({4, 4, 4, 1, 1});
    topology->print();
    //Query query = Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());
    Query query = Query::from("car").joinWith(Query::from("car2")).where(Attribute("id")).equalsTo(Attribute("id")).window(TumblingWindow::of(IngestionTime(), Seconds(10))).sink(PrintSinkDescriptor::create());
    //todo: why do 2 operators also work with on a node with resources = 1
    //Query query = Query::from("car").filter(Attribute("id") < 45).map(Attribute("value") = 2 * Attribute("value")).sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan = query.getQueryPlan();

    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
    queryPlan = queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getSharedQueryId();
    auto queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, z3Context, false);
    queryPlacementPhase->execute(NES::PlacementStrategy::BottomUp, sharedQueryPlan);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    //Assertion
    ASSERT_EQ(executionNodes.size(), 3u);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1u) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1u);
            auto querySubPlan = querySubPlans[0u];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1u);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1u);
            for (const auto& children : actualRootOperator->getChildren()) {
                EXPECT_TRUE(children->instanceOf<SourceLogicalOperatorNode>());
            }
        } else if (executionNode->getId() == 2 ){
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1u);
            auto querySubPlan = querySubPlans[0];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1u);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            for (const auto& children : actualRootOperator->getChildren()) {
                EXPECT_TRUE(children->instanceOf<FilterLogicalOperatorNode>());
            }
        } else if (executionNode->getId() == 3 ) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1u);
            auto querySubPlan = querySubPlans[0];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1u);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            for (const auto& children : actualRootOperator->getChildren()) {
                EXPECT_TRUE(children->instanceOf<FilterLogicalOperatorNode>());
            }
        } else if (executionNode->getId() == 4) {

        } else {
            FAIL();
        }
    }
    auto pathNode1 = topology->findNodeWithId(2);
    auto pathNode2 = topology->findNodeWithId(3);
    auto mobileNode1 = topology->findNodeWithId(4);
    auto mobileNode2 = topology->findNodeWithId(5);
    topology->removeNodeAsChild(pathNode1, mobileNode1);
    topology->addNewTopologyNodeAsChild(pathNode2, mobileNode1);
    //todo: make constructor private and use create function
    auto topologyReconfigurationPhase = Optimizer::Experimental::TopologyReconfigurationPhase(globalExecutionPlan, topology);


}

/* Test query placement with top down strategy  */
TEST_F(TopologyReconfigurationPhaseTest, testPlacingQueryWithTopDownStrategy) {

    setupTopologyAndSourceCatalog({4, 4, 4});

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);

    Query query = Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan = query.getQueryPlan();

    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
    queryPlan = queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getSharedQueryId();
    auto queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, z3Context, false);
    queryPlacementPhase->execute(NES::PlacementStrategy::TopDown, sharedQueryPlan);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    //Assertion
    ASSERT_EQ(executionNodes.size(), 3u);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1u) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1u);
            auto querySubPlan = querySubPlans[0];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1u);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            std::vector<SourceLogicalOperatorNodePtr> sourceOperators = querySubPlan->getSourceOperators();
            ASSERT_EQ(sourceOperators.size(), 2u);
            for (const auto& sourceOperator : sourceOperators) {
                EXPECT_TRUE(sourceOperator->instanceOf<SourceLogicalOperatorNode>());
            }
        } else {
            EXPECT_TRUE(executionNode->getId() == 2 || executionNode->getId() == 3);
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1u);
            auto querySubPlan = querySubPlans[0];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1u);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            for (const auto& children : actualRootOperator->getChildren()) {
                EXPECT_TRUE(children->instanceOf<SourceLogicalOperatorNode>());
            }
        }
    }
}
