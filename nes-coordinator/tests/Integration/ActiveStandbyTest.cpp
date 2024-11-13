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

#include <API/Expressions/Expressions.hpp>
#include <API/Query.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Optimizer/Phases/TopologySpecificQueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Services/QueryParsingService.hpp>
#include <z3++.h>

using namespace NES;

// class ActiveStandbyTest : public NES::Testing::NESBaseTest {
//   protected:
//     z3::ContextPtr z3Context;
//
//   public:
//     std::shared_ptr<QueryParsingService> queryParsingService;
//     SourceCatalogPtr sourceCatalog;
//     TopologyPtr topology;
//
//     /* Will be called before any test in this class are executed. */
//     static void SetUpTestCase() { std::cout << "Setup ActiveStandbyTest test class." << std::endl; }
//
//     /* Will be called before a test is executed. */
//     void SetUp() override {
//         NES::Logger::setupLogging("ActiveStandbyTest.log", NES::LogLevel::LOG_DEBUG);
//         auto cppCompiler = Compiler::CPPCompiler::create();
//         auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
//         queryParsingService = QueryParsingService::create(jitCompiler);
//     }
//
//     /* Will be called before a test is executed. */
//     void TearDown() override { std::cout << "Setup ActiveStandbyTest test case." << std::endl; }
//
//     /* Will be called after all tests in this class are finished. */
//     static void TearDownTestCase() { std::cout << "Tear down ActiveStandbyTest test class." << std::endl; }
//
//     void setupTopologyAndSourceCatalogSimpleShortDiamond() {
//         topology = Topology::create();
//
//         TopologyNodePtr rootNode = TopologyNode::create(4, "localhost", 123, 124, 5);
//         topology->setAsRoot(rootNode);
//
//         TopologyNodePtr middleNode1 = TopologyNode::create(3, "localhost", 123, 124, 10);
//         topology->addNewTopologyNodeAsChild(rootNode, middleNode1);
//
//         TopologyNodePtr middleNode2 = TopologyNode::create(2, "localhost", 123, 124, 10);
//         topology->addNewTopologyNodeAsChild(rootNode, middleNode2);
//
//         middleNode1->addAlternativeNode(middleNode2);
//         middleNode2->addAlternativeNode(middleNode1);
//
//         TopologyNodePtr sourceNode = TopologyNode::create(1, "localhost", 123, 124, 10);
//         topology->addNewTopologyNodeAsChild(middleNode1, sourceNode);
//         topology->addNewTopologyNodeAsChild(middleNode2, sourceNode);
//
//         std::string schema = "Schema::create()->addField(\"id\", BasicType::UINT32)"
//                              "->addField(\"value\", BasicType::UINT64);";
//         const std::string sourceName = "car";
//
//         sourceCatalog = std::make_shared<SourceCatalog>(queryParsingService);
//         sourceCatalog->addLogicalSource(sourceName, schema);
//         auto logicalSource = sourceCatalog->getSourceForLogicalSource(sourceName);
//         CSVSourceTypePtr csvSourceType = CSVSourceType::create();
//         csvSourceType->setGatheringInterval(0);
//         csvSourceType->setNumberOfTuplesToProducePerBuffer(0);
//         auto physicalSource = PhysicalSource::create(sourceName, "test2", csvSourceType);
//         SourceCatalogEntryPtr sourceCatalogEntry =
//             std::make_shared<SourceCatalogEntry>(physicalSource, logicalSource, sourceNode);
//         sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry);
//     }
// };
//
// TEST_F(ActiveStandbyTest, testActiveStandbyDiamond) {
//     setupTopologyAndSourceCatalogSimpleShortDiamond();
//
//     GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
//     auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog);
//     auto queryPlacementPhase = Optimizer::QueryPlacementPhase::create(globalExecutionPlan,
//                                                                       topology,
//                                                                       typeInferencePhase,
//                                                                       z3Context,
//                                                                       false /*query reconfiguration*/);
//
//     Query query = Query::from("car").map(Attribute("c") = Attribute("value")).sink(PrintSinkDescriptor::create());
//
//     QueryPlanPtr queryPlan = query.getQueryPlan();
//     queryPlan->setFaultToleranceType(FaultToleranceType::AS);
//     auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
//     auto queryId = sharedQueryPlan->getSharedQueryId();
//
//     auto topologySpecificQueryRewrite =
//         Optimizer::TopologySpecificQueryRewritePhase::create(sourceCatalog, Configurations::OptimizerConfiguration());
//     topologySpecificQueryRewrite->execute(queryPlan);
//     typeInferencePhase->execute(queryPlan);
//
//     queryPlacementPhase->execute(PlacementStrategy::BottomUp, sharedQueryPlan);
//     std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);
//
//     std::cout << "topology " + topology->toString() << std::endl;
//     std::cout << "query plan " + queryPlan->toString() << std::endl;
//
//     ASSERT_EQ(executionNodes.size(), 4U);
//     for (const auto& executionNode : executionNodes) {
//         if (executionNode->getId() == 1) {
//             std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
//             ASSERT_EQ(querySubPlans.size(), 1U);
//             auto querySubPlan = querySubPlans[0U];
//             std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
//             ASSERT_EQ(actualRootOperators.size(), 2U);
//             EXPECT_TRUE(actualRootOperators[0]->instanceOf<SinkLogicalOperatorNode>());
//             ASSERT_EQ(actualRootOperators[0]->getChildren().size(), 1U);
//             EXPECT_TRUE(actualRootOperators[0]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
//
//             EXPECT_TRUE(actualRootOperators[1]->instanceOf<SinkLogicalOperatorNode>());
//             ASSERT_EQ(actualRootOperators[1]->getChildren().size(), 1U);
//             EXPECT_TRUE(actualRootOperators[1]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
//         } else if (executionNode->getId() == 3) {
//             // map should be placed on cloud node
//             std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
//             ASSERT_EQ(querySubPlans.size(), 1U);
//             auto querySubPlan = querySubPlans[0U];
//             std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
//             ASSERT_EQ(actualRootOperators.size(), 1U);
//             OperatorNodePtr actualRootOperator = actualRootOperators[0];
//             ASSERT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
//             EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
//             ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
//             EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<MapLogicalOperatorNode>());
//             // map should have 2 sources
//             ASSERT_EQ(actualRootOperator->getChildren()[0]->getChildren().size(), 2U);
//         } else if (executionNode->getId() == 2) {
//             // map should be placed on cloud node
//             std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
//             ASSERT_EQ(querySubPlans.size(), 1U);
//             auto querySubPlan = querySubPlans[0U];
//             std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
//             ASSERT_EQ(actualRootOperators.size(), 1U);
//             OperatorNodePtr actualRootOperator = actualRootOperators[0];
//             EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
//             ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
//             EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<MapLogicalOperatorNode>());
//             // map should have 2 sources
//             ASSERT_EQ(actualRootOperator->getChildren()[0]->getChildren().size(), 2U);
//         } else {
//             std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
//             ASSERT_EQ(querySubPlans.size(), 1U);
//             auto querySubPlan = querySubPlans[0U];
//             std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
//             ASSERT_EQ(actualRootOperators.size(), 1U);
//             OperatorNodePtr actualRootOperator = actualRootOperators[0];
//             ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
//             EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
//             EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<SinkLogicalOperatorNode>());
//         }
//     }
// }