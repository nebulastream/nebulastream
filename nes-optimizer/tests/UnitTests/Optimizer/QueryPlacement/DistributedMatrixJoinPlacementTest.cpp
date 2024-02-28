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
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Source/SourceCatalogEntry.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <Operators/LogicalOperators/LogicalInferModelOperator.hpp>
#include <Operators/LogicalOperators/Network/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkAssignerLogicalOperator.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinOperator.hpp>
#include <Optimizer/Phases/QueryPlacementAmendmentPhase.hpp>
#include <Optimizer/Phases/QueryRewritePhase.hpp>
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
using namespace Optimizer;

static const std::string logSourceNameLeft = "log_left";
static const std::string logSourceNameRight = "log_right";

class DistributedMatrixJoinPlacementTest : public Testing::BaseUnitTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("DistributedMatrixJoinPlacementTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup DistributedMatrixJoinPlacementTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        NES_DEBUG("Setup DistributedMatrixJoinPlacementTest test case.");
    }

    /**
     * Create topology based on given layers, intermediate nodes, and source nodes
     */
    static TopologyPtr setupTopology(uint64_t layers, uint64_t nodesPerNode, uint64_t leafNodesPerNode) {
        uint64_t resources = 100;
        uint64_t nodeId = 1;
        uint64_t leafNodes = 0;

        std::vector<WorkerId> nodes;
        std::vector<WorkerId> parents;

        std::map<std::string, std::any> properties;
        properties[NES::Worker::Properties::MAINTENANCE] = false;

        // Setup the topology
        TopologyPtr topology = Topology::create();
        topology->registerWorker(nodeId, "localhost", 4000, 5000, resources, properties, 0, 0);
        topology->setRootTopologyNodeId(nodeId);
        nodes.emplace_back(nodeId);
        parents.emplace_back(nodeId);

        for (uint64_t i = 2; i <= layers; i++) {
            std::vector<WorkerId> newParents;
            for (auto parent : parents) {
                uint64_t nodeCnt = nodesPerNode;
                if (i == layers) {
                    nodeCnt = leafNodesPerNode;
                }
                for (uint64_t j = 0; j < nodeCnt; j++) {
                    if (i == layers) {
                        leafNodes++;
                    }
                    nodeId++;
                    topology->registerWorker(nodeId, "localhost", 4000 + nodeId, 5000 + nodeId, resources, properties, 0, 0);
                    topology->addTopologyNodeAsChild(parent, nodeId);
                    nodes.emplace_back(nodeId);
                    newParents.emplace_back(nodeId);
                }
            }
            parents = newParents;
        }

        NES_DEBUG("DistributedMatrixJoinPlacementTest: topology: {}", topology->toString());
        return topology;
    }

    static Catalogs::Source::SourceCatalogPtr setupSourceCatalog(const std::vector<WorkerId>& sourceNodes) {
        // Prepare the source and schema
        auto schema = Schema::create()
                          ->addField("id", BasicType::UINT32)
                          ->addField("value", BasicType::UINT64)
                          ->addField("timestamp", DataTypeFactory::createUInt64());
        Catalogs::Source::SourceCatalogPtr sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
        sourceCatalog->addLogicalSource(logSourceNameLeft, schema);
        sourceCatalog->addLogicalSource(logSourceNameRight, schema);

        for (auto nodeId : sourceNodes) {
            const auto isEven = nodeId % 2 == 0;
            std::string logSourceName = isEven ? logSourceNameLeft : logSourceNameRight;
            std::string phySourceName = isEven ? "left_" + std::to_string(nodeId) : "right_" + std::to_string(nodeId);
            CSVSourceTypePtr csvSourceType = CSVSourceType::create(logSourceName, phySourceName);

            auto physicalSource = PhysicalSource::create(csvSourceType);
            auto logicalSource = sourceCatalog->getLogicalSource(logSourceName);

            auto entry = Catalogs::Source::SourceCatalogEntry::create(physicalSource, logicalSource, nodeId);
            sourceCatalog->addPhysicalSource(logSourceName, entry);
        }
        return sourceCatalog;
    }

    static std::tuple<uint64_t, GlobalExecutionPlanPtr> runPlacement(const Query& query,
                                                                     const TopologyPtr& topology,
                                                                     const Catalogs::Source::SourceCatalogPtr& sourceCatalog,
                                                                     const OptimizerConfiguration& optimizerConfig) {
        std::shared_ptr<Catalogs::UDF::UDFCatalog> udfCatalog = Catalogs::UDF::UDFCatalog::create();
        auto testQueryPlan = query.getQueryPlan();
        testQueryPlan->setPlacementStrategy(Optimizer::PlacementStrategy::BottomUp);

        // Prepare the placement
        GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
        auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);

        // Execute optimization phases prior to placement
        testQueryPlan = typeInferencePhase->execute(testQueryPlan);
        auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
        auto queryReWritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
        testQueryPlan = queryReWritePhase->execute(testQueryPlan);
        typeInferencePhase->execute(testQueryPlan);

        auto topologySpecificQueryRewrite =
            Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, optimizerConfig);
        topologySpecificQueryRewrite->execute(testQueryPlan);
        typeInferencePhase->execute(testQueryPlan);

        // Execute the placement
        auto sharedQueryPlan = SharedQueryPlan::create(testQueryPlan);
        auto sharedQueryId = sharedQueryPlan->getId();
        auto queryPlacementPhase = Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan,
                                                                                   topology,
                                                                                   typeInferencePhase,
                                                                                   coordinatorConfiguration);
        queryPlacementPhase->execute(sharedQueryPlan);
        return {sharedQueryId, globalExecutionPlan};
    }

    template<typename T>
    static void verifyChildrenOfType(const std::vector<DecomposedQueryPlanPtr>& querySubPlans,
                                     uint64_t expectedSubPlanSize = 1,
                                     uint64_t expectedRootOperators = 1) {
        ASSERT_EQ(querySubPlans.size(), expectedSubPlanSize);
        for (auto& querySubPlan : querySubPlans) {
            std::vector<OperatorPtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), expectedRootOperators);
            OperatorPtr actualRootOperator = actualRootOperators[0];
            ASSERT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperator>());
            auto children = actualRootOperator->getChildren();
            for (const auto& child : children) {
                ASSERT_TRUE(child->instanceOf<T>());
            }
        }
    }

    template<typename T>
    static void verifySourceOperators(std::vector<DecomposedQueryPlanPtr>& querySubPlans,
                                      uint64_t expectedSubPlanSize,
                                      uint64_t expectedSourceOperatorSize) {
        ASSERT_EQ(querySubPlans.size(), expectedSubPlanSize);
        auto querySubPlan = querySubPlans[0];
        std::vector<SourceLogicalOperatorPtr> sourceOperators = querySubPlan->getSourceOperators();
        ASSERT_EQ(sourceOperators.size(), expectedSourceOperatorSize);
        for (const auto& sourceOperator : sourceOperators) {
            EXPECT_TRUE(sourceOperator->instanceOf<SourceLogicalOperator>());
            auto sourceDescriptor = sourceOperator->as_if<SourceLogicalOperator>()->getSourceDescriptor();
            ASSERT_TRUE(sourceDescriptor->instanceOf<T>());
        }
    }
};

TEST_F(DistributedMatrixJoinPlacementTest, testNemoJoinPlacement) {
    // create flat topology with 1 coordinator and 4 sources
    const std::vector<WorkerId>& sourceNodes = {2, 3, 4, 5};
    auto topology = setupTopology(2, 3, 4);
    auto sourceCatalog = setupSourceCatalog(sourceNodes);

    auto optimizerConfig = Configurations::OptimizerConfiguration();
    optimizerConfig.performDistributedWindowOptimization = true;

    //run the placement
    Query query = Query::from(logSourceNameLeft)
                      .joinWith(Query::from(logSourceNameRight))
                      .where(Attribute("id"))
                      .equalsTo(Attribute("id"))
                      .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
                      .sink(NullOutputSinkDescriptor::create());

    auto outputTuple = runPlacement(query, topology, sourceCatalog, optimizerConfig);
    auto queryId = std::get<0>(outputTuple);
    auto executionPlan = std::get<1>(outputTuple);
    auto executionNodes = executionPlan->getLockedExecutionNodesHostingSharedQueryId(queryId);
    EXPECT_EQ(executionNodes.size(), 5);// topology contains 1 coordinator, 4 workers

    for (const auto& executionNode : executionNodes) {
        std::vector<DecomposedQueryPlanPtr> querySubPlans = executionNode->operator*()->getAllDecomposedQueryPlans(queryId);
        if (executionNode->operator*()->getId() == 1u) {
            // coordinator should have 1 sink, 8 network sources
            verifyChildrenOfType<LogicalJoinOperator>(querySubPlans, 1, 1);
            verifySourceOperators<NES::Network::NetworkSourceDescriptor>(querySubPlans, 1, 2 * sourceNodes.size());
        } else {
            // 2x replication factor, therefore each source should have 2 network sources
            verifyChildrenOfType<WatermarkAssignerLogicalOperator>(querySubPlans, 1, 2);
            verifySourceOperators<LogicalSourceDescriptor>(querySubPlans, 1, 1);
        }
    }
    // std::this_thread::sleep_for(std::chrono::milliseconds(4000));
}