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
#include <Configurations/Enums/PlacementAmenderMode.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/InferModelLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Network/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkAssignerLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windows/Joins/JoinLogicalOperatorNode.hpp>
#include <Optimizer/Exceptions/QueryPlacementException.hpp>
#include <Optimizer/Phases/QueryPlacementPhase.hpp>
#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <Optimizer/Phases/SignatureInferencePhase.hpp>
#include <Optimizer/Phases/TopologySpecificQueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryMerger/Z3SignatureBasedCompleteQueryMergerRule.hpp>
#include <Optimizer/QueryMerger/Z3SignatureBasedPartialQueryMergerRule.hpp>
#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Mobility/SpatialType.hpp>
#include <gtest/gtest.h>
#include <z3++.h>

using namespace NES;
using namespace z3;
using namespace Configurations;

static const std::string sourceNameLeft = "log_left";
static const std::string sourceNameRight = "log_right";

class NemoJoinPlacementTest : public Testing::BaseUnitTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("QueryPlacementTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup QueryPlacementTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        NES_DEBUG("Setup QueryPlacementTest test case.");
    }

    static TopologyPtr setupTopology(uint64_t layers, uint64_t nodesPerNode, uint64_t leafNodesPerNode) {
        uint64_t resources = 100;
        uint64_t nodeId = 1;
        uint64_t leafNodes = 0;

        std::vector<WorkerId> nodes;
        std::vector<WorkerId> parents;

        std::map<std::string, std::any> properties;
        properties[NES::Worker::Properties::MAINTENANCE] = false;
        properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

        // Setup the topology
        TopologyPtr topology = Topology::create();
        topology->registerTopologyNode(nodeId, "localhost", 4000, 5000, resources, properties);
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
                    topology->registerTopologyNode(nodeId, "localhost", 4000 + nodeId, 5000 + nodeId, resources, properties);
                    topology->addTopologyNodeAsChild(parent, nodeId);
                    nodes.emplace_back(nodeId);
                    newParents.emplace_back(nodeId);
                }
            }
            parents = newParents;
        }

        NES_DEBUG("NemoPlacementTest: topology: {}", topology->toString());
        return topology;
    }

    static Catalogs::Source::SourceCatalogPtr setupSourceCatalog(const std::vector<WorkerId>& sourceNodes) {
        // Prepare the source and schema
        auto schema = Schema::create()
                          ->addField("id", BasicType::UINT32)
                          ->addField("value", BasicType::UINT64)
                          ->addField("timestamp", DataTypeFactory::createUInt64());
        Catalogs::Source::SourceCatalogPtr sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
        sourceCatalog->addLogicalSource(sourceNameLeft, schema);
        sourceCatalog->addLogicalSource(sourceNameRight, schema);

        for (auto nodeId : sourceNodes) {
            CSVSourceTypePtr csvSourceType;
            std::string sourceName;
            if (nodeId % 2 == 0) {
                // put even node ids to left join
                sourceName = sourceNameLeft;
                csvSourceType = CSVSourceType::create(sourceName, "left_" + std::to_string(nodeId));
            } else {
                // put odd node ids to right join
                sourceName = sourceNameRight;
                csvSourceType = CSVSourceType::create(sourceName, "right_" + std::to_string(nodeId));
            }
            auto physicalSource = PhysicalSource::create(csvSourceType);
            auto logicalSource = sourceCatalog->getLogicalSource(sourceName);

            auto sourceCatalogEntry1 = Catalogs::Source::SourceCatalogEntry::create(physicalSource, logicalSource, nodeId);
            sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry1);
        }
        return sourceCatalog;
    }

    std::vector<ExecutionNodePtr> runNemoPlacement(const Query& query,
                                                   const TopologyPtr& topology,
                                                   const Catalogs::Source::SourceCatalogPtr& sourceCatalog,
                                                   const Catalogs::UDF::UDFCatalogPtr& udfCatalog,
                                                   OptimizerConfiguration optimizerConfig) const {
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
        auto queryPlacementPhase =
            Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, coordinatorConfiguration);
        queryPlacementPhase->execute(sharedQueryPlan);
        return globalExecutionPlan->getExecutionNodesByQueryId(sharedQueryId);
    }
};

TEST_F(NemoJoinPlacementTest, testNemoJoinPlacement) {
    auto topology = setupTopology(3, 3, 2);
    auto sourceCatalog = setupSourceCatalog({7, 9, 10});
    std::shared_ptr<Catalogs::UDF::UDFCatalog> udfCatalog = Catalogs::UDF::UDFCatalog::create();

    auto optimizerConfig = Configurations::OptimizerConfiguration();
    //optimizerConfig.enableNemoPlacement = true;

    //run the placement
    Query query = Query::from(sourceNameLeft)
                      .joinWith(Query::from(sourceNameRight))
                      .where(Attribute("id"))
                      .equalsTo(Attribute("id"))
                      .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
                      .sink(NullOutputSinkDescriptor::create());

    auto executionNodes = runNemoPlacement(query, topology, sourceCatalog, udfCatalog, optimizerConfig);
}