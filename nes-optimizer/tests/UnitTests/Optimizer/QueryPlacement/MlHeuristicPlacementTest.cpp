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
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Optimizer/Phases/QueryPlacementAmendmentPhase.hpp>
#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <Optimizer/Phases/TopologySpecificQueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>

#include <Services/RequestHandlerService.hpp>
#include <Util/Mobility/SpatialType.hpp>
#include <Util/Placement/PlacementStrategy.hpp>
#include <z3++.h>

using namespace NES;
using namespace z3;
using namespace Configurations;

class MlHeuristicPlacementTest : public Testing::BaseUnitTest {
  public:
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    TopologyPtr topology;
    NES::Optimizer::GlobalExecutionPlanPtr globalExecutionPlan;
    Optimizer::TypeInferencePhasePtr typeInferencePhase;
    std::shared_ptr<Catalogs::UDF::UDFCatalog> udfCatalog;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("MlHeuristicPlacementTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup MlHeuristicPlacementTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        NES_DEBUG("Setup MlHeuristicPlacementTest test case.");

        udfCatalog = Catalogs::UDF::UDFCatalog::create();
    }

    void topologyGenerator() {
        topology = Topology::create();

        std::vector<int> parents = {-1, 0, 0, 0, 1, 2, 3, 3, 4, 5, 5, 6, 7};
        std::vector<int> resources = {20, 20, 20, 20, 21, 22, 23, 23, 24, 25, 25, 26, 27};

        std::vector<int> tf_enabled_nodes = {1, 2, 3, 4, 5, 6, 7, 8, 11, 12};
        std::vector<int> low_throughput_sources = {11};
        std::vector<int> ml_hardwares = {};

        std::vector<int> sources{8, 9, 10, 11, 12};

        for (int i = 0; i < (int) resources.size(); i++) {
            WorkerId workerId = i + 1;

            std::map<std::string, std::any> properties;
            properties[NES::Worker::Properties::MAINTENANCE] = false;
            properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;
            if (std::count(tf_enabled_nodes.begin(), tf_enabled_nodes.end(), i)) {
                properties["tf_installed"] = true;
            }
            if (std::count(low_throughput_sources.begin(), low_throughput_sources.end(), i)) {
                properties["low_throughput_source"] = true;
            }
            if (std::count(ml_hardwares.begin(), ml_hardwares.end(), i)) {
                properties["ml_hardware"] = true;
            }

            workerId = topology->registerWorker(workerId, "localhost", 123, 124, resources[i], properties, 0, 0);
            if (i == 0) {
                topology->setRootTopologyNodeId(workerId);
            } else if (i > 1) {
                topology->addTopologyNodeAsChild(workerId - 1, workerId);
                topology->removeTopologyNodeAsChild(1, workerId);
            }
        }

        topology->print();

        auto irisSchema = Schema::create()
                              ->addField(createField("id", BasicType::UINT64))
                              ->addField(createField("SepalLengthCm", BasicType::FLOAT32))
                              ->addField(createField("SepalWidthCm", BasicType::FLOAT32))
                              ->addField(createField("PetalLengthCm", BasicType::FLOAT32))
                              ->addField(createField("PetalWidthCm", BasicType::FLOAT32))
                              ->addField(createField("SpeciesCode", BasicType::UINT64));

        const std::string streamName = "iris";
        //        SchemaPtr irisSchema = Schema::create()
        //                                   ->addField(createField("id", BasicType::UINT64))
        //                                   ->addField(createField("SepalLengthCm", BasicType::FLOAT32))
        //                                   ->addField(createField("SepalWidthCm", BasicType::FLOAT32))
        //                                   ->addField(createField("PetalLengthCm", BasicType::FLOAT32))
        //                                   ->addField(createField("PetalWidthCm", BasicType::FLOAT32))
        //                                   ->addField(createField("SpeciesCode", BasicType::UINT64))
        //                                   ->addField(createField("CreationTime", BasicType::UINT64));

        sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
        sourceCatalog->addLogicalSource(streamName, irisSchema);
        auto logicalSource = sourceCatalog->getLogicalSource(streamName);

        CSVSourceTypePtr csvSourceType = CSVSourceType::create(streamName, "test2");
        csvSourceType->setGatheringInterval(0);
        csvSourceType->setNumberOfTuplesToProducePerBuffer(0);
        auto physicalSource = PhysicalSource::create(csvSourceType);

        for (int source : sources) {
            auto streamCatalogEntry = Catalogs::Source::SourceCatalogEntry::create(physicalSource, logicalSource, source + 1);
            sourceCatalog->addPhysicalSource(streamName, streamCatalogEntry);
        }

        globalExecutionPlan = Optimizer::GlobalExecutionPlan::create();
        typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    }
};

/* Test query placement with Ml heuristic strategy  */
TEST_F(MlHeuristicPlacementTest, testPlacingQueryWithMlHeuristicStrategy) {

    topologyGenerator();
    Query query =
        Query::from("iris")
            .inferModel(
                std::string(TEST_DATA_DIRECTORY) + "/iris.tflite",
                {Attribute("SepalLengthCm"), Attribute("SepalWidthCm"), Attribute("PetalLengthCm"), Attribute("PetalWidthCm")},
                {Attribute("iris0", BasicType::FLOAT32),
                 Attribute("iris1", BasicType::FLOAT32),
                 Attribute("iris2", BasicType::FLOAT32)})
            .filter(Attribute("iris0") < 3.0)
            .project(Attribute("iris1"), Attribute("iris2"))
            .sink(PrintSinkDescriptor::create());

    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan->setPlacementStrategy(Optimizer::PlacementStrategy::MlHeuristic);

    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
    queryPlan = queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getId();
    auto queryPlacementAmendmentPhase = Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan,
                                                                                        topology,
                                                                                        typeInferencePhase,
                                                                                        coordinatorConfiguration);
    queryPlacementAmendmentPhase->execute(sharedQueryPlan);

    NES_DEBUG("MlHeuristicPlacementTest: topology: \n{}", topology->toString());
    NES_DEBUG("MlHeuristicPlacementTest: query plan \n{}", globalExecutionPlan->getAsString());
    NES_DEBUG("MlHeuristicPlacementTest: shared plan \n{}", sharedQueryPlan->getQueryPlan()->toString());

    auto executionNodes = globalExecutionPlan->getLockedExecutionNodesHostingSharedQueryId(queryId);
    ASSERT_EQ(executionNodes.size(), 13U);

    // Index represents the id of the execution node
    uint64_t totalQuerySubPlansOnNode1 = 1;
    uint64_t totalQuerySubPlansOnNode2 = 5;
    uint64_t totalQuerySubPlansOnNode3 = 5;
    uint64_t totalQuerySubPlansOnNode4 = 5;
    uint64_t totalQuerySubPlansOnNode5 = 5;
    uint64_t totalQuerySubPlansOnNode6 = 5;
    uint64_t totalQuerySubPlansOnNode7 = 5;
    uint64_t totalQuerySubPlansOnNode8 = 5;
    uint64_t totalQuerySubPlansOnNode9 = 5;
    uint64_t totalQuerySubPlansOnNode10 = 4;
    uint64_t totalQuerySubPlansOnNode11 = 3;
    uint64_t totalQuerySubPlansOnNode12 = 2;
    uint64_t totalQuerySubPlansOnNode13 = 1;
    std::vector<uint64_t> querySubPlanSizeCompare = {totalQuerySubPlansOnNode1,
                                                     totalQuerySubPlansOnNode2,
                                                     totalQuerySubPlansOnNode3,
                                                     totalQuerySubPlansOnNode4,
                                                     totalQuerySubPlansOnNode5,
                                                     totalQuerySubPlansOnNode6,
                                                     totalQuerySubPlansOnNode7,
                                                     totalQuerySubPlansOnNode8,
                                                     totalQuerySubPlansOnNode9,
                                                     totalQuerySubPlansOnNode10,
                                                     totalQuerySubPlansOnNode11,
                                                     totalQuerySubPlansOnNode12,
                                                     totalQuerySubPlansOnNode13};
    for (const auto& executionNode : executionNodes) {
        auto querySubPlans = executionNode->operator*()->getAllDecomposedQueryPlans(queryId);
        NES_INFO("Worker Id {} ", executionNode->operator*()->getId());
        EXPECT_EQ(querySubPlans.size(), querySubPlanSizeCompare[executionNode->operator*()->getId() - 1]);
        auto querySubPlan = querySubPlans[0];
        std::vector<OperatorPtr> actualRootOperators = querySubPlan->getRootOperators();
        EXPECT_EQ(actualRootOperators.size(), 1U);
    }
}