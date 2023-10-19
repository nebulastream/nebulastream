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
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Phases/QueryPlacementPhase.hpp>
#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <Optimizer/Phases/TopologySpecificQueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Services/QueryParsingService.hpp>
#include <Services/QueryService.hpp>
#include <Util/Mobility/SpatialType.hpp>
#include <Util/PlacementStrategy.hpp>
#include <z3++.h>

using namespace NES;
using namespace z3;
using namespace Configurations;

class MlHeuristicPlacementTest : public Testing::BaseUnitTest {
  public:
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    TopologyPtr topology;
    QueryParsingServicePtr queryParsingService;
    GlobalExecutionPlanPtr globalExecutionPlan;
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
        auto cppCompiler = Compiler::CPPCompiler::create();
        auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        queryParsingService = QueryParsingService::create(jitCompiler);
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

        std::map<std::string, std::any> properties;
        properties[NES::Worker::Properties::MAINTENANCE] = false;
        properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;
        std::vector<TopologyNodePtr> nodes;
        for (int i = 0; i < (int) resources.size(); i++) {
            nodes.push_back(TopologyNode::create(i, "localhost", 123, 124, resources[i], properties));
            if (i == 0) {
                topology->setAsRoot(nodes[i]);
            } else {
                topology->addNewTopologyNodeAsChild(nodes[parents[i]], nodes[i]);
            }
            if (std::count(tf_enabled_nodes.begin(), tf_enabled_nodes.end(), i)) {
                nodes[i]->addNodeProperty("tf_installed", true);
            }
            if (std::count(low_throughput_sources.begin(), low_throughput_sources.end(), i)) {
                nodes[i]->addNodeProperty("low_throughput_source", true);
            }
            if (std::count(ml_hardwares.begin(), ml_hardwares.end(), i)) {
                nodes[i]->addNodeProperty("ml_hardware", true);
            }
        }

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
            Catalogs::Source::SourceCatalogEntryPtr streamCatalogEntry =
                std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, nodes[source]);
            sourceCatalog->addPhysicalSource(streamName, streamCatalogEntry);
        }

        globalExecutionPlan = GlobalExecutionPlan::create();
        typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    }
};

/* Test query placement with Ml heuristic strategy  */
TEST_F(MlHeuristicPlacementTest, testPlacingQueryWithMlHeuristicStrategy) {

    topologyGenerator();
    Query query =
        Query::from("iris")
            .inferModel(
                "../../../test_data/iris.tflite",
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
    auto queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, coordinatorConfiguration);
    queryPlacementPhase->execute(sharedQueryPlan);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    NES_DEBUG("MlHeuristicPlacementTest: topology: \n{}", topology->toString());
    NES_DEBUG("MlHeuristicPlacementTest: query plan \n{}", globalExecutionPlan->getAsString());
    NES_DEBUG("MlHeuristicPlacementTest: shared plan \n{}", sharedQueryPlan->getQueryPlan()->toString());

    ASSERT_EQ(executionNodes.size(), 13U);
    // Index represents the id of the execution node
    std::vector<uint64_t> querySubPlanSizeCompare = {1, 1, 2, 2, 1, 2, 1, 1, 1, 1, 1, 1, 1};
    for (const auto& executionNode : executionNodes) {
        std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
        auto querySubPlan = querySubPlans[0];
        ASSERT_EQ(querySubPlans.size(), querySubPlanSizeCompare[executionNode->getId()]);
        std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
        ASSERT_EQ(actualRootOperators.size(), 1U);
    }
}