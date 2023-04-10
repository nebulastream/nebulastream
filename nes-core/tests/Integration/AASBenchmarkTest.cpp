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
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/LambdaSourceType.hpp>
#include <Catalogs/UDF/UdfCatalog.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <NesBaseTest.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/ProjectionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Phases/QueryMergerPhase.hpp>
#include <Optimizer/Phases/QueryPlacementPhase.hpp>
#include <Optimizer/Phases/TopologySpecificQueryRewritePhase.hpp>
#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryPlacement/AdaptiveActiveStandby.hpp>
#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <Optimizer/QueryPlacement/PlacementStrategyFactory.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Services/QueryService.hpp>
#include <Services/QueryParsingService.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <chrono>
#include <gtest/gtest.h>
#include <thread>

using namespace std;
namespace NES {

using namespace Configurations;
const int nSourceWorkers = 4;   // should be an even number for correct topologies
const std::chrono::milliseconds runtime{120000}; // 120s
auto placementStrategyAAS = PlacementStrategy::defaultStrategyAAS;

class AASBenchmarkTest : public Testing::NESBaseTest {
  public:
    std::string ipAddress = "127.0.0.1";
    CoordinatorConfigurationPtr coordinatorConfig;
    WorkerConfigurationPtr level2WorkerConfig, level3WorkerConfig, reserveWorkerConfig;
    WorkerConfigurationPtr sourceWorkerConfigs[nSourceWorkers];
    SchemaPtr inputSchema;
    const std::string sourceName = "A";
    const LinkPropertyPtr linkProperty = std::make_shared<LinkProperty>(LinkProperty(100, 0));

    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    Catalogs::UDF::UdfCatalogPtr udfCatalog;
    std::shared_ptr<QueryParsingService> queryParsingService;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("AASBenchmarkTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup AASBenchmarkTest test class.");
    }

    void SetUp() override {
        Testing::NESBaseTest::SetUp();

        auto cppCompiler = Compiler::CPPCompiler::create();
        auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        queryParsingService = QueryParsingService::create(jitCompiler);
        udfCatalog = Catalogs::UDF::UdfCatalog::create();

        // id = 1
        coordinatorConfig = CoordinatorConfiguration::create();
        coordinatorConfig->rpcPort = *rpcCoordinatorPort;
        coordinatorConfig->restPort = *restPort;
        coordinatorConfig->restServerType = ServerType::Oatpp;

        level2WorkerConfig = WorkerConfiguration::create();
        level2WorkerConfig->coordinatorPort = *rpcCoordinatorPort;
        level2WorkerConfig->enableStatisticOutput = true;
        level2WorkerConfig->numberOfSlots = 10;

        level3WorkerConfig = WorkerConfiguration::create();
        level3WorkerConfig->coordinatorPort = *rpcCoordinatorPort;
        level3WorkerConfig->enableStatisticOutput = true;
        level3WorkerConfig->numberOfSlots = 6;

        reserveWorkerConfig = WorkerConfiguration::create();
        reserveWorkerConfig->coordinatorPort = *rpcCoordinatorPort;
        reserveWorkerConfig->enableStatisticOutput = true;
        reserveWorkerConfig->numberOfSlots = 5;

        for (int i = 0; i < nSourceWorkers; ++i) {
            auto sourceWorkerConfig = WorkerConfiguration::create();
            sourceWorkerConfig->coordinatorPort = *rpcCoordinatorPort;
            sourceWorkerConfig->enableStatisticOutput = true;
            sourceWorkerConfig->lambdaSource = i+1;
            sourceWorkerConfig->numberOfSlots = 0;
            sourceWorkerConfigs[i] = sourceWorkerConfig;
        }

        inputSchema = Schema::create()
                          ->addField("a", DataTypeFactory::createUInt64())
                          ->addField("b", DataTypeFactory::createUInt64())
                          ->addField("c", DataTypeFactory::createUInt64())
                          ->addField("d", DataTypeFactory::createUInt64())
                          ->addField("e", DataTypeFactory::createUInt64())
                          ->addField("f", DataTypeFactory::createUInt64())
                          ->addField("g", DataTypeFactory::createUInt64())
                          ->addField("h", DataTypeFactory::createUInt64())
//                          ->addField("i", DataTypeFactory::createUInt64())
//                          ->addField("j", DataTypeFactory::createUInt64())
//                          ->addField("k", DataTypeFactory::createUInt64())
//                          ->addField("l", DataTypeFactory::createUInt64())
//                          ->addField("m", DataTypeFactory::createUInt64())
//                          ->addField("n", DataTypeFactory::createUInt64())
//                          ->addField("o", DataTypeFactory::createUInt64())
//                          ->addField("p", DataTypeFactory::createUInt64())
//                          ->addField("q", DataTypeFactory::createUInt64())
//                          ->addField("r", DataTypeFactory::createUInt64())
//                          ->addField("s", DataTypeFactory::createUInt64())
//                          ->addField("t", DataTypeFactory::createUInt64())
//                          ->addField("u", DataTypeFactory::createUInt64())
//                          ->addField("v", DataTypeFactory::createUInt64())
//                          ->addField("w", DataTypeFactory::createUInt64())
//                          ->addField("x", DataTypeFactory::createUInt64())
                          ->addField("timestamp1", DataTypeFactory::createUInt64())
                          ->addField("timestamp2", DataTypeFactory::createUInt64());
    }

    string createQueryString(const string& outputFilePath) {
        return
            "Query::from(\"" + sourceName + "\")"
            + ".filter(Attribute(\"c\") > 0)"       // avg DMF = 0.66
            + ".filter(Attribute(\"e\") < 4)"      // avg DMF = 0.8
            + R"(.project(Attribute("a"),Attribute("b"),Attribute("c"),Attribute("d"),Attribute("e"),Attribute("f"),Attribute("g"),Attribute("h"),Attribute("timestamp1"),Attribute("timestamp2"),Attribute("a").as("a2"),Attribute("a").as("a3"),Attribute("a").as("a4"),Attribute("a").as("a5"),Attribute("a").as("a6"),Attribute("a").as("a7"),Attribute("a").as("a8"),Attribute("a").as("a9"),Attribute("a").as("a10"),Attribute("a").as("a11")))"   // DMF = 1.1
            + ".sink(FileSinkDescriptor::create(\""
            + outputFilePath
            + R"(", "CSV_FORMAT", "APPEND"));)";
    }

    void addSlotsAndLinkProperties(const NesCoordinatorPtr& crd) {
        TopologyPtr topology = crd->getTopology();
        std::deque<TopologyNodePtr> nodeQueue;
        nodeQueue.push_back(topology->getRoot());
        while (!nodeQueue.empty()) {
            TopologyNodePtr currentNode = nodeQueue.front()->as<TopologyNode>();
            nodeQueue.pop_front();

            if (currentNode->getId() == topology->getRoot()->getId())
                currentNode->addNodeProperty("slots", 0);
            else
                currentNode->addNodeProperty("slots", (int) currentNode->getAvailableResources());

            for (const auto& target: currentNode->getParents()) {
                TopologyNodePtr targetTopologyNode = target->as<TopologyNode>();
                currentNode->addLinkProperty(targetTopologyNode, linkProperty);
                NES_DEBUG("AASBenchmarkTest: adding link between " << currentNode->getId() << " and " << targetTopologyNode->getId());
            }

            for (const auto& target: currentNode->getChildren()) {
                TopologyNodePtr targetTopologyNode = target->as<TopologyNode>();
                currentNode->addLinkProperty(targetTopologyNode, linkProperty);
                NES_DEBUG("AASBenchmarkTest: adding link between " << currentNode->getId() << " and " << targetTopologyNode->getId());

                nodeQueue.push_back(targetTopologyNode);
            }
        }
    }

    /*
    *     1
    *     2
    *     3
    * /   |    \
    * 4   5 .. n+3
    */
    TopologyPtr setupTopologyAndSourceCatalogSequential(int n) {
        auto topology = Topology::create();

        TopologyNodePtr rootNode = TopologyNode::create(1, ipAddress, 123, 124, UINT16_MAX);
        rootNode->addNodeProperty("slots", 0);
        topology->setAsRoot(rootNode);

        TopologyNodePtr level2Node = TopologyNode::create(2, ipAddress, 123, 124, 3*n-2);
        level2Node->addNodeProperty("slots", 3*n-2);
        topology->addNewTopologyNodeAsChild(rootNode, level2Node);

        TopologyNodePtr level3Node = TopologyNode::create(3, ipAddress, 123, 124, 2*n-2);
        level3Node->addNodeProperty("slots", 2*n-2);
        topology->addNewTopologyNodeAsChild(level2Node, level3Node);

        std::map<int, TopologyNodePtr> sourceNodes;
        for (int i = 0; i < n; ++i) {
            TopologyNodePtr sourceNode = TopologyNode::create(i+4, ipAddress, 123, 124, 0);
            sourceNode->addNodeProperty("slots", 0);
            topology->addNewTopologyNodeAsChild(level3Node, sourceNode);

            sourceNodes[i] = sourceNode;
        }

        rootNode->addLinkProperty(level2Node, linkProperty);
        level2Node->addLinkProperty(rootNode, linkProperty);

        level2Node->addLinkProperty(level3Node, linkProperty);
        level3Node->addLinkProperty(level2Node, linkProperty);

        for (int i = 0; i < n; ++i) {
            sourceNodes[i]->addLinkProperty(level3Node, linkProperty);
            level3Node->addLinkProperty(sourceNodes[i], linkProperty);
        }

        sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(queryParsingService);
        sourceCatalog->addLogicalSource(sourceName, inputSchema);
        auto logicalSource = sourceCatalog->getLogicalSource(sourceName);
        CSVSourceTypePtr csvSourceType = CSVSourceType::create();
        csvSourceType->setGatheringInterval(0);
        csvSourceType->setNumberOfTuplesToProducePerBuffer(0);

        for (int i = 0; i < n; ++i) {
            std::stringstream name;
            name << "A" << i+1;

            auto physicalSource = PhysicalSource::create(sourceName, name.str(), csvSourceType);
            Catalogs::Source::SourceCatalogEntryPtr sourceCatalogEntry =
                std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, sourceNodes[i]);
            sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry);
        }

        return topology;
    }

    /*
     *        1
     * /      |      \
     * 2      3    .. n+1
     * n+2   n+3   .. 2n+1
     * 2n+2  2n+3  .. 3n+1
     */
    TopologyPtr setupTopologyAndSourceCatalogFatLowConnectivity(int n) {
        auto topology = Topology::create();

        TopologyNodePtr rootNode = TopologyNode::create(1, ipAddress, 123, 124, UINT16_MAX);
        rootNode->addNodeProperty("slots", 0);
        topology->setAsRoot(rootNode);

        std::map<int, TopologyNodePtr> level2Nodes, level3Nodes;
        for (int i = 0; i < n; ++i) {
            TopologyNodePtr level2Node = TopologyNode::create(i+2, ipAddress, 123, 124, 10);
            level2Node->addNodeProperty("slots", 10);
            topology->addNewTopologyNodeAsChild(rootNode, level2Node);
            level2Nodes[i] = level2Node;

            TopologyNodePtr level3Node = TopologyNode::create(n + i + 2, ipAddress, 123, 124, 6);
            level3Node->addNodeProperty("slots", 6);
            topology->addNewTopologyNodeAsChild(level2Node, level3Node);
            level3Nodes[i] = level3Node;
        }

        std::map<int, TopologyNodePtr> sourceNodes;
        for (int i = 0; i < n; ++i) {
            TopologyNodePtr sourceNode = TopologyNode::create(2*n + i + 2, ipAddress, 123, 124, 0);
            sourceNode->addNodeProperty("slots", 0);
            topology->addNewTopologyNodeAsChild(level3Nodes[i], sourceNode);
            sourceNodes[i] = sourceNode;
        }

        for (int i = 0; i < n; ++i) {
            sourceNodes[i]->addLinkProperty(level3Nodes[i], linkProperty);
            level3Nodes[i]->addLinkProperty(sourceNodes[i], linkProperty);

            level2Nodes[i]->addLinkProperty(level3Nodes[i], linkProperty);
            level3Nodes[i]->addLinkProperty(level2Nodes[i], linkProperty);

            level2Nodes[i]->addLinkProperty(rootNode, linkProperty);
            rootNode->addLinkProperty(level2Nodes[i], linkProperty);
        }

        sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(queryParsingService);
        sourceCatalog->addLogicalSource(sourceName, inputSchema);
        auto logicalSource = sourceCatalog->getLogicalSource(sourceName);
        CSVSourceTypePtr csvSourceType = CSVSourceType::create();
        csvSourceType->setGatheringInterval(0);
        csvSourceType->setNumberOfTuplesToProducePerBuffer(0);

        for (int i = 0; i < n; ++i) {
            std::stringstream name;
            name << "A" << i+1;

            auto physicalSource = PhysicalSource::create(sourceName, name.str(), csvSourceType);
            Catalogs::Source::SourceCatalogEntryPtr sourceCatalogEntry =
                std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, sourceNodes[i]);
            sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry);
        }

        return topology;
    }


    /*
     *           1
     * /      /     \     \
     * 2      3     ..    n+1
     * |  \/  |      | \/ |
     * |  /\  |      | /\ |
     * n+2   n+3    ..    2n+1
     * |  \/  |      | \/ |
     * |  /\  |      | /\ |
     * 2n+2  2n+3   ..    3n+1
     */
    TopologyPtr setupTopologyAndSourceCatalogFatHighConnectivity(int n) {
        auto topology = Topology::create();

        TopologyNodePtr rootNode = TopologyNode::create(1, ipAddress, 123, 124, UINT16_MAX);
        rootNode->addNodeProperty("slots", 0);
        topology->setAsRoot(rootNode);

        std::map<int, TopologyNodePtr> level2Nodes, level3Nodes;
        for (int i = 0; i < n; ++i) {
            TopologyNodePtr level2Node = TopologyNode::create(i+2, ipAddress, 123, 124, 10);
            level2Node->addNodeProperty("slots", 10);
            topology->addNewTopologyNodeAsChild(rootNode, level2Node);
            level2Nodes[i] = level2Node;

            TopologyNodePtr level3Node = TopologyNode::create(n + i + 2, ipAddress, 123, 124, 6);
            level3Node->addNodeProperty("slots", 6);
            topology->addNewTopologyNodeAsChild(level2Node, level3Node);
            level3Nodes[i] = level3Node;
        }

        std::map<int, TopologyNodePtr> sourceNodes;
        for (int i = 0; i < n; ++i) {
            TopologyNodePtr sourceNode = TopologyNode::create(2*n + i + 2, ipAddress, 123, 124, 0);
            sourceNode->addNodeProperty("slots", 0);
            topology->addNewTopologyNodeAsChild(level3Nodes[i], sourceNode);
            sourceNodes[i] = sourceNode;
        }

        for (int i = 0; i < n; ++i) {
            sourceNodes[i]->addLinkProperty(level3Nodes[i], linkProperty);
            level3Nodes[i]->addLinkProperty(sourceNodes[i], linkProperty);

            level2Nodes[i]->addLinkProperty(level3Nodes[i], linkProperty);
            level3Nodes[i]->addLinkProperty(level2Nodes[i], linkProperty);

            level2Nodes[i]->addLinkProperty(rootNode, linkProperty);
            rootNode->addLinkProperty(level2Nodes[i], linkProperty);

//            if (i != 0) {
//                topology->addNewTopologyNodeAsChild(level3Nodes[i], sourceNodes[i-1]);
//                sourceNodes[i-1]->addLinkProperty(level3Nodes[i], linkProperty);
//                level3Nodes[i]->addLinkProperty(sourceNodes[i-1], linkProperty);
//
//                topology->addNewTopologyNodeAsChild(level2Nodes[i-1], level3Nodes[i]);
//                level2Nodes[i-1]->addLinkProperty(level3Nodes[i], linkProperty);
//                level3Nodes[i]->addLinkProperty(level2Nodes[i-1], linkProperty);
//            }
//
//            if (i !=  n-1) {
//                topology->addNewTopologyNodeAsChild(level3Nodes[i], sourceNodes[i+1]);
//                sourceNodes[i+1]->addLinkProperty(level3Nodes[i], linkProperty);
//                level3Nodes[i]->addLinkProperty(sourceNodes[i+1], linkProperty);
//
//                topology->addNewTopologyNodeAsChild(level2Nodes[i+1], level3Nodes[i]);
//                level2Nodes[i+1]->addLinkProperty(level3Nodes[i], linkProperty);
//                level3Nodes[i]->addLinkProperty(level2Nodes[i+1], linkProperty);
//            }

            if (i % 2 == 0) {
                topology->addNewTopologyNodeAsChild(level3Nodes[i], sourceNodes[i+1]);
                sourceNodes[i+1]->addLinkProperty(level3Nodes[i], linkProperty);
                level3Nodes[i]->addLinkProperty(sourceNodes[i+1], linkProperty);

                topology->addNewTopologyNodeAsChild(level2Nodes[i+1], level3Nodes[i]);
                level2Nodes[i+1]->addLinkProperty(level3Nodes[i], linkProperty);
                level3Nodes[i]->addLinkProperty(level2Nodes[i+1], linkProperty);
            } else {
                topology->addNewTopologyNodeAsChild(level3Nodes[i], sourceNodes[i-1]);
                sourceNodes[i-1]->addLinkProperty(level3Nodes[i], linkProperty);
                level3Nodes[i]->addLinkProperty(sourceNodes[i-1], linkProperty);

                topology->addNewTopologyNodeAsChild(level2Nodes[i-1], level3Nodes[i]);
                level2Nodes[i-1]->addLinkProperty(level3Nodes[i], linkProperty);
                level3Nodes[i]->addLinkProperty(level2Nodes[i-1], linkProperty);
            }
        }

        sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(queryParsingService);
        sourceCatalog->addLogicalSource(sourceName, inputSchema);
        auto logicalSource = sourceCatalog->getLogicalSource(sourceName);
        CSVSourceTypePtr csvSourceType = CSVSourceType::create();
        csvSourceType->setGatheringInterval(0);
        csvSourceType->setNumberOfTuplesToProducePerBuffer(0);

        for (int i = 0; i < n; ++i) {
            std::stringstream name;
            name << "A" << i+1;

            auto physicalSource = PhysicalSource::create(sourceName, name.str(), csvSourceType);
            Catalogs::Source::SourceCatalogEntryPtr sourceCatalogEntry =
                std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, sourceNodes[i]);
            sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry);
        }

        return topology;
    }

    void assignOperatorProperties(const QueryPlanPtr& queryPlan) {
        auto sources = queryPlan->getLeafOperators();   // sources
        for (const auto& srcOperator: sources) {
            // source
            double dmf = 1;
            double input = 100;
            double output = input * dmf;
            srcOperator->addProperty("output", output);

            // ".filter(Attribute(\"c\") > 0)"       // avg DMF = 0.66
            auto firstFilterOperator = srcOperator->getParents()[0]->as<OperatorNode>();
            dmf = 0.66;
            input = output;
            output = input * dmf;
            firstFilterOperator->addProperty("output", output);

            // ".filter(Attribute(\"e\") < 4)"      // avg DMF = 0.8
            auto secondFilterOperator = firstFilterOperator->getParents()[0]->as<OperatorNode>();
            dmf = 0.8;
            input = output;
            output = input * dmf;
            secondFilterOperator->addProperty("output", output);

            // R"(.project(...))"   // DMF = 2
            auto projectOperator = secondFilterOperator->getParents()[0]->as<OperatorNode>();
            dmf = 2;
            input = output;
            output = input * dmf;
            projectOperator->addProperty("output", output);
            projectOperator->addProperty("cost", 2);
        }
    }
};


/*
* @brief test
*/
TEST_F(AASBenchmarkTest, test) {
    int nLevel2Workers = 1;
    int nLevel3Workers = 1;

    // setup coordinator

    // level 1
    NES_INFO("AASBenchmarkTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getSourceCatalogService()->registerLogicalSource(sourceName, inputSchema);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("AASBenchmarkTest: Coordinator started successfully");

    // setup workers

    // level 2
    std::map<int, NesWorkerPtr> level2Workers;
    for (int i = 0; i < nLevel2Workers; ++i) {
        NES_INFO("AASBenchmarkTest: Start level2Worker" << i);
        NesWorkerPtr level2Worker = std::make_shared<NesWorker>(std::move(level2WorkerConfig));
        bool retStart = level2Worker->start(/**blocking**/ false, /**withConnect**/ true);
        EXPECT_TRUE(retStart);
        NES_INFO("AASBenchmarkTest: level2Worker" << i << " started successfully");
        level2Workers[i] = level2Worker;
    }

    // level 3
    std::map<int, NesWorkerPtr> level3Workers;
    for (int i = 0; i < nLevel3Workers; ++i) {
        NES_INFO("AASBenchmarkTest: Start level3Worker" << i);
        NesWorkerPtr level3Worker = std::make_shared<NesWorker>(std::move(level3WorkerConfig));
        bool retStart = level3Worker->start(/**blocking**/ false, /**withConnect**/ true);
        EXPECT_TRUE(retStart);
        level3Worker->removeParent(1);
        level3Worker->addParent(level2Workers[0]->getWorkerId());
        NES_INFO("AASBenchmarkTest: level3Worker" << i << " started successfully");
        level3Workers[i] = level3Worker;
    }

    // source level
    std::map<int, NesWorkerPtr> sourceWorkers;
    for (int i = 0; i < nSourceWorkers; ++i) {
        NES_INFO("AASBenchmarkTest: Start sourceWorker" << i);
        NesWorkerPtr srcWrk = std::make_shared<NesWorker>(std::move(sourceWorkerConfigs[i]));
        bool retStart = srcWrk->start(/**blocking**/ false, /**withConnect**/ true);
        EXPECT_TRUE(retStart);
        srcWrk->removeParent(1);
//        srcWrk->addParent(level3Workers[0]->getWorkerId());
        srcWrk->addParent(level3Workers[0]->getWorkerId());
//        srcWrk->addParent(level2Workers[1]->getWorkerId());
        NES_INFO("AASBenchmarkTest: srcWrk" << i << " started successfully");
        sourceWorkers[i] = srcWrk;
    }

    // finish setting up the topology by adding slots and linkProperties
    addSlotsAndLinkProperties(crd);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = "AASBenchmarkTest.out";
    remove(outputFilePath.c_str());

    NES_INFO("AASBenchmarkTest: Submit query");
    string query = createQueryString(outputFilePath);

    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, "ILP", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));

    for (const auto& [i, level2Worker] : level2Workers)
        EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(level2Worker, queryId, globalQueryPlan, 1));
    for (const auto& [i, level3Worker] : level3Workers)
        EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(level3Worker, queryId, globalQueryPlan, 1));
    for (const auto& [i, sourceWorker] : sourceWorkers)
        EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(sourceWorker, queryId, globalQueryPlan, 1));

    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    NES_DEBUG("AASBenchmarkTest:: Topology print: " << crd->getTopology()->toString());


    NES_DEBUG("AASBenchmarkTest: Sleep");
    std::this_thread::sleep_for(runtime);

    NES_INFO("AASBenchmarkTest: Remove query");
    queryService->validateAndQueueStopQueryRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    for (const auto& [i, sourceWorker] : sourceWorkers) {
        NES_INFO("AASBenchmarkTest: Stop worker sourceWorker" << i);
        bool retStopWrk = sourceWorker->stop(true);
        EXPECT_TRUE(retStopWrk);
    }

    for (const auto& [i, level3Worker] : level3Workers) {
        NES_INFO("AASBenchmarkTest: Stop worker level3Worker" << i);
        bool retStopWrk = level3Worker->stop(true);
        EXPECT_TRUE(retStopWrk);
    }

    for (const auto& [i, level2Worker] : level2Workers) {
        NES_INFO("AASBenchmarkTest: Stop worker level2Worker" << i);
        bool retStopWrk = level2Worker->stop(true);
        EXPECT_TRUE(retStopWrk);
    }


    NES_INFO("AASBenchmarkTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("AASBenchmarkTest: Test finished");
}


// TODO: how to fail a node?
TEST_F(AASBenchmarkTest, testDiamondAAS) {
    int nLevel2Workers = 2;
    int nLevel3Workers = 2;

    // setup coordinator

    // level 1
    NES_INFO("AASBenchmarkTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getSourceCatalogService()->registerLogicalSource(sourceName, inputSchema);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("AASBenchmarkTest: Coordinator started successfully");

    // setup workers

    // level 2
    std::map<int, NesWorkerPtr> level2Workers;
    for (int i = 0; i < nLevel2Workers; ++i) {
        NES_INFO("AASBenchmarkTest: Start level2Worker" << i);
        NesWorkerPtr level2Worker = std::make_shared<NesWorker>(std::move(level2WorkerConfig));
        bool retStart = level2Worker->start(/**blocking**/ false, /**withConnect**/ true);
        EXPECT_TRUE(retStart);
        NES_INFO("AASBenchmarkTest: level2Worker" << i << " started successfully");
        level2Workers[i] = level2Worker;
    }

    // level 3
    std::map<int, NesWorkerPtr> level3Workers;
    for (int i = 0; i < nLevel3Workers; ++i) {
        NES_INFO("AASBenchmarkTest: Start level3Worker" << i);
        NesWorkerPtr level3Worker = std::make_shared<NesWorker>(std::move(level3WorkerConfig));
        bool retStart = level3Worker->start(/**blocking**/ false, /**withConnect**/ true);
        EXPECT_TRUE(retStart);
        level3Worker->removeParent(1);
        level3Worker->addParent(level2Workers[i]->getWorkerId());
        NES_INFO("AASBenchmarkTest: level3Worker" << i << " started successfully");
        level3Workers[i] = level3Worker;
    }


    // source level
    std::map<int, NesWorkerPtr> sourceWorkers;
    for (int i = 0; i < nSourceWorkers; ++i) {
        NES_INFO("AASBenchmarkTest: Start sourceWorker" << i);
        NesWorkerPtr srcWrk = std::make_shared<NesWorker>(std::move(sourceWorkerConfigs[i]));
        bool retStart = srcWrk->start(/**blocking**/ false, /**withConnect**/ true);
        EXPECT_TRUE(retStart);
        srcWrk->removeParent(1);
        srcWrk->addParent(level3Workers[1]->getWorkerId());
        srcWrk->addParent(level3Workers[0]->getWorkerId());
        NES_INFO("AASBenchmarkTest: srcWrk" << i << " started successfully");
        sourceWorkers[i] = srcWrk;
    }

    // finish setting up the topology by adding slots and linkProperties
    addSlotsAndLinkProperties(crd);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = "AASBenchmarkTest.out";
    remove(outputFilePath.c_str());

    NES_INFO("AASBenchmarkTest: Submit query");
    string query = createQueryString(outputFilePath);

    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, "ILP", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));

    for (const auto& [i, level2Worker] : level2Workers)
        EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(level2Worker, queryId, globalQueryPlan, 1));
    for (const auto& [i, sourceWorker] : sourceWorkers)
        EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(sourceWorker, queryId, globalQueryPlan, 1));

    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    NES_DEBUG("AASBenchmarkTest:: Topology print: " << crd->getTopology()->toString());


    NES_DEBUG("AASBenchmarkTest: Sleep");
    std::this_thread::sleep_for(std::chrono::milliseconds(5000));

    // TODO: leads to errors
//    NES_INFO("AASBenchmarkTest: Manually fail level2Worker" << 1);
//    bool retStopWrk1 = level2Workers[1]->disconnect();
//    retStopWrk1 = level2Workers[1]->stop(true);
//    EXPECT_TRUE(retStopWrk1);
//
//    NES_DEBUG("AASBenchmarkTest: Sleep again");
//    std::this_thread::sleep_for(std::chrono::milliseconds(5000));

    NES_INFO("AASBenchmarkTest: Remove query");
    queryService->validateAndQueueStopQueryRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    for (const auto& [i, sourceWorker] : sourceWorkers) {
        NES_INFO("AASBenchmarkTest: Stop worker sourceWorker" << i);
        bool retStopWrk = sourceWorker->stop(true);
        EXPECT_TRUE(retStopWrk);
    }

    NES_INFO("AASBenchmarkTest: Stop worker level2Worker" << 2);
    bool retStopWrk = level2Workers[1]->stop(true);
    EXPECT_TRUE(retStopWrk);

    NES_INFO("AASBenchmarkTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("AASBenchmarkTest: Test finished");
}

/// ------------------------------------ 1. Sequential topology with 4 sources ---------------------------------------------------
/*
*      1
*      2
*      3
* /  |  |  \
* 4  5  6  7
*/

// 1.1 Test the deployment of new nodes and operator replicas
TEST_F(AASBenchmarkTest, testSequentialAASDeployment) {
    auto topology = setupTopologyAndSourceCatalogSequential(nSourceWorkers); // tests written for nSourceWorkers = 4

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto queryPlacementPhase = Optimizer::QueryPlacementPhase::create(globalExecutionPlan,
                                                                      topology,
                                                                      typeInferencePhase,
                                                                      false /*query reconfiguration*/);

    auto query = queryParsingService->createQueryFromCodeString(createQueryString("testSequentialAASDeployment.out"));

    QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(PlanIdGenerator::getNextQueryId());

    assignOperatorProperties(queryPlan);

    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getSharedQueryId();

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    queryPlacementPhase->execute(NES::PlacementStrategy::ILP, sharedQueryPlan,
                                 placementStrategyAAS);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    std::cout << "topology " + topology->toString() << std::endl;
    std::cout << "query plan " + queryPlan->toString() << std::endl;

    //Assertion
    ASSERT_EQ(executionNodes.size(), 12U);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 4 ||
            executionNode->getId() == 5 ||
            executionNode->getId() == 6 ||
            executionNode->getId() == 7) {
            // only the source operator should be placed on the source node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            // for primary and secondary paths
            ASSERT_EQ(actualRootOperators.size(), 2U);
            EXPECT_TRUE(actualRootOperators[0]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[0]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[0]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());

            EXPECT_TRUE(actualRootOperators[1]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[1]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[1]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == 3) {
            // 6 primary filters on node 3
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 4U);
            for (auto const& querySubPlan : querySubPlans) {
                std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1U);
                OperatorNodePtr actualRootOperator = actualRootOperators[0];
                ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            }
            ASSERT_EQ(topology->findNodeWithId(executionNode->getId())->getAvailableResources(), 6 - 6*1);
        } else if (executionNode->getId() == 2) {
            // primary maps and remaining 2 primary filters placed on node 2
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 4U);
            for (const auto& querySubPlan: querySubPlans) {
                std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1U);
                OperatorNodePtr actualRootOperator = actualRootOperators[0];
                EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
                ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<ProjectionLogicalOperatorNode>());
                ASSERT_EQ(actualRootOperator->getChildren()[0]->getChildren().size(), 1U);
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>() ||
                            actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
            }
            ASSERT_EQ(topology->findNodeWithId(executionNode->getId())->getAvailableResources(), 10 - 4*2-2*1);
        } else if (executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart + 1 ||
                   executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart + 2 ||
                   executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart + 3 ||
                   executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart + 4) {
            // secondary filters on bottom new nodes
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlans[0U]->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            auto op = actualRootOperator->getChildren()[0];
            if (placementStrategyAAS == PlacementStrategy::Greedy_AAS) {
                EXPECT_TRUE(op->instanceOf<ProjectionLogicalOperatorNode>());
                ASSERT_EQ(op->getChildren().size(), 1U);
                op = op->getChildren()[0];
            }
            EXPECT_TRUE(op->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(op->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(op->getChildren()[0]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == 1) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            if (placementStrategyAAS == PlacementStrategy::Greedy_AAS) {
                ASSERT_EQ(actualRootOperator->getChildren().size(), 8U);
                for (int i = 0; i < 8; i++)
                    EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
            } else {
                // secondary maps on node 1 for LS and ILP
                ASSERT_EQ(actualRootOperator->getChildren().size(), 8U);// 4 maps + 4 sources
                for (int i = 0; i < 8; i++)
                    EXPECT_TRUE(actualRootOperator->getChildren()[i]->instanceOf<ProjectionLogicalOperatorNode>()
                                || actualRootOperator->getChildren()[i]->instanceOf<SourceLogicalOperatorNode>());
            }
        } else {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            // other nodes should have no operators placed on them
            // or only for forwarding data
            for (const auto& querySubPlan: querySubPlans) {
                std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1U);
                OperatorNodePtr actualRootOperator = actualRootOperators[0];
                EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
                ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
            }
        }
    }
}

// 1.2 Test during runtime
TEST_F(AASBenchmarkTest, testSequentialRuntime) {
    int nLevel2Workers = 1;
    int nLevel3Workers = 1;

    // setup coordinator

    // level 1
    NES_INFO("AASBenchmarkTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getSourceCatalogService()->registerLogicalSource(sourceName, inputSchema);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("AASBenchmarkTest: Coordinator started successfully");

    // setup workers

    // level 2
    std::map<int, NesWorkerPtr> level2Workers;
    for (int i = 0; i < nLevel2Workers; ++i) {
        NES_INFO("AASBenchmarkTest: Start level2Worker" << i);
        NesWorkerPtr level2Worker = std::make_shared<NesWorker>(std::move(level2WorkerConfig));
        bool retStart = level2Worker->start(/**blocking**/ false, /**withConnect**/ true);
        EXPECT_TRUE(retStart);
        NES_INFO("AASBenchmarkTest: level2Worker" << i << " started successfully");
        level2Workers[i] = level2Worker;
    }

    // level 3
    std::map<int, NesWorkerPtr> level3Workers;
    for (int i = 0; i < nLevel3Workers; ++i) {
        NES_INFO("AASBenchmarkTest: Start level3Worker" << i);
        NesWorkerPtr level3Worker = std::make_shared<NesWorker>(std::move(level3WorkerConfig));
        bool retStart = level3Worker->start(/**blocking**/ false, /**withConnect**/ true);
        EXPECT_TRUE(retStart);
        level3Worker->removeParent(1);
        level3Worker->addParent(level2Workers[0]->getWorkerId());
        NES_INFO("AASBenchmarkTest: level3Worker" << i << " started successfully");
        level3Workers[i] = level3Worker;
    }

    // source level
    std::map<int, NesWorkerPtr> sourceWorkers;
    for (int i = 0; i < nSourceWorkers; ++i) {
        NES_INFO("AASBenchmarkTest: Start sourceWorker" << i);
        NesWorkerPtr srcWrk = std::make_shared<NesWorker>(std::move(sourceWorkerConfigs[i]));
        bool retStart = srcWrk->start(/**blocking**/ false, /**withConnect**/ true);
        EXPECT_TRUE(retStart);
        srcWrk->removeParent(1);
        srcWrk->addParent(level3Workers[0]->getWorkerId());
        NES_INFO("AASBenchmarkTest: srcWrk" << i << " started successfully");
        sourceWorkers[i] = srcWrk;
    }

    // reserve workers
    std::map<int, NesWorkerPtr> reserveWorkers;
    for (int i = 0; i < nSourceWorkers+1; ++i) {
        NES_INFO("AASBenchmarkTest: Start reserveWorker" << i);
        NesWorkerPtr reserveWorker = std::make_shared<NesWorker>(std::move(reserveWorkerConfig));
        bool retStart = reserveWorker->start(/**blocking**/ false, /**withConnect**/ true);
        EXPECT_TRUE(retStart);
        if (i > 0) {
            reserveWorker->removeParent(1);
            reserveWorker->addParent(reserveWorkers[0]->getWorkerId());

            sourceWorkers[i-1]->addParent(reserveWorker->getWorkerId());
        }
        NES_INFO("AASBenchmarkTest: reserveWorker" << i << " started successfully");
        reserveWorkers[i] = reserveWorker;
    }

    // finish setting up the topology by adding slots and linkProperties
    addSlotsAndLinkProperties(crd);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = "AASBenchmarkTestSequential.out";
    remove(outputFilePath.c_str());

    NES_INFO("AASBenchmarkTest: Submit query");
    string query = createQueryString(outputFilePath);

    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, "ILP", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));

    NES_DEBUG("AASBenchmarkTest:: Topology print: " << crd->getTopology()->toString());

    for (const auto& [i, level2Worker] : level2Workers)
        EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(level2Worker, queryId, globalQueryPlan, 1));
    for (const auto& [i, reserveWorker] : reserveWorkers)
        EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(reserveWorker, queryId, globalQueryPlan, 1));
    for (const auto& [i, level3Worker] : level3Workers)
        EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(level3Worker, queryId, globalQueryPlan, 1));
    for (const auto& [i, sourceWorker] : sourceWorkers)
        EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(sourceWorker, queryId, globalQueryPlan, 1));

    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    NES_DEBUG("AASBenchmarkTest:: Topology print: " << crd->getTopology()->toString());


    NES_DEBUG("AASBenchmarkTest: Sleep");
    std::this_thread::sleep_for(runtime);

    NES_INFO("AASBenchmarkTest: Remove query");
    queryService->validateAndQueueStopQueryRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    for (const auto& [i, sourceWorker] : sourceWorkers) {
        NES_INFO("AASBenchmarkTest: Stop worker sourceWorker" << i);
        bool retStopWrk = sourceWorker->stop(true);
        EXPECT_TRUE(retStopWrk);
    }

    for (const auto& [i, level3Worker] : level3Workers) {
        NES_INFO("AASBenchmarkTest: Stop worker level3Worker" << i);
        bool retStopWrk = level3Worker->stop(true);
        EXPECT_TRUE(retStopWrk);
    }

    for (const auto& [i, reserveWorker] : reserveWorkers) {
        if (i == 0)
            continue;
        NES_INFO("AASBenchmarkTest: Stop worker reserveWorker" << i);
        bool retStopWrk = reserveWorker->stop(true);
        EXPECT_TRUE(retStopWrk);
    }

    NES_INFO("AASBenchmarkTest: Stop worker reserveWorker" << 0);
    bool retStopWrk0 = reserveWorkers[0]->stop(true);
    EXPECT_TRUE(retStopWrk0);

    for (const auto& [i, level2Worker] : level2Workers) {
        NES_INFO("AASBenchmarkTest: Stop worker level2Worker" << i);
        bool retStopWrk = level2Worker->stop(true);
        EXPECT_TRUE(retStopWrk);
    }

    NES_INFO("AASBenchmarkTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("AASBenchmarkTest: Test finished");
}

/// ------------------------- 2. Fat topology with 4 sources with separate paths -------------------------------------------------
/* Fat topology with 4 sources with separate paths, low connectivity
 *     1
 * /  |  |  \
 * 2  3  4  5
 * 6  7  8  9
 * 10 11 12 13
 */

// 2.1 Test the deployment of new nodes and operator replicas
TEST_F(AASBenchmarkTest, testFatLowConnectivityAASDeployment) {
    auto topology = setupTopologyAndSourceCatalogFatLowConnectivity(nSourceWorkers); // tests written for nSourceWorkers = 4

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto queryPlacementPhase = Optimizer::QueryPlacementPhase::create(globalExecutionPlan,
                                                                      topology,
                                                                      typeInferencePhase,
                                                                      false /*query reconfiguration*/);

    auto query = queryParsingService->createQueryFromCodeString(createQueryString("testFatLowConnectivityAASDeployment.out"));

    QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(PlanIdGenerator::getNextQueryId());

    assignOperatorProperties(queryPlan);

    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getSharedQueryId();

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    queryPlacementPhase->execute(NES::PlacementStrategy::ILP, sharedQueryPlan,
                                 placementStrategyAAS);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    std::cout << "topology " + topology->toString() << std::endl;
    std::cout << "query plan " + queryPlan->toString() << std::endl;

    //Assertion
    ASSERT_EQ(executionNodes.size(), 17U);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 10 ||
            executionNode->getId() == 11 ||
            executionNode->getId() == 12 ||
            executionNode->getId() == 13) {
            // only the source operator should be placed on the source node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            // for primary and secondary paths
            ASSERT_EQ(actualRootOperators.size(), 2U);
            EXPECT_TRUE(actualRootOperators[0]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[0]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[0]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());

            EXPECT_TRUE(actualRootOperators[1]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[1]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[1]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == 2 ||
                   executionNode->getId() == 3 ||
                   executionNode->getId() == 4 ||
                   executionNode->getId() == 5) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 2U); // 1 primary + 1 secondary
            // primary map
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlans[0U]->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<ProjectionLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
            // secondary path
            actualRootOperator = querySubPlans[1U]->getRootOperators()[0];
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == 6 ||
                   executionNode->getId() == 7 ||
                   executionNode->getId() == 8 ||
                   executionNode->getId() == 9) {
            // primary filters
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlans[0U]->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->getChildren()[0]
                            ->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart ||
                   executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart + 1 ||
                   executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart + 2 ||
                   executionNode->getId() == NES::Optimizer::AdaptiveActiveStandby::newTopologyNodeIdStart + 3) {
            // secondary filters on bottom new nodes
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlans[0U]->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            auto op = actualRootOperator->getChildren()[0];
            if (placementStrategyAAS == PlacementStrategy::Greedy_AAS) {
                EXPECT_TRUE(op->instanceOf<ProjectionLogicalOperatorNode>());
                ASSERT_EQ(op->getChildren().size(), 1U);
                op = op->getChildren()[0];
            }
            EXPECT_TRUE(op->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(op->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
            EXPECT_TRUE(op->getChildren()[0]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == 1) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            if (placementStrategyAAS == PlacementStrategy::Greedy_AAS) {
                ASSERT_EQ(actualRootOperator->getChildren().size(), 8U);
                for (int i = 0; i < 8; i++)
                    EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
            } else {
                // secondary maps on node 1 for LS and ILP
                ASSERT_EQ(actualRootOperator->getChildren().size(), 8U);// 4 maps + 4 sources
                for (int i = 0; i < 8; i++)
                    EXPECT_TRUE(actualRootOperator->getChildren()[i]->instanceOf<ProjectionLogicalOperatorNode>()
                                || actualRootOperator->getChildren()[i]->instanceOf<SourceLogicalOperatorNode>());
            }
        } else {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            // other nodes should have no operators placed on them
            // or only for forwarding data
            for (const auto& querySubPlan: querySubPlans) {
                std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1U);
                OperatorNodePtr actualRootOperator = actualRootOperators[0];
                EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
                ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
            }
        }
    }
}

// 2.2 Test during runtime
TEST_F(AASBenchmarkTest, testFatLowConnectivityRuntime) {
    int nLevel2Workers = nSourceWorkers;
    int nLevel3Workers = nSourceWorkers;

    // setup coordinator

    // level 1
    NES_INFO("AASBenchmarkTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getSourceCatalogService()->registerLogicalSource(sourceName, inputSchema);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("AASBenchmarkTest: Coordinator started successfully");

    // setup workers

    // level 2
    std::map<int, NesWorkerPtr> level2Workers;
    for (int i = 0; i < nLevel2Workers; ++i) {
        NES_INFO("AASBenchmarkTest: Start level2Worker" << i);
        NesWorkerPtr level2Worker = std::make_shared<NesWorker>(std::move(level2WorkerConfig));
        bool retStart = level2Worker->start(/**blocking**/ false, /**withConnect**/ true);
        EXPECT_TRUE(retStart);
        NES_INFO("AASBenchmarkTest: level2Worker" << i << " started successfully");
        level2Workers[i] = level2Worker;
    }

    // level 3
    std::map<int, NesWorkerPtr> level3Workers;
    for (int i = 0; i < nLevel3Workers; ++i) {
        NES_INFO("AASBenchmarkTest: Start level3Worker" << i);
        NesWorkerPtr level3Worker = std::make_shared<NesWorker>(std::move(level3WorkerConfig));
        bool retStart = level3Worker->start(/**blocking**/ false, /**withConnect**/ true);
        EXPECT_TRUE(retStart);
        level3Worker->removeParent(1);
        level3Worker->addParent(level2Workers[i]->getWorkerId());
        NES_INFO("AASBenchmarkTest: level3Worker" << i << " started successfully");
        level3Workers[i] = level3Worker;
    }

    // source level
    std::map<int, NesWorkerPtr> sourceWorkers;
    for (int i = 0; i < nSourceWorkers; ++i) {
        NES_INFO("AASBenchmarkTest: Start sourceWorker" << i);
        NesWorkerPtr srcWrk = std::make_shared<NesWorker>(std::move(sourceWorkerConfigs[i]));
        bool retStart = srcWrk->start(/**blocking**/ false, /**withConnect**/ true);
        EXPECT_TRUE(retStart);
        srcWrk->removeParent(1);
        srcWrk->addParent(level3Workers[i]->getWorkerId());
        NES_INFO("AASBenchmarkTest: srcWrk" << i << " started successfully");
        sourceWorkers[i] = srcWrk;
    }

    // reserve workers
    std::map<int, NesWorkerPtr> reserveWorkers;
    for (int i = 0; i < nSourceWorkers; ++i) {
        NES_INFO("AASBenchmarkTest: Start reserveWorker" << i);
        NesWorkerPtr reserveWorker = std::make_shared<NesWorker>(std::move(reserveWorkerConfig));
        bool retStart = reserveWorker->start(/**blocking**/ false, /**withConnect**/ true);
        EXPECT_TRUE(retStart);
        reserveWorker->removeParent(1);

        /*
         * 0 -> 1
         * 1 -> 0
         *
         * 2 -> 3
         * 3 -> 2
         *
         *   ...
         */

        if (i % 2 == 0)
            reserveWorker->addParent(level2Workers[i+1]->getWorkerId());
        else
            reserveWorker->addParent(level2Workers[i-1]->getWorkerId());

        sourceWorkers[i]->addParent(reserveWorker->getWorkerId());
        NES_INFO("AASBenchmarkTest: reserveWorker" << i << " started successfully");
        reserveWorkers[i] = reserveWorker;
    }

    // finish setting up the topology by adding slots and linkProperties
    addSlotsAndLinkProperties(crd);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = "AASBenchmarkTestFatLowConnectivity.out";
    remove(outputFilePath.c_str());

    NES_INFO("AASBenchmarkTest: Submit query");
    string query = createQueryString(outputFilePath);

    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, "ILP", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));

    NES_DEBUG("AASBenchmarkTest:: Topology print: " << crd->getTopology()->toString());

    for (const auto& [i, level2Worker] : level2Workers)
        EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(level2Worker, queryId, globalQueryPlan, 1));
    for (const auto& [i, reserveWorker] : reserveWorkers)
        EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(reserveWorker, queryId, globalQueryPlan, 1));
    for (const auto& [i, level3Worker] : level3Workers)
        EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(level3Worker, queryId, globalQueryPlan, 1));
    for (const auto& [i, sourceWorker] : sourceWorkers)
        EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(sourceWorker, queryId, globalQueryPlan, 1));

    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    NES_DEBUG("AASBenchmarkTest:: Topology print: " << crd->getTopology()->toString());


    NES_DEBUG("AASBenchmarkTest: Sleep");
    std::this_thread::sleep_for(runtime);

    NES_INFO("AASBenchmarkTest: Remove query");
    queryService->validateAndQueueStopQueryRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    for (const auto& [i, sourceWorker] : sourceWorkers) {
        NES_INFO("AASBenchmarkTest: Stop worker sourceWorker" << i);
        bool retStopWrk = sourceWorker->stop(true);
        EXPECT_TRUE(retStopWrk);
    }

    for (const auto& [i, level3Worker] : level3Workers) {
        NES_INFO("AASBenchmarkTest: Stop worker level3Worker" << i);
        bool retStopWrk = level3Worker->stop(true);
        EXPECT_TRUE(retStopWrk);
    }

    for (const auto& [i, reserveWorker] : reserveWorkers) {
        if (i == 0)
            continue;
        NES_INFO("AASBenchmarkTest: Stop worker reserveWorker" << i);
        bool retStopWrk = reserveWorker->stop(true);
        EXPECT_TRUE(retStopWrk);
    }

    NES_INFO("AASBenchmarkTest: Stop worker reserveWorker" << 0);
    bool retStopWrk0 = reserveWorkers[0]->stop(true);
    EXPECT_TRUE(retStopWrk0);

    for (const auto& [i, level2Worker] : level2Workers) {
        NES_INFO("AASBenchmarkTest: Stop worker level2Worker" << i);
        bool retStopWrk = level2Worker->stop(true);
        EXPECT_TRUE(retStopWrk);
    }

    NES_INFO("AASBenchmarkTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("AASBenchmarkTest: Test finished");
}

/// ----------------- 3. Fat topology with 4 sources with separate paths and high connectivity  ----------------------------------
/* Fat topology with 4 sources with separate paths, high connectivity
 *      1
 * /  /   \  \
 * 2  3   4  5
 * |\/|   |\/|
 * |/\|   |/\|
 * 6  7   8  9
 * |\/|   |\/|
 * |/\|   |/\|
 * 10 11  12 13
 */

// 3.1 Test the deployment of new nodes and operator replicas
TEST_F(AASBenchmarkTest, testFatHighConnectivityAASDeployment) {
    auto topology = setupTopologyAndSourceCatalogFatHighConnectivity(nSourceWorkers); // tests written for nSourceWorkers = 4

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto queryPlacementPhase = Optimizer::QueryPlacementPhase::create(globalExecutionPlan,
                                                                      topology,
                                                                      typeInferencePhase,
                                                                      false /*query reconfiguration*/);

    auto query = queryParsingService->createQueryFromCodeString(createQueryString("testFatHighConnectivityAASDeployment.out"));

    QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(PlanIdGenerator::getNextQueryId());

    assignOperatorProperties(queryPlan);

    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getSharedQueryId();

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    queryPlacementPhase->execute(NES::PlacementStrategy::ILP, sharedQueryPlan,
                                 placementStrategyAAS);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    std::cout << "topology " + topology->toString() << std::endl;
    std::cout << "query plan " + queryPlan->toString() << std::endl;

    //Assertion
    ASSERT_EQ(executionNodes.size(), 13U);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 10 ||
            executionNode->getId() == 11 ||
            executionNode->getId() == 12 ||
            executionNode->getId() == 13) {
            // only the source operator should be placed on the source node
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            // for primary and secondary paths
            ASSERT_EQ(actualRootOperators.size(), 2U);
            EXPECT_TRUE(actualRootOperators[0]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[0]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[0]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());

            EXPECT_TRUE(actualRootOperators[1]->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperators[1]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[1]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == 2 ||
                   executionNode->getId() == 3 ||
                   executionNode->getId() == 4 ||
                   executionNode->getId() == 5) {
            // primary maps
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 2U); // 1 primary + 1 secondary
            // primary map
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlans[0U]->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<ProjectionLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
            // secondary path
            actualRootOperator = querySubPlans[1U]->getRootOperators()[0];
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == 6 ||
                   executionNode->getId() == 7 ||
                   executionNode->getId() == 8 ||
                   executionNode->getId() == 9) {
            // filters
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 2U);
            for (int i = 0; i < 2; ++i) {
                std::vector<OperatorNodePtr> actualRootOperators = querySubPlans[i]->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1U);
                OperatorNodePtr actualRootOperator = actualRootOperators[0];
                EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
                ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
                if (actualRootOperator->getChildren()[0]->as<OperatorNode>()->getId()
                    < NES::Optimizer::AdaptiveActiveStandby::newOperatorNodeIdStart) {
                    // primary
                    EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
                    EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
                    EXPECT_TRUE(actualRootOperator->getChildren()[0]
                                    ->getChildren()[0]
                                    ->getChildren()[0]
                                    ->instanceOf<SourceLogicalOperatorNode>());
                } else {
                    // secondary
                    auto op = actualRootOperator->getChildren()[0];
                    if (placementStrategyAAS == PlacementStrategy::Greedy_AAS) {
                        EXPECT_TRUE(op->instanceOf<ProjectionLogicalOperatorNode>());
                        ASSERT_EQ(op->getChildren().size(), 1U);
                        op = op->getChildren()[0];
                    }
                    EXPECT_TRUE(op->instanceOf<FilterLogicalOperatorNode>());
                    EXPECT_TRUE(op->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
                    EXPECT_TRUE(op->getChildren()[0]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
                }
            }
        } else if (executionNode->getId() == 1) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            if (placementStrategyAAS == PlacementStrategy::Greedy_AAS) {
                ASSERT_EQ(actualRootOperator->getChildren().size(), 8U);
                for (int i = 0; i < 8; i++)
                    EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
            } else {
                // secondary maps on node 1 for LS and ILP
                ASSERT_EQ(actualRootOperator->getChildren().size(), 8U);// 4 maps + 4 sources
                for (int i = 0; i < 8; i++)
                    EXPECT_TRUE(actualRootOperator->getChildren()[i]->instanceOf<ProjectionLogicalOperatorNode>()
                                || actualRootOperator->getChildren()[i]->instanceOf<SourceLogicalOperatorNode>());
            }
        } else {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            // other nodes should have no operators placed on them
            // or only for forwarding data
            for (const auto& querySubPlan: querySubPlans) {
                std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1U);
                OperatorNodePtr actualRootOperator = actualRootOperators[0];
                EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
                ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
            }
        }
    }
}

// 3.2 Test during runtime
TEST_F(AASBenchmarkTest, testFatHighConnectivityRuntime) {
    int nLevel2Workers = nSourceWorkers;
    int nLevel3Workers = nSourceWorkers;

    // setup coordinator

    // level 1
    NES_INFO("AASBenchmarkTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getSourceCatalogService()->registerLogicalSource(sourceName, inputSchema);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("AASBenchmarkTest: Coordinator started successfully");

    // setup workers

    // level 2
    std::map<int, NesWorkerPtr> level2Workers;
    for (int i = 0; i < nLevel2Workers; ++i) {
        NES_INFO("AASBenchmarkTest: Start level2Worker" << i);
        NesWorkerPtr level2Worker = std::make_shared<NesWorker>(std::move(level2WorkerConfig));
        bool retStart = level2Worker->start(/**blocking**/ false, /**withConnect**/ true);
        EXPECT_TRUE(retStart);
        NES_INFO("AASBenchmarkTest: level2Worker" << i << " started successfully");
        level2Workers[i] = level2Worker;
    }

    // level 3
    std::map<int, NesWorkerPtr> level3Workers;
    for (int i = 0; i < nLevel3Workers; ++i) {
        NES_INFO("AASBenchmarkTest: Start level3Worker" << i);
        NesWorkerPtr level3Worker = std::make_shared<NesWorker>(std::move(level3WorkerConfig));
        bool retStart = level3Worker->start(/**blocking**/ false, /**withConnect**/ true);
        EXPECT_TRUE(retStart);
        level3Worker->removeParent(1);
        level3Worker->addParent(level2Workers[i]->getWorkerId());

        if (i % 2 == 0)
            level3Worker->addParent(level2Workers[i+1]->getWorkerId());
        else
            level3Worker->addParent(level2Workers[i-1]->getWorkerId());

        NES_INFO("AASBenchmarkTest: level3Worker" << i << " started successfully");
        level3Workers[i] = level3Worker;
    }

    // source level
    std::map<int, NesWorkerPtr> sourceWorkers;
    for (int i = 0; i < nSourceWorkers; ++i) {
        NES_INFO("AASBenchmarkTest: Start sourceWorker" << i);
        NesWorkerPtr srcWrk = std::make_shared<NesWorker>(std::move(sourceWorkerConfigs[i]));
        bool retStart = srcWrk->start(/**blocking**/ false, /**withConnect**/ true);
        EXPECT_TRUE(retStart);
        srcWrk->removeParent(1);
        srcWrk->addParent(level3Workers[i]->getWorkerId());

        if (i % 2 == 0)
            srcWrk->addParent(level3Workers[i+1]->getWorkerId());
        else
            srcWrk->addParent(level3Workers[i-1]->getWorkerId());

        NES_INFO("AASBenchmarkTest: srcWrk" << i << " started successfully");
        sourceWorkers[i] = srcWrk;
    }

    // finish setting up the topology by adding slots and linkProperties
    addSlotsAndLinkProperties(crd);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = "AASBenchmarkTestFatHighConnectivity.out";
    remove(outputFilePath.c_str());

    NES_INFO("AASBenchmarkTest: Submit query");
    string query = createQueryString(outputFilePath);

    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, "ILP", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));

    NES_DEBUG("AASBenchmarkTest:: Topology print: " << crd->getTopology()->toString());

    for (const auto& [i, level2Worker] : level2Workers)
        EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(level2Worker, queryId, globalQueryPlan, 1));
    for (const auto& [i, level3Worker] : level3Workers)
        EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(level3Worker, queryId, globalQueryPlan, 1));
    for (const auto& [i, sourceWorker] : sourceWorkers)
        EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(sourceWorker, queryId, globalQueryPlan, 1));

    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    NES_DEBUG("AASBenchmarkTest:: Topology print: " << crd->getTopology()->toString());


    NES_DEBUG("AASBenchmarkTest: Sleep");
    std::this_thread::sleep_for(runtime);

    NES_INFO("AASBenchmarkTest: Remove query");
    queryService->validateAndQueueStopQueryRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    for (const auto& [i, sourceWorker] : sourceWorkers) {
        NES_INFO("AASBenchmarkTest: Stop worker sourceWorker" << i);
        bool retStopWrk = sourceWorker->stop(true);
        EXPECT_TRUE(retStopWrk);
    }

    for (const auto& [i, level3Worker] : level3Workers) {
        NES_INFO("AASBenchmarkTest: Stop worker level3Worker" << i);
        bool retStopWrk = level3Worker->stop(true);
        EXPECT_TRUE(retStopWrk);
    }

    for (const auto& [i, level2Worker] : level2Workers) {
        NES_INFO("AASBenchmarkTest: Stop worker level2Worker" << i);
        bool retStopWrk = level2Worker->stop(true);
        EXPECT_TRUE(retStopWrk);
    }

    NES_INFO("AASBenchmarkTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("AASBenchmarkTest: Test finished");
}

TEST_F(AASBenchmarkTest, testScalability) {
    for (int i = 2; i <= 8; i += 2) {
        for (int j = 0; j < 3; ++j) {
            TopologyPtr topology;

            if (j == 0) {
                NES_DEBUG("AASBenchmarkTest::testScalability: Sequential " << i);
                topology = setupTopologyAndSourceCatalogSequential(i);
            } else if (j == 1) {
                NES_DEBUG("AASBenchmarkTest::testScalability: Fat Low Connectivity " << i);
                topology = setupTopologyAndSourceCatalogFatLowConnectivity(i);
            } else {
                NES_DEBUG("AASBenchmarkTest::testScalability: Fat High Connectivity " << i);
                topology = setupTopologyAndSourceCatalogFatHighConnectivity(i);
            }

            GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
            auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
            auto queryPlacementPhase = Optimizer::QueryPlacementPhase::create(globalExecutionPlan,
                                                                              topology,
                                                                              typeInferencePhase,
                                                                              false /*query reconfiguration*/);

            auto query = queryParsingService->createQueryFromCodeString(createQueryString("scalabilityAnalysis.out"));

            QueryPlanPtr queryPlan = query->getQueryPlan();
            queryPlan->setQueryId(PlanIdGenerator::getNextQueryId());

            assignOperatorProperties(queryPlan);

            auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
            auto queryId = sharedQueryPlan->getSharedQueryId();

            auto topologySpecificQueryRewrite =
                Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                                     sourceCatalog,
                                                                     Configurations::OptimizerConfiguration());
            topologySpecificQueryRewrite->execute(queryPlan);
            typeInferencePhase->execute(queryPlan);

            queryPlacementPhase->execute(NES::PlacementStrategy::ILP, sharedQueryPlan, placementStrategyAAS);
        }
    }
}
}// namespace NES