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
const int nSourceWorkers = 2;

class AASBenchmarkTest : public Testing::NESBaseTest {
  public:
    std::string ipAddress = "127.0.0.1";
    CoordinatorConfigurationPtr coordinatorConfig;
    WorkerConfigurationPtr layer2WorkerConfig, layer3WorkerConfig, reserveWorkerConfig;
    WorkerConfigurationPtr sourceWorkerConfigs[nSourceWorkers];
    SchemaPtr inputSchema;
    const std::string sourceName = "A";
    const LinkPropertyPtr linkProperty = std::make_shared<LinkProperty>(LinkProperty(512, 100));

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

        layer2WorkerConfig = WorkerConfiguration::create();
        layer2WorkerConfig->coordinatorPort = *rpcCoordinatorPort;
        layer2WorkerConfig->enableStatisticOutput = true;
        layer2WorkerConfig->numberOfSlots = 10;

        layer3WorkerConfig = WorkerConfiguration::create();
        layer3WorkerConfig->coordinatorPort = *rpcCoordinatorPort;
        //        layer3WorkerConfig->enableStatisticOutput = true;
        layer3WorkerConfig->numberOfSlots = 6;

        reserveWorkerConfig = WorkerConfiguration::create();
        reserveWorkerConfig->coordinatorPort = *rpcCoordinatorPort;
        //        layer3WorkerConfig->enableStatisticOutput = true;
        reserveWorkerConfig->numberOfSlots = 5;

        for (int i = 0; i < nSourceWorkers; ++i) {
            auto sourceWorkerConfig = WorkerConfiguration::create();
            sourceWorkerConfig->coordinatorPort = *rpcCoordinatorPort;
            //        sourceWorker->enableStatisticOutput = true;
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
            + ".filter(Attribute(\"c\") == 1)"       // avg DMF = 0.33
            + ".filter(Attribute(\"d\") >= 2)"      // avg DMF = 0.5
            + R"(.map(Attribute("i") = Attribute("h") * 2))"   // DMF = 1.1
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

    TopologyPtr setupTopologyAndSourceCatalogNPhysicalSourcesSamePath(int n) {
        auto topology = Topology::create();

        TopologyNodePtr rootNode = TopologyNode::create(1, ipAddress, 123, 124, UINT16_MAX);
        rootNode->addNodeProperty("slots", 0);
        topology->setAsRoot(rootNode);

        TopologyNodePtr middleNode2 = TopologyNode::create(2, ipAddress, 123, 124, 10);
        middleNode2->addNodeProperty("slots", 10);
        topology->addNewTopologyNodeAsChild(rootNode, middleNode2);

        TopologyNodePtr middleNode1 = TopologyNode::create(3, ipAddress, 123, 124, 6);
        middleNode1->addNodeProperty("slots", 6);
        topology->addNewTopologyNodeAsChild(middleNode2, middleNode1);

        std::map<int, TopologyNodePtr> sourceNodes;
        for (int i = 0; i < n; ++i) {
            TopologyNodePtr sourceNode = TopologyNode::create(i+4, ipAddress, 123, 124, 0);
            sourceNode->addNodeProperty("slots", 0);
            topology->addNewTopologyNodeAsChild(middleNode1, sourceNode);

            sourceNodes[i] = sourceNode;
        }

        rootNode->addLinkProperty(middleNode2, linkProperty);
        middleNode2->addLinkProperty(rootNode, linkProperty);

        middleNode2->addLinkProperty(middleNode1, linkProperty);
        middleNode1->addLinkProperty(middleNode2, linkProperty);

        for (int i = 0; i < n; ++i) {
            sourceNodes[i]->addLinkProperty(middleNode1, linkProperty);
            middleNode1->addLinkProperty(sourceNodes[i], linkProperty);
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

            // ".filter(Attribute(\"c\") == 1)"       // avg DMF = 0.33
            auto firstFilterOperator = srcOperator->getParents()[0]->as<OperatorNode>();
//            dmf = 0.3367;   // compensate for DMF_map = 0.99
            dmf = 0.33;
            input = output;
            output = input * dmf;
            firstFilterOperator->addProperty("output", output);

            // ".filter(Attribute(\"d\") >= 2)"      // avg DMF = 0.5
            auto secondFilterOperator = firstFilterOperator->getParents()[0]->as<OperatorNode>();
            dmf = 0.5;
            input = output;
            output = input * dmf;
            secondFilterOperator->addProperty("output", output);

            // R"(.map(Attribute("f") = Attribute("e") * 2))"   // DMF = 1
            auto mapOperator = secondFilterOperator->getParents()[0]->as<OperatorNode>();
//            dmf = 0.99;     // to help optimizer make a proper decision in case of ties (which could be caused by dmf = 1)
            dmf = 1.1;
            input = output;
            output = input * dmf;
            mapOperator->addProperty("output", output);
        }
    }
};

//measure time:

//    auto start = std::chrono::steady_clock::now();
//    ...
//
//    auto currentTime = std::chrono::steady_clock::now();
//    elapsedMilliseconds = duration_cast<std::chrono::milliseconds>(currentTime - start);
//    elapsedMilliseconds.count()

TEST_F(AASBenchmarkTest, testDELETE) {
    int nLayer2Workers = 2;
    int nLayer3Workers = 0;

    // setup coordinator

    // layer 1
    NES_INFO("AASBenchmarkTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getSourceCatalogService()->registerLogicalSource(sourceName, inputSchema);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("AASBenchmarkTest: Coordinator started successfully");

    // setup workers

    // layer 2
    std::map<int, NesWorkerPtr> layer2Workers;
    for (int i = 0; i < nLayer2Workers; ++i) {
        NES_INFO("AASBenchmarkTest: Start layer2Worker" << i);
        NesWorkerPtr layer2Worker = std::make_shared<NesWorker>(std::move(layer2WorkerConfig));
        bool retStart = layer2Worker->start(/**blocking**/ false, /**withConnect**/ true);
        EXPECT_TRUE(retStart);
        NES_INFO("AASBenchmarkTest: layer2Worker" << i << " started successfully");
        layer2Workers[i] = layer2Worker;
    }

    // layer 3
    std::map<int, NesWorkerPtr> layer3Workers;
    for (int i = 0; i < nLayer3Workers; ++i) {
        NES_INFO("AASBenchmarkTest: Start layer3Worker" << i);
        NesWorkerPtr layer3Worker = std::make_shared<NesWorker>(std::move(layer3WorkerConfig));
        bool retStart = layer3Worker->start(/**blocking**/ false, /**withConnect**/ true);
        EXPECT_TRUE(retStart);
        layer3Worker->removeParent(1);
        layer3Worker->addParent(layer2Workers[0]->getWorkerId());
        NES_INFO("AASBenchmarkTest: layer3Worker" << i << " started successfully");
        layer3Workers[i] = layer3Worker;
    }

    // source layer
    std::map<int, NesWorkerPtr> sourceWorkers;
    for (int i = 0; i < nSourceWorkers; ++i) {
        NES_INFO("AASBenchmarkTest: Start sourceWorker" << i);
        NesWorkerPtr srcWrk = std::make_shared<NesWorker>(std::move(sourceWorkerConfigs[i]));
        bool retStart = srcWrk->start(/**blocking**/ false, /**withConnect**/ true);
        EXPECT_TRUE(retStart);
        srcWrk->removeParent(1);
//        //        srcWrk->addParent(layer3Workers[0]->getWorkerId());
        srcWrk->addParent(layer2Workers[0]->getWorkerId());
        srcWrk->addParent(layer2Workers[1]->getWorkerId());
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

    for (const auto& [i, layer2Worker] : layer2Workers)
        EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(layer2Worker, queryId, globalQueryPlan, 1));
    for (const auto& [i, layer3Worker] : layer3Workers)
        EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(layer3Worker, queryId, globalQueryPlan, 1));
    for (const auto& [i, sourceWorker] : sourceWorkers)
        EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(sourceWorker, queryId, globalQueryPlan, 1));

    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    NES_DEBUG("AASBenchmarkTest:: Topology print: " << crd->getTopology()->toString());


    NES_DEBUG("AASBenchmarkTest: Sleep");
    std::this_thread::sleep_for(std::chrono::milliseconds(5000));

    NES_INFO("AASBenchmarkTest: Remove query");
    queryService->validateAndQueueStopQueryRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    for (const auto& [i, sourceWorker] : sourceWorkers) {
        NES_INFO("AASBenchmarkTest: Stop worker sourceWorker" << i);
        bool retStopWrk = sourceWorker->stop(true);
        EXPECT_TRUE(retStopWrk);
    }

    for (const auto& [i, layer3Worker] : layer3Workers) {
        NES_INFO("AASBenchmarkTest: Stop worker layer3Worker" << i);
        bool retStopWrk = layer3Worker->stop(true);
        EXPECT_TRUE(retStopWrk);
    }

    for (const auto& [i, layer2Worker] : layer2Workers) {
        NES_INFO("AASBenchmarkTest: Stop worker layer2Worker" << i);
        bool retStopWrk = layer2Worker->stop(true);
        EXPECT_TRUE(retStopWrk);
    }


    NES_INFO("AASBenchmarkTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("AASBenchmarkTest: Test finished");
}

/*
* @brief test
*/
TEST_F(AASBenchmarkTest, test) {
    int nLayer2Workers = 2;
    int nLayer3Workers = 0;

    // setup coordinator

    // layer 1
    NES_INFO("AASBenchmarkTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getSourceCatalogService()->registerLogicalSource(sourceName, inputSchema);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("AASBenchmarkTest: Coordinator started successfully");

    // setup workers

    // layer 2
    std::map<int, NesWorkerPtr> layer2Workers;
    for (int i = 0; i < nLayer2Workers; ++i) {
        NES_INFO("AASBenchmarkTest: Start layer2Worker" << i);
        NesWorkerPtr layer2Worker = std::make_shared<NesWorker>(std::move(layer2WorkerConfig));
        bool retStart = layer2Worker->start(/**blocking**/ false, /**withConnect**/ true);
        EXPECT_TRUE(retStart);
        NES_INFO("AASBenchmarkTest: layer2Worker" << i << " started successfully");
        layer2Workers[i] = layer2Worker;
    }

    // layer 3
    std::map<int, NesWorkerPtr> layer3Workers;
    for (int i = 0; i < nLayer3Workers; ++i) {
        NES_INFO("AASBenchmarkTest: Start layer3Worker" << i);
        NesWorkerPtr layer3Worker = std::make_shared<NesWorker>(std::move(layer3WorkerConfig));
        bool retStart = layer3Worker->start(/**blocking**/ false, /**withConnect**/ true);
        EXPECT_TRUE(retStart);
        layer3Worker->removeParent(1);
        layer3Worker->addParent(layer2Workers[0]->getWorkerId());
        NES_INFO("AASBenchmarkTest: layer3Worker" << i << " started successfully");
        layer3Workers[i] = layer3Worker;
    }

    // source layer
    std::map<int, NesWorkerPtr> sourceWorkers;
    for (int i = 0; i < nSourceWorkers; ++i) {
        NES_INFO("AASBenchmarkTest: Start sourceWorker" << i);
        NesWorkerPtr srcWrk = std::make_shared<NesWorker>(std::move(sourceWorkerConfigs[i]));
        bool retStart = srcWrk->start(/**blocking**/ false, /**withConnect**/ true);
        EXPECT_TRUE(retStart);
        srcWrk->removeParent(1);
//        srcWrk->addParent(layer3Workers[0]->getWorkerId());
        srcWrk->addParent(layer2Workers[0]->getWorkerId());
        srcWrk->addParent(layer2Workers[1]->getWorkerId());
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

    for (const auto& [i, layer2Worker] : layer2Workers)
        EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(layer2Worker, queryId, globalQueryPlan, 1));
    for (const auto& [i, layer3Worker] : layer3Workers)
        EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(layer3Worker, queryId, globalQueryPlan, 1));
    for (const auto& [i, sourceWorker] : sourceWorkers)
        EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(sourceWorker, queryId, globalQueryPlan, 1));

    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    NES_DEBUG("AASBenchmarkTest:: Topology print: " << crd->getTopology()->toString());


    NES_DEBUG("AASBenchmarkTest: Sleep");
    std::this_thread::sleep_for(std::chrono::milliseconds(5000));

    NES_INFO("AASBenchmarkTest: Remove query");
    queryService->validateAndQueueStopQueryRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    for (const auto& [i, sourceWorker] : sourceWorkers) {
        NES_INFO("AASBenchmarkTest: Stop worker sourceWorker" << i);
        bool retStopWrk = sourceWorker->stop(true);
        EXPECT_TRUE(retStopWrk);
    }

    for (const auto& [i, layer3Worker] : layer3Workers) {
        NES_INFO("AASBenchmarkTest: Stop worker layer3Worker" << i);
        bool retStopWrk = layer3Worker->stop(true);
        EXPECT_TRUE(retStopWrk);
    }

    for (const auto& [i, layer2Worker] : layer2Workers) {
        NES_INFO("AASBenchmarkTest: Stop worker layer2Worker" << i);
        bool retStopWrk = layer2Worker->stop(true);
        EXPECT_TRUE(retStopWrk);
    }


    NES_INFO("AASBenchmarkTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("AASBenchmarkTest: Test finished");
}

// TODO: should also work with 4 sources
TEST_F(AASBenchmarkTest, testSequential4SourcesNoAAS) {
    // setup coordinator

    // layer 1
    NES_INFO("AASBenchmarkTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getSourceCatalogService()->registerLogicalSource(sourceName, inputSchema);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("AASBenchmarkTest: Coordinator started successfully");

    // setup workers

    // layer 2
    NES_INFO("AASBenchmarkTest: Start layer2Worker");
    NesWorkerPtr layer2Worker = std::make_shared<NesWorker>(std::move(layer2WorkerConfig));
    bool retStart2 = layer2Worker->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("AASBenchmarkTest: layer2Worker started successfully");

    // layer 3
    NES_INFO("AASBenchmarkTest: Start layer3Worker");
    NesWorkerPtr layer3Worker = std::make_shared<NesWorker>(std::move(layer3WorkerConfig));
    bool retStart3 = layer3Worker->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    layer3Worker->removeParent(1);
    layer3Worker->addParent(layer2Worker->getWorkerId());
    NES_INFO("AASBenchmarkTest: layer3Worker started successfully");

    // source layer
    std::map<int, NesWorkerPtr> sourceWorkers;
    for (int i = 0; i < nSourceWorkers; ++i) {
        NES_INFO("AASBenchmarkTest: Start sourceWorker" << i);
        NesWorkerPtr srcWrk = std::make_shared<NesWorker>(std::move(sourceWorkerConfigs[i]));
        bool retStart = srcWrk->start(/**blocking**/ false, /**withConnect**/ true);
        EXPECT_TRUE(retStart);
        srcWrk->removeParent(1);
        srcWrk->addParent(layer3Worker->getWorkerId());
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

    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(layer2Worker, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(layer3Worker, queryId, globalQueryPlan, 1));
    for (const auto& [i, sourceWorker] : sourceWorkers)
        EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(sourceWorker, queryId, globalQueryPlan, 1));

    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    NES_DEBUG("AASBenchmarkTest:: Topology print: " << crd->getTopology()->toString());


    NES_DEBUG("AASBenchmarkTest: Sleep");
    std::this_thread::sleep_for(std::chrono::milliseconds(5000));

    NES_INFO("AASBenchmarkTest: Remove query");
    queryService->validateAndQueueStopQueryRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    for (const auto& [i, sourceWorker] : sourceWorkers) {
        NES_INFO("AASBenchmarkTest: Stop worker sourceWorker" << i);
        bool retStopWrk = sourceWorker->stop(true);
        EXPECT_TRUE(retStopWrk);
    }

    NES_INFO("AASBenchmarkTest: Stop worker layer3Worker");
    bool retStopWrk3 = layer3Worker->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_INFO("AASBenchmarkTest: Stop worker layer2Worker");
    bool retStopWrk2 = layer2Worker->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("AASBenchmarkTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("AASBenchmarkTest: Test finished");
}

// TODO: how to fail a node?
TEST_F(AASBenchmarkTest, testDiamondAAS) {
    int nLayer2Workers = 2;

    // setup coordinator

    // layer 1
    NES_INFO("AASBenchmarkTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getSourceCatalogService()->registerLogicalSource(sourceName, inputSchema);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("AASBenchmarkTest: Coordinator started successfully");

    // setup workers

    // layer 2
    std::map<int, NesWorkerPtr> layer2Workers;
    for (int i = 0; i < nLayer2Workers; ++i) {
        NES_INFO("AASBenchmarkTest: Start layer2Worker" << i);
        NesWorkerPtr layer2Worker = std::make_shared<NesWorker>(std::move(layer2WorkerConfig));
        bool retStart = layer2Worker->start(/**blocking**/ false, /**withConnect**/ true);
        EXPECT_TRUE(retStart);
        NES_INFO("AASBenchmarkTest: layer2Worker" << i << " started successfully");
        layer2Workers[i] = layer2Worker;
    }


    // source layer
    std::map<int, NesWorkerPtr> sourceWorkers;
    for (int i = 0; i < nSourceWorkers; ++i) {
        NES_INFO("AASBenchmarkTest: Start sourceWorker" << i);
        NesWorkerPtr srcWrk = std::make_shared<NesWorker>(std::move(sourceWorkerConfigs[i]));
        bool retStart = srcWrk->start(/**blocking**/ false, /**withConnect**/ true);
        EXPECT_TRUE(retStart);
        srcWrk->removeParent(1);
        srcWrk->addParent(layer2Workers[0]->getWorkerId());
        srcWrk->addParent(layer2Workers[1]->getWorkerId());
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

    for (const auto& [i, layer2Worker] : layer2Workers)
        EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(layer2Worker, queryId, globalQueryPlan, 1));
    for (const auto& [i, sourceWorker] : sourceWorkers)
        EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(sourceWorker, queryId, globalQueryPlan, 1));

    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    NES_DEBUG("AASBenchmarkTest:: Topology print: " << crd->getTopology()->toString());


    NES_DEBUG("AASBenchmarkTest: Sleep");
    std::this_thread::sleep_for(std::chrono::milliseconds(5000));

    // TODO: leads to errors
//    NES_INFO("AASBenchmarkTest: Manually fail layer2Worker" << 1);
//    bool retStopWrk1 = layer2Workers[1]->disconnect();
//    retStopWrk1 = layer2Workers[1]->stop(true);
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

    NES_INFO("AASBenchmarkTest: Stop worker layer2Worker" << 2);
    bool retStopWrk = layer2Workers[1]->stop(true);
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
    auto placementStrategyAAS = PlacementStrategy::Greedy_AAS;

    auto topology = setupTopologyAndSourceCatalogNPhysicalSourcesSamePath(nSourceWorkers);

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
                EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<MapLogicalOperatorNode>());
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
                EXPECT_TRUE(op->instanceOf<MapLogicalOperatorNode>());
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
                    EXPECT_TRUE(actualRootOperator->getChildren()[i]->instanceOf<MapLogicalOperatorNode>()
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
    int nLayer2Workers = 1;
    int nLayer3Workers = 1;

    // setup coordinator

    // layer 1
    NES_INFO("AASBenchmarkTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getSourceCatalogService()->registerLogicalSource(sourceName, inputSchema);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("AASBenchmarkTest: Coordinator started successfully");

    // setup workers

    // layer 2
    std::map<int, NesWorkerPtr> layer2Workers;
    for (int i = 0; i < nLayer2Workers; ++i) {
        NES_INFO("AASBenchmarkTest: Start layer2Worker" << i);
        NesWorkerPtr layer2Worker = std::make_shared<NesWorker>(std::move(layer2WorkerConfig));
        bool retStart = layer2Worker->start(/**blocking**/ false, /**withConnect**/ true);
        EXPECT_TRUE(retStart);
        NES_INFO("AASBenchmarkTest: layer2Worker" << i << " started successfully");
        layer2Workers[i] = layer2Worker;
    }

    // layer 3
    std::map<int, NesWorkerPtr> layer3Workers;
    for (int i = 0; i < nLayer3Workers; ++i) {
        NES_INFO("AASBenchmarkTest: Start layer3Worker" << i);
        NesWorkerPtr layer3Worker = std::make_shared<NesWorker>(std::move(layer3WorkerConfig));
        bool retStart = layer3Worker->start(/**blocking**/ false, /**withConnect**/ true);
        EXPECT_TRUE(retStart);
        layer3Worker->removeParent(1);
        layer3Worker->addParent(layer2Workers[0]->getWorkerId());
        NES_INFO("AASBenchmarkTest: layer3Worker" << i << " started successfully");
        layer3Workers[i] = layer3Worker;
    }

    // source layer
    std::map<int, NesWorkerPtr> sourceWorkers;
    for (int i = 0; i < nSourceWorkers; ++i) {
        NES_INFO("AASBenchmarkTest: Start sourceWorker" << i);
        NesWorkerPtr srcWrk = std::make_shared<NesWorker>(std::move(sourceWorkerConfigs[i]));
        bool retStart = srcWrk->start(/**blocking**/ false, /**withConnect**/ true);
        EXPECT_TRUE(retStart);
        srcWrk->removeParent(1);
        srcWrk->addParent(layer3Workers[0]->getWorkerId());
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

    std::string outputFilePath = "AASBenchmarkTest.out";
    remove(outputFilePath.c_str());

    NES_INFO("AASBenchmarkTest: Submit query");
    string query = createQueryString(outputFilePath);

    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, "ILP", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));

    NES_DEBUG("AASBenchmarkTest:: Topology print: " << crd->getTopology()->toString());

    for (const auto& [i, layer2Worker] : layer2Workers)
        EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(layer2Worker, queryId, globalQueryPlan, 1));
    for (const auto& [i, reserveWorker] : reserveWorkers)
        EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(reserveWorker, queryId, globalQueryPlan, 1));
    for (const auto& [i, layer3Worker] : layer3Workers)
        EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(layer3Worker, queryId, globalQueryPlan, 1));
    for (const auto& [i, sourceWorker] : sourceWorkers)
        EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(sourceWorker, queryId, globalQueryPlan, 1));

    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    NES_DEBUG("AASBenchmarkTest:: Topology print: " << crd->getTopology()->toString());


    NES_DEBUG("AASBenchmarkTest: Sleep");
    std::this_thread::sleep_for(std::chrono::milliseconds(5000));

    NES_INFO("AASBenchmarkTest: Remove query");
    queryService->validateAndQueueStopQueryRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    for (const auto& [i, sourceWorker] : sourceWorkers) {
        NES_INFO("AASBenchmarkTest: Stop worker sourceWorker" << i);
        bool retStopWrk = sourceWorker->stop(true);
        EXPECT_TRUE(retStopWrk);
    }

    for (const auto& [i, layer3Worker] : layer3Workers) {
        NES_INFO("AASBenchmarkTest: Stop worker layer3Worker" << i);
        bool retStopWrk = layer3Worker->stop(true);
        EXPECT_TRUE(retStopWrk);
    }

    for (const auto& [i, reserveWorker] : reserveWorkers) {
        NES_INFO("AASBenchmarkTest: Stop worker reserveWorker" << i);
        bool retStopWrk = reserveWorker->stop(true);
        EXPECT_TRUE(retStopWrk);
    }

    for (const auto& [i, layer2Worker] : layer2Workers) {
        NES_INFO("AASBenchmarkTest: Stop worker layer2Worker" << i);
        bool retStopWrk = layer2Worker->stop(true);
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


/// ----------------- 3. Fat topology with 4 sources with separate paths and high connectivity  ----------------------------------
/* Fat topology with 4 sources with separate paths, high connectivity
 *     1
 * /  |  |  \
 * 2  3  4  5
 * |\/|\/|\/|
 * |/\|/\|/\|
 * 6  7  8  9
 * |\/|\/|\/|
 * |/\|/\|/\|
 * 10 11 12 13
 */


}// namespace NES