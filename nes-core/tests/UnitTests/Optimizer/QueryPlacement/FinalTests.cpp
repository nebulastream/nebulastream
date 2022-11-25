#include "Util/Logger/Logger.hpp"
#include <gtest/gtest.h>
#include "z3++.h"
#include "Optimizer/QueryMerger/SyntaxBasedCompleteQueryMergerRule.hpp"
#include "z3++.h"
#include <API/QueryAPI.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Source/SourceCatalogEntry.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <FaultTolerance//FaultToleranceConfiguration.hpp>
#include <NesBaseTest.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Phases/QueryPlacementPhase.hpp>
#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <Optimizer/Phases/SignatureInferencePhase.hpp>
#include <Optimizer/Phases/TopologySpecificQueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryMerger/Z3SignatureBasedPartialQueryMergerRule.hpp>
#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <Optimizer/QueryPlacement/PlacementStrategyFactory.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/BufferStorage.hpp>
#include <Services/QueryParsingService.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <fstream>
#include <gtest/gtest.h>
#include <utility>
//
// Created by noah on 23.11.22.
//
class FinalTests : public ::testing::TestWithParam<std::tuple<int,int,int,Query>>{
  public:
    z3::ContextPtr z3Context;
    SourceCatalogPtr sourceCatalog;
    TopologyPtr topology;
    QueryParsingServicePtr queryParsingService;
    GlobalExecutionPlanPtr globalExecutionPlan;
    Optimizer::TypeInferencePhasePtr typeInferencePhase;
    std::shared_ptr<Catalogs::UdfCatalog> udfCatalog;
    std::vector<ExecutionNodePtr> executionNodes;
    QueryId queryId;
    FaultToleranceConfigurationPtr ftConfig;
    std::ofstream logFile;
    static void SetUpTestCase(){
        std::ofstream logFile;
        logFile.open("/home/noah/Desktop/worldCupPlacement/results.csv");                \
        logFile << "schema, queryId, processingGuarantee, placedFTApproach, tupleSizeInByte, ackIntervalInTuples, ingestionRateInTuplesPerSecond, usedCpuSlots, additionalCpuSlots, additionalNetworking, additionalMemoryBytes,"
                << " availableCpuSlotsNode1, availableCpuSlotsNode2, availableCpuSlotsNode3,"
                << "availableBandwidthInBytePerSecondNode1, availableBandwidthInBytePerSecondNode2, availableBandwidthInBytePerSecondNode3,"
                << "availableBuffersInByteNode1, availableBuffersInByteNode2, availableBuffersInByteNode3," << "\n";
        logFile.close();

    }
    void SetUp(){
        NES::Logger::setupLogging("QueryPlacementTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup QueryPlacementTest test case." << std::endl;
        z3Context = std::make_shared<z3::context>();
        auto cppCompiler = Compiler::CPPCompiler::create();
        auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        queryParsingService = QueryParsingService::create(jitCompiler);
        udfCatalog = Catalogs::UdfCatalog::create();
        ftConfig = FaultToleranceConfiguration::create();
        logFile.open("/home/noah/Desktop/worldCupPlacement/results.csv", std::ios_base::app);
    }
    void TearDown(){

    }
    void setupTopologyWith3Nodes(std::vector<int> availableSlots, std::vector<int> availableMemory, std::vector<int> availableBandwidth){
        topology = Topology::create();

        TopologyNodePtr node1 = TopologyNode::create(1, "localhost", 123, 124, availableSlots[0]);
        topology->setAsRoot(node1);
        node1->setAvailableBuffers(availableMemory[0]);
        node1->setAvailableBandwidth(availableBandwidth[0]);


        TopologyNodePtr node2 = TopologyNode::create(2,"localhost", 125, 126, availableSlots[1]);
        topology->addNewTopologyNodeAsChild(node1, node2);
        node2->setAvailableBuffers(availableMemory[1]);
        node2->setAvailableBandwidth(availableBandwidth[1]);

        TopologyNodePtr node3 = TopologyNode::create(3,"localhost", 127, 128, availableSlots[2]);
        topology->addNewTopologyNodeAsChild(node2, node3);
        node3->setAvailableBuffers(availableMemory[2]);
        node3->setAvailableBandwidth(availableBandwidth[2]);

        TopologyNodePtr node4 = TopologyNode::create(4,"localhost", 129, 130, availableSlots[1]);
        topology->addNewTopologyNodeAsChild(node1, node4);
        node3->setAvailableBuffers(availableMemory[1]);
        node3->setAvailableBandwidth(availableBandwidth[1]);

        std::string schema = "Schema::create()->addField(\"timestamp\", BasicType::UINT32)"
                             "->addField(\"clientID\", BasicType::UINT32)"
                             "->addField(\"objectID\", BasicType::UINT32)"
                             "->addField(\"size\", BasicType::UINT32)"
                             "->addField(\"method\", BasicType::UINT8)"
                             "->addField(\"status\", BasicType::UINT8)"
                             "->addField(\"type\", BasicType::UINT8)"
                             "->addField(\"server\", BasicType::UINT8);";
        const std::string sourceName = "worldCup";

        sourceCatalog = std::make_shared<SourceCatalog>(queryParsingService);
        sourceCatalog->addLogicalSource(sourceName, schema);
        auto logicalSource = sourceCatalog->getLogicalSource(sourceName);

        CSVSourceTypePtr csvSourceType = CSVSourceType::create();
        csvSourceType->setGatheringInterval(12);
        csvSourceType->setNumberOfTuplesToProducePerBuffer(0);
        auto physicalSource = PhysicalSource::create(sourceName, "worldCup", csvSourceType);

        SourceCatalogEntryPtr sourceCatalogEntry = std::make_shared<SourceCatalogEntry>(physicalSource, logicalSource, node3);

        sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry);

        globalExecutionPlan = GlobalExecutionPlan::create();
        typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    }
    void setupQueryIris(Query q){
        Query query = q;
        QueryPlanPtr queryPlan = query.getQueryPlan();

        auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
        queryPlan = queryReWritePhase->execute(queryPlan);
        typeInferencePhase->execute(queryPlan);

        auto topologySpecificQueryRewrite =
            Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());
        topologySpecificQueryRewrite->execute(queryPlan);
        typeInferencePhase->execute(queryPlan);

        auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
        queryId = sharedQueryPlan->getSharedQueryId();
        auto queryPlacementPhase =
            Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, z3Context, false);
        queryPlacementPhase->execute(NES::PlacementStrategy::BottomUp, sharedQueryPlan);
        executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

        NES_INFO("\n"+globalExecutionPlan->getAsString())
    }
    void setupQueryWorldCup(Query q){
        Query query = q;
        QueryPlanPtr queryPlan = query.getQueryPlan();

        auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
        queryPlan = queryReWritePhase->execute(queryPlan);
        typeInferencePhase->execute(queryPlan);

        auto topologySpecificQueryRewrite =
            Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());
        topologySpecificQueryRewrite->execute(queryPlan);
        typeInferencePhase->execute(queryPlan);

        auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
        queryId = sharedQueryPlan->getSharedQueryId();
        auto queryPlacementPhase =
            Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, z3Context, false);
        queryPlacementPhase->execute(NES::PlacementStrategy::BottomUp, sharedQueryPlan);
        executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

        NES_INFO("\n"+globalExecutionPlan->getAsString())
    }
};

/*TEST_P(FinalTests, test1){
    int t1 = std::get<0>(GetParam());
    int t2 = std::get<1>(GetParam());
    int t3 = std::get<2>(GetParam());
    NES_INFO("\nnums: " << t1 << ", " << t2 << ", " << t3);
    std::cout << "T";
}*/

/*TEST_P(FinalTests, test2){
    const int x = std::get<0>(GetParam());
    const std::string y = std::get<1>(GetParam());
    const bool z = std::get<2>(GetParam());
    NES_INFO("\n" <<x << ", " << y << ", " << z);
}*/


TEST_P(FinalTests, testTest){
    //setupTopologyWith3Nodes({5,3,1},{5120,5120,5120},{2000000,1000000});
    //setupTopologyWith3Nodes({200,200,200},{5120,5120,5120},{2000000,1000000});
    setupTopologyWith3Nodes({std::get<0>(GetParam()),std::get<1>(GetParam()),std::get<2>(GetParam())},{5120,5120,5120},{2000000,2000000,2000000});
    Query q = std::get<3>(GetParam());
    setupQueryWorldCup(q);

    //Optimizer::BasePlacementStrategy::initNetworkConnectivities(topology, globalExecutionPlan, queryId);
    Optimizer::BasePlacementStrategy::initAdjustedCosts(topology, globalExecutionPlan, globalExecutionPlan->getExecutionNodeByNodeId(1), queryId);

    CSVSourceTypePtr csvSourceType = CSVSourceType::create();
    csvSourceType->setGatheringInterval(500);

    ftConfig->setIngestionRate(csvSourceType->getGatheringInterval()->getValue());
    ftConfig->setTupleSize(sourceCatalog->getSchemaForLogicalSource("worldCup")->getSchemaSizeInBytes());
    ftConfig->setAckRate(750);
    ftConfig->setAckSize(8);
    ftConfig->setProcessingGuarantee(FaultToleranceType::AT_LEAST_ONCE);
    ftConfig->setQueryId(queryId);

    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    TopologyPtr top = topology;
    executionNodes.erase(std::remove_if(
                             executionNodes.begin(), executionNodes.end(),
                             [top](const ExecutionNodePtr& node) {
                                 return node->getId() == top->getRoot()->getId(); // put your condition here
                             }), executionNodes.end());

    std::vector<ExecutionNodePtr> sortedList =
        Optimizer::BasePlacementStrategy::getSortedListForFirstFit(executionNodes, ftConfig, topology, globalExecutionPlan);

    FaultToleranceType placedFT = Optimizer::BasePlacementStrategy::firstFitQueryPlacement(sortedList, ftConfig, topology);

    NES_INFO("\nFOR QUERY#" << ftConfig->getQueryId() << ", CHOOSE " << toString(placedFT));

    TopologyNodePtr topNode1 = topology->findNodeWithId(1);
    TopologyNodePtr topNode2 = topology->findNodeWithId(2);
    TopologyNodePtr topNode3 = topology->findNodeWithId(3);

    int usedCpuSlots = topology->findNodeWithId(1)->getUsedResources();
    int additionalOperatorSlots = 0;
    double additionalMemoryBytes = 0;
    double additionalNetworkingBytesPerSecond = 0;

    for(auto& node : executionNodes){

        usedCpuSlots += topology->findNodeWithId(node->getId())->getUsedResources();
        float networkConnectivityInSeconds = Optimizer::BasePlacementStrategy::getNetworkConnectivity(topology, node) / 1000;

        switch(placedFT){
            case FaultToleranceType::ACTIVE_STANDBY:
                additionalOperatorSlots += Optimizer::BasePlacementStrategy::getExecutionNodeOperatorCosts(
                    Optimizer::BasePlacementStrategy::getExecutionNodeParent(node), ftConfig->getQueryId());
                if(ftConfig->getProcessingGuarantee() == FaultToleranceType::AT_LEAST_ONCE){
                    additionalNetworkingBytesPerSecond += (ftConfig->getIngestionRate() * ftConfig->getTupleSize() * ftConfig->getAckInterval())
                        + (3 * (ftConfig->getAckSize() * ftConfig->getAckInterval()));
                }else{
                    additionalNetworkingBytesPerSecond += (ftConfig->getIngestionRate() * ftConfig->getTupleSize() * ftConfig->getAckInterval())
                        + (3 * (ftConfig->getAckSize() * ftConfig->getAckInterval()));
                }
                additionalMemoryBytes += ftConfig->getOutputQueueSize(networkConnectivityInSeconds) * 2;
                break;
            case FaultToleranceType::CHECKPOINTING:
                additionalOperatorSlots += Optimizer::BasePlacementStrategy::getExecutionNodeOperatorCosts(
                    Optimizer::BasePlacementStrategy::getExecutionNodeParent(node), ftConfig->getQueryId());
                additionalNetworkingBytesPerSecond += 2 * (ftConfig->getAckSize() * ftConfig->getAckInterval());
                additionalNetworkingBytesPerSecond = 2 * (ftConfig->getAckSize() * ftConfig->getAckInterval());
                additionalMemoryBytes += ftConfig->getOutputQueueSize(networkConnectivityInSeconds) + ftConfig->getCheckpointSize();
                break;
            case FaultToleranceType::UPSTREAM_BACKUP:
                if(ftConfig->getProcessingGuarantee() == FaultToleranceType::AT_LEAST_ONCE){
                    additionalNetworkingBytesPerSecond += ((ftConfig->getIngestionRate() / ftConfig->getAckRate()) * ftConfig->getAckSize());
                }else{
                    if(Optimizer::BasePlacementStrategy::checkIfSubPlanIsArbitrary(node, ftConfig->getQueryId())){
                        additionalNetworkingBytesPerSecond += ((ftConfig->getIngestionRate() / ftConfig->getAckRate()) * ftConfig->getAckSize())
                            + (ftConfig->getIngestionRate() * ftConfig->getAckInterval());
                    }else{
                        additionalNetworkingBytesPerSecond += ((ftConfig->getIngestionRate() / ftConfig->getAckRate()) * ftConfig->getAckSize())
                            + ((node->getChildren().size() * ftConfig->getAckSize()) * ftConfig->getAckInterval());
                    }
                }
                additionalMemoryBytes += Optimizer::BasePlacementStrategy::calcUpstreamBackupMemorySingleNode(node, topology, ftConfig);
            default:
                break;
        }
    }


    logFile << "worldCup," << queryId << "," << ftConfig->getProcessingGuarantee() << "," << placedFT << "," << ftConfig->getTupleSize() << ","
            << ftConfig->getAckRate() << "," << ftConfig->getIngestionRate() << "," << additionalOperatorSlots << "," << usedCpuSlots << ","
            << additionalNetworkingBytesPerSecond << "," << additionalMemoryBytes << ","
            << topNode1->getAvailableResources() << "," << topNode2->getAvailableResources() << ","
            << topNode3->getAvailableResources() << ","
            << topNode1->getAvailableBandwidth() << "," << topNode2->getAvailableBandwidth() << "," << topNode3->getAvailableBandwidth() << ","
            << topNode1->getAvailableBuffers() << "," << topNode2->getAvailableBuffers() << "," << topNode3->getAvailableBuffers() << "," << "\n";



}

/*INSTANTIATE_TEST_CASE_P(test1,
                        FinalTests,
                        ::testing::Values(std::make_tuple(1,2,3),
                                          std::make_tuple(4,5,6),
                                          std::make_tuple(7,8,9)));*/

INSTANTIATE_TEST_SUITE_P(TestSanity, FinalTests, testing::Combine(
                                                     //testing::Values(3,4,5,6,7,8,9,10),
                                                     //testing::Values(2,3,4,5,6,7,8,9),
                                                     //testing::Values(1,2,3,4,5,6,7,8),
                                                     testing::Values(10,9,8,7,6,5,4,3,2,1),
                                                     testing::Values(10,9,8,7,6,5,4,3,2,1),
                                                     testing::Values(10,9,8,7,6,5,4,3,2,1),
                                                     testing::Values(Query::from("worldCup").project(Attribute("clientID"),Attribute("objectID"),Attribute("size"),Attribute("timestamp")).map(Attribute("size")=9317 + Attribute("clientID")).filter(Attribute("clientID")<=2491).sink(NullOutputSinkDescriptor::create()),
                                                                     Query::from("worldCup").project(Attribute("clientID").as("ecvem"),Attribute("objectID").as("iyqps"),Attribute("size").as("szhwx"),Attribute("timestamp")).map(Attribute("ecvem")=1396 - Attribute("iyqps")).filter(Attribute("ecvem")>4766).sink(NullOutputSinkDescriptor::create()),
                                                                     Query::from("worldCup").filter(Attribute("objectID")<1656).map(Attribute("objectID")=3296 * Attribute("size")).project(Attribute("clientID").as("sdxdo"),Attribute("objectID").as("mfmcm"),Attribute("timestamp")).sink(NullOutputSinkDescriptor::create()))
                                                         ));

