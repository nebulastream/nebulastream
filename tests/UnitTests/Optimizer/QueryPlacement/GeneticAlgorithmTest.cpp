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
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <Optimizer/Phases/TopologySpecificQueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <Optimizer/QueryPlacement/PlacementStrategyFactory.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <Services/QueryParsingService.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <gtest/gtest.h>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <Windowing/TimeCharacteristic.hpp>

using namespace NES;
using namespace web;

class GeneticAlgorithmStrategyTest : public testing::Test {
  public:
    std::shared_ptr<QueryParsingService> queryParsingService;
    // Will be called before any test in this class are executed.
    static void SetUpTestCase() { std::cout << "Setup QueryPlacementTest test class." << std::endl; }

    // Will be called before a test is executed.
    void SetUp() override {
        NES::setupLogging("QueryPlacementTest.log", NES::LOG_DEBUG);
        std::cout << "Setup QueryPlacementTest test case." << std::endl;
        auto cppCompiler = Compiler::CPPCompiler::create();
        auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        queryParsingService = QueryParsingService::create(jitCompiler);
    }

    // Will be called before a test is executed.
    void TearDown() { std::cout << "Setup QueryPlacementTest test case." << std::endl; }

    // Will be called after all tests in this class are finished.
    static void TearDownTestCase() { std::cout << "Tear down QueryPlacementTest test class." << std::endl; }

    void setupTopologyAndStreamCatalogForGA(int numOfMidNodes) {

        topology = Topology::create();
        // create the topology for the test
        uint32_t grpcPort = 4000;
        uint32_t dataPort = 5000;
        int nodeID = 1;
        TopologyNodePtr sinkNode = TopologyNode::create(nodeID++, "localhost", grpcPort, dataPort, 50);
        topology->setAsRoot(sinkNode);
        LinkPropertyPtr linkProperty = std::make_shared<LinkProperty>(LinkProperty(100, 64));
        auto previousNode = sinkNode;
        for(int i = 0; i < numOfMidNodes; i++) {
            TopologyNodePtr midNode = TopologyNode::create(nodeID++, "localhost", 123, 124, 10);
            topology->addNewPhysicalNodeAsChild(previousNode, midNode);
            midNode->addLinkProperty(previousNode, linkProperty);
            previousNode->addLinkProperty(midNode, linkProperty);
            previousNode = midNode;
        }


        TopologyNodePtr sourceNode = TopologyNode::create(nodeID++, "localhost", grpcPort, dataPort, 4);
        topology->addNewPhysicalNodeAsChild(previousNode, sourceNode);
        previousNode->addLinkProperty(sourceNode, linkProperty);
        sourceNode->addLinkProperty(previousNode, linkProperty);

        std::string schema = "Schema::create()->addField(\"id\", BasicType::UINT32)"
                             "->addField(\"value\", BasicType::UINT64);";
        const std::string streamName = "car";

        streamCatalog = std::make_shared<StreamCatalog>(queryParsingService);
        streamCatalog->addLogicalStream(streamName, schema);

        SourceConfigPtr sourceConfig = SourceConfig::create();
        sourceConfig->setSourceFrequency(0);
        sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
        sourceConfig->setPhysicalStreamName("test1");
        sourceConfig->setLogicalStreamName("car");

        PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(sourceConfig);

        StreamCatalogEntryPtr streamCatalogEntry1 = std::make_shared<StreamCatalogEntry>(conf, sourceNode);

        streamCatalog->addPhysicalStream("car", streamCatalogEntry1);
    }

    StreamCatalogPtr streamCatalog;
    TopologyPtr topology;
};

TEST_F(GeneticAlgorithmStrategyTest, testPlacingQueryWithGeneticAlgorithmStrategy1) {
    std::ofstream outfile;
    outfile.open ("benchmark.txt");

    setupTopologyAndStreamCatalogForGA(10);

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);
    auto placementStrategy = Optimizer::PlacementStrategyFactory::getStrategy("GeneticAlgorithm",
                                                                              globalExecutionPlan,
                                                                              topology,
                                                                              typeInferencePhase,
                                                                              streamCatalog);

    Query query = Query::from("car").filter(Attribute("id") < 45).map(Attribute("newId2") = Attribute("id") * 2).sink(PrintSinkDescriptor::create());

    QueryPlanPtr queryPlan = query.getQueryPlan();
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    queryPlan->setQueryId(queryId);

    // Execute optimization phases prior to placement
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
    queryPlan = queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto topologySpecificQueryRewrite = Optimizer::TopologySpecificQueryRewritePhase::create(streamCatalog);
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);
    outfile << "The topology is:\n"<<topology->toString();
    outfile << "Query Plan:"<<queryPlan->toString()<<"\n";
    outfile.close();

    auto start = std::chrono::high_resolution_clock::now();
    ASSERT_TRUE(placementStrategy->updateGlobalExecutionPlan(queryPlan));
    auto stop = std::chrono::high_resolution_clock::now();
    long count = std::chrono::duration_cast<std::chrono::milliseconds>(stop - start).count();
    NES_DEBUG("Found Solution in " << count << "ms");
}
TEST_F(GeneticAlgorithmStrategyTest, testPlacingQueryWithGeneticAlgorithmStrategy2) {
    // Setup the topology
    auto sinkNode = TopologyNode::create(1, "localhost", 4000, 5000, 4);
    auto midNode = TopologyNode::create(2, "localhost", 4001, 5001, 4);
    auto srcNode = TopologyNode::create(3, "localhost", 4002, 5002, 4);

    TopologyPtr topology = Topology::create();
    topology->setAsRoot(sinkNode);

    topology->addNewPhysicalNodeAsChild(sinkNode, midNode);
    topology->addNewPhysicalNodeAsChild(midNode, srcNode);

    LinkPropertyPtr linkProperty1 = std::make_shared<LinkProperty>(LinkProperty(10, 512));
    sinkNode->addLinkProperty(midNode, linkProperty1);
    midNode->addLinkProperty(sinkNode, linkProperty1);
    midNode->addLinkProperty(srcNode, linkProperty1);
    srcNode->addLinkProperty(midNode, linkProperty1);

    ASSERT_TRUE(sinkNode->containAsChild(midNode));
    ASSERT_TRUE(midNode->containAsChild(srcNode));

    // Prepare the source and schema
    std::string schema = "Schema::create()->addField(\"id\", BasicType::UINT32)"
                         "->addField(\"value\", BasicType::UINT64);";
    const std::string streamName = "car";

    streamCatalog = std::make_shared<StreamCatalog>(queryParsingService);
    streamCatalog->addLogicalStream(streamName, schema);

    SourceConfigPtr sourceConfig = SourceConfig::create();
    sourceConfig->setSourceFrequency(0);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig->setPhysicalStreamName("test2");
    sourceConfig->setLogicalStreamName("car");

    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(sourceConfig);
    StreamCatalogEntryPtr streamCatalogEntry1 = std::make_shared<StreamCatalogEntry>(conf, srcNode);

    streamCatalog->addPhysicalStream("car", streamCatalogEntry1);

    // Prepare the query
    auto sinkOperator = LogicalOperatorFactory::createSinkOperator(PrintSinkDescriptor::create());
    auto filterOperator = LogicalOperatorFactory::createFilterOperator(Attribute("id") < 45);
    auto sourceOperator = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("car"));

    sinkOperator->addChild(filterOperator);
    filterOperator->addChild(sourceOperator);

    QueryPlanPtr testQueryPlan = QueryPlan::create();
    testQueryPlan->addRootOperator(sinkOperator);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    testQueryPlan->setQueryId(queryId);

    std::vector<std::map<std::string, std::any>> properties;

    // adding property of the source
    std::map<std::string, std::any> srcProp;
    srcProp.insert(std::make_pair("load", 1));
    srcProp.insert(std::make_pair("dmf", 5.0));

    // adding property of the filter
    std::map<std::string, std::any> filterProp;
    filterProp.insert(std::make_pair("load", 3));
    filterProp.insert(std::make_pair("dmf", 0.2));

    // adding property of the sink
    std::map<std::string, std::any> sinkProp;
    sinkProp.insert(std::make_pair("load", 4));
    sinkProp.insert(std::make_pair("dmf", 4.0));

    properties.push_back(sinkProp);
    properties.push_back(filterProp);
    properties.push_back(srcProp);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);
    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto placementStrategy = Optimizer::PlacementStrategyFactory::getStrategy("GeneticAlgorithm",
                                                                              globalExecutionPlan,
                                                                              topology,
                                                                              typeInferencePhase,
                                                                              streamCatalog);

    // Execute optimization phases prior to placement
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
    testQueryPlan = queryReWritePhase->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);

    auto topologySpecificQueryRewrite = Optimizer::TopologySpecificQueryRewritePhase::create(streamCatalog);
    topologySpecificQueryRewrite->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);

    UtilityFunctions::assignPropertiesToQueryOperators(testQueryPlan, properties);
    placementStrategy->updateGlobalExecutionPlan(testQueryPlan);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    EXPECT_EQ(executionNodes.size(), 3UL);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1U) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(testQueryPlan->getQueryId());
            EXPECT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            EXPECT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
        } else if (executionNode->getId() == 2U) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(testQueryPlan->getQueryId());
            EXPECT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            EXPECT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());//system generated source
        } else if (executionNode->getId() == 3U) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(testQueryPlan->getQueryId());
            EXPECT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            EXPECT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());// Filter
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());// stream source
        }
    }
}
TEST_F(GeneticAlgorithmStrategyTest, testPlacingQueryWithGeneticAlgorithmStrategy3) {
    std::ofstream outfile;
    outfile.open ("benchmark.txt");
    // Setup the topology
    auto sinkNode = TopologyNode::create(1, "localhost", 4000, 5000, 4);
    auto midNode1 = TopologyNode::create(2, "localhost", 4001, 5001, 4);
    auto srcNode1 = TopologyNode::create(3, "localhost", 4002, 5002, 3);
    auto midNode2 = TopologyNode::create(4, "localhost", 4001, 5001, 4);
    auto srcNode2 = TopologyNode::create(5, "localhost", 4002, 5002, 3);
    auto srcNode3 = TopologyNode::create(6, "localhost", 4002, 5002, 3);
    auto srcNode4 = TopologyNode::create(7, "localhost", 4002, 5002, 3);
    auto srcNode5 = TopologyNode::create(8, "localhost", 4002, 5002, 3);

    TopologyPtr topology = Topology::create();
    topology->setAsRoot(sinkNode);

    topology->addNewPhysicalNodeAsChild(sinkNode, midNode1);
    topology->addNewPhysicalNodeAsChild(sinkNode, srcNode1);
    topology->addNewPhysicalNodeAsChild(midNode1, midNode2);
    topology->addNewPhysicalNodeAsChild(midNode1, srcNode2);
    topology->addNewPhysicalNodeAsChild(midNode2, srcNode3);
    topology->addNewPhysicalNodeAsChild(midNode2, srcNode4);
    topology->addNewPhysicalNodeAsChild(midNode2, srcNode5);

    LinkPropertyPtr linkProperty1 = std::make_shared<LinkProperty>(LinkProperty(100, 128));
    LinkPropertyPtr linkProperty2 = std::make_shared<LinkProperty>(LinkProperty(50, 256));
    LinkPropertyPtr linkProperty3 = std::make_shared<LinkProperty>(LinkProperty(20, 512));
    sinkNode->addLinkProperty(midNode1, linkProperty1);
    midNode1->addLinkProperty(sinkNode, linkProperty1);
    sinkNode->addLinkProperty(srcNode1, linkProperty1);
    srcNode1->addLinkProperty(sinkNode, linkProperty1);

    midNode1->addLinkProperty(midNode2, linkProperty2);
    midNode2->addLinkProperty(midNode1, linkProperty2);
    midNode1->addLinkProperty(srcNode2, linkProperty2);
    srcNode2->addLinkProperty(midNode1, linkProperty2);

    midNode2->addLinkProperty(srcNode3, linkProperty3);
    srcNode3->addLinkProperty(midNode2, linkProperty3);
    midNode2->addLinkProperty(srcNode4, linkProperty3);
    srcNode4->addLinkProperty(midNode2, linkProperty3);
    midNode2->addLinkProperty(srcNode5, linkProperty3);
    srcNode5->addLinkProperty(midNode2, linkProperty3);

    // Prepare the source and schema
    std::string schema = "Schema::create()->addField(\"id\", BasicType::UINT32)"
                         "->addField(\"value\", BasicType::UINT64);";
    const std::string streamName1 = "car1";
    const std::string streamName2 = "car2";
    const std::string streamName3 = "car3";
    const std::string streamName4 = "car4";
    const std::string streamName5 = "car5";

    streamCatalog = std::make_shared<StreamCatalog>(queryParsingService);
    streamCatalog->addLogicalStream(streamName1, schema);
    streamCatalog->addLogicalStream(streamName2, schema);
    streamCatalog->addLogicalStream(streamName3, schema);
    streamCatalog->addLogicalStream(streamName4, schema);
    streamCatalog->addLogicalStream(streamName5, schema);

    SourceConfigPtr sourceConfig1 = SourceConfig::create();
    sourceConfig1->setSourceFrequency(0);
    sourceConfig1->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig1->setPhysicalStreamName("test3");
    sourceConfig1->setLogicalStreamName(streamName1);

    SourceConfigPtr sourceConfig2 = SourceConfig::create();
    sourceConfig2->setSourceFrequency(0);
    sourceConfig2->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig2->setPhysicalStreamName("test3");
    sourceConfig2->setLogicalStreamName(streamName2);

    SourceConfigPtr sourceConfig3 = SourceConfig::create();
    sourceConfig3->setSourceFrequency(0);
    sourceConfig3->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig3->setPhysicalStreamName("test3");
    sourceConfig3->setLogicalStreamName(streamName3);

    SourceConfigPtr sourceConfig4 = SourceConfig::create();
    sourceConfig4->setSourceFrequency(0);
    sourceConfig4->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig4->setPhysicalStreamName("test3");
    sourceConfig4->setLogicalStreamName(streamName4);

    SourceConfigPtr sourceConfig5 = SourceConfig::create();
    sourceConfig5->setSourceFrequency(0);
    sourceConfig5->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig5->setPhysicalStreamName("test3");
    sourceConfig5->setLogicalStreamName(streamName5);

    PhysicalStreamConfigPtr conf1 = PhysicalStreamConfig::create(sourceConfig1);
    StreamCatalogEntryPtr streamCatalogEntry1 = std::make_shared<StreamCatalogEntry>(conf1, srcNode1);
    PhysicalStreamConfigPtr conf2 = PhysicalStreamConfig::create(sourceConfig2);
    StreamCatalogEntryPtr streamCatalogEntry2 = std::make_shared<StreamCatalogEntry>(conf2, srcNode2);
    PhysicalStreamConfigPtr conf3 = PhysicalStreamConfig::create(sourceConfig3);
    StreamCatalogEntryPtr streamCatalogEntry3 = std::make_shared<StreamCatalogEntry>(conf3, srcNode3);
    PhysicalStreamConfigPtr conf4 = PhysicalStreamConfig::create(sourceConfig4);
    StreamCatalogEntryPtr streamCatalogEntry4 = std::make_shared<StreamCatalogEntry>(conf4, srcNode4);
    PhysicalStreamConfigPtr conf5 = PhysicalStreamConfig::create(sourceConfig5);
    StreamCatalogEntryPtr streamCatalogEntry5 = std::make_shared<StreamCatalogEntry>(conf5, srcNode5);

    streamCatalog->addPhysicalStream(streamName1, streamCatalogEntry1);
    streamCatalog->addPhysicalStream(streamName2, streamCatalogEntry2);
    streamCatalog->addPhysicalStream(streamName3, streamCatalogEntry3);
    streamCatalog->addPhysicalStream(streamName4, streamCatalogEntry4);
    streamCatalog->addPhysicalStream(streamName5, streamCatalogEntry5);

    // Prepare the query
    Query subQuery4 = Query::from("car4").filter(Attribute("id") < 45);
    Query subQuery3 = Query::from("car3").filter(Attribute("id") < 45);
    Query subQuery2 = Query::from("car2").filter(Attribute("id") < 45);
    Query subQuery1 = Query::from("car1").filter(Attribute("id") < 45);

    Query query = Query::from("car5")
        .filter(Attribute("id") < 45)
        .unionWith(&subQuery4);
    query.unionWith(&subQuery3);
    query.unionWith(&subQuery2);
    query.unionWith(&subQuery1);
    query.sink(PrintSinkDescriptor::create());

    QueryPlanPtr queryPlan = query.getQueryPlan();
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    queryPlan->setQueryId(queryId);


    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);
    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto placementStrategy = Optimizer::PlacementStrategyFactory::getStrategy("GeneticAlgorithm",
                                                                              globalExecutionPlan,
                                                                              topology,
                                                                              typeInferencePhase,
                                                                              streamCatalog);

    // Execute optimization phases prior to placement
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
    queryPlan = queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    outfile << "The topology is:\n"<<topology->toString();
    outfile << "Query Plan:"<<queryPlan->toString()<<"\n";
    outfile.close();

    auto topologySpecificQueryRewrite = Optimizer::TopologySpecificQueryRewritePhase::create(streamCatalog);
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    ASSERT_TRUE(placementStrategy->updateGlobalExecutionPlan(queryPlan));
}

TEST_F(GeneticAlgorithmStrategyTest, testPlacingQueryWithGeneticAlgorithmStrategy4) {
    std::ofstream outfile;
    outfile.open ("benchmark.txt");


    TopologyPtr topology = Topology::create();
    // create the topology for the test
    uint32_t grpcPort = 4000;
    uint32_t dataPort = 5000;

    TopologyNodePtr rootNode = TopologyNode::create(1, "localhost", grpcPort, dataPort, 14);
    topology->setAsRoot(rootNode);

    TopologyNodePtr fogNode4 = TopologyNode::create(2, "localhost", grpcPort, dataPort, 10);
    topology->addNewPhysicalNodeAsChild(rootNode, fogNode4);

    TopologyNodePtr fogNode3 = TopologyNode::create(3, "localhost", grpcPort, dataPort, 8);
    topology->addNewPhysicalNodeAsChild(fogNode4, fogNode3);

    TopologyNodePtr fogNode1 = TopologyNode::create(4, "localhost", grpcPort, dataPort, 5);
    topology->addNewPhysicalNodeAsChild(fogNode3, fogNode1);

    TopologyNodePtr fogNode2 = TopologyNode::create(5, "localhost", grpcPort, dataPort, 5);
    topology->addNewPhysicalNodeAsChild(fogNode3, fogNode2);

    TopologyNodePtr sourceNode1 = TopologyNode::create(6, "localhost", grpcPort, dataPort, 3);
    topology->addNewPhysicalNodeAsChild(fogNode1, sourceNode1);

    TopologyNodePtr sourceNode2 = TopologyNode::create(7, "localhost", grpcPort, dataPort, 3);
    topology->addNewPhysicalNodeAsChild(fogNode1, sourceNode2);

    TopologyNodePtr sourceNode3 = TopologyNode::create(8, "localhost", grpcPort, dataPort, 3);
    topology->addNewPhysicalNodeAsChild(fogNode2, sourceNode3);

    LinkPropertyPtr linkProperty3 = std::make_shared<LinkProperty>(LinkProperty(100, 64));
    fogNode4->addLinkProperty(rootNode, linkProperty3);
    rootNode->addLinkProperty(fogNode4, linkProperty3);
    fogNode3->addLinkProperty(fogNode4, linkProperty3);
    fogNode4->addLinkProperty(fogNode3, linkProperty3);

    LinkPropertyPtr linkProperty2 = std::make_shared<LinkProperty>(LinkProperty(50, 256));
    fogNode2->addLinkProperty(fogNode3, linkProperty2);
    fogNode3->addLinkProperty(fogNode2, linkProperty2);
    fogNode1->addLinkProperty(fogNode3, linkProperty2);
    fogNode3->addLinkProperty(fogNode1, linkProperty2);

    LinkPropertyPtr linkProperty1 = std::make_shared<LinkProperty>(LinkProperty(10, 512));
    sourceNode3->addLinkProperty(fogNode2, linkProperty1);
    fogNode2->addLinkProperty(sourceNode3, linkProperty1);
    sourceNode2->addLinkProperty(fogNode1, linkProperty1);
    fogNode1->addLinkProperty(sourceNode2, linkProperty1);
    sourceNode1->addLinkProperty(fogNode1, linkProperty1);
    fogNode1->addLinkProperty(sourceNode1, linkProperty1);

    std::string schema = "Schema::create()->addField(\"id\", BasicType::UINT32)"
                         "->addField(\"value\", BasicType::UINT64);";
    const std::string streamName1 = "car";
    const std::string streamName2 = "truck";
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>(queryParsingService);
    streamCatalog->addLogicalStream(streamName1, schema);
    streamCatalog->addLogicalStream(streamName2, schema);

    SourceConfigPtr sourceConfig1 = SourceConfig::create();
    sourceConfig1->setSourceFrequency(0);
    sourceConfig1->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig1->setPhysicalStreamName("test4");
    sourceConfig1->setLogicalStreamName(streamName1);

    SourceConfigPtr sourceConfig2 = SourceConfig::create();
    sourceConfig2->setSourceFrequency(0);
    sourceConfig2->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig2->setPhysicalStreamName("test4");
    sourceConfig2->setLogicalStreamName(streamName2);

    PhysicalStreamConfigPtr conf1 = PhysicalStreamConfig::create(sourceConfig1);
    PhysicalStreamConfigPtr conf2 = PhysicalStreamConfig::create(sourceConfig2);

    StreamCatalogEntryPtr streamCatalogEntry1 = std::make_shared<StreamCatalogEntry>(conf1, sourceNode1);
    StreamCatalogEntryPtr streamCatalogEntry2 = std::make_shared<StreamCatalogEntry>(conf1, sourceNode2);
    StreamCatalogEntryPtr streamCatalogEntry3 = std::make_shared<StreamCatalogEntry>(conf2, sourceNode3);

    streamCatalog->addPhysicalStream(streamName1, streamCatalogEntry1);
    streamCatalog->addPhysicalStream(streamName1, streamCatalogEntry2);
    streamCatalog->addPhysicalStream(streamName2, streamCatalogEntry3);

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);
    auto placementStrategy = Optimizer::PlacementStrategyFactory::getStrategy("GeneticAlgorithm",
                                                                              globalExecutionPlan,
                                                                              topology,
                                                                              typeInferencePhase,
                                                                              streamCatalog);

    Query subQuery1 = Query::from("truck").filter(Attribute("id") < 45).map(Attribute("newId2") = Attribute("id") * 2);

    Query query = Query::from("car")
        .filter(Attribute("id") < 45).map(Attribute("newId2") = Attribute("id") * 2)
        .unionWith(&subQuery1).sink(PrintSinkDescriptor::create());

    QueryPlanPtr queryPlan = query.getQueryPlan();
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    queryPlan->setQueryId(queryId);

    std::vector<std::map<std::string, std::any>> properties;
    // adding property of the source2
    std::map<std::string, std::any> srcProp2;
    srcProp2.insert(std::make_pair("load", 1));
    srcProp2.insert(std::make_pair("dmf", 1.0));

    // adding property of the filter2
    std::map<std::string, std::any> filterProp2;
    filterProp2.insert(std::make_pair("load", 2));
    filterProp2.insert(std::make_pair("dmf", 0.4));

    // adding property of the map2
    std::map<std::string, std::any> mapProp2;
    mapProp2.insert(std::make_pair("load", 4));
    mapProp2.insert(std::make_pair("dmf", 4.0));

    // adding property of the source1
    std::map<std::string, std::any> srcProp1;
    srcProp1.insert(std::make_pair("load", 1));
    srcProp1.insert(std::make_pair("dmf", 1.0));

    // adding property of the filter1
    std::map<std::string, std::any> filterProp1;
    filterProp1.insert(std::make_pair("load", 3));
    filterProp1.insert(std::make_pair("dmf", 0.6));

    // adding property of the map1
    std::map<std::string, std::any> mapProp1;
    mapProp1.insert(std::make_pair("load", 3));
    mapProp1.insert(std::make_pair("dmf", 3.0));

    // adding property of the union
    std::map<std::string, std::any> unionProp;
    unionProp.insert(std::make_pair("load", 5));
    unionProp.insert(std::make_pair("dmf", 5.0));

    // adding property of the sink
    std::map<std::string, std::any> sinkProp;
    sinkProp.insert(std::make_pair("load", 6));
    sinkProp.insert(std::make_pair("dmf", 6.0));

    properties.push_back(sinkProp);
    properties.push_back(unionProp);
    properties.push_back(mapProp1);
    properties.push_back(filterProp1);
    properties.push_back(srcProp1);
    properties.push_back(mapProp2);
    properties.push_back(filterProp2);
    properties.push_back(srcProp2);
    properties.push_back(mapProp1);
    properties.push_back(filterProp1);
    properties.push_back(srcProp1);


    // Execute optimization phases prior to placement
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
    queryPlan = queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto topologySpecificQueryRewrite = Optimizer::TopologySpecificQueryRewritePhase::create(streamCatalog);
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    outfile << "The topology is:\n"<<topology->toString();
    outfile << "Query Plan:"<<queryPlan->toString()<<"\n";
    outfile.close();

    UtilityFunctions::assignPropertiesToQueryOperators(queryPlan, properties);
    auto start = std::chrono::high_resolution_clock::now();
    ASSERT_TRUE(placementStrategy->updateGlobalExecutionPlan(queryPlan));
    auto stop = std::chrono::high_resolution_clock::now();
    long count = std::chrono::duration_cast<std::chrono::milliseconds>(stop - start).count();
    NES_DEBUG("Found Solution in " << count << "ms");
}

TEST_F(GeneticAlgorithmStrategyTest, testPlacingQueryWithGeneticAlgorithmStrategy55) {

    std::ofstream outfile;
    outfile.open ("benchmark.txt");


    TopologyPtr topology = Topology::create();
    // create the topology for the test
    uint32_t grpcPort = 4000;
    uint32_t dataPort = 5000;

    TopologyNodePtr rootNode = TopologyNode::create(1, "localhost", grpcPort, dataPort, 14);
    topology->setAsRoot(rootNode);

    TopologyNodePtr sourceNode1 = TopologyNode::create(2, "localhost", grpcPort, dataPort, 3);
    topology->addNewPhysicalNodeAsChild(rootNode, sourceNode1);

    TopologyNodePtr sourceNode2 = TopologyNode::create(3, "localhost", grpcPort, dataPort, 3);
    topology->addNewPhysicalNodeAsChild(rootNode, sourceNode2);

    TopologyNodePtr sourceNode3 = TopologyNode::create(4, "localhost", grpcPort, dataPort, 3);
    topology->addNewPhysicalNodeAsChild(rootNode, sourceNode3);

    LinkPropertyPtr linkProperty1 = std::make_shared<LinkProperty>(LinkProperty(10, 512));
    rootNode->addLinkProperty(sourceNode1, linkProperty1);
    sourceNode1->addLinkProperty(rootNode, linkProperty1);
    rootNode->addLinkProperty(sourceNode2, linkProperty1);
    sourceNode2->addLinkProperty(rootNode, linkProperty1);
    rootNode->addLinkProperty(sourceNode3, linkProperty1);
    sourceNode3->addLinkProperty(rootNode, linkProperty1);

    std::string schema = "Schema::create()->addField(\"id\", BasicType::UINT32)"
                         "->addField(\"value\", BasicType::UINT64);";
    const std::string streamName = "car";

    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>(queryParsingService);
    streamCatalog->addLogicalStream(streamName, schema);

    SourceConfigPtr sourceConfig = SourceConfig::create();
    sourceConfig->setSourceFrequency(0);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig->setPhysicalStreamName("test5");
    sourceConfig->setLogicalStreamName(streamName);

    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(sourceConfig);

    StreamCatalogEntryPtr streamCatalogEntry1 = std::make_shared<StreamCatalogEntry>(conf, sourceNode1);
    StreamCatalogEntryPtr streamCatalogEntry2 = std::make_shared<StreamCatalogEntry>(conf, sourceNode2);
    StreamCatalogEntryPtr streamCatalogEntry3 = std::make_shared<StreamCatalogEntry>(conf, sourceNode3);

    streamCatalog->addPhysicalStream(streamName, streamCatalogEntry1);
    streamCatalog->addPhysicalStream(streamName, streamCatalogEntry2);
    streamCatalog->addPhysicalStream(streamName, streamCatalogEntry3);

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);
    auto placementStrategy = Optimizer::PlacementStrategyFactory::getStrategy("GeneticAlgorithm",
                                                                              globalExecutionPlan,
                                                                              topology,
                                                                              typeInferencePhase,
                                                                              streamCatalog);

    Query query = Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());

    QueryPlanPtr queryPlan = query.getQueryPlan();
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    queryPlan->setQueryId(queryId);

    std::vector<std::map<std::string, std::any>> properties;

    // adding property of the source
    std::map<std::string, std::any> srcProp;
    srcProp.insert(std::make_pair("load", 1));
    srcProp.insert(std::make_pair("dmf", 1.0));

    // adding property of the filter
    std::map<std::string, std::any> filterProp;
    filterProp.insert(std::make_pair("load", 1));
    filterProp.insert(std::make_pair("dmf", 0.3));

    // adding property of the sink
    std::map<std::string, std::any> sinkProp;
    sinkProp.insert(std::make_pair("load", 5));
    sinkProp.insert(std::make_pair("dmf", 4.0));

    properties.push_back(sinkProp);
    properties.push_back(filterProp);
    properties.push_back(srcProp);
    properties.push_back(filterProp);
    properties.push_back(srcProp);
    properties.push_back(filterProp);
    properties.push_back(srcProp);

    // Execute optimization phases prior to placement
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
    queryPlan = queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto topologySpecificQueryRewrite = Optimizer::TopologySpecificQueryRewritePhase::create(streamCatalog);
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    outfile << "The topology is:\n"<<topology->toString();
    outfile << "Query Plan:"<<queryPlan->toString()<<"\n";
    outfile.close();

    UtilityFunctions::assignPropertiesToQueryOperators(queryPlan, properties);
    auto start = std::chrono::high_resolution_clock::now();
    ASSERT_TRUE(placementStrategy->updateGlobalExecutionPlan(queryPlan));
    auto stop = std::chrono::high_resolution_clock::now();
    long count = std::chrono::duration_cast<std::chrono::milliseconds>(stop - start).count();
    NES_DEBUG("Found Solution in " << count << "ms");
}
class GeneticAlgorithmStrategyEvaluationTest : public testing::Test{
  public:
    std::shared_ptr<QueryParsingService> queryParsingService;
    // Will be called before any test in this class are executed.
    static void SetUpTestCase() { std::cout << "Setup QueryPlacementTest test class." << std::endl; }

    // Will be called before a test is executed.
    void SetUp() override {
        NES::setupLogging("QueryPlacementTest.log", NES::LOG_DEBUG);
        std::cout << "Setup QueryPlacementTest test case." << std::endl;
        auto cppCompiler = Compiler::CPPCompiler::create();
        auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        queryParsingService = QueryParsingService::create(jitCompiler);
    }

    // Will be called before a test is executed.
    void TearDown() { std::cout << "Setup QueryPlacementTest test case." << std::endl; }

    // Will be called after all tests in this class are finished.
    static void TearDownTestCase() { std::cout << "Tear down QueryPlacementTest test class." << std::endl; }

    void setupTopologyAndStreamCatalogForGA() {

        topology = Topology::create();
    // create the topology for the test
    uint32_t grpcPort = 4000;
    uint32_t dataPort = 5000;

    TopologyNodePtr rootNode = TopologyNode::create(1, "localhost", grpcPort, dataPort, 14);
    topology->setAsRoot(rootNode);

    TopologyNodePtr fogTopNode1 = TopologyNode::create(2, "localhost", grpcPort, dataPort, 10);
    topology->addNewPhysicalNodeAsChild(rootNode, fogTopNode1);

    TopologyNodePtr fogTopNode2 = TopologyNode::create(3, "localhost", grpcPort, dataPort, 8);
    topology->addNewPhysicalNodeAsChild(rootNode, fogTopNode2);

    TopologyNodePtr fogBottomNode1 = TopologyNode::create(4, "localhost", grpcPort, dataPort, 5);
    topology->addNewPhysicalNodeAsChild(fogTopNode1, fogBottomNode1);

    TopologyNodePtr fogBottomNode2 = TopologyNode::create(5, "localhost", grpcPort, dataPort, 5);
    topology->addNewPhysicalNodeAsChild(fogTopNode1, fogBottomNode2);

    TopologyNodePtr fogBottomNode3 = TopologyNode::create(6, "localhost", grpcPort, dataPort, 5);
    topology->addNewPhysicalNodeAsChild(fogTopNode2, fogBottomNode3);

    TopologyNodePtr fogBottomNode4 = TopologyNode::create(7, "localhost", grpcPort, dataPort, 5);
    topology->addNewPhysicalNodeAsChild(fogTopNode2, fogBottomNode4);

    TopologyNodePtr sourceNode1 = TopologyNode::create(8, "localhost", grpcPort, dataPort, 3);
    topology->addNewPhysicalNodeAsChild(fogBottomNode1, sourceNode1);

    TopologyNodePtr sourceNode2 = TopologyNode::create(9, "localhost", grpcPort, dataPort, 3);
    topology->addNewPhysicalNodeAsChild(fogBottomNode1, sourceNode2);

    TopologyNodePtr sourceNode3 = TopologyNode::create(10, "localhost", grpcPort, dataPort, 3);
    topology->addNewPhysicalNodeAsChild(fogBottomNode2, sourceNode3);

    TopologyNodePtr sourceNode4 = TopologyNode::create(11, "localhost", grpcPort, dataPort, 3);
    topology->addNewPhysicalNodeAsChild(fogBottomNode2, sourceNode4);

    TopologyNodePtr sourceNode5 = TopologyNode::create(12, "localhost", grpcPort, dataPort, 3);
    topology->addNewPhysicalNodeAsChild(fogBottomNode3, sourceNode5);

    TopologyNodePtr sourceNode6 = TopologyNode::create(13, "localhost", grpcPort, dataPort, 3);
    topology->addNewPhysicalNodeAsChild(fogBottomNode3, sourceNode6);

    TopologyNodePtr sourceNode7 = TopologyNode::create(14, "localhost", grpcPort, dataPort, 3);
    topology->addNewPhysicalNodeAsChild(fogBottomNode4, sourceNode7);

    TopologyNodePtr sourceNode8 = TopologyNode::create(15, "localhost", grpcPort, dataPort, 3);
    topology->addNewPhysicalNodeAsChild(fogBottomNode4, sourceNode8);

    LinkPropertyPtr linkProperty1 = std::make_shared<LinkProperty>(LinkProperty(100, 64));
    rootNode->addLinkProperty(fogTopNode1, linkProperty1);
    fogTopNode1->addLinkProperty(rootNode, linkProperty1);
    rootNode->addLinkProperty(fogTopNode2, linkProperty1);
    fogTopNode2->addLinkProperty(rootNode, linkProperty1);

    LinkPropertyPtr linkProperty2 = std::make_shared<LinkProperty>(LinkProperty(50, 256));
    fogTopNode1->addLinkProperty(fogBottomNode1, linkProperty2);
    fogBottomNode1->addLinkProperty(fogTopNode1, linkProperty2);
    fogTopNode1->addLinkProperty(fogBottomNode2, linkProperty2);
    fogBottomNode2->addLinkProperty(fogTopNode1, linkProperty2);
    fogTopNode2->addLinkProperty(fogBottomNode3, linkProperty2);
    fogBottomNode3->addLinkProperty(fogTopNode2, linkProperty2);
    fogTopNode2->addLinkProperty(fogBottomNode4, linkProperty2);
    fogBottomNode4->addLinkProperty(fogTopNode2, linkProperty2);

    LinkPropertyPtr linkProperty3 = std::make_shared<LinkProperty>(LinkProperty(10, 512));
    fogBottomNode1->addLinkProperty(sourceNode1, linkProperty3);
    sourceNode1->addLinkProperty(fogBottomNode1, linkProperty3);
    fogBottomNode1->addLinkProperty(sourceNode2, linkProperty3);
    sourceNode2->addLinkProperty(fogBottomNode1, linkProperty3);
    fogBottomNode2->addLinkProperty(sourceNode3, linkProperty3);
    sourceNode3->addLinkProperty(fogBottomNode2, linkProperty3);
    fogBottomNode2->addLinkProperty(sourceNode4, linkProperty3);
    sourceNode4->addLinkProperty(fogBottomNode2, linkProperty3);
    fogBottomNode3->addLinkProperty(sourceNode5, linkProperty3);
    sourceNode5->addLinkProperty(fogBottomNode3, linkProperty3);
    fogBottomNode3->addLinkProperty(sourceNode6, linkProperty3);
    sourceNode6->addLinkProperty(fogBottomNode3, linkProperty3);
    fogBottomNode4->addLinkProperty(sourceNode7, linkProperty3);
    sourceNode7->addLinkProperty(fogBottomNode4, linkProperty3);
    fogBottomNode4->addLinkProperty(sourceNode8, linkProperty3);
    sourceNode8->addLinkProperty(fogBottomNode4, linkProperty3);

    std::string schema = "Schema::create()->addField(\"id\", BasicType::UINT32)"
                         "->addField(\"timestamp\", BasicType::UINT64)"
                         "->addField(\"value\", BasicType::UINT64);";
    const std::string streamName1 = "car";
    const std::string streamName2 = "truck";
    /*
    const std::string streamName3 = "car3";
    const std::string streamName4 = "car4";
    const std::string streamName5 = "car5";
    const std::string streamName6 = "car6";
    const std::string streamName7 = "car7";
    const std::string streamName8 = "car8"; */
    streamCatalog = std::make_shared<StreamCatalog>(queryParsingService);
    streamCatalog->addLogicalStream(streamName1, schema);
    streamCatalog->addLogicalStream(streamName2, schema);

    SourceConfigPtr sourceConfig1 = SourceConfig::create();
    sourceConfig1->setSourceFrequency(0);
    sourceConfig1->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig1->setPhysicalStreamName("evaluationTest2");
    sourceConfig1->setLogicalStreamName(streamName1);

    SourceConfigPtr sourceConfig2 = SourceConfig::create();
    sourceConfig2->setSourceFrequency(0);
    sourceConfig2->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig2->setPhysicalStreamName("evaluationTest2");
    sourceConfig2->setLogicalStreamName(streamName2);

    PhysicalStreamConfigPtr conf1 = PhysicalStreamConfig::create(sourceConfig1);
    PhysicalStreamConfigPtr conf2 = PhysicalStreamConfig::create(sourceConfig2);

    StreamCatalogEntryPtr streamCatalogEntry1 = std::make_shared<StreamCatalogEntry>(conf1, sourceNode1);
    StreamCatalogEntryPtr streamCatalogEntry2 = std::make_shared<StreamCatalogEntry>(conf1, sourceNode2);
    StreamCatalogEntryPtr streamCatalogEntry3 = std::make_shared<StreamCatalogEntry>(conf1, sourceNode3);
    StreamCatalogEntryPtr streamCatalogEntry4 = std::make_shared<StreamCatalogEntry>(conf1, sourceNode4);
    StreamCatalogEntryPtr streamCatalogEntry5 = std::make_shared<StreamCatalogEntry>(conf2, sourceNode5);
    StreamCatalogEntryPtr streamCatalogEntry6 = std::make_shared<StreamCatalogEntry>(conf2, sourceNode6);
    StreamCatalogEntryPtr streamCatalogEntry7 = std::make_shared<StreamCatalogEntry>(conf2, sourceNode7);
    StreamCatalogEntryPtr streamCatalogEntry8 = std::make_shared<StreamCatalogEntry>(conf2, sourceNode8);

    streamCatalog->addPhysicalStream(streamName1, streamCatalogEntry1);
    streamCatalog->addPhysicalStream(streamName1, streamCatalogEntry2);
    streamCatalog->addPhysicalStream(streamName1, streamCatalogEntry3);
    streamCatalog->addPhysicalStream(streamName1, streamCatalogEntry4);
    streamCatalog->addPhysicalStream(streamName2, streamCatalogEntry5);
    streamCatalog->addPhysicalStream(streamName2, streamCatalogEntry6);
    streamCatalog->addPhysicalStream(streamName2, streamCatalogEntry7);
    streamCatalog->addPhysicalStream(streamName2, streamCatalogEntry8);
    }

    StreamCatalogPtr streamCatalog;
    TopologyPtr topology;
};
TEST_F(GeneticAlgorithmStrategyEvaluationTest, evaluationTest1) {
    std::ofstream outfile;
    outfile.open ("benchmark.txt");


    setupTopologyAndStreamCatalogForGA();
    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);
    auto placementStrategy = Optimizer::PlacementStrategyFactory::getStrategy("GeneticAlgorithm",
                                                                              globalExecutionPlan,
                                                                              topology,
                                                                              typeInferencePhase,
                                                                              streamCatalog);

   /* Query subQuery1 = Query::from("car2").filter(Attribute("id") < 45).map(Attribute("newId2") = Attribute("id") * 2);

    Query query = Query::from("car1")
                      .filter(Attribute("id") < 45).map(Attribute("newId2") = Attribute("id") * 2)
                      .unionWith(&subQuery1) .window(TumblingWindow::of(TimeCharacteristic::createIngestionTime(), Seconds(10)))
                      .byKey(Attribute("id"))
                      .apply(Sum(Attribute("value"))).sink(PrintSinkDescriptor::create());  */

    Query query = Query::from("car").filter(Attribute("id") < 45).filter(Attribute("id") < 40)
        .joinWith(Query::from("truck").filter(Attribute("id") < 45).filter(Attribute("id") < 40))
                      .where(Attribute("id")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
        .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(2))).byKey(Attribute("id")).apply(Sum(Attribute("value")))
    .sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan = query.getQueryPlan();
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    queryPlan->setQueryId(queryId);
    NES_DEBUG(queryPlan->toString());

    std::vector<std::map<std::string, std::any>> properties;
    // adding property of the source2
    std::map<std::string, std::any> srcProp2;
    srcProp2.insert(std::make_pair("load", 1));
    srcProp2.insert(std::make_pair("dmf", 1.0));

    // adding property of the filter4
    std::map<std::string, std::any> filterProp4;
    filterProp4.insert(std::make_pair("load", 2));
    filterProp4.insert(std::make_pair("dmf", 0.4));

    // adding property of the filter3
    std::map<std::string, std::any> filterProp3;
    filterProp3.insert(std::make_pair("load", 4));
    filterProp3.insert(std::make_pair("dmf", 0.3));

    // adding property of the waterMarkAssigner2
    std::map<std::string, std::any> waterMarkAssignerProp2;
    waterMarkAssignerProp2.insert(std::make_pair("load", 4));
    waterMarkAssignerProp2.insert(std::make_pair("dmf", 3.0));

    // adding property of the source1
    std::map<std::string, std::any> srcProp1;
    srcProp1.insert(std::make_pair("load", 1));
    srcProp1.insert(std::make_pair("dmf", 1.0));

    // adding property of the filter2
    std::map<std::string, std::any> filterProp2;
    filterProp2.insert(std::make_pair("load", 2));
    filterProp2.insert(std::make_pair("dmf", 0.3));

    // adding property of the filter1
    std::map<std::string, std::any> filterProp1;
    filterProp1.insert(std::make_pair("load", 4));
    filterProp1.insert(std::make_pair("dmf", 3.0));

    // adding property of the waterMarkAssigner1
    std::map<std::string, std::any> waterMarkAssignerProp1;
    waterMarkAssignerProp1.insert(std::make_pair("load", 4));
    waterMarkAssignerProp1.insert(std::make_pair("dmf", 3.0));

    // adding property of the join
    std::map<std::string, std::any> joinProp;
    joinProp.insert(std::make_pair("load", 3));
    joinProp.insert(std::make_pair("dmf", 4.0));

    // adding property of waterMarkAssigner
    std::map<std::string, std::any> waterMarkAssignerProp;
    waterMarkAssignerProp.insert(std::make_pair("load", 3));
    waterMarkAssignerProp.insert(std::make_pair("dmf", 4.0));

    // adding property of the window
    std::map<std::string, std::any> windowProp;
    windowProp.insert(std::make_pair("load", 3));
    windowProp.insert(std::make_pair("dmf", 4.0));

    // adding property of the sink
    std::map<std::string, std::any> sinkProp;
    sinkProp.insert(std::make_pair("load", 5));
    sinkProp.insert(std::make_pair("dmf", 4.0));

    properties.push_back(sinkProp);
    properties.push_back(windowProp);
    properties.push_back(waterMarkAssignerProp);
    properties.push_back(joinProp);
    properties.push_back(waterMarkAssignerProp1);
    properties.push_back(filterProp1);
    properties.push_back(filterProp2);
    properties.push_back(srcProp1);
    properties.push_back(waterMarkAssignerProp2);
    properties.push_back(filterProp3);
    properties.push_back(filterProp4);
    properties.push_back(srcProp2);
    properties.push_back(waterMarkAssignerProp2);
    properties.push_back(filterProp3);
    properties.push_back(filterProp4);
    properties.push_back(srcProp2);
    properties.push_back(waterMarkAssignerProp2);
    properties.push_back(filterProp3);
    properties.push_back(filterProp4);
    properties.push_back(srcProp2);
    properties.push_back(waterMarkAssignerProp2);
    properties.push_back(filterProp3);
    properties.push_back(filterProp4);
    properties.push_back(srcProp2);
    properties.push_back(waterMarkAssignerProp1);
    properties.push_back(filterProp1);
    properties.push_back(filterProp2);
    properties.push_back(srcProp1);
    properties.push_back(waterMarkAssignerProp1);
    properties.push_back(filterProp1);
    properties.push_back(filterProp2);
    properties.push_back(srcProp1);
    properties.push_back(waterMarkAssignerProp1);
    properties.push_back(filterProp1);
    properties.push_back(filterProp2);
    properties.push_back(srcProp1);

    // Execute optimization phases prior to placement
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
    queryPlan = queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto topologySpecificQueryRewrite = Optimizer::TopologySpecificQueryRewritePhase::create(streamCatalog);
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);
    outfile << "The topology is:\n"<<topology->toString();
    outfile << "Query Plan:"<<queryPlan->toString()<<"\n";
    outfile.close();

    UtilityFunctions::assignPropertiesToQueryOperators(queryPlan, properties);
    auto start = std::chrono::high_resolution_clock::now();
    ASSERT_TRUE(placementStrategy->updateGlobalExecutionPlan(queryPlan));
    auto stop = std::chrono::high_resolution_clock::now();
    long count = std::chrono::duration_cast<std::chrono::milliseconds>(stop - start).count();
    NES_DEBUG("Found Solution in " << count << "ms");
}

TEST_F(GeneticAlgorithmStrategyEvaluationTest, evaluationTest2) {

    std::ofstream outfile;
    outfile.open ("benchmark.txt");
    // create the topology for the test
    TopologyPtr topology = Topology::create();
    uint32_t grpcPort = 4000;
    uint32_t dataPort = 5000;
    TopologyNodePtr rootNode = TopologyNode::create(1, "localhost", grpcPort, dataPort, 50);
    topology->setAsRoot(rootNode);

    TopologyNodePtr fogTopNode1 = TopologyNode::create(2, "localhost", grpcPort, dataPort, 10);
    topology->addNewPhysicalNodeAsChild(rootNode, fogTopNode1);

    TopologyNodePtr fogTopNode2 = TopologyNode::create(3, "localhost", grpcPort, dataPort, 10);
    topology->addNewPhysicalNodeAsChild(rootNode, fogTopNode2);

    TopologyNodePtr fogBottomNode1 = TopologyNode::create(4, "localhost", grpcPort, dataPort, 5);
    topology->addNewPhysicalNodeAsChild(fogTopNode1, fogBottomNode1);

    TopologyNodePtr fogBottomNode2 = TopologyNode::create(5, "localhost", grpcPort, dataPort, 5);
    topology->addNewPhysicalNodeAsChild(fogTopNode1, fogBottomNode2);

    TopologyNodePtr fogBottomNode3 = TopologyNode::create(6, "localhost", grpcPort, dataPort, 5);
    topology->addNewPhysicalNodeAsChild(fogTopNode2, fogBottomNode3);

    TopologyNodePtr fogBottomNode4 = TopologyNode::create(7, "localhost", grpcPort, dataPort, 5);
    topology->addNewPhysicalNodeAsChild(fogTopNode2, fogBottomNode4);

    TopologyNodePtr sourceNode1 = TopologyNode::create(8, "localhost", grpcPort, dataPort, 3);
    topology->addNewPhysicalNodeAsChild(fogBottomNode1, sourceNode1);

    TopologyNodePtr sourceNode2 = TopologyNode::create(9, "localhost", grpcPort, dataPort, 3);
    topology->addNewPhysicalNodeAsChild(fogBottomNode1, sourceNode2);

    TopologyNodePtr sourceNode3 = TopologyNode::create(10, "localhost", grpcPort, dataPort, 3);
    topology->addNewPhysicalNodeAsChild(fogBottomNode2, sourceNode3);

    TopologyNodePtr sourceNode4 = TopologyNode::create(11, "localhost", grpcPort, dataPort, 3);
    topology->addNewPhysicalNodeAsChild(fogBottomNode2, sourceNode4);

    TopologyNodePtr sourceNode5 = TopologyNode::create(12, "localhost", grpcPort, dataPort, 3);
    topology->addNewPhysicalNodeAsChild(fogBottomNode3, sourceNode5);

    TopologyNodePtr sourceNode6 = TopologyNode::create(13, "localhost", grpcPort, dataPort, 3);
    topology->addNewPhysicalNodeAsChild(fogBottomNode3, sourceNode6);

    TopologyNodePtr sourceNode7 = TopologyNode::create(14, "localhost", grpcPort, dataPort, 3);
    topology->addNewPhysicalNodeAsChild(fogBottomNode4, sourceNode7);

    TopologyNodePtr sourceNode8 = TopologyNode::create(15, "localhost", grpcPort, dataPort, 3);
    topology->addNewPhysicalNodeAsChild(fogBottomNode4, sourceNode8);

    LinkPropertyPtr linkProperty1 = std::make_shared<LinkProperty>(LinkProperty(100, 64));
    rootNode->addLinkProperty(fogTopNode1, linkProperty1);
    fogTopNode1->addLinkProperty(rootNode, linkProperty1);
    rootNode->addLinkProperty(fogTopNode2, linkProperty1);
    fogTopNode2->addLinkProperty(rootNode, linkProperty1);

    LinkPropertyPtr linkProperty2 = std::make_shared<LinkProperty>(LinkProperty(50, 256));
    fogTopNode1->addLinkProperty(fogBottomNode1, linkProperty2);
    fogBottomNode1->addLinkProperty(fogTopNode1, linkProperty2);
    fogTopNode1->addLinkProperty(fogBottomNode2, linkProperty2);
    fogBottomNode2->addLinkProperty(fogTopNode1, linkProperty2);
    fogTopNode2->addLinkProperty(fogBottomNode3, linkProperty2);
    fogBottomNode3->addLinkProperty(fogTopNode2, linkProperty2);
    fogTopNode2->addLinkProperty(fogBottomNode4, linkProperty2);
    fogBottomNode4->addLinkProperty(fogTopNode2, linkProperty2);

    LinkPropertyPtr linkProperty3 = std::make_shared<LinkProperty>(LinkProperty(10, 512));
    fogBottomNode1->addLinkProperty(sourceNode1, linkProperty3);
    sourceNode1->addLinkProperty(fogBottomNode1, linkProperty3);
    fogBottomNode1->addLinkProperty(sourceNode2, linkProperty3);
    sourceNode2->addLinkProperty(fogBottomNode1, linkProperty3);
    fogBottomNode2->addLinkProperty(sourceNode3, linkProperty3);
    sourceNode3->addLinkProperty(fogBottomNode2, linkProperty3);
    fogBottomNode2->addLinkProperty(sourceNode4, linkProperty3);
    sourceNode4->addLinkProperty(fogBottomNode2, linkProperty3);
    fogBottomNode3->addLinkProperty(sourceNode5, linkProperty3);
    sourceNode5->addLinkProperty(fogBottomNode3, linkProperty3);
    fogBottomNode3->addLinkProperty(sourceNode6, linkProperty3);
    sourceNode6->addLinkProperty(fogBottomNode3, linkProperty3);
    fogBottomNode4->addLinkProperty(sourceNode7, linkProperty3);
    sourceNode7->addLinkProperty(fogBottomNode4, linkProperty3);
    fogBottomNode4->addLinkProperty(sourceNode8, linkProperty3);
    sourceNode8->addLinkProperty(fogBottomNode4, linkProperty3);

    std::string schema = "Schema::create()->addField(\"id\", BasicType::UINT32)"
                         "->addField(\"timestamp\", BasicType::UINT64)"
                         "->addField(\"value\", BasicType::UINT64);";
    const std::string streamName1 = "car";

    streamCatalog = std::make_shared<StreamCatalog>(queryParsingService);
    streamCatalog->addLogicalStream(streamName1, schema);

    SourceConfigPtr sourceConfig1 = SourceConfig::create();
    sourceConfig1->setSourceFrequency(0);
    sourceConfig1->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig1->setPhysicalStreamName("evaluationTest2");
    sourceConfig1->setLogicalStreamName(streamName1);

    PhysicalStreamConfigPtr conf1 = PhysicalStreamConfig::create(sourceConfig1);
    StreamCatalogEntryPtr streamCatalogEntry1 = std::make_shared<StreamCatalogEntry>(conf1, sourceNode1);
    StreamCatalogEntryPtr streamCatalogEntry2 = std::make_shared<StreamCatalogEntry>(conf1, sourceNode2);
    StreamCatalogEntryPtr streamCatalogEntry3 = std::make_shared<StreamCatalogEntry>(conf1, sourceNode3);
    StreamCatalogEntryPtr streamCatalogEntry4 = std::make_shared<StreamCatalogEntry>(conf1, sourceNode4);
    StreamCatalogEntryPtr streamCatalogEntry5 = std::make_shared<StreamCatalogEntry>(conf1, sourceNode5);
    StreamCatalogEntryPtr streamCatalogEntry6 = std::make_shared<StreamCatalogEntry>(conf1, sourceNode6);
    StreamCatalogEntryPtr streamCatalogEntry7 = std::make_shared<StreamCatalogEntry>(conf1, sourceNode7);
    StreamCatalogEntryPtr streamCatalogEntry8 = std::make_shared<StreamCatalogEntry>(conf1, sourceNode8);

    streamCatalog->addPhysicalStream(streamName1, streamCatalogEntry1);
    streamCatalog->addPhysicalStream(streamName1, streamCatalogEntry2);
    streamCatalog->addPhysicalStream(streamName1, streamCatalogEntry3);
    streamCatalog->addPhysicalStream(streamName1, streamCatalogEntry4);
    streamCatalog->addPhysicalStream(streamName1, streamCatalogEntry5);
    streamCatalog->addPhysicalStream(streamName1, streamCatalogEntry6);
    streamCatalog->addPhysicalStream(streamName1, streamCatalogEntry7);
    streamCatalog->addPhysicalStream(streamName1, streamCatalogEntry8);

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);
    auto placementStrategy = Optimizer::PlacementStrategyFactory::getStrategy("GeneticAlgorithm",
                                                                              globalExecutionPlan,
                                                                              topology,
                                                                              typeInferencePhase,
                                                                              streamCatalog);

    std::vector<std::map<std::string, std::any>> properties;

    // adding property of the source
    std::map<std::string, std::any> srcProp;
    srcProp.insert(std::make_pair("load", 1));
    srcProp.insert(std::make_pair("dmf", 1.0));

    // adding property of the filter
    std::map<std::string, std::any> filterProp;
    filterProp.insert(std::make_pair("load", 2));
    filterProp.insert(std::make_pair("dmf", 0.6));

    // adding property of the map
    std::map<std::string, std::any> mapProp;
    mapProp.insert(std::make_pair("load", 3));
    mapProp.insert(std::make_pair("dmf", 1.2));

    // adding property of the sink
    std::map<std::string, std::any> sinkProp;
    sinkProp.insert(std::make_pair("load", 5));
    sinkProp.insert(std::make_pair("dmf", 4.0));

    Query query = Query::from("car");
    properties.push_back(sinkProp);

    int n = 2;
    for(int i = 0; i < n; i+=2){
        query.filter(Attribute("id") < n-i);
    }
    for(int i = 1; i < n; i+=2){
        query.map(Attribute("value2") = Attribute("value") * 2);
    }
    for(int i = 0; i < 8; i++){
        for(int j = 1; j < n; j+=2){
            properties.push_back(mapProp);
        }
        for(int j = 0; j < n; j+=2){
            properties.push_back(filterProp);
        }
        properties.push_back(srcProp);
    }
    query.sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan = query.getQueryPlan();
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    queryPlan->setQueryId(queryId);
    // Execute optimization phases prior to placement
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
    queryPlan = queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto topologySpecificQueryRewrite = Optimizer::TopologySpecificQueryRewritePhase::create(streamCatalog);
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    outfile << "The topology is:\n"<<topology->toString();
    outfile << "Query Plan:"<<queryPlan->toString()<<"\n";
    outfile.close();


    UtilityFunctions::assignPropertiesToQueryOperators(queryPlan, properties);
    NES_DEBUG(queryPlan->toString());
    auto testOperators =  QueryPlanIterator(queryPlan).snapshot();
    auto start = std::chrono::high_resolution_clock::now();
    ASSERT_TRUE(placementStrategy->updateGlobalExecutionPlan(queryPlan));
    auto stop = std::chrono::high_resolution_clock::now();
    long count = std::chrono::duration_cast<std::chrono::milliseconds>(stop - start).count();
    NES_DEBUG("Found Solution in " << count << "ms");
}

TEST_F(GeneticAlgorithmStrategyTest, testPlacingQueryWithGeneticAlgorithmStrategy5) {

    setupTopologyAndStreamCatalogForGA(10);

    Query query = Query::from("car");
    int m = 3;
    for(int i = 0; i < m; i++){
        query.filter(Attribute("id") < 45);
    }
    for(int i = 0; i < m; i++){
        query.map(Attribute("c") = Attribute("id") + Attribute("value"));
    }
    query.sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan = query.getQueryPlan();
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    queryPlan->setQueryId(queryId);

    std::vector<std::map<std::string, std::any>> properties;

    // adding property of the source
    std::map<std::string, std::any> srcProp;
    srcProp.insert(std::make_pair("load", 1));
    srcProp.insert(std::make_pair("dmf", 1.0));

    // adding property of the filter
    std::map<std::string, std::any> filterProp;
    filterProp.insert(std::make_pair("load", 2));
    filterProp.insert(std::make_pair("dmf", 0.3));

    // adding property of the map
    std::map<std::string, std::any> mapProp;
    mapProp.insert(std::make_pair("load", 3));
    mapProp.insert(std::make_pair("dmf", 3.0));

    // adding property of the sink
    std::map<std::string, std::any> sinkProp;
    sinkProp.insert(std::make_pair("load", 5));
    sinkProp.insert(std::make_pair("dmf", 4.0));

    properties.push_back(sinkProp);
    for(int i = 0; i < m; i++){
        properties.push_back(mapProp);
    }
    for(int i = 0; i < m; i++){
        properties.push_back(filterProp);
    }
    properties.push_back(srcProp);
    int repetitions = 10;
    std::vector<long> counts_n;
    for(int j = 0; j < repetitions; j++) {
        GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
        auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);
        auto placementStrategy = Optimizer::PlacementStrategyFactory::getStrategy("GeneticAlgorithm",
                                                                                  globalExecutionPlan,
                                                                                  topology,
                                                                                  typeInferencePhase,
                                                                                  streamCatalog);
        UtilityFunctions::assignPropertiesToQueryOperators(queryPlan, properties);

        typeInferencePhase->execute(queryPlan);
        auto start = std::chrono::high_resolution_clock::now();
        ASSERT_TRUE(placementStrategy->updateGlobalExecutionPlan(queryPlan));
        auto stop = std::chrono::high_resolution_clock::now();
        long count = std::chrono::duration_cast<std::chrono::milliseconds>(stop - start).count();
        counts_n.push_back(count);
        globalExecutionPlan->removeQuerySubPlans(queryId);
    }
    std::sort(counts_n.begin(), counts_n.end());
    long median = counts_n[counts_n.size() / 2];
    if (counts_n.size() % 2 == 0) {
        median = (median + counts_n[(counts_n.size() - 1) / 2]) / 2;
    }
    std::stringstream ss;
    for (auto& count : counts_n) {
        ss << count << ", ";
    }

    NES_INFO("N: " << 10 << ", median: " << median << ", measures: " << ss.str());
}

class GeneticAlgorithmBenchmark : public testing::Test {
  public:
    std::shared_ptr<QueryParsingService> queryParsingService;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() { std::cout << "Setup QueryPlacementTest test class."  << std::endl; }

    /* Will be called before a test is executed. */
    void SetUp() override {
        NES::setupLogging("QueryPlacementTest.log", NES::LOG_DEBUG);
        std::cout << "Setup QueryPlacementTest test case." << std::endl;
        auto cppCompiler = Compiler::CPPCompiler::create();
        auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        queryParsingService = QueryParsingService::create(jitCompiler);
    }

    /* Will be called before a test is executed. */
    void TearDown() { std::cout << "Setup GeneticAlgorithmBenchmark test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down GeneticAlgorithmBenchmark test class." << std::endl; }

    void setupTopologyAndStreamCatalogForGA(int n, int SourcePerMiddle) {
        int nodeID = 1;

        topology = Topology::create();
        TopologyNodePtr rootNode = TopologyNode::create(nodeID++, "localhost", 123, 124, 1000000);
        topology->setAsRoot(rootNode);
        streamCatalog = std::make_shared<StreamCatalog>(queryParsingService);
        LinkPropertyPtr linkProperty = std::make_shared<LinkProperty>(LinkProperty(512, 100));
        std::string schema = "Schema::create()->addField(\"id\", BasicType::UINT32)"
                             "->addField(\"value\", BasicType::UINT64);";
        std::string streamName = "car";
        streamCatalog->addLogicalStream(streamName, schema);
        SourceConfigPtr sourceConfig = SourceConfig::create();
        sourceConfig->setSourceFrequency(0);
        sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
        sourceConfig->setPhysicalStreamName(  "GeneticAlgorithmBenchmark");
        sourceConfig->setLogicalStreamName(streamName);
        PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(sourceConfig);

        for(int i = 0; i < n; i++) {
            TopologyNodePtr middleNode = TopologyNode::create(nodeID++, "localhost", 123, 124, 500000);
            topology->addNewPhysicalNodeAsChild(rootNode, middleNode);
            middleNode->addLinkProperty(rootNode, linkProperty);
            rootNode->addLinkProperty(middleNode, linkProperty);

            for(int j = 0; j < SourcePerMiddle; j++) {
                TopologyNodePtr sourceNode = TopologyNode::create(nodeID++, "localhost", 123, 124, 500000);
                topology->addNewPhysicalNodeAsChild(middleNode, sourceNode);
                sourceNode->addLinkProperty(middleNode, linkProperty);
                middleNode->addLinkProperty(sourceNode, linkProperty);
                StreamCatalogEntryPtr streamCatalogEntry = std::make_shared<StreamCatalogEntry>(conf, sourceNode);
                streamCatalog->addPhysicalStream(streamName, streamCatalogEntry);
            }
        }
    }

    StreamCatalogPtr streamCatalog;
    TopologyPtr topology;
};


TEST_F(GeneticAlgorithmBenchmark, testPlacingQueryWithGeneticAlgorithmStrategyFixedTopologyWithDynamicQuery) {

    std::list<int> listOfInts({20});
    std::map<int, std::vector<long>> counts;
    int SourcePerMiddle = 3;
    int repetitions = 1;



    std::vector<std::map<std::string, std::any>> properties;

    // adding property of the source
    std::map<std::string, std::any> srcProp;
    srcProp.insert(std::make_pair("load", 1));
    srcProp.insert(std::make_pair("dmf", 1.0));

    // adding property of the filter
    std::map<std::string, std::any> filterProp;
    filterProp.insert(std::make_pair("load", 2));
    filterProp.insert(std::make_pair("dmf", 0.4));

    // adding property of the map
    std::map<std::string, std::any> mapProp;
    mapProp.insert(std::make_pair("load", 4));
    mapProp.insert(std::make_pair("dmf", 2.0));

    // adding property of the sink
    std::map<std::string, std::any> sinkProp;
    sinkProp.insert(std::make_pair("load", 6));
    sinkProp.insert(std::make_pair("dmf", 4.0));

    for(int n : listOfInts) {
        setupTopologyAndStreamCatalogForGA(20,SourcePerMiddle);
        GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
        auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);
        auto placementStrategy = Optimizer::PlacementStrategyFactory::getStrategy("GeneticAlgorithm",
                                                                                  globalExecutionPlan,
                                                                                  topology,
                                                                                  typeInferencePhase,
                                                                                  streamCatalog);

        Query query = Query::from("car");
        properties.push_back(sinkProp);

        for(int i = 0; i < n; i+=2){
            query.filter(Attribute("id") < n-i);
        }
        for(int i = 1; i < n; i+=2){
            query.map(Attribute("value2") = Attribute("value") * 2);
        }
        for(int i = 0; i < 12; i++){
            for(int j = 1; j < n; j+=2){
                properties.push_back(mapProp);
            }
            for(int j = 0; j < n; j+=2){
                properties.push_back(filterProp);
            }
            properties.push_back(srcProp);
        }
        query.sink(PrintSinkDescriptor::create());


        std::vector<long> counts_n;
        for(int j = 0; j < repetitions; j++) {
            std::ofstream outfile;
            outfile.open ("benchmark.txt");

            QueryPlanPtr testQueryPlan = query.getQueryPlan();
            QueryId queryId = PlanIdGenerator::getNextQueryId();
            testQueryPlan->setQueryId(queryId);

            // Execute optimization phases prior to placement
            auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
            testQueryPlan = queryReWritePhase->execute(testQueryPlan);
            typeInferencePhase->execute(testQueryPlan);

            auto topologySpecificQueryRewrite = Optimizer::TopologySpecificQueryRewritePhase::create(streamCatalog);
            topologySpecificQueryRewrite->execute(testQueryPlan);
            typeInferencePhase->execute(testQueryPlan);

            UtilityFunctions::assignPropertiesToQueryOperators(testQueryPlan, properties);
            outfile << "The topology is:\n"<<topology->toString();
            outfile << "Query Plan:"<<testQueryPlan->toString()<<"\n";
            outfile.close();
            auto start = std::chrono::high_resolution_clock::now();
            ASSERT_TRUE(placementStrategy->updateGlobalExecutionPlan(testQueryPlan));
            auto stop = std::chrono::high_resolution_clock::now();
            long count = std::chrono::duration_cast<std::chrono::milliseconds>(stop - start).count();
            NES_DEBUG("Found Solution in " << count << "ms");
            counts_n.push_back(count);
            globalExecutionPlan->removeQuerySubPlans(queryId);
        }
        counts.insert(std::make_pair(n, counts_n));
        properties.clear();
    }
    for (auto& [n, counts_n] : counts) {
        std::sort(counts_n.begin(), counts_n.end());
        long median = counts_n[counts_n.size() / 2];
        if (counts_n.size() % 2 == 0) {
            median = (median + counts_n[(counts_n.size() - 1) / 2]) / 2;
        }
        std::stringstream ss;
        for (auto& count : counts_n) {
            ss << count << ", ";
        }

        NES_INFO("N: " << n << ", median: " << median << ", measures: " << ss.str());
    }
}

/* Test query placement with genetic algorithm strategy  */
TEST_F(GeneticAlgorithmBenchmark, testPlacingQueryWithGAStrategyFixedQueryWithDynamicTopologyWidth) {
    std::list<int> listOfInts( {100});

    int repetitions = 5;
    int SourcePerMiddle = 3;
    std::map<int, std::vector<long>> counts;

    // Prepare the query
    auto sinkOperator = LogicalOperatorFactory::createSinkOperator(PrintSinkDescriptor::create());
    auto filterOperator = LogicalOperatorFactory::createFilterOperator(Attribute("id") < 45);
    auto mapOperator = LogicalOperatorFactory::createMapOperator(Attribute("value") = Attribute("value") * 2);
    auto sourceOperator = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("car"));

    sinkOperator->addChild(mapOperator);
    mapOperator->addChild(filterOperator);
    filterOperator->addChild(sourceOperator);





    for(int n : listOfInts) {
        QueryPlanPtr queryPlan = QueryPlan::create();
        queryPlan->addRootOperator(sinkOperator);
        QueryId queryId = PlanIdGenerator::getNextQueryId();
        queryPlan->setQueryId(queryId);

        //setupTopologyAndStreamCatalogForGA(n);
        setupTopologyAndStreamCatalogForGA(n, SourcePerMiddle);
        GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
        auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);
        auto placementStrategy = Optimizer::PlacementStrategyFactory::getStrategy("GeneticAlgorithm",
                                                                                  globalExecutionPlan,
                                                                                  topology,
                                                                                  typeInferencePhase,
                                                                                  streamCatalog);
        // Execute optimization phases prior to placement
        auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
        queryPlan = queryReWritePhase->execute(queryPlan);
        typeInferencePhase->execute(queryPlan);

        auto topologySpecificQueryRewrite = Optimizer::TopologySpecificQueryRewritePhase::create(streamCatalog);
        topologySpecificQueryRewrite->execute(queryPlan);
        typeInferencePhase->execute(queryPlan);

        std::vector<long> counts_n;
        for(int j = 0; j < repetitions; j++) {
            auto start = std::chrono::high_resolution_clock::now();
            ASSERT_TRUE(placementStrategy->updateGlobalExecutionPlan(queryPlan));
            auto stop = std::chrono::high_resolution_clock::now();
            long count = std::chrono::duration_cast<std::chrono::milliseconds>(stop - start).count();
            //NES_DEBUG("Solved Placement for " << num_operators << " Operators, " << n << " Sources and " << n * 2 + 1 << " Topology nodes");
            NES_DEBUG("Found Solution in " << count << "ms");
            counts_n.push_back(count);
            globalExecutionPlan->removeQuerySubPlans(queryId);
        }
        counts.insert(std::make_pair(n, counts_n));
    }
    for (auto& [n, counts_n] : counts) {
        std::sort(counts_n.begin(), counts_n.end());
        long median = counts_n[counts_n.size() / 2];
        if (counts_n.size() % 2 == 0) {
            median = (median + counts_n[(counts_n.size() - 1) / 2]) / 2;
        }
        std::stringstream ss;
        for (auto& count : counts_n) {
            ss << count << ", ";
        }

        NES_INFO("N: " << n << ", median: " << median << ", measures: " << ss.str());
    }
}

class GeneticAlgorithmBenchmark2 : public testing::Test {
  public:
    std::shared_ptr<QueryParsingService> queryParsingService;
    // Will be called before any test in this class are executed.
    static void SetUpTestCase() { std::cout << "Setup QueryPlacementTest test class." << std::endl; }

    // Will be called before a test is executed.
    void SetUp() override {
        NES::setupLogging("QueryPlacementTest.log", NES::LOG_DEBUG);
        std::cout << "Setup QueryPlacementTest test case." << std::endl;
        auto cppCompiler = Compiler::CPPCompiler::create();
        auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        queryParsingService = QueryParsingService::create(jitCompiler);
    }

    // Will be called before a test is executed.
    void TearDown() { std::cout << "Setup QueryPlacementTest test case." << std::endl; }

    // Will be called after all tests in this class are finished.
    static void TearDownTestCase() { std::cout << "Tear down QueryPlacementTest test class." << std::endl; }

    void setupTopologyAndStreamCatalogForGA(int numOfMidNodes) {

        topology = Topology::create();
        // create the topology for the test
        uint32_t grpcPort = 4000;
        uint32_t dataPort = 5000;
        int nodeID = 1;
        TopologyNodePtr sinkNode = TopologyNode::create(nodeID++, "localhost", grpcPort, dataPort, 5000);
        topology->setAsRoot(sinkNode);
        LinkPropertyPtr linkProperty = std::make_shared<LinkProperty>(LinkProperty(100, 64));
        auto previousNode = sinkNode;
        for(int i = 0; i < numOfMidNodes; i++) {
            TopologyNodePtr midNode = TopologyNode::create(nodeID++, "localhost", 123, 124, 100);
            topology->addNewPhysicalNodeAsChild(previousNode, midNode);
            midNode->addLinkProperty(previousNode, linkProperty);
            previousNode->addLinkProperty(midNode, linkProperty);
            previousNode = midNode;
        }


        TopologyNodePtr sourceNode = TopologyNode::create(nodeID++, "localhost", grpcPort, dataPort, 10000);
        topology->addNewPhysicalNodeAsChild(previousNode, sourceNode);
        previousNode->addLinkProperty(sourceNode, linkProperty);
        sourceNode->addLinkProperty(previousNode, linkProperty);

        std::string schema = "Schema::create()->addField(\"id\", BasicType::UINT32)"
                             "->addField(\"value\", BasicType::UINT64);";
        const std::string streamName = "car";

        streamCatalog = std::make_shared<StreamCatalog>(queryParsingService);
        streamCatalog->addLogicalStream(streamName, schema);

        SourceConfigPtr sourceConfig = SourceConfig::create();
        sourceConfig->setSourceFrequency(0);
        sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
        sourceConfig->setPhysicalStreamName("test1");
        sourceConfig->setLogicalStreamName("car");

        PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(sourceConfig);

        StreamCatalogEntryPtr streamCatalogEntry1 = std::make_shared<StreamCatalogEntry>(conf, sourceNode);

        streamCatalog->addPhysicalStream("car", streamCatalogEntry1);
    }

    StreamCatalogPtr streamCatalog;
    TopologyPtr topology;
};

TEST_F(GeneticAlgorithmBenchmark2, testPlacingQueryWithGeneticAlgorithmStrategyFixedQueryWithDynamicTopologyHeight) {
    std::list<int> listOfInts( {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40});
    std::map<int, std::vector<long>> counts;
    int repetitions = 5;

    // Prepare the query
    auto sinkOperator = LogicalOperatorFactory::createSinkOperator(PrintSinkDescriptor::create());
    auto filterOperator = LogicalOperatorFactory::createFilterOperator(Attribute("id") < 45);
    auto mapOperator = LogicalOperatorFactory::createMapOperator(Attribute("newId") = Attribute("id") * 2);
    auto sourceOperator = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("car"));

    sinkOperator->addChild(mapOperator);
    mapOperator->addChild(filterOperator);
    filterOperator->addChild(sourceOperator);

    QueryPlanPtr testQueryPlan = QueryPlan::create();
    testQueryPlan->addRootOperator(sinkOperator);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    testQueryPlan->setQueryId(queryId);

    std::vector<std::map<std::string, std::any>> properties;

    // adding property of the source
    std::map<std::string, std::any> srcProp;
    srcProp.insert(std::make_pair("load", 1));
    srcProp.insert(std::make_pair("dmf", 1.0));

    // adding property of the filter
    std::map<std::string, std::any> filterProp;
    filterProp.insert(std::make_pair("load", 2));
    filterProp.insert(std::make_pair("dmf", 0.4));

    // adding property of the map
    std::map<std::string, std::any> mapProp;
    mapProp.insert(std::make_pair("load", 4));
    mapProp.insert(std::make_pair("dmf", 2.0));

    // adding property of the sink
    std::map<std::string, std::any> sinkProp;
    sinkProp.insert(std::make_pair("load", 6));
    sinkProp.insert(std::make_pair("dmf", 4.0));

    properties.push_back(sinkProp);
    properties.push_back(mapProp);
    properties.push_back(filterProp);
    properties.push_back(srcProp);
    for(int n : listOfInts) {
        setupTopologyAndStreamCatalogForGA(n);
        GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
        auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);
        auto placementStrategy = Optimizer::PlacementStrategyFactory::getStrategy("GeneticAlgorithm",
                                                                                  globalExecutionPlan,
                                                                                  topology,
                                                                                  typeInferencePhase,
                                                                                  streamCatalog);
        // Execute optimization phases prior to placement
        auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
        testQueryPlan = queryReWritePhase->execute(testQueryPlan);
        typeInferencePhase->execute(testQueryPlan);

        auto topologySpecificQueryRewrite = Optimizer::TopologySpecificQueryRewritePhase::create(streamCatalog);
        topologySpecificQueryRewrite->execute(testQueryPlan);
        typeInferencePhase->execute(testQueryPlan);

        UtilityFunctions::assignPropertiesToQueryOperators(testQueryPlan, properties);
        std::vector<long> counts_n;
        for(int j = 0; j < repetitions; j++) {
            auto start = std::chrono::high_resolution_clock::now();
            ASSERT_TRUE(placementStrategy->updateGlobalExecutionPlan(testQueryPlan));
            auto stop = std::chrono::high_resolution_clock::now();
            long count = std::chrono::duration_cast<std::chrono::milliseconds>(stop - start).count();
            NES_DEBUG("Found Solution in " << count << "ms");
            counts_n.push_back(count);
            globalExecutionPlan->removeQuerySubPlans(queryId);
        }
        counts.insert(std::make_pair(n, counts_n));
    }
    for (auto& [n, counts_n] : counts) {
        std::sort(counts_n.begin(), counts_n.end());
        long median = counts_n[counts_n.size() / 2];
        if (counts_n.size() % 2 == 0) {
            median = (median + counts_n[(counts_n.size() - 1) / 2]) / 2;
        }
        std::stringstream ss;
        for (auto& count : counts_n) {
            ss << count << ", ";
        }

        NES_INFO("N: " << n << ", median: " << median << ", measures: " << ss.str());
    }
}


TEST_F(GeneticAlgorithmBenchmark2, testPlacingQueryWithGeneticAlgorithmStrategyFixedTopologyWithDynamicQuery) {
    std::list<int> listOfInts( {10});
    std::map<int, std::vector<long>> counts;
    int repetitions = 5;

    setupTopologyAndStreamCatalogForGA(10);
    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);
    auto placementStrategy = Optimizer::PlacementStrategyFactory::getStrategy("GeneticAlgorithm",
                                                                              globalExecutionPlan,
                                                                              topology,
                                                                              typeInferencePhase,
                                                                              streamCatalog);

    std::vector<std::map<std::string, std::any>> properties;

    // adding property of the source
    std::map<std::string, std::any> srcProp;
    srcProp.insert(std::make_pair("load", 1));
    srcProp.insert(std::make_pair("dmf", 1.0));

    // adding property of the filter
    std::map<std::string, std::any> filterProp;
    filterProp.insert(std::make_pair("load", 2));
    filterProp.insert(std::make_pair("dmf", 0.4));

    // adding property of the map
    std::map<std::string, std::any> mapProp;
    mapProp.insert(std::make_pair("load", 4));
    mapProp.insert(std::make_pair("dmf", 2.0));

    // adding property of the sink
    std::map<std::string, std::any> sinkProp;
    sinkProp.insert(std::make_pair("load", 6));
    sinkProp.insert(std::make_pair("dmf", 4.0));

    for(int n : listOfInts) {
        Query query = Query::from("car");
        properties.push_back(sinkProp);
        for(int i = 0; i < n; i+=2){
            query.filter(Attribute("id") < n-i);
        }
        for(int i = 1; i < n; i+=2){
            query.map(Attribute("value") = Attribute("value") * 2);
        }
        for(int i = 1; i < n; i+=2){
            properties.push_back(mapProp);
        }
        for(int i = 0; i < n; i+=2){
            properties.push_back(filterProp);
        }
        properties.push_back(srcProp);
        query.sink(PrintSinkDescriptor::create());
        QueryPlanPtr testQueryPlan = query.getQueryPlan();
        QueryId queryId = PlanIdGenerator::getNextQueryId();
        testQueryPlan->setQueryId(queryId);

        // Execute optimization phases prior to placement
        auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
        testQueryPlan = queryReWritePhase->execute(testQueryPlan);
        typeInferencePhase->execute(testQueryPlan);

        auto topologySpecificQueryRewrite = Optimizer::TopologySpecificQueryRewritePhase::create(streamCatalog);
        topologySpecificQueryRewrite->execute(testQueryPlan);
        typeInferencePhase->execute(testQueryPlan);
        UtilityFunctions::assignPropertiesToQueryOperators(testQueryPlan, properties);
        std::vector<long> counts_n;
        for(int j = 0; j < repetitions; j++) {
            auto start = std::chrono::high_resolution_clock::now();
            ASSERT_TRUE(placementStrategy->updateGlobalExecutionPlan(testQueryPlan));
            auto stop = std::chrono::high_resolution_clock::now();
            long count = std::chrono::duration_cast<std::chrono::milliseconds>(stop - start).count();
            NES_DEBUG("Found Solution in " << count << "ms");
            counts_n.push_back(count);
            globalExecutionPlan->removeQuerySubPlans(queryId);
        }
        counts.insert(std::make_pair(n, counts_n));
        properties.clear();
    }
    for (auto& [n, counts_n] : counts) {
        std::sort(counts_n.begin(), counts_n.end());
        long median = counts_n[counts_n.size() / 2];
        if (counts_n.size() % 2 == 0) {
            median = (median + counts_n[(counts_n.size() - 1) / 2]) / 2;
        }
        std::stringstream ss;
        for (auto& count : counts_n) {
            ss << count << ", ";
        }

        NES_INFO("N: " << n << ", median: " << median << ", measures: " << ss.str());
    }
}
TEST_F(GeneticAlgorithmBenchmark2, testPlacingQueryWithGeneticAlgorithmStrategyEvaluation) {
    std::list<int> listOfInts( {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40});
    std::map<int, std::vector<long>> counts;
    int repetitions = 1;

    setupTopologyAndStreamCatalogForGA(10);
    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);
    auto placementStrategy = Optimizer::PlacementStrategyFactory::getStrategy("GeneticAlgorithm",
                                                                              globalExecutionPlan,
                                                                              topology,
                                                                              typeInferencePhase,
                                                                              streamCatalog);

    std::vector<std::map<std::string, std::any>> properties;

    // adding property of the source
    std::map<std::string, std::any> srcProp;
    srcProp.insert(std::make_pair("load", 1));
    srcProp.insert(std::make_pair("dmf", 1.0));

    // adding property of the filter
    std::map<std::string, std::any> filterProp;
    filterProp.insert(std::make_pair("load", 4));
    filterProp.insert(std::make_pair("dmf", 0.5));

    // adding property of the map
    std::map<std::string, std::any> mapProp;
    mapProp.insert(std::make_pair("load", 4));
    mapProp.insert(std::make_pair("dmf", 2.0));

    // adding property of the sink
    std::map<std::string, std::any> sinkProp;
    sinkProp.insert(std::make_pair("load", 6));
    sinkProp.insert(std::make_pair("dmf", 4.0));

    for(int n : listOfInts) {
        Query query = Query::from("car");
        properties.push_back(sinkProp);
        for(int i = 0; i < n; i+=2){
            query.filter(Attribute("id") < n-i);
        }
        for(int i = 1; i < n; i+=2){
            query.map(Attribute("value") = Attribute("value") * 2);
        }
        for(int i = 1; i < n; i+=2){
            properties.push_back(mapProp);
        }
        for(int i = 0; i < n; i+=2){
            properties.push_back(filterProp);
        }
        properties.push_back(srcProp);
        query.sink(PrintSinkDescriptor::create());
        QueryPlanPtr testQueryPlan = query.getQueryPlan();
        QueryId queryId = PlanIdGenerator::getNextQueryId();
        testQueryPlan->setQueryId(queryId);

        // Execute optimization phases prior to placement
        auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
        testQueryPlan = queryReWritePhase->execute(testQueryPlan);
        typeInferencePhase->execute(testQueryPlan);

        auto topologySpecificQueryRewrite = Optimizer::TopologySpecificQueryRewritePhase::create(streamCatalog);
        topologySpecificQueryRewrite->execute(testQueryPlan);
        typeInferencePhase->execute(testQueryPlan);

        UtilityFunctions::assignPropertiesToQueryOperators(testQueryPlan, properties);
        std::vector<long> counts_n;
        for(int j = 0; j < repetitions; j++) {
            auto start = std::chrono::high_resolution_clock::now();
            ASSERT_TRUE(placementStrategy->updateGlobalExecutionPlan(testQueryPlan));
            auto stop = std::chrono::high_resolution_clock::now();
            long count = std::chrono::duration_cast<std::chrono::milliseconds>(stop - start).count();
            NES_DEBUG("Found Solution in " << count << "ms");
            counts_n.push_back(count);
            globalExecutionPlan->removeQuerySubPlans(queryId);
        }
        counts.insert(std::make_pair(n, counts_n));
        properties.clear();
    }
    for (auto& [n, counts_n] : counts) {
        std::sort(counts_n.begin(), counts_n.end());
        long median = counts_n[counts_n.size() / 2];
        if (counts_n.size() % 2 == 0) {
            median = (median + counts_n[(counts_n.size() - 1) / 2]) / 2;
        }
        std::stringstream ss;
        for (auto& count : counts_n) {
            ss << count << ", ";
        }

        NES_INFO("N: " << n << ", median: " << median << ", measures: " << ss.str());
    }
}