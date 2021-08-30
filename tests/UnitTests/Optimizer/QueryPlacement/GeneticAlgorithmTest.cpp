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
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <gtest/gtest.h>

using namespace NES;
using namespace web;

/*
class GeneticAlgorithmStrategyTest : public testing::Test {
    class LogicalSourceExpansionRule;
    typedef std::shared_ptr<LogicalSourceExpansionRule> LogicalSourceExpansionRulePtr;

  public:
    // Will be called before any test in this class are executed.
    static void SetUpTestCase() { std::cout << "Setup QueryPlacementTest test class." << std::endl; }

    // Will be called before a test is executed.
    void SetUp() {
        NES::setupLogging("QueryPlacementTest.log", NES::LOG_DEBUG);
        StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
        std::cout << "Setup QueryPlacementTest test case." << std::endl;
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

        TopologyNodePtr rootNode = TopologyNode::create(8, "localhost", grpcPort, dataPort, 14);
        topology->setAsRoot(rootNode);

        TopologyNodePtr fogNode4 = TopologyNode::create(7, "localhost", grpcPort, dataPort, 10);
        topology->addNewPhysicalNodeAsChild(rootNode, fogNode4);

        TopologyNodePtr fogNode3 = TopologyNode::create(6, "localhost", grpcPort, dataPort, 8);
        topology->addNewPhysicalNodeAsChild(fogNode4, fogNode3);

        TopologyNodePtr fogNode2 = TopologyNode::create(5, "localhost", grpcPort, dataPort, 5);
        topology->addNewPhysicalNodeAsChild(fogNode3, fogNode2);

        TopologyNodePtr fogNode1 = TopologyNode::create(4, "localhost", grpcPort, dataPort, 5);
        topology->addNewPhysicalNodeAsChild(fogNode3, fogNode1);

        TopologyNodePtr sourceNode3 = TopologyNode::create(3, "localhost", grpcPort, dataPort, 3);
        topology->addNewPhysicalNodeAsChild(fogNode2, sourceNode3);

        TopologyNodePtr sourceNode2 = TopologyNode::create(2, "localhost", grpcPort, dataPort, 3);
        topology->addNewPhysicalNodeAsChild(fogNode1, sourceNode2);

        TopologyNodePtr sourceNode1 = TopologyNode::create(1, "localhost", grpcPort, dataPort, 3);
        topology->addNewPhysicalNodeAsChild(fogNode1, sourceNode1);

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
        const std::string streamName = "car";

        streamCatalog = std::make_shared<StreamCatalog>();
        streamCatalog->addLogicalStream(streamName, schema);

        SourceConfigPtr sourceConfig = SourceConfig::create();
        sourceConfig->setSourceFrequency(0);
        sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
        sourceConfig->setPhysicalStreamName("test3");
        sourceConfig->setLogicalStreamName("car");

        PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(sourceConfig);

        StreamCatalogEntryPtr streamCatalogEntry1 = std::make_shared<StreamCatalogEntry>(conf, sourceNode1);
        StreamCatalogEntryPtr streamCatalogEntry2 = std::make_shared<StreamCatalogEntry>(conf, sourceNode2);
        StreamCatalogEntryPtr streamCatalogEntry3 = std::make_shared<StreamCatalogEntry>(conf, sourceNode3);

        streamCatalog->addPhysicalStream("car", streamCatalogEntry1);
        streamCatalog->addPhysicalStream("car", streamCatalogEntry2);
        streamCatalog->addPhysicalStream("car", streamCatalogEntry3);
    }

    StreamCatalogPtr streamCatalog;
    TopologyPtr topology;
};
// test the cost function
TEST_F(GeneticAlgorithmStrategyTest, testPlacingQueryWithGeneticAlgorithmStrategy) {

    setupTopologyAndStreamCatalogForGA();

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);
    auto placementStrategy = Optimizer::PlacementStrategyFactory::getStrategy("GeneticAlgorithm",
                                                                              globalExecutionPlan,
                                                                              topology,
                                                                              typeInferencePhase,
                                                                              streamCatalog);

    auto sourceOperator = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("car"));
    auto filterOperator = LogicalOperatorFactory::createFilterOperator(Attribute("id") < 45);
    auto mapOperator = LogicalOperatorFactory::createMapOperator(Attribute("id") = Attribute("newId") * 2);
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    auto sinkOperator = LogicalOperatorFactory::createSinkOperator(printSinkDescriptor);

    sinkOperator->addChild(mapOperator);
    mapOperator->addChild(filterOperator);
    filterOperator->addChild(sourceOperator);

    QueryPlanPtr queryPlan = QueryPlan::create();
    queryPlan->addRootOperator(sinkOperator);

    QueryId queryId = PlanIdGenerator::getNextQueryId();
    queryPlan->setQueryId(queryId);
    NES_DEBUG("Query_Plan: " << queryPlan->toString());

    std::vector<std::map<std::string, std::any>> properties;

    // adding property of the source
    std::map<std::string, std::any> srcProp;
    srcProp.insert(std::make_pair("load", 1));
    srcProp.insert(std::make_pair("dmf", 9.0));
    properties.push_back(srcProp);

    // adding property of the filter
    std::map<std::string, std::any> filterProp;
    filterProp.insert(std::make_pair("load", 2));
    filterProp.insert(std::make_pair("dmf", 0.3));
    properties.push_back(filterProp);

    // adding property of the map
    std::map<std::string, std::any> mapProp;
    mapProp.insert(std::make_pair("load", 4));
    mapProp.insert(std::make_pair("dmf", 4.0));
    properties.push_back(mapProp);

    // adding property of the sink
    std::map<std::string, std::any> sinkProp;
    sinkProp.insert(std::make_pair("load", 3));
    sinkProp.insert(std::make_pair("dmf", 2.0));
    properties.push_back(sinkProp);

    UtilityFunctions::assignPropertiesToQueryOperators(queryPlan, properties);
    NES_DEBUG("Query_Plan: " << queryPlan->toString());
    auto queryOperators = QueryPlanIterator(queryPlan).snapshot();
    std::cout << queryPlan->toString();

    placementStrategy->updateGlobalExecutionPlan(queryPlan);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);
}*/

/*
class GeneticAlgorithmStrategyTest : public testing::Test {
    class LogicalSourceExpansionRule;
    typedef std::shared_ptr<LogicalSourceExpansionRule> LogicalSourceExpansionRulePtr;

  public:
    // Will be called before any test in this class are executed.
    static void SetUpTestCase() { std::cout << "Setup QueryPlacementTest test class." << std::endl; }

    // Will be called before a test is executed.
    void SetUp() {
        NES::setupLogging("QueryPlacementTest.log", NES::LOG_DEBUG);
        StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
        std::cout << "Setup QueryPlacementTest test case." << std::endl;
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

        TopologyNodePtr rootNode = TopologyNode::create(8, "localhost", grpcPort, dataPort, 14);
        topology->setAsRoot(rootNode);

        TopologyNodePtr fogNode4 = TopologyNode::create(7, "localhost", grpcPort, dataPort, 10);
        topology->addNewPhysicalNodeAsChild(rootNode, fogNode4);

        TopologyNodePtr fogNode3 = TopologyNode::create(6, "localhost", grpcPort, dataPort, 8);
        topology->addNewPhysicalNodeAsChild(fogNode4, fogNode3);

        TopologyNodePtr fogNode2 = TopologyNode::create(5, "localhost", grpcPort, dataPort, 5);
        topology->addNewPhysicalNodeAsChild(fogNode3, fogNode2);

        TopologyNodePtr fogNode1 = TopologyNode::create(4, "localhost", grpcPort, dataPort, 5);
        topology->addNewPhysicalNodeAsChild(fogNode3, fogNode1);

        TopologyNodePtr sourceNode3 = TopologyNode::create(3, "localhost", grpcPort, dataPort, 3);
        topology->addNewPhysicalNodeAsChild(fogNode2, sourceNode3);

        TopologyNodePtr sourceNode2 = TopologyNode::create(2, "localhost", grpcPort, dataPort, 3);
        topology->addNewPhysicalNodeAsChild(fogNode1, sourceNode2);

        TopologyNodePtr sourceNode1 = TopologyNode::create(1, "localhost", grpcPort, dataPort, 3);
        topology->addNewPhysicalNodeAsChild(fogNode1, sourceNode1);

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
        const std::string streamName2 = "bus";

        streamCatalog = std::make_shared<StreamCatalog>();
        streamCatalog->addLogicalStream(streamName1, schema);
        streamCatalog->addLogicalStream(streamName2, schema);

        SourceConfigPtr sourceConfig1 = SourceConfig::create();
        sourceConfig1->setSourceFrequency(0);
        sourceConfig1->setNumberOfTuplesToProducePerBuffer(0);
        sourceConfig1->setPhysicalStreamName("test3");
        sourceConfig1->setLogicalStreamName("car");

        SourceConfigPtr sourceConfig2 = SourceConfig::create();
        sourceConfig2->setSourceFrequency(0);
        sourceConfig2->setNumberOfTuplesToProducePerBuffer(0);
        sourceConfig2->setPhysicalStreamName("test3");
        sourceConfig2->setLogicalStreamName("bus");


        PhysicalStreamConfigPtr conf1 = PhysicalStreamConfig::create(sourceConfig1);
        PhysicalStreamConfigPtr conf2 = PhysicalStreamConfig::create(sourceConfig2);

        StreamCatalogEntryPtr streamCatalogEntry1 = std::make_shared<StreamCatalogEntry>(conf1, sourceNode1);
        StreamCatalogEntryPtr streamCatalogEntry2 = std::make_shared<StreamCatalogEntry>(conf1, sourceNode2);
        StreamCatalogEntryPtr streamCatalogEntry3 = std::make_shared<StreamCatalogEntry>(conf2, sourceNode3);

        streamCatalog->addPhysicalStream("car", streamCatalogEntry1);
        streamCatalog->addPhysicalStream("car", streamCatalogEntry2);
        streamCatalog->addPhysicalStream("bus", streamCatalogEntry3);
    }

    StreamCatalogPtr streamCatalog;
    TopologyPtr topology;
};
// test the cost function
TEST_F(GeneticAlgorithmStrategyTest, testPlacingQueryWithGeneticAlgorithmStrategy) {

    setupTopologyAndStreamCatalogForGA();

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);
    auto placementStrategy = Optimizer::PlacementStrategyFactory::getStrategy("GeneticAlgorithm",
                                                                              globalExecutionPlan,
                                                                              topology,
                                                                              typeInferencePhase,
                                                                              streamCatalog);

    auto sourceOperator1 = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("car"));
    auto sourceOperator2 = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("bus"));
    auto filterOperator1 = LogicalOperatorFactory::createFilterOperator(Attribute("id") < 45);
    auto filterOperator2 = LogicalOperatorFactory::createFilterOperator(Attribute("id") < 45);
    auto mapOperator1 = LogicalOperatorFactory::createMapOperator(Attribute("id") = Attribute("newId") * 2);
    auto mapOperator2 = LogicalOperatorFactory::createMapOperator(Attribute("id") = Attribute("newId") * 2);
    auto mapOperator3 = LogicalOperatorFactory::createMapOperator(Attribute("id") = Attribute("newId") * 2);
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    auto sinkOperator = LogicalOperatorFactory::createSinkOperator(printSinkDescriptor);

    sinkOperator->addChild(mapOperator1);
    mapOperator1->addChild(filterOperator1);
    mapOperator1->addChild(filterOperator2);
    filterOperator1->addChild(mapOperator2);
    filterOperator2->addChild(mapOperator3);
    mapOperator2->addChild(sourceOperator1);
    mapOperator3->addChild(sourceOperator2);

    QueryPlanPtr queryPlan = QueryPlan::create();
    queryPlan->addRootOperator(sinkOperator);

    QueryId queryId = PlanIdGenerator::getNextQueryId();
    queryPlan->setQueryId(queryId);
    NES_DEBUG("Query_Plan: " << queryPlan->toString());

    std::vector<std::map<std::string, std::any>> properties;

    // adding property of the source2
    std::map<std::string, std::any> srcProp2;
    srcProp2.insert(std::make_pair("load", 1));
    srcProp2.insert(std::make_pair("dmf", 8.0));
    properties.push_back(srcProp2);

    // adding property of the map3
    std::map<std::string, std::any> mapProp3;
    mapProp3.insert(std::make_pair("load", 4));
    mapProp3.insert(std::make_pair("dmf", 10.0));
    properties.push_back(mapProp3);

    // adding property of the filter2
    std::map<std::string, std::any> filterProp2;
    filterProp2.insert(std::make_pair("load", 2));
    filterProp2.insert(std::make_pair("dmf", 0.6));
    properties.push_back(filterProp2);

    // adding property of the source1
    std::map<std::string, std::any> srcProp1;
    srcProp1.insert(std::make_pair("load", 1));
    srcProp1.insert(std::make_pair("dmf", 4.0));
    properties.push_back(srcProp1);

    // adding property of the map2
    std::map<std::string, std::any> mapProp2;
    mapProp2.insert(std::make_pair("load", 4));
    mapProp2.insert(std::make_pair("dmf", 5.0));
    properties.push_back(mapProp2);

    // adding property of the filter1
    std::map<std::string, std::any> filterProp1;
    filterProp1.insert(std::make_pair("load", 2));
    filterProp1.insert(std::make_pair("dmf", 0.3));
    properties.push_back(filterProp1);

    // adding property of the map1
    std::map<std::string, std::any> mapProp1;
    mapProp1.insert(std::make_pair("load", 4));
    mapProp1.insert(std::make_pair("dmf", 15.0));
    properties.push_back(mapProp1);

    // adding property of the sink
    std::map<std::string, std::any> sinkProp;
    sinkProp.insert(std::make_pair("load", 3));
    sinkProp.insert(std::make_pair("dmf", 2.0));
    properties.push_back(sinkProp);

    UtilityFunctions::assignPropertiesToQueryOperators(queryPlan, properties);
    NES_DEBUG("Query_Plan: " << queryPlan->toString());
    auto queryOperators = QueryPlanIterator(queryPlan).snapshot();
    std::cout << queryPlan->toString();

    placementStrategy->updateGlobalExecutionPlan(queryPlan);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);
}
 */
class GeneticAlgorithmStrategyTest : public testing::Test {
    class LogicalSourceExpansionRule;
    typedef std::shared_ptr<LogicalSourceExpansionRule> LogicalSourceExpansionRulePtr;

  public:
    // Will be called before any test in this class are executed.
    static void SetUpTestCase() { std::cout << "Setup QueryPlacementTest test class." << std::endl; }

    // Will be called before a test is executed.
    void SetUp() {
        NES::setupLogging("QueryPlacementTest.log", NES::LOG_DEBUG);
        StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
        std::cout << "Setup QueryPlacementTest test case." << std::endl;
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

        TopologyNodePtr rootNode = TopologyNode::create(1, "localhost", grpcPort, dataPort, 64);
        topology->setAsRoot(rootNode);

        TopologyNodePtr fogTopNode1 = TopologyNode::create(2, "localhost", grpcPort, dataPort, 32);
        topology->addNewPhysicalNodeAsChild(rootNode, fogTopNode1);

        TopologyNodePtr fogTopNode2 = TopologyNode::create(3, "localhost", grpcPort, dataPort, 32);
        topology->addNewPhysicalNodeAsChild(rootNode, fogTopNode2);

        TopologyNodePtr fogBottomNode1 = TopologyNode::create(4, "localhost", grpcPort, dataPort, 8);
        topology->addNewPhysicalNodeAsChild(fogTopNode1, fogBottomNode1);
        topology->addNewPhysicalNodeAsChild(fogTopNode2, fogBottomNode1);

        TopologyNodePtr fogBottomNode2 = TopologyNode::create(5, "localhost", grpcPort, dataPort, 8);
        topology->addNewPhysicalNodeAsChild(fogTopNode1, fogBottomNode2);
        topology->addNewPhysicalNodeAsChild(fogTopNode2, fogBottomNode2);

        TopologyNodePtr fogBottomNode3 = TopologyNode::create(6, "localhost", grpcPort, dataPort, 8);
        topology->addNewPhysicalNodeAsChild(fogTopNode1, fogBottomNode3);
        topology->addNewPhysicalNodeAsChild(fogTopNode2, fogBottomNode3);

        TopologyNodePtr fogBottomNode4 = TopologyNode::create(7, "localhost", grpcPort, dataPort, 8);
        topology->addNewPhysicalNodeAsChild(fogTopNode1, fogBottomNode4);
        topology->addNewPhysicalNodeAsChild(fogTopNode2, fogBottomNode4);

        TopologyNodePtr sourceNode1 = TopologyNode::create(8, "localhost", grpcPort, dataPort, 4);
        topology->addNewPhysicalNodeAsChild(fogBottomNode1, sourceNode1);
        topology->addNewPhysicalNodeAsChild(fogBottomNode2, sourceNode1);

        TopologyNodePtr sourceNode2 = TopologyNode::create(9, "localhost", grpcPort, dataPort, 4);
        topology->addNewPhysicalNodeAsChild(fogBottomNode1, sourceNode2);
        topology->addNewPhysicalNodeAsChild(fogBottomNode2, sourceNode2);

        TopologyNodePtr sourceNode3 = TopologyNode::create(10, "localhost", grpcPort, dataPort, 4);
        topology->addNewPhysicalNodeAsChild(fogBottomNode1, sourceNode3);
        topology->addNewPhysicalNodeAsChild(fogBottomNode2, sourceNode3);

        TopologyNodePtr sourceNode4 = TopologyNode::create(11, "localhost", grpcPort, dataPort, 3);
        topology->addNewPhysicalNodeAsChild(fogBottomNode1, sourceNode4);
        topology->addNewPhysicalNodeAsChild(fogBottomNode2, sourceNode4);

        TopologyNodePtr sourceNode5 = TopologyNode::create(12, "localhost", grpcPort, dataPort, 4);
        topology->addNewPhysicalNodeAsChild(fogBottomNode1, sourceNode5);
        topology->addNewPhysicalNodeAsChild(fogBottomNode2, sourceNode5);

        TopologyNodePtr sourceNode6 = TopologyNode::create(13, "localhost", grpcPort, dataPort, 4);
        topology->addNewPhysicalNodeAsChild(fogBottomNode1, sourceNode6);
        topology->addNewPhysicalNodeAsChild(fogBottomNode2, sourceNode6);

        TopologyNodePtr sourceNode7 = TopologyNode::create(14, "localhost", grpcPort, dataPort, 4);
        topology->addNewPhysicalNodeAsChild(fogBottomNode3, sourceNode7);
        topology->addNewPhysicalNodeAsChild(fogBottomNode4, sourceNode7);

        TopologyNodePtr sourceNode8 = TopologyNode::create(15, "localhost", grpcPort, dataPort, 4);
        topology->addNewPhysicalNodeAsChild(fogBottomNode3, sourceNode8);
        topology->addNewPhysicalNodeAsChild(fogBottomNode4, sourceNode8);

        TopologyNodePtr sourceNode9 = TopologyNode::create(16, "localhost", grpcPort, dataPort, 4);
        topology->addNewPhysicalNodeAsChild(fogBottomNode3, sourceNode9);
        topology->addNewPhysicalNodeAsChild(fogBottomNode4, sourceNode9);

        TopologyNodePtr sourceNode10 = TopologyNode::create(17, "localhost", grpcPort, dataPort, 4);
        topology->addNewPhysicalNodeAsChild(fogBottomNode3, sourceNode10);
        topology->addNewPhysicalNodeAsChild(fogBottomNode4, sourceNode10);

        TopologyNodePtr sourceNode11 = TopologyNode::create(18, "localhost", grpcPort, dataPort, 4);
        topology->addNewPhysicalNodeAsChild(fogBottomNode3, sourceNode11);
        topology->addNewPhysicalNodeAsChild(fogBottomNode4, sourceNode11);

        TopologyNodePtr sourceNode12 = TopologyNode::create(19, "localhost", grpcPort, dataPort, 4);
        topology->addNewPhysicalNodeAsChild(fogBottomNode3, sourceNode12);
        topology->addNewPhysicalNodeAsChild(fogBottomNode4, sourceNode12);

        LinkPropertyPtr linkProperty3 = std::make_shared<LinkProperty>(LinkProperty(100, 64));
        fogTopNode1->addLinkProperty(rootNode, linkProperty3);
        rootNode->addLinkProperty(fogTopNode1, linkProperty3);
        fogTopNode2->addLinkProperty(rootNode, linkProperty3);
        rootNode->addLinkProperty(fogTopNode2, linkProperty3);

        LinkPropertyPtr linkProperty2 = std::make_shared<LinkProperty>(LinkProperty(50, 256));
        fogTopNode1->addLinkProperty(fogBottomNode1, linkProperty2);
        fogBottomNode1->addLinkProperty(fogTopNode1, linkProperty2);
        fogTopNode1->addLinkProperty(fogBottomNode2, linkProperty2);
        fogBottomNode2->addLinkProperty(fogTopNode1, linkProperty2);

        fogTopNode2->addLinkProperty(fogBottomNode1, linkProperty2);
        fogBottomNode1->addLinkProperty(fogTopNode2, linkProperty2);
        fogTopNode2->addLinkProperty(fogBottomNode2, linkProperty2);
        fogBottomNode2->addLinkProperty(fogTopNode2, linkProperty2);

        fogTopNode1->addLinkProperty(fogBottomNode3, linkProperty2);
        fogBottomNode3->addLinkProperty(fogTopNode1, linkProperty2);
        fogTopNode1->addLinkProperty(fogBottomNode4, linkProperty2);
        fogBottomNode4->addLinkProperty(fogTopNode1, linkProperty2);

        fogTopNode2->addLinkProperty(fogBottomNode3, linkProperty2);
        fogBottomNode3->addLinkProperty(fogTopNode2, linkProperty2);
        fogTopNode2->addLinkProperty(fogBottomNode4, linkProperty2);
        fogBottomNode4->addLinkProperty(fogTopNode2, linkProperty2);

        LinkPropertyPtr linkProperty1 = std::make_shared<LinkProperty>(LinkProperty(10, 512));
        sourceNode1->addLinkProperty(fogBottomNode1, linkProperty1);
        fogBottomNode1->addLinkProperty(sourceNode1, linkProperty1);
        sourceNode1->addLinkProperty(fogBottomNode2, linkProperty1);
        fogBottomNode2->addLinkProperty(sourceNode1, linkProperty1);

        sourceNode2->addLinkProperty(fogBottomNode1, linkProperty1);
        fogBottomNode1->addLinkProperty(sourceNode2, linkProperty1);
        sourceNode2->addLinkProperty(fogBottomNode2, linkProperty1);
        fogBottomNode2->addLinkProperty(sourceNode2, linkProperty1);

        sourceNode3->addLinkProperty(fogBottomNode1, linkProperty1);
        fogBottomNode1->addLinkProperty(sourceNode3, linkProperty1);
        sourceNode3->addLinkProperty(fogBottomNode2, linkProperty1);
        fogBottomNode2->addLinkProperty(sourceNode3, linkProperty1);

        sourceNode4->addLinkProperty(fogBottomNode1, linkProperty1);
        fogBottomNode1->addLinkProperty(sourceNode4, linkProperty1);
        sourceNode4->addLinkProperty(fogBottomNode2, linkProperty1);
        fogBottomNode2->addLinkProperty(sourceNode4, linkProperty1);

        sourceNode5->addLinkProperty(fogBottomNode1, linkProperty1);
        fogBottomNode1->addLinkProperty(sourceNode5, linkProperty1);
        sourceNode5->addLinkProperty(fogBottomNode2, linkProperty1);
        fogBottomNode2->addLinkProperty(sourceNode5, linkProperty1);

        sourceNode6->addLinkProperty(fogBottomNode1, linkProperty1);
        fogBottomNode1->addLinkProperty(sourceNode6, linkProperty1);
        sourceNode6->addLinkProperty(fogBottomNode2, linkProperty1);
        fogBottomNode2->addLinkProperty(sourceNode6, linkProperty1);

        sourceNode7->addLinkProperty(fogBottomNode3, linkProperty1);
        fogBottomNode3->addLinkProperty(sourceNode7, linkProperty1);
        sourceNode7->addLinkProperty(fogBottomNode4, linkProperty1);
        fogBottomNode4->addLinkProperty(sourceNode7, linkProperty1);

        sourceNode8->addLinkProperty(fogBottomNode3, linkProperty1);
        fogBottomNode3->addLinkProperty(sourceNode8, linkProperty1);
        sourceNode8->addLinkProperty(fogBottomNode4, linkProperty1);
        fogBottomNode4->addLinkProperty(sourceNode8, linkProperty1);

        sourceNode9->addLinkProperty(fogBottomNode3, linkProperty1);
        fogBottomNode3->addLinkProperty(sourceNode9, linkProperty1);
        sourceNode9->addLinkProperty(fogBottomNode4, linkProperty1);
        fogBottomNode4->addLinkProperty(sourceNode9, linkProperty1);

        sourceNode10->addLinkProperty(fogBottomNode3, linkProperty1);
        fogBottomNode3->addLinkProperty(sourceNode10, linkProperty1);
        sourceNode10->addLinkProperty(fogBottomNode4, linkProperty1);
        fogBottomNode4->addLinkProperty(sourceNode10, linkProperty1);

        sourceNode11->addLinkProperty(fogBottomNode3, linkProperty1);
        fogBottomNode3->addLinkProperty(sourceNode11, linkProperty1);
        sourceNode11->addLinkProperty(fogBottomNode4, linkProperty1);
        fogBottomNode4->addLinkProperty(sourceNode11, linkProperty1);

        sourceNode12->addLinkProperty(fogBottomNode3, linkProperty1);
        fogBottomNode3->addLinkProperty(sourceNode12, linkProperty1);
        sourceNode12->addLinkProperty(fogBottomNode4, linkProperty1);
        fogBottomNode4->addLinkProperty(sourceNode12, linkProperty1);

        std::string schema = "Schema::create()->addField(\"id\", BasicType::UINT32)"
                             "->addField(\"value\", BasicType::UINT64);";
        const std::string streamName1 = "car";
        const std::string streamName2 = "truck";

        streamCatalog = std::make_shared<StreamCatalog>();
        streamCatalog->addLogicalStream(streamName1, schema);
        streamCatalog->addLogicalStream(streamName2, schema);

        SourceConfigPtr sourceConfig1 = SourceConfig::create();
        sourceConfig1->setSourceFrequency(0);
        sourceConfig1->setNumberOfTuplesToProducePerBuffer(0);
        sourceConfig1->setPhysicalStreamName("test3");
        sourceConfig1->setLogicalStreamName("car");

        SourceConfigPtr sourceConfig2 = SourceConfig::create();
        sourceConfig2->setSourceFrequency(0);
        sourceConfig2->setNumberOfTuplesToProducePerBuffer(0);
        sourceConfig2->setPhysicalStreamName("test4");
        sourceConfig2->setLogicalStreamName("truck");

        PhysicalStreamConfigPtr conf1 = PhysicalStreamConfig::create(sourceConfig1);
        PhysicalStreamConfigPtr conf2 = PhysicalStreamConfig::create(sourceConfig2);

        StreamCatalogEntryPtr streamCatalogEntry1 = std::make_shared<StreamCatalogEntry>(conf1, sourceNode1);
        StreamCatalogEntryPtr streamCatalogEntry2 = std::make_shared<StreamCatalogEntry>(conf2, sourceNode2);
        StreamCatalogEntryPtr streamCatalogEntry3 = std::make_shared<StreamCatalogEntry>(conf1, sourceNode3);
        StreamCatalogEntryPtr streamCatalogEntry4 = std::make_shared<StreamCatalogEntry>(conf1, sourceNode4);
        StreamCatalogEntryPtr streamCatalogEntry5 = std::make_shared<StreamCatalogEntry>(conf2, sourceNode5);
        StreamCatalogEntryPtr streamCatalogEntry6 = std::make_shared<StreamCatalogEntry>(conf1, sourceNode6);
        StreamCatalogEntryPtr streamCatalogEntry7 = std::make_shared<StreamCatalogEntry>(conf1, sourceNode7);
        StreamCatalogEntryPtr streamCatalogEntry8 = std::make_shared<StreamCatalogEntry>(conf1, sourceNode8);
        StreamCatalogEntryPtr streamCatalogEntry9 = std::make_shared<StreamCatalogEntry>(conf2, sourceNode9);
        StreamCatalogEntryPtr streamCatalogEntry10 = std::make_shared<StreamCatalogEntry>(conf2, sourceNode10);
        StreamCatalogEntryPtr streamCatalogEntry11 = std::make_shared<StreamCatalogEntry>(conf1, sourceNode11);
        StreamCatalogEntryPtr streamCatalogEntry12 = std::make_shared<StreamCatalogEntry>(conf1, sourceNode12);

        streamCatalog->addPhysicalStream("car", streamCatalogEntry1);
        streamCatalog->addPhysicalStream("truck", streamCatalogEntry2);
        streamCatalog->addPhysicalStream("car", streamCatalogEntry3);
        streamCatalog->addPhysicalStream("car", streamCatalogEntry4);
        streamCatalog->addPhysicalStream("truck", streamCatalogEntry5);
        streamCatalog->addPhysicalStream("car", streamCatalogEntry6);
        streamCatalog->addPhysicalStream("car", streamCatalogEntry7);
        streamCatalog->addPhysicalStream("car", streamCatalogEntry8);
        streamCatalog->addPhysicalStream("truck", streamCatalogEntry9);
        streamCatalog->addPhysicalStream("truck", streamCatalogEntry10);
        streamCatalog->addPhysicalStream("car", streamCatalogEntry11);
        streamCatalog->addPhysicalStream("car", streamCatalogEntry12);
    }

    StreamCatalogPtr streamCatalog;
    TopologyPtr topology;
};
 /*
// test the cost function
TEST_F(GeneticAlgorithmStrategyTest, testPlacingQueryWithGeneticAlgorithmStrategy) {

    setupTopologyAndStreamCatalogForGA();

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);
    auto placementStrategy = Optimizer::PlacementStrategyFactory::getStrategy("GeneticAlgorithm",
                                                                              globalExecutionPlan,
                                                                              topology,
                                                                              typeInferencePhase,
                                                                              streamCatalog);
    //////////////////////
    auto sourceOperator = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("car"));
    auto filterOperator = LogicalOperatorFactory::createFilterOperator(Attribute("id") < 45);
    auto mapOperator = LogicalOperatorFactory::createMapOperator(Attribute("id") = Attribute("newId") * 2);
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    auto sinkOperator = LogicalOperatorFactory::createSinkOperator(printSinkDescriptor);

    sinkOperator->addChild(mapOperator);
    mapOperator->addChild(filterOperator);
    filterOperator->addChild(sourceOperator);

    QueryPlanPtr queryPlan = QueryPlan::create();
    queryPlan->addRootOperator(sinkOperator);
    ////////////////////////////////////////////////
    auto subQuery = Query::from("truck").filter(Attribute("id") < 45);

    Query query = Query::from("car")
        .filter(Attribute("id") < 45)
        .unionWith(&subQuery)
        .sink(PrintSinkDescriptor::create());

    QueryPlanPtr queryPlan = query.getQueryPlan();
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    queryPlan->setQueryId(queryId);

    NES_DEBUG("Query_Plan: " << queryPlan->toString());

    std::vector<std::map<std::string, std::any>> properties;

    // adding property of the source2
    std::map<std::string, std::any> src2Prop;
    src2Prop.insert(std::make_pair("load", 5));
    src2Prop.insert(std::make_pair("dmf", 5.0));

    // adding property of the filter2
    std::map<std::string, std::any> filter2Prop;
    filter2Prop.insert(std::make_pair("load", 5));
    filter2Prop.insert(std::make_pair("dmf", 0.5));

    // adding property of the source1
    std::map<std::string, std::any> src1Prop;
    src1Prop.insert(std::make_pair("load", 4));
    src1Prop.insert(std::make_pair("dmf", 4.0));

    // adding property of the filter1Prop
    std::map<std::string, std::any> filter1Prop;
    filter1Prop.insert(std::make_pair("load", 4));
    filter1Prop.insert(std::make_pair("dmf", 4.0));

    // adding property of the union
    std::map<std::string, std::any> unionProp;
    unionProp.insert(std::make_pair("load", 3));
    unionProp.insert(std::make_pair("dmf", 2.0));

    // adding property of the sink
    std::map<std::string, std::any> sinkProp;
    sinkProp.insert(std::make_pair("load", 3));
    sinkProp.insert(std::make_pair("dmf", 2.0));

    properties.push_back(sinkProp);
    properties.push_back(unionProp);
    properties.push_back(filter1Prop);
    properties.push_back(src1Prop);
    properties.push_back(filter2Prop);
    properties.push_back(src2Prop);

    UtilityFunctions::assignPropertiesToQueryOperators(queryPlan, properties);
    NES_DEBUG("Query_Plan: " << queryPlan->toString());
    auto queryOperators = QueryPlanIterator(queryPlan).snapshot();
    std::cout << queryPlan->toString();
    placementStrategy->updateGlobalExecutionPlan(queryPlan);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);
    NES_DEBUG("Query_Plan: " << queryPlan->toString());
}
*/

TEST_F(GeneticAlgorithmStrategyTest, testPlacingQueryWithGeneticAlgorithmStrategy) {
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

    streamCatalog = std::make_shared<StreamCatalog>();
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

    properties.push_back(srcProp);
    properties.push_back(filterProp);
    properties.push_back(sinkProp);

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

    bool m = UtilityFunctions::assignPropertiesToQueryOperators(testQueryPlan, properties);
    std::cout <<m;

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
