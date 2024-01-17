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
#include <Catalogs/Query/QueryCatalog.hpp>
#include <Catalogs/Query/QueryCatalogEntry.hpp>
#include <Catalogs/Query/QueryCatalogService.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Coordinator/OptimizerConfiguration.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Network/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Network/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Network/NodeLocation.hpp>
#include <Operators/LogicalOperators/ProjectionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/NullOutputSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/CsvSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/UnionLogicalOperatorNode.hpp>
#include <Optimizer/Exceptions/GlobalQueryPlanUpdateException.hpp>
#include <Optimizer/Phases/QueryPlacementAmendmentPhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/RequestTypes/QueryRequests/AddQueryRequest.hpp>
#include <Optimizer/RequestTypes/QueryRequests/StopQueryRequest.hpp>
#include <Optimizer/RequestTypes/TopologyRequests/RemoveTopologyLinkRequest.hpp>
#include <Optimizer/RequestTypes/TopologyRequests/RemoveTopologyNodeRequest.hpp>
#include <Phases/GlobalQueryPlanUpdatePhase.hpp>
#include <Plans/ChangeLog/ChangeLog.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <RequestProcessor/RequestTypes/TopologyChangeRequest.hpp>
#include <RequestProcessor/StorageHandles/SerialStorageHandler.hpp>
#include <RequestProcessor/StorageHandles/StorageDataStructures.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Mobility/SpatialType.hpp>
#include <Util/Placement/PlacementConstants.hpp>
#include <gtest/gtest.h>
#include <z3++.h>

uint32_t EVENT_CHANNEL_RETRY_TIMES = 1;
auto WAIT_TIME = std::chrono::milliseconds(1);

uint32_t DATA_CHANNEL_RETRY_TIMES = 1;
uint64_t DEFAULT_NUMBER_OF_ORIGINS = 1;
namespace NES {

class TopologyChangeRequestTest : public Testing::BaseUnitTest {
  public:
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    Catalogs::Query::QueryCatalogPtr queryCatalog;
    QueryCatalogServicePtr queryCatalogService;
    Catalogs::UDF::UDFCatalogPtr udfCatalog;
    TopologyPtr topology;
    Optimizer::GlobalExecutionPlanPtr globalExecutionPlan;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("TopologyChangeRequestTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup TopologyChangeRequestTest test case.");
    }

    /* Will be called before a  test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        context = std::make_shared<z3::context>();
        queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
        queryCatalogService = std::make_shared<QueryCatalogService>(queryCatalog);

        //Setup source catalog
        sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();

        //Setup topology
        topology = Topology::create();

        udfCatalog = Catalogs::UDF::UDFCatalog::create();
        globalExecutionPlan = Optimizer::GlobalExecutionPlan::create();
    }

    z3::ContextPtr context;
};

/**
* @brief In this test we execute query merger phase on a single invalid query plan.
*/
TEST_F(TopologyChangeRequestTest, testFindUpstreamNetworkSource) {
    //Coordinator configuration
    const auto globalQueryPlan = GlobalQueryPlan::create();
    auto coordinatorConfig = Configurations::CoordinatorConfiguration::createDefault();
    auto optimizerConfiguration = Configurations::OptimizerConfiguration();
    optimizerConfiguration.queryMergerRule = Optimizer::QueryMergerRule::SyntaxBasedCompleteQueryMergerRule;
    coordinatorConfig->optimizer = optimizerConfiguration;

    auto sharedQueryId = 1;
    auto worker1Id = 1;
    topology->registerTopologyNode(worker1Id, "localhost", 123, 124, 1, {});
    auto executionNode1 = Optimizer::ExecutionNode::createExecutionNode(topology->getCopyOfTopologyNodeWithId(worker1Id));

    auto worker2Id = 2;
    topology->registerTopologyNode(worker2Id, "localhost", 123, 124, 1, {});
    auto executionNode2 = Optimizer::ExecutionNode::createExecutionNode(topology->getCopyOfTopologyNodeWithId(worker2Id));

    globalExecutionPlan->addExecutionNode(executionNode1);
    globalExecutionPlan->addExecutionNode(executionNode2);

    auto schema = Schema::create()->addField(createField("value", BasicType::UINT64));
    auto planId1 = 1;
    auto planId2 = 2;
    auto sourceId1 = 1;
    auto sinkId1 = 2;
    auto version = 0;
    auto plan1 = DecomposedQueryPlan::create(sharedQueryId, planId1);
    auto sinkLocationWrk2 = NES::Network::NodeLocation(worker2Id, "localhost", 124);
    auto networkSourceWrk1Partition = NES::Network::NesPartition(sharedQueryId, sourceId1, 0, 0);
    auto uniqueId = 1;
    auto networkSourceDescriptorWrk1 = Network::NetworkSourceDescriptor::create(schema,
                                                                                networkSourceWrk1Partition,
                                                                                sinkLocationWrk2,
                                                                                WAIT_TIME,
                                                                                EVENT_CHANNEL_RETRY_TIMES,
                                                                                version, uniqueId);
    uniqueId++;
    auto sourceOperatorNodeWrk1 = std::make_shared<SourceLogicalOperatorNode>(networkSourceDescriptorWrk1, sourceId1);
    plan1->addRootOperator(sourceOperatorNodeWrk1);
    executionNode1->registerNewDecomposedQueryPlan(sharedQueryId, plan1);

    auto sourceLocationWrk1 = NES::Network::NodeLocation(worker1Id, "localhost", 124);
    auto plan2 = DecomposedQueryPlan::create(sharedQueryId, planId2);
    auto networkSinkDescriptor2 = Network::NetworkSinkDescriptor::create(sourceLocationWrk1,
                                                                         networkSourceWrk1Partition,
                                                                         WAIT_TIME,
                                                                         DATA_CHANNEL_RETRY_TIMES,
                                                                         version,
                                                                         DEFAULT_NUMBER_OF_ORIGINS,
                                                                         sinkId1);
    auto networkSinkOperatorNodeWrk2 = std::make_shared<SinkLogicalOperatorNode>(networkSinkDescriptor2, worker2Id);
    plan2->addRootOperator(networkSinkOperatorNodeWrk2);

    executionNode2->registerNewDecomposedQueryPlan(sharedQueryId, plan2);

    auto [sinkOperator, id2] = RequestProcessor::Experimental::TopologyChangeRequest::findUpstreamNetworkSinkAndWorkerId(
        sharedQueryId,
        worker1Id,
        std::static_pointer_cast<Network::NetworkSourceDescriptor>(networkSourceDescriptorWrk1),
        globalExecutionPlan);
    ASSERT_EQ(sinkOperator, networkSinkOperatorNodeWrk2);
    ASSERT_EQ(id2, worker2Id);

    auto [sourceOperator, id1] = RequestProcessor::Experimental::TopologyChangeRequest::findDownstreamNetworkSourceAndWorkerId(
        sharedQueryId,
        worker2Id,
        std::static_pointer_cast<Network::NetworkSinkDescriptor>(networkSinkDescriptor2),
        globalExecutionPlan);
    ASSERT_EQ(sourceOperator, sourceOperatorNodeWrk1);
    ASSERT_EQ(id1, worker1Id);

    auto linkedSinkSourcePairs = RequestProcessor::Experimental::TopologyChangeRequest::findNetworkOperatorsForLink(sharedQueryId, executionNode2, executionNode1);
    auto [upstreamSink, downstreamSource] = linkedSinkSourcePairs.front();
    ASSERT_EQ(upstreamSink, networkSinkOperatorNodeWrk2);
    ASSERT_EQ(downstreamSource, sourceOperatorNodeWrk1);
}

TEST_F(TopologyChangeRequestTest, testFindingIncrementalUpstreamAndDownstream) {
    const auto globalQueryPlan = GlobalQueryPlan::create();
    auto coordinatorConfig = Configurations::CoordinatorConfiguration::createDefault();
    auto optimizerConfiguration = Configurations::OptimizerConfiguration();
    optimizerConfiguration.queryMergerRule = Optimizer::QueryMergerRule::SyntaxBasedCompleteQueryMergerRule;
    coordinatorConfig->optimizer = optimizerConfiguration;

    RequestProcessor::StorageDataStructures storageDataStructures(coordinatorConfig,
                                                                  topology,
                                                                  globalExecutionPlan,
                                                                  queryCatalogService,
                                                                  globalQueryPlan,
                                                                  sourceCatalog,
                                                                  udfCatalog);

    auto storageHandler = RequestProcessor::SerialStorageHandler::create(storageDataStructures);


    uint8_t maxRetries = 1;
    RequestProcessor::Experimental::TopologyChangeRequestPtr topologyChangeRequest;
    topologyChangeRequest =
        RequestProcessor::Experimental::TopologyChangeRequest::create({{8888, 9999}}, {{7777, 6666}}, maxRetries);

    auto schema = Schema::create()->addField(createField("value", BasicType::UINT64));
    WorkerId workerIdCounter = 1;
    const SharedQueryId sharedQueryId = 1;
    const uint64_t version = 0;
    DecomposedQueryPlanId subPlanId = 1;
    Network::NodeLocation sinkLocation;
    Network::NodeLocation sourceLocation;
    SourceDescriptorPtr networkSourceDescriptor;
    SourceLogicalOperatorNodePtr sourceLogicalOperatorNode;
    SinkDescriptorPtr networkSinkDescriptor;
    SinkLogicalOperatorNodePtr sinkLogicalOperatorNode;
    Optimizer::ExecutionNodePtr executionNode;
    LogicalUnaryOperatorNodePtr unaryOperatorNode;
    LogicalBinaryOperatorNodePtr binaryOperatorNode;
    auto pred1 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "1"));

    int64_t restPort = 123;
    int64_t dataPort = 124;
    std::string workerAddress = "localhost";
    std::string outputFileName = "dummy.out";
    std::string inputFileName = "dummy.in";
    DecomposedQueryPlanPtr subPlan;

    //root node
    //id = 1
    topology->registerTopologyNode(workerIdCounter, workerAddress, restPort, dataPort, 1, {});
    topology->setRootTopologyNodeId(workerIdCounter);
    executionNode = Optimizer::ExecutionNode::createExecutionNode(topology->getCopyOfTopologyNodeWithId(workerIdCounter));
    globalExecutionPlan->addExecutionNode(executionNode);
    workerIdCounter++;

    //id = 2
    topology->registerTopologyNode(workerIdCounter, workerAddress, restPort, dataPort, 1, {});
    topology->addTopologyNodeAsChild(1, workerIdCounter);
    executionNode = Optimizer::ExecutionNode::createExecutionNode(topology->getCopyOfTopologyNodeWithId(workerIdCounter));
    globalExecutionPlan->addExecutionNode(executionNode);
    workerIdCounter++;

    //id = 3
    topology->registerTopologyNode(workerIdCounter, workerAddress, restPort, dataPort, 1, {});
    topology->addTopologyNodeAsChild(2, workerIdCounter);
    executionNode = Optimizer::ExecutionNode::createExecutionNode(topology->getCopyOfTopologyNodeWithId(workerIdCounter));
    globalExecutionPlan->addExecutionNode(executionNode);
    workerIdCounter++;

    //id = 4
    topology->registerTopologyNode(workerIdCounter, workerAddress, restPort, dataPort, 1, {});
    topology->addTopologyNodeAsChild(2, workerIdCounter);
    executionNode = Optimizer::ExecutionNode::createExecutionNode(topology->getCopyOfTopologyNodeWithId(workerIdCounter));
    globalExecutionPlan->addExecutionNode(executionNode);
    workerIdCounter++;

    //id = 5
    topology->registerTopologyNode(workerIdCounter, workerAddress, restPort, dataPort, 1, {});
    topology->addTopologyNodeAsChild(3, workerIdCounter);
    executionNode = Optimizer::ExecutionNode::createExecutionNode(topology->getCopyOfTopologyNodeWithId(workerIdCounter));
    globalExecutionPlan->addExecutionNode(executionNode);
    workerIdCounter++;

    //id = 6
    topology->registerTopologyNode(workerIdCounter, workerAddress, restPort, dataPort, 1, {});
    topology->addTopologyNodeAsChild(4, workerIdCounter);
    executionNode = Optimizer::ExecutionNode::createExecutionNode(topology->getCopyOfTopologyNodeWithId(workerIdCounter));
    globalExecutionPlan->addExecutionNode(executionNode);
    workerIdCounter++;

    //id = 7
    topology->registerTopologyNode(workerIdCounter, workerAddress, restPort, dataPort, 1, {});
    topology->addTopologyNodeAsChild(5, workerIdCounter);
    topology->addTopologyNodeAsChild(6, workerIdCounter);
    executionNode = Optimizer::ExecutionNode::createExecutionNode(topology->getCopyOfTopologyNodeWithId(workerIdCounter));
    globalExecutionPlan->addExecutionNode(executionNode);
    workerIdCounter++;

    //id = 8
    topology->registerTopologyNode(workerIdCounter, workerAddress, restPort, dataPort, 1, {});
    topology->addTopologyNodeAsChild(6, workerIdCounter);
    executionNode = Optimizer::ExecutionNode::createExecutionNode(topology->getCopyOfTopologyNodeWithId(workerIdCounter));
    globalExecutionPlan->addExecutionNode(executionNode);
    workerIdCounter++;

    //id = 9
    topology->registerTopologyNode(workerIdCounter, workerAddress, restPort, dataPort, 1, {});
    topology->addTopologyNodeAsChild(3, workerIdCounter);
    executionNode = Optimizer::ExecutionNode::createExecutionNode(topology->getCopyOfTopologyNodeWithId(workerIdCounter));
    globalExecutionPlan->addExecutionNode(executionNode);

    std::cout << topology->toString() << std::endl;


    auto innerSharedQueryPlan = QueryPlan::create();

    subPlan = DecomposedQueryPlan::create(subPlanId, sharedQueryId);
    auto fileSinkOperatorId = getNextOperatorId();
    auto fileSinkDescriptor = FileSinkDescriptor::create(outputFileName, "CSV_FORMAT", "APPEND");
    auto fileSinkOperatorNode = std::make_shared<SinkLogicalOperatorNode>(fileSinkDescriptor, fileSinkOperatorId);
    WorkerId pinnedId = 1;
    fileSinkOperatorNode->addProperty(Optimizer::PINNED_WORKER_ID, pinnedId);
    subPlan->addRootOperator(fileSinkOperatorNode);
    auto networkSourceId = getNextOperatorId();
    auto nesPartition = Network::NesPartition(sharedQueryId, networkSourceId, 0, 0);
    auto networkSinkHostWorkerId = 2;
    auto uniqueId = 1;
    networkSourceDescriptor = Network::NetworkSourceDescriptor::create(schema,
                                                                       nesPartition,
                                                                       Network::NodeLocation(networkSinkHostWorkerId, workerAddress, dataPort),
                                                                       WAIT_TIME,
                                                                       EVENT_CHANNEL_RETRY_TIMES,
                                                                       version, uniqueId);
    uniqueId++;
    sourceLogicalOperatorNode = std::make_shared<SourceLogicalOperatorNode>(networkSourceDescriptor, networkSourceId);
    fileSinkOperatorNode->addChild(sourceLogicalOperatorNode);
    globalExecutionPlan->getExecutionNodeById(1)->registerNewDecomposedQueryPlan(sharedQueryId, subPlan);
    subPlanId++;

    auto networkSinkId = getNextOperatorId();
    //sub plan on node 2
    subPlan = DecomposedQueryPlan::create(subPlanId, sharedQueryId);
    networkSinkDescriptor = Network::NetworkSinkDescriptor::create(Network::NodeLocation(1, workerAddress, dataPort),
                                                                   nesPartition,
                                                                   WAIT_TIME,
                                                                   DATA_CHANNEL_RETRY_TIMES,
                                                                   version,
                                                                   DEFAULT_NUMBER_OF_ORIGINS,
                                                                   networkSinkId);
    sinkLogicalOperatorNode = std::make_shared<SinkLogicalOperatorNode>(networkSinkDescriptor, networkSinkId);
    subPlan->addRootOperator(sinkLogicalOperatorNode);
    auto unaryOperatorId = getNextOperatorId();
    unaryOperatorNode = LogicalOperatorFactory::createFilterOperator(pred1, unaryOperatorId);
    pinnedId = 2;
    auto copiedUnaryOperator = unaryOperatorNode->copy();
    copiedUnaryOperator->addProperty(Optimizer::PINNED_WORKER_ID, pinnedId);
    innerSharedQueryPlan->addRootOperator(copiedUnaryOperator);
    sinkLogicalOperatorNode->addChild(unaryOperatorNode);
    networkSourceId = getNextOperatorId();
    nesPartition = Network::NesPartition(sharedQueryId, networkSourceId, 0, 0);
    networkSourceDescriptor = Network::NetworkSourceDescriptor::create(schema,
                                                                       nesPartition,
                                                                       Network::NodeLocation(3, workerAddress, dataPort),
                                                                       WAIT_TIME,
                                                                       EVENT_CHANNEL_RETRY_TIMES,
                                                                       version, uniqueId);
    uniqueId++;
    sourceLogicalOperatorNode = std::make_shared<SourceLogicalOperatorNode>(networkSourceDescriptor, networkSourceId);
    unaryOperatorNode->addChild(sourceLogicalOperatorNode);
    globalExecutionPlan->getExecutionNodeById(2)->registerNewDecomposedQueryPlan(sharedQueryId, subPlan);
    subPlanId++;

    //test link
    auto linkedSinkSourcePairs = topologyChangeRequest->findNetworkOperatorsForLink(sharedQueryId,
                                                                                    globalExecutionPlan->getExecutionNodeById(2),
                                                                                    globalExecutionPlan->getExecutionNodeById(1));
    ASSERT_EQ(linkedSinkSourcePairs.size(), 1);
    auto [upstreamSink, downstreamSource] = linkedSinkSourcePairs.front();
    ASSERT_EQ(upstreamSink, sinkLogicalOperatorNode);
    ASSERT_EQ(downstreamSource, globalExecutionPlan->getExecutionNodeById(1)->getAllDecomposedQueryPlans(sharedQueryId).front()->getSourceOperators().front());

    networkSinkId = getNextOperatorId();
    //sub plan on node 3
    subPlan = DecomposedQueryPlan::create(subPlanId, sharedQueryId);
    networkSinkDescriptor = Network::NetworkSinkDescriptor::create(Network::NodeLocation(2, workerAddress, dataPort),
                                                                   nesPartition,
                                                                   WAIT_TIME,
                                                                   DATA_CHANNEL_RETRY_TIMES,
                                                                   version,
                                                                   DEFAULT_NUMBER_OF_ORIGINS,
                                                                   networkSinkId);
    sinkLogicalOperatorNode = std::make_shared<SinkLogicalOperatorNode>(networkSinkDescriptor, networkSinkId);
    subPlan->addRootOperator(sinkLogicalOperatorNode);
    auto binaryOperatorId = getNextOperatorId();
    binaryOperatorNode = LogicalOperatorFactory::createUnionOperator(binaryOperatorId);
    pinnedId = 3;
    auto copiedBinaryOperator = binaryOperatorNode->copy();
    copiedBinaryOperator->addProperty(Optimizer::PINNED_WORKER_ID, pinnedId);
    copiedUnaryOperator->addChild(copiedBinaryOperator);
    sinkLogicalOperatorNode->addChild(binaryOperatorNode);
    //network source left
    networkSourceId = getNextOperatorId();
    nesPartition = Network::NesPartition(sharedQueryId, networkSourceId, 0, 0);
    networkSourceDescriptor = Network::NetworkSourceDescriptor::create(schema,
                                                                       nesPartition,
                                                                       Network::NodeLocation(6, workerAddress, dataPort),
                                                                       WAIT_TIME,
                                                                       EVENT_CHANNEL_RETRY_TIMES,
                                                                       version, uniqueId);
    uniqueId++;
    auto leftsourceLogicalOperatorNode = std::make_shared<SourceLogicalOperatorNode>(networkSourceDescriptor, networkSourceId);
    binaryOperatorNode->addChild(leftsourceLogicalOperatorNode);
    //network source right
    networkSourceId = getNextOperatorId();
    nesPartition = Network::NesPartition(sharedQueryId, networkSourceId, 0, 0);
    networkSourceDescriptor = Network::NetworkSourceDescriptor::create(schema,
                                                                       nesPartition,
                                                                       Network::NodeLocation(6, workerAddress, dataPort),
                                                                       WAIT_TIME,
                                                                       EVENT_CHANNEL_RETRY_TIMES,
                                                                       version, uniqueId);
    uniqueId++;
    auto rightsourceLogicalOperatorNode = std::make_shared<SourceLogicalOperatorNode>(networkSourceDescriptor, networkSourceId);
    binaryOperatorNode->addChild(rightsourceLogicalOperatorNode);
    globalExecutionPlan->getExecutionNodeById(3)->registerNewDecomposedQueryPlan(sharedQueryId, subPlan);
    subPlanId++;

    networkSinkId = getNextOperatorId();
    //first sub plan on node 6
    subPlan = DecomposedQueryPlan::create(subPlanId, sharedQueryId);
    networkSinkDescriptor = Network::NetworkSinkDescriptor::create(Network::NodeLocation(3, workerAddress, dataPort),
                                                                   leftsourceLogicalOperatorNode->getSourceDescriptor()->as<Network::NetworkSourceDescriptor>()->getNesPartition(),
                                                                   WAIT_TIME,
                                                                   DATA_CHANNEL_RETRY_TIMES,
                                                                   version,
                                                                   DEFAULT_NUMBER_OF_ORIGINS,
                                                                   networkSinkId);
    sinkLogicalOperatorNode = std::make_shared<SinkLogicalOperatorNode>(networkSinkDescriptor, networkSinkId);
    subPlan->addRootOperator(sinkLogicalOperatorNode);
    networkSourceId = getNextOperatorId();
    nesPartition = Network::NesPartition(sharedQueryId, networkSourceId, 0, 0);
    networkSourceDescriptor = Network::NetworkSourceDescriptor::create(schema,
                                                                       nesPartition,
                                                                       Network::NodeLocation(7, workerAddress, dataPort),
                                                                       WAIT_TIME,
                                                                       EVENT_CHANNEL_RETRY_TIMES,
                                                                       version, uniqueId);
    uniqueId++;
    sourceLogicalOperatorNode = std::make_shared<SourceLogicalOperatorNode>(networkSourceDescriptor, networkSourceId);
    sinkLogicalOperatorNode->addChild(sourceLogicalOperatorNode);
    globalExecutionPlan->getExecutionNodeById(6)->registerNewDecomposedQueryPlan(sharedQueryId, subPlan);
    subPlanId++;

    networkSinkId = getNextOperatorId();
    //sub plan on node 7
    subPlan = DecomposedQueryPlan::create(subPlanId, sharedQueryId);
    networkSinkDescriptor = Network::NetworkSinkDescriptor::create(Network::NodeLocation(6, workerAddress, dataPort),
                                                                   nesPartition,
                                                                   WAIT_TIME,
                                                                   DATA_CHANNEL_RETRY_TIMES,
                                                                   version,
                                                                   DEFAULT_NUMBER_OF_ORIGINS,
                                                                   networkSinkId);
    sinkLogicalOperatorNode = std::make_shared<SinkLogicalOperatorNode>(networkSinkDescriptor, networkSinkId);
    subPlan->addRootOperator(sinkLogicalOperatorNode);
    auto defaultSourceIdLeft = getNextOperatorId();
    auto csvSourceType = CSVSourceType::create("physicalName", "logicalName");
    auto defaultSourcedescriptor = CsvSourceDescriptor::create(schema, csvSourceType);
    auto defaultSourceLeft = std::make_shared<SourceLogicalOperatorNode>(defaultSourcedescriptor, defaultSourceIdLeft);
    pinnedId = 7;
    auto copiedDefaultSourceLeft = defaultSourceLeft->copy();
    copiedDefaultSourceLeft->addProperty(Optimizer::PINNED_WORKER_ID, pinnedId);
    copiedBinaryOperator->addChild(copiedDefaultSourceLeft);
    sinkLogicalOperatorNode->addChild(defaultSourceLeft);
    globalExecutionPlan->getExecutionNodeById(7)->registerNewDecomposedQueryPlan(sharedQueryId, subPlan);
    subPlanId++;

    networkSinkId = getNextOperatorId();
    //second sub plan on node 6
    subPlan = DecomposedQueryPlan::create(subPlanId, sharedQueryId);
    networkSinkDescriptor = Network::NetworkSinkDescriptor::create(Network::NodeLocation(3, workerAddress, dataPort),
                                                                   rightsourceLogicalOperatorNode->getSourceDescriptor()->as<Network::NetworkSourceDescriptor>()->getNesPartition(),
                                                                   WAIT_TIME,
                                                                   DATA_CHANNEL_RETRY_TIMES,
                                                                   version,
                                                                   DEFAULT_NUMBER_OF_ORIGINS,
                                                                   networkSinkId);
    sinkLogicalOperatorNode = std::make_shared<SinkLogicalOperatorNode>(networkSinkDescriptor, networkSinkId);
    subPlan->addRootOperator(sinkLogicalOperatorNode);
    networkSourceId = getNextOperatorId();
    nesPartition = Network::NesPartition(sharedQueryId, networkSourceId, 0, 0);
    networkSourceDescriptor = Network::NetworkSourceDescriptor::create(schema,
                                                                       nesPartition,
                                                                       Network::NodeLocation(8, workerAddress, dataPort),
                                                                       WAIT_TIME,
                                                                       EVENT_CHANNEL_RETRY_TIMES,
                                                                       version, uniqueId);
    uniqueId++;
    sourceLogicalOperatorNode = std::make_shared<SourceLogicalOperatorNode>(networkSourceDescriptor, networkSourceId);
    sinkLogicalOperatorNode->addChild(sourceLogicalOperatorNode);
    globalExecutionPlan->getExecutionNodeById(6)->registerNewDecomposedQueryPlan(sharedQueryId, subPlan);
    subPlanId++;

    networkSinkId = getNextOperatorId();
    //sub plan on node 7
    subPlan = DecomposedQueryPlan::create(subPlanId, sharedQueryId);
    networkSinkDescriptor = Network::NetworkSinkDescriptor::create(Network::NodeLocation(6, workerAddress, dataPort),
                                                                   nesPartition,
                                                                   WAIT_TIME,
                                                                   DATA_CHANNEL_RETRY_TIMES,
                                                                   version,
                                                                   DEFAULT_NUMBER_OF_ORIGINS,
                                                                   networkSinkId);
    sinkLogicalOperatorNode = std::make_shared<SinkLogicalOperatorNode>(networkSinkDescriptor, networkSinkId);
    subPlan->addRootOperator(sinkLogicalOperatorNode);
    auto defaultSourceIdRight = getNextOperatorId();
    auto defaultSourceRight = std::make_shared<SourceLogicalOperatorNode>(defaultSourcedescriptor, defaultSourceIdRight);
    pinnedId = 8;
    auto copiedDefaultSourceRight = defaultSourceRight->copy();
    copiedDefaultSourceRight->addProperty(Optimizer::PINNED_WORKER_ID, pinnedId);
    copiedBinaryOperator->addChild(copiedDefaultSourceRight);
    sinkLogicalOperatorNode->addChild(defaultSourceRight);
    globalExecutionPlan->getExecutionNodeById(8)->registerNewDecomposedQueryPlan(sharedQueryId, subPlan);
    subPlanId++;

    //additional operators on node 3
    //second network source on the right
    networkSourceId = getNextOperatorId();
    nesPartition = Network::NesPartition(sharedQueryId, networkSourceId, 0, 0);
    networkSourceDescriptor = Network::NetworkSourceDescriptor::create(schema,
                                                                       nesPartition,
                                                                       Network::NodeLocation(9, workerAddress, dataPort),
                                                                       WAIT_TIME,
                                                                       EVENT_CHANNEL_RETRY_TIMES,
                                                                       version, uniqueId);
    uniqueId++;
    auto secondRightsourceLogicalOperatorNode = std::make_shared<SourceLogicalOperatorNode>(networkSourceDescriptor, networkSourceId);
    binaryOperatorNode->addChild(secondRightsourceLogicalOperatorNode);



    networkSinkId = getNextOperatorId();
    //sub plan on node 8
    subPlan = DecomposedQueryPlan::create(subPlanId, sharedQueryId);
    networkSinkDescriptor = Network::NetworkSinkDescriptor::create(Network::NodeLocation(3, workerAddress, dataPort),
                                                                   nesPartition,
                                                                   WAIT_TIME,
                                                                   DATA_CHANNEL_RETRY_TIMES,
                                                                   version,
                                                                   DEFAULT_NUMBER_OF_ORIGINS,
                                                                   networkSinkId);
    sinkLogicalOperatorNode = std::make_shared<SinkLogicalOperatorNode>(networkSinkDescriptor, networkSinkId);
    subPlan->addRootOperator(sinkLogicalOperatorNode);
    auto secondDefaultSourceIdRight = getNextOperatorId();
    auto secondDefaultSourceRight =
        std::make_shared<SourceLogicalOperatorNode>(defaultSourcedescriptor, secondDefaultSourceIdRight);
    auto copiedSecondDefaultSourceRight = secondDefaultSourceRight->copy();
    copiedSecondDefaultSourceRight->addProperty(Optimizer::PINNED_WORKER_ID, pinnedId);
    copiedBinaryOperator->addChild(copiedSecondDefaultSourceRight);
    sinkLogicalOperatorNode->addChild(secondDefaultSourceRight);
    globalExecutionPlan->getExecutionNodeById(9)->registerNewDecomposedQueryPlan(sharedQueryId, subPlan);
    subPlanId++;

    //checks
    //node 1
    auto startWorker = 1;
    //file sink
    LogicalOperatorNodePtr startOperator = globalExecutionPlan->getExecutionNodeById(startWorker)
                                               ->getAllDecomposedQueryPlans(sharedQueryId)
                                               .front()
                                               ->getSinkOperators()
                                               .front();
    auto downstreamNonSystemOperator =
        topologyChangeRequest->findDownstreamNonSystemOperators(startOperator, startWorker, sharedQueryId, globalExecutionPlan);
    ASSERT_EQ(downstreamNonSystemOperator, startOperator);
    auto upstreamNonSystemOperator =
        topologyChangeRequest->findUpstreamNonSystemOperators(startOperator, startWorker, sharedQueryId, globalExecutionPlan);
    ASSERT_EQ(upstreamNonSystemOperator, startOperator);
    //network source
    startOperator = globalExecutionPlan->getExecutionNodeById(startWorker)
                        ->getAllDecomposedQueryPlans(sharedQueryId)
                        .front()
                        ->getSourceOperators()
                        .front();
    downstreamNonSystemOperator =
        topologyChangeRequest->findDownstreamNonSystemOperators(startOperator, startWorker, sharedQueryId, globalExecutionPlan);
    ASSERT_EQ(downstreamNonSystemOperator, fileSinkOperatorNode);
    upstreamNonSystemOperator =
        topologyChangeRequest->findUpstreamNonSystemOperators(startOperator, startWorker, sharedQueryId, globalExecutionPlan);
    ASSERT_EQ(upstreamNonSystemOperator, unaryOperatorNode);

    //node 2
    startWorker = 2;
    //network sink
    startOperator = globalExecutionPlan->getExecutionNodeById(startWorker)
                        ->getAllDecomposedQueryPlans(sharedQueryId)
                        .front()
                        ->getSinkOperators()
                        .front();
    upstreamNonSystemOperator =
        topologyChangeRequest->findDownstreamNonSystemOperators(startOperator, startWorker, sharedQueryId, globalExecutionPlan);
    ASSERT_EQ(upstreamNonSystemOperator, fileSinkOperatorNode);
    upstreamNonSystemOperator =
        topologyChangeRequest->findUpstreamNonSystemOperators(startOperator, startWorker, sharedQueryId, globalExecutionPlan);
    ASSERT_EQ(upstreamNonSystemOperator, unaryOperatorNode);
    //filter
    startOperator = unaryOperatorNode;
    downstreamNonSystemOperator =
        topologyChangeRequest->findDownstreamNonSystemOperators(startOperator, startWorker, sharedQueryId, globalExecutionPlan);
    ASSERT_EQ(downstreamNonSystemOperator, startOperator);
    upstreamNonSystemOperator =
        topologyChangeRequest->findUpstreamNonSystemOperators(startOperator, startWorker, sharedQueryId, globalExecutionPlan);
    ASSERT_EQ(upstreamNonSystemOperator, startOperator);
    //network source
    startOperator = globalExecutionPlan->getExecutionNodeById(startWorker)
                        ->getAllDecomposedQueryPlans(sharedQueryId)
                        .front()
                        ->getSourceOperators()
                        .front();
    downstreamNonSystemOperator =
        topologyChangeRequest->findDownstreamNonSystemOperators(startOperator, startWorker, sharedQueryId, globalExecutionPlan);
    ASSERT_EQ(downstreamNonSystemOperator, unaryOperatorNode);
    upstreamNonSystemOperator =
        topologyChangeRequest->findUpstreamNonSystemOperators(startOperator, startWorker, sharedQueryId, globalExecutionPlan);
    ASSERT_EQ(upstreamNonSystemOperator, binaryOperatorNode);

    //node3
    startWorker = 3;
    //network sink
    startOperator = globalExecutionPlan->getExecutionNodeById(startWorker)
                        ->getAllDecomposedQueryPlans(sharedQueryId)
                        .front()
                        ->getSinkOperators()
                        .front();
    upstreamNonSystemOperator =
        topologyChangeRequest->findDownstreamNonSystemOperators(startOperator, startWorker, sharedQueryId, globalExecutionPlan);
    ASSERT_EQ(upstreamNonSystemOperator, unaryOperatorNode);
    upstreamNonSystemOperator =
        topologyChangeRequest->findUpstreamNonSystemOperators(startOperator, startWorker, sharedQueryId, globalExecutionPlan);
    ASSERT_EQ(upstreamNonSystemOperator, binaryOperatorNode);
    //union
    startOperator = binaryOperatorNode;
    downstreamNonSystemOperator =
        topologyChangeRequest->findDownstreamNonSystemOperators(startOperator, startWorker, sharedQueryId, globalExecutionPlan);
    ASSERT_EQ(downstreamNonSystemOperator, startOperator);
    upstreamNonSystemOperator =
        topologyChangeRequest->findUpstreamNonSystemOperators(startOperator, startWorker, sharedQueryId, globalExecutionPlan);
    ASSERT_EQ(upstreamNonSystemOperator, startOperator);
    //left network source
    startOperator = leftsourceLogicalOperatorNode;
    downstreamNonSystemOperator =
        topologyChangeRequest->findDownstreamNonSystemOperators(startOperator, startWorker, sharedQueryId, globalExecutionPlan);
    ASSERT_EQ(downstreamNonSystemOperator, binaryOperatorNode);
    upstreamNonSystemOperator =
        topologyChangeRequest->findUpstreamNonSystemOperators(startOperator, startWorker, sharedQueryId, globalExecutionPlan);
    ASSERT_EQ(upstreamNonSystemOperator, defaultSourceLeft);
    //right network source
    startOperator = rightsourceLogicalOperatorNode;
    downstreamNonSystemOperator =
        topologyChangeRequest->findDownstreamNonSystemOperators(startOperator, startWorker, sharedQueryId, globalExecutionPlan);
    ASSERT_EQ(downstreamNonSystemOperator, binaryOperatorNode);
    upstreamNonSystemOperator =
        topologyChangeRequest->findUpstreamNonSystemOperators(startOperator, startWorker, sharedQueryId, globalExecutionPlan);
    ASSERT_EQ(upstreamNonSystemOperator, defaultSourceRight);
    //2nd right network source
    startOperator = secondRightsourceLogicalOperatorNode;
    downstreamNonSystemOperator =
        topologyChangeRequest->findDownstreamNonSystemOperators(startOperator, startWorker, sharedQueryId, globalExecutionPlan);
    ASSERT_EQ(downstreamNonSystemOperator, binaryOperatorNode);
    upstreamNonSystemOperator =
        topologyChangeRequest->findUpstreamNonSystemOperators(startOperator, startWorker, sharedQueryId, globalExecutionPlan);
    ASSERT_EQ(upstreamNonSystemOperator, secondDefaultSourceRight);


    auto sharedQueryPlan = SharedQueryPlan::create(innerSharedQueryPlan);

    auto [upstreamPinned, downStreamPinned] =
        topologyChangeRequest->findUpstreamAndDownstreamPinnedOperators(sharedQueryPlan,
                                                                        globalExecutionPlan->getExecutionNodeById(6),
                                                                        globalExecutionPlan->getExecutionNodeById(3),
                                                                        topology,
                                                                        globalExecutionPlan);
    ASSERT_EQ(upstreamPinned.size(), 3);
    ASSERT_TRUE(upstreamPinned.contains(13));
    ASSERT_TRUE(upstreamPinned.contains(17));
    ASSERT_TRUE(upstreamPinned.contains(20));
    ASSERT_EQ(downStreamPinned.size(), 1);
    ASSERT_TRUE(downStreamPinned.contains(4));
}
}// namespace NES
