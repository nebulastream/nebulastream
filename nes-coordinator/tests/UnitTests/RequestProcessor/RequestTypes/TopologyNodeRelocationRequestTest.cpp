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
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Coordinator/OptimizerConfiguration.hpp>
#include <Operators/LogicalOperators/LogicalUnionOperator.hpp>
#include <Operators/LogicalOperators/Network/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Network/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Network/NodeLocation.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/CsvSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Plans/ChangeLog/ChangeLog.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <RequestProcessor/StorageHandles/SerialStorageHandler.hpp>
#include <RequestProcessor/StorageHandles/StorageDataStructures.hpp>
#include <Util/IncrementalPlacementUtils.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Placement/PlacementConstants.hpp>
#include <gtest/gtest.h>
#include <z3++.h>

uint32_t EVENT_CHANNEL_RETRY_TIMES = 1;
auto WAIT_TIME = std::chrono::milliseconds(1);

uint32_t DATA_CHANNEL_RETRY_TIMES = 1;
uint64_t DEFAULT_NUMBER_OF_ORIGINS = 1;

namespace NES::RequestProcessor {

class TopologyNodeRelocationRequestTest : public Testing::BaseUnitTest {
  public:
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    Catalogs::Query::QueryCatalogPtr queryCatalog;
    Catalogs::UDF::UDFCatalogPtr udfCatalog;
    TopologyPtr topology;
    Optimizer::GlobalExecutionPlanPtr globalExecutionPlan;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("TopologyNodeRelocationRequestTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup TopologyNodeRelocationRequestTest test case.");
    }

    /* Will be called before a  test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        context = std::make_shared<z3::context>();
        queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();

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
* @brief Test the algorithm to identify the upstream and downstream operators used as an input for an incremental
* placement to be performed due to a topology link removal. Construct a topology, place query subplans and calculate
* the sets of upstream and downstream operators of an incremental placement.
*
*/
TEST_F(TopologyNodeRelocationRequestTest, testFindingIncrementalUpstreamAndDownstream) {
    const auto globalQueryPlan = GlobalQueryPlan::create();
    auto coordinatorConfig = Configurations::CoordinatorConfiguration::createDefault();
    auto optimizerConfiguration = Configurations::OptimizerConfiguration();
    optimizerConfiguration.queryMergerRule = Optimizer::QueryMergerRule::SyntaxBasedCompleteQueryMergerRule;
    coordinatorConfig->optimizer = optimizerConfiguration;

    RequestProcessor::StorageDataStructures storageDataStructures(coordinatorConfig,
                                                                  topology,
                                                                  globalExecutionPlan,
                                                                  globalQueryPlan,
                                                                  queryCatalog,
                                                                  sourceCatalog,
                                                                  udfCatalog);

    auto storageHandler = RequestProcessor::SerialStorageHandler::create(storageDataStructures);

    auto schema = Schema::create()->addField(createField("value", BasicType::UINT64));
    WorkerId workerIdCounter = 1;
    const SharedQueryId sharedQueryId = 1;
    const uint64_t version = 0;
    DecomposedQueryPlanId subPlanId = 1;
    Network::NodeLocation sinkLocation;
    Network::NodeLocation sourceLocation;
    SourceDescriptorPtr networkSourceDescriptor;
    SourceLogicalOperatorPtr sourceLogicalOperator;
    SinkDescriptorPtr networkSinkDescriptor;
    SinkLogicalOperatorPtr sinkLogicalOperator;
    Optimizer::ExecutionNodePtr executionNode;
    LogicalUnaryOperatorPtr unaryOperator;
    LogicalBinaryOperatorPtr binaryOperator;
    auto pred1 = ConstantValueExpressionNode::create(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), "1"));

    int64_t restPort = 123;
    int64_t dataPort = 124;
    std::string workerAddress = "localhost";
    std::string outputFileName = "dummy.out";
    std::string inputFileName = "dummy.in";
    DecomposedQueryPlanPtr decomposedQueryPlan;

    //root node
    //id = 1
    topology->registerWorker(workerIdCounter, workerAddress, restPort, dataPort, 1, {}, 0, 0);
    topology->addAsRootWorkerId(workerIdCounter);
    workerIdCounter++;

    //id = 2
    topology->registerWorker(workerIdCounter, workerAddress, restPort, dataPort, 1, {}, 0, 0);
    topology->addTopologyNodeAsChild(1, workerIdCounter);
    workerIdCounter++;

    //id = 3
    topology->registerWorker(workerIdCounter, workerAddress, restPort, dataPort, 1, {}, 0, 0);
    topology->addTopologyNodeAsChild(2, workerIdCounter);
    workerIdCounter++;

    //id = 4
    topology->registerWorker(workerIdCounter, workerAddress, restPort, dataPort, 1, {}, 0, 0);
    topology->addTopologyNodeAsChild(2, workerIdCounter);
    workerIdCounter++;

    //id = 5
    topology->registerWorker(workerIdCounter, workerAddress, restPort, dataPort, 1, {}, 0, 0);
    topology->addTopologyNodeAsChild(3, workerIdCounter);
    workerIdCounter++;

    //id = 6
    topology->registerWorker(workerIdCounter, workerAddress, restPort, dataPort, 1, {}, 0, 0);
    topology->addTopologyNodeAsChild(4, workerIdCounter);
    workerIdCounter++;

    //id = 7
    topology->registerWorker(workerIdCounter, workerAddress, restPort, dataPort, 1, {}, 0, 0);
    topology->addTopologyNodeAsChild(5, workerIdCounter);
    topology->addTopologyNodeAsChild(6, workerIdCounter);
    workerIdCounter++;

    //id = 8
    topology->registerWorker(workerIdCounter, workerAddress, restPort, dataPort, 1, {}, 0, 0);
    topology->addTopologyNodeAsChild(6, workerIdCounter);
    workerIdCounter++;

    //id = 9
    topology->registerWorker(workerIdCounter, workerAddress, restPort, dataPort, 1, {}, 0, 0);
    topology->addTopologyNodeAsChild(3, workerIdCounter);

    std::cout << topology->toString() << std::endl;

    auto innerSharedQueryPlan = QueryPlan::create();

    WorkerId pinnedId = 1;
    decomposedQueryPlan = DecomposedQueryPlan::create(subPlanId, sharedQueryId, pinnedId);
    auto fileSinkOperatorId = getNextOperatorId();
    auto fileSinkDescriptor = FileSinkDescriptor::create(outputFileName, "CSV_FORMAT", "APPEND");
    auto fileSinkOperator = std::make_shared<SinkLogicalOperator>(fileSinkDescriptor, fileSinkOperatorId);
    fileSinkOperator->addProperty(Optimizer::PINNED_WORKER_ID, pinnedId);
    decomposedQueryPlan->addRootOperator(fileSinkOperator);
    auto networkSourceId = getNextOperatorId();
    auto nesPartition = Network::NesPartition(sharedQueryId, networkSourceId, 0, 0);
    auto networkSinkHostWorkerId = 2;
    auto uniqueId = 1;
    networkSourceDescriptor =
        Network::NetworkSourceDescriptor::create(schema,
                                                 nesPartition,
                                                 Network::NodeLocation(networkSinkHostWorkerId, workerAddress, dataPort),
                                                 WAIT_TIME,
                                                 EVENT_CHANNEL_RETRY_TIMES,
                                                 version,
                                                 uniqueId);
    uniqueId++;
    sourceLogicalOperator = std::make_shared<SourceLogicalOperator>(networkSourceDescriptor, networkSourceId);
    sourceLogicalOperator->addProperty(Optimizer::DOWNSTREAM_LOGICAL_OPERATOR_ID, OperatorId{1});
    sourceLogicalOperator->addProperty(Optimizer::UPSTREAM_LOGICAL_OPERATOR_ID, OperatorId{4});
    fileSinkOperator->addChild(sourceLogicalOperator);
    auto lockedTopologyNode = topology->lockTopologyNode(pinnedId);
    globalExecutionPlan->addDecomposedQueryPlan(lockedTopologyNode, decomposedQueryPlan);
    lockedTopologyNode->unlock();
    subPlanId++;

    auto networkSinkId = getNextOperatorId();
    //sub plan on node 2
    pinnedId = 2;
    decomposedQueryPlan = DecomposedQueryPlan::create(subPlanId, sharedQueryId, pinnedId);
    networkSinkDescriptor = Network::NetworkSinkDescriptor::create(Network::NodeLocation(1, workerAddress, dataPort),
                                                                   nesPartition,
                                                                   WAIT_TIME,
                                                                   DATA_CHANNEL_RETRY_TIMES,
                                                                   version,
                                                                   DEFAULT_NUMBER_OF_ORIGINS,
                                                                   networkSinkId);
    sinkLogicalOperator = std::make_shared<SinkLogicalOperator>(networkSinkDescriptor, networkSinkId);
    sinkLogicalOperator->addProperty(Optimizer::DOWNSTREAM_LOGICAL_OPERATOR_ID, OperatorId{1});
    sinkLogicalOperator->addProperty(Optimizer::UPSTREAM_LOGICAL_OPERATOR_ID, OperatorId{4});
    decomposedQueryPlan->addRootOperator(sinkLogicalOperator);
    auto unaryOperatorId = getNextOperatorId();
    unaryOperator = LogicalOperatorFactory::createFilterOperator(pred1, unaryOperatorId);
    auto copiedUnaryOperator = unaryOperator->copy();
    copiedUnaryOperator->addProperty(Optimizer::PINNED_WORKER_ID, pinnedId);
    innerSharedQueryPlan->addRootOperator(copiedUnaryOperator);
    sinkLogicalOperator->addChild(unaryOperator);
    networkSourceId = getNextOperatorId();
    nesPartition = Network::NesPartition(sharedQueryId, networkSourceId, 0, 0);
    networkSourceDescriptor = Network::NetworkSourceDescriptor::create(schema,
                                                                       nesPartition,
                                                                       Network::NodeLocation(3, workerAddress, dataPort),
                                                                       WAIT_TIME,
                                                                       EVENT_CHANNEL_RETRY_TIMES,
                                                                       version,
                                                                       uniqueId);
    uniqueId++;
    sourceLogicalOperator = std::make_shared<SourceLogicalOperator>(networkSourceDescriptor, networkSourceId);
    sourceLogicalOperator->addProperty(Optimizer::DOWNSTREAM_LOGICAL_OPERATOR_ID, OperatorId{4});
    sourceLogicalOperator->addProperty(Optimizer::UPSTREAM_LOGICAL_OPERATOR_ID, OperatorId{7});
    unaryOperator->addChild(sourceLogicalOperator);
    lockedTopologyNode = topology->lockTopologyNode(pinnedId);
    globalExecutionPlan->addDecomposedQueryPlan(lockedTopologyNode, decomposedQueryPlan);
    lockedTopologyNode->unlock();
    subPlanId++;

    //test link
    auto linkedSinkSourcePairs = Experimental::findNetworkOperatorsForLink(sharedQueryId,
                                                                           globalExecutionPlan->getLockedExecutionNode(2),
                                                                           globalExecutionPlan->getLockedExecutionNode(1));
    ASSERT_EQ(linkedSinkSourcePairs.size(), 1);
    auto [upstreamSink, downstreamSource] = linkedSinkSourcePairs.front();
    ASSERT_EQ(upstreamSink, sinkLogicalOperator);
    ASSERT_EQ(downstreamSource,
              globalExecutionPlan->getLockedExecutionNode(1)
                  ->
                  operator*()
                  ->getAllDecomposedQueryPlans(sharedQueryId)
                  .front()
                  ->getSourceOperators()
                  .front());

    networkSinkId = getNextOperatorId();

    //sub plan on node 3
    pinnedId = 3;
    decomposedQueryPlan = DecomposedQueryPlan::create(subPlanId, sharedQueryId, pinnedId);
    networkSinkDescriptor = Network::NetworkSinkDescriptor::create(Network::NodeLocation(2, workerAddress, dataPort),
                                                                   nesPartition,
                                                                   WAIT_TIME,
                                                                   DATA_CHANNEL_RETRY_TIMES,
                                                                   version,
                                                                   DEFAULT_NUMBER_OF_ORIGINS,
                                                                   networkSinkId);
    sinkLogicalOperator = std::make_shared<SinkLogicalOperator>(networkSinkDescriptor, networkSinkId);
    sinkLogicalOperator->addProperty(Optimizer::DOWNSTREAM_LOGICAL_OPERATOR_ID, OperatorId{4});
    sinkLogicalOperator->addProperty(Optimizer::UPSTREAM_LOGICAL_OPERATOR_ID, OperatorId{7});
    decomposedQueryPlan->addRootOperator(sinkLogicalOperator);
    auto binaryOperatorId = getNextOperatorId();
    binaryOperator = LogicalOperatorFactory::createUnionOperator(binaryOperatorId);
    auto copiedBinaryOperator = binaryOperator->copy();
    copiedBinaryOperator->addProperty(Optimizer::PINNED_WORKER_ID, pinnedId);
    copiedUnaryOperator->addChild(copiedBinaryOperator);
    sinkLogicalOperator->addChild(binaryOperator);
    //network source left
    networkSourceId = getNextOperatorId();
    nesPartition = Network::NesPartition(sharedQueryId, networkSourceId, 0, 0);
    networkSourceDescriptor = Network::NetworkSourceDescriptor::create(schema,
                                                                       nesPartition,
                                                                       Network::NodeLocation(6, workerAddress, dataPort),
                                                                       WAIT_TIME,
                                                                       EVENT_CHANNEL_RETRY_TIMES,
                                                                       version,
                                                                       uniqueId);
    uniqueId++;
    auto leftsourceLogicalOperator = std::make_shared<SourceLogicalOperator>(networkSourceDescriptor, networkSourceId);
    leftsourceLogicalOperator->addProperty(Optimizer::DOWNSTREAM_LOGICAL_OPERATOR_ID, OperatorId{7});
    leftsourceLogicalOperator->addProperty(Optimizer::UPSTREAM_LOGICAL_OPERATOR_ID, OperatorId{13});
    binaryOperator->addChild(leftsourceLogicalOperator);
    //network source right
    networkSourceId = getNextOperatorId();
    nesPartition = Network::NesPartition(sharedQueryId, networkSourceId, 0, 0);
    networkSourceDescriptor = Network::NetworkSourceDescriptor::create(schema,
                                                                       nesPartition,
                                                                       Network::NodeLocation(6, workerAddress, dataPort),
                                                                       WAIT_TIME,
                                                                       EVENT_CHANNEL_RETRY_TIMES,
                                                                       version,
                                                                       uniqueId);
    uniqueId++;
    auto rightsourceLogicalOperator = std::make_shared<SourceLogicalOperator>(networkSourceDescriptor, networkSourceId);
    rightsourceLogicalOperator->addProperty(Optimizer::DOWNSTREAM_LOGICAL_OPERATOR_ID, OperatorId{7});
    rightsourceLogicalOperator->addProperty(Optimizer::UPSTREAM_LOGICAL_OPERATOR_ID, OperatorId{17});
    binaryOperator->addChild(rightsourceLogicalOperator);
    lockedTopologyNode = topology->lockTopologyNode(3);
    globalExecutionPlan->addDecomposedQueryPlan(lockedTopologyNode, decomposedQueryPlan);
    lockedTopologyNode->unlock();
    subPlanId++;

    networkSinkId = getNextOperatorId();
    //first sub plan on node 6
    decomposedQueryPlan = DecomposedQueryPlan::create(subPlanId, sharedQueryId, 6);
    networkSinkDescriptor = Network::NetworkSinkDescriptor::create(
        Network::NodeLocation(3, workerAddress, dataPort),
        leftsourceLogicalOperator->getSourceDescriptor()->as<Network::NetworkSourceDescriptor>()->getNesPartition(),
        WAIT_TIME,
        DATA_CHANNEL_RETRY_TIMES,
        version,
        DEFAULT_NUMBER_OF_ORIGINS,
        networkSinkId);
    sinkLogicalOperator = std::make_shared<SinkLogicalOperator>(networkSinkDescriptor, networkSinkId);
    sinkLogicalOperator->addProperty(Optimizer::DOWNSTREAM_LOGICAL_OPERATOR_ID, OperatorId{7});
    sinkLogicalOperator->addProperty(Optimizer::UPSTREAM_LOGICAL_OPERATOR_ID, OperatorId{13});
    decomposedQueryPlan->addRootOperator(sinkLogicalOperator);
    networkSourceId = getNextOperatorId();
    nesPartition = Network::NesPartition(sharedQueryId, networkSourceId, 0, 0);
    networkSourceDescriptor = Network::NetworkSourceDescriptor::create(schema,
                                                                       nesPartition,
                                                                       Network::NodeLocation(7, workerAddress, dataPort),
                                                                       WAIT_TIME,
                                                                       EVENT_CHANNEL_RETRY_TIMES,
                                                                       version,
                                                                       uniqueId);
    uniqueId++;
    sourceLogicalOperator = std::make_shared<SourceLogicalOperator>(networkSourceDescriptor, networkSourceId);
    sourceLogicalOperator->addProperty(Optimizer::DOWNSTREAM_LOGICAL_OPERATOR_ID, OperatorId{7});
    sourceLogicalOperator->addProperty(Optimizer::UPSTREAM_LOGICAL_OPERATOR_ID, OperatorId{13});
    sinkLogicalOperator->addChild(sourceLogicalOperator);
    lockedTopologyNode = topology->lockTopologyNode(6);
    globalExecutionPlan->addDecomposedQueryPlan(lockedTopologyNode, decomposedQueryPlan);
    lockedTopologyNode->unlock();
    subPlanId++;

    networkSinkId = getNextOperatorId();
    //sub plan on node 7
    pinnedId = 7;
    decomposedQueryPlan = DecomposedQueryPlan::create(subPlanId, sharedQueryId, pinnedId);
    networkSinkDescriptor = Network::NetworkSinkDescriptor::create(Network::NodeLocation(6, workerAddress, dataPort),
                                                                   nesPartition,
                                                                   WAIT_TIME,
                                                                   DATA_CHANNEL_RETRY_TIMES,
                                                                   version,
                                                                   DEFAULT_NUMBER_OF_ORIGINS,
                                                                   networkSinkId);
    sinkLogicalOperator = std::make_shared<SinkLogicalOperator>(networkSinkDescriptor, networkSinkId);
    sinkLogicalOperator->addProperty(Optimizer::DOWNSTREAM_LOGICAL_OPERATOR_ID, OperatorId{7});
    sinkLogicalOperator->addProperty(Optimizer::UPSTREAM_LOGICAL_OPERATOR_ID, OperatorId{13});
    decomposedQueryPlan->addRootOperator(sinkLogicalOperator);
    auto defaultSourceIdLeft = getNextOperatorId();
    auto csvSourceType = CSVSourceType::create("physicalName", "logicalName");
    auto defaultSourcedescriptor = CsvSourceDescriptor::create(schema, csvSourceType);
    auto defaultSourceLeft = std::make_shared<SourceLogicalOperator>(defaultSourcedescriptor, defaultSourceIdLeft);
    auto copiedDefaultSourceLeft = defaultSourceLeft->copy();
    copiedDefaultSourceLeft->addProperty(Optimizer::PINNED_WORKER_ID, pinnedId);
    copiedBinaryOperator->addChild(copiedDefaultSourceLeft);
    sinkLogicalOperator->addChild(defaultSourceLeft);
    lockedTopologyNode = topology->lockTopologyNode(7);
    globalExecutionPlan->addDecomposedQueryPlan(lockedTopologyNode, decomposedQueryPlan);
    lockedTopologyNode->unlock();
    subPlanId++;

    networkSinkId = getNextOperatorId();
    //second sub plan on node 6
    decomposedQueryPlan = DecomposedQueryPlan::create(subPlanId, sharedQueryId, 6);
    networkSinkDescriptor = Network::NetworkSinkDescriptor::create(
        Network::NodeLocation(3, workerAddress, dataPort),
        rightsourceLogicalOperator->getSourceDescriptor()->as<Network::NetworkSourceDescriptor>()->getNesPartition(),
        WAIT_TIME,
        DATA_CHANNEL_RETRY_TIMES,
        version,
        DEFAULT_NUMBER_OF_ORIGINS,
        networkSinkId);
    sinkLogicalOperator = std::make_shared<SinkLogicalOperator>(networkSinkDescriptor, networkSinkId);
    sinkLogicalOperator->addProperty(Optimizer::DOWNSTREAM_LOGICAL_OPERATOR_ID, OperatorId{7});
    sinkLogicalOperator->addProperty(Optimizer::UPSTREAM_LOGICAL_OPERATOR_ID, OperatorId{17});
    decomposedQueryPlan->addRootOperator(sinkLogicalOperator);
    networkSourceId = getNextOperatorId();
    nesPartition = Network::NesPartition(sharedQueryId, networkSourceId, 0, 0);
    networkSourceDescriptor = Network::NetworkSourceDescriptor::create(schema,
                                                                       nesPartition,
                                                                       Network::NodeLocation(8, workerAddress, dataPort),
                                                                       WAIT_TIME,
                                                                       EVENT_CHANNEL_RETRY_TIMES,
                                                                       version,
                                                                       uniqueId);
    uniqueId++;
    sourceLogicalOperator = std::make_shared<SourceLogicalOperator>(networkSourceDescriptor, networkSourceId);
    sourceLogicalOperator->addProperty(Optimizer::DOWNSTREAM_LOGICAL_OPERATOR_ID, OperatorId{7});
    sourceLogicalOperator->addProperty(Optimizer::UPSTREAM_LOGICAL_OPERATOR_ID, OperatorId{17});
    sinkLogicalOperator->addChild(sourceLogicalOperator);
    lockedTopologyNode = topology->lockTopologyNode(6);
    globalExecutionPlan->addDecomposedQueryPlan(lockedTopologyNode, decomposedQueryPlan);
    lockedTopologyNode->unlock();
    subPlanId++;

    networkSinkId = getNextOperatorId();
    //sub plan on node 8
    pinnedId = 8;
    decomposedQueryPlan = DecomposedQueryPlan::create(subPlanId, sharedQueryId, pinnedId);
    networkSinkDescriptor = Network::NetworkSinkDescriptor::create(Network::NodeLocation(6, workerAddress, dataPort),
                                                                   nesPartition,
                                                                   WAIT_TIME,
                                                                   DATA_CHANNEL_RETRY_TIMES,
                                                                   version,
                                                                   DEFAULT_NUMBER_OF_ORIGINS,
                                                                   networkSinkId);
    sinkLogicalOperator = std::make_shared<SinkLogicalOperator>(networkSinkDescriptor, networkSinkId);
    sinkLogicalOperator->addProperty(Optimizer::DOWNSTREAM_LOGICAL_OPERATOR_ID, OperatorId{7});
    sinkLogicalOperator->addProperty(Optimizer::UPSTREAM_LOGICAL_OPERATOR_ID, OperatorId{17});
    decomposedQueryPlan->addRootOperator(sinkLogicalOperator);
    auto defaultSourceIdRight = getNextOperatorId();
    auto defaultSourceRight = std::make_shared<SourceLogicalOperator>(defaultSourcedescriptor, defaultSourceIdRight);
    auto copiedDefaultSourceRight = defaultSourceRight->copy();
    copiedDefaultSourceRight->addProperty(Optimizer::PINNED_WORKER_ID, pinnedId);
    copiedBinaryOperator->addChild(copiedDefaultSourceRight);
    sinkLogicalOperator->addChild(defaultSourceRight);
    lockedTopologyNode = topology->lockTopologyNode(8);
    globalExecutionPlan->addDecomposedQueryPlan(lockedTopologyNode, decomposedQueryPlan);
    lockedTopologyNode->unlock();
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
                                                                       version,
                                                                       uniqueId);
    uniqueId++;
    auto secondRightsourceLogicalOperator = std::make_shared<SourceLogicalOperator>(networkSourceDescriptor, networkSourceId);
    secondRightsourceLogicalOperator->addProperty(Optimizer::DOWNSTREAM_LOGICAL_OPERATOR_ID, OperatorId{7});
    secondRightsourceLogicalOperator->addProperty(Optimizer::UPSTREAM_LOGICAL_OPERATOR_ID, OperatorId{20});
    binaryOperator->addChild(secondRightsourceLogicalOperator);

    networkSinkId = getNextOperatorId();
    //sub plan on node 8
    decomposedQueryPlan = DecomposedQueryPlan::create(subPlanId, sharedQueryId, pinnedId);
    networkSinkDescriptor = Network::NetworkSinkDescriptor::create(Network::NodeLocation(3, workerAddress, dataPort),
                                                                   nesPartition,
                                                                   WAIT_TIME,
                                                                   DATA_CHANNEL_RETRY_TIMES,
                                                                   version,
                                                                   DEFAULT_NUMBER_OF_ORIGINS,
                                                                   networkSinkId);
    sinkLogicalOperator = std::make_shared<SinkLogicalOperator>(networkSinkDescriptor, networkSinkId);
    sinkLogicalOperator->addProperty(Optimizer::DOWNSTREAM_LOGICAL_OPERATOR_ID, 7);
    sinkLogicalOperator->addProperty(Optimizer::UPSTREAM_LOGICAL_OPERATOR_ID, 20);
    decomposedQueryPlan->addRootOperator(sinkLogicalOperator);
    auto secondDefaultSourceIdRight = getNextOperatorId();
    auto secondDefaultSourceRight = std::make_shared<SourceLogicalOperator>(defaultSourcedescriptor, secondDefaultSourceIdRight);
    auto copiedSecondDefaultSourceRight = secondDefaultSourceRight->copy();
    copiedSecondDefaultSourceRight->addProperty(Optimizer::PINNED_WORKER_ID, pinnedId);
    copiedBinaryOperator->addChild(copiedSecondDefaultSourceRight);
    sinkLogicalOperator->addChild(secondDefaultSourceRight);
    lockedTopologyNode = topology->lockTopologyNode(9);
    globalExecutionPlan->addDecomposedQueryPlan(lockedTopologyNode, decomposedQueryPlan);
    lockedTopologyNode->unlock();
    subPlanId++;

    auto sharedQueryPlan = SharedQueryPlan::create(innerSharedQueryPlan);

    auto [upstreamPinned, downStreamPinned] =
        Experimental::findUpstreamAndDownstreamPinnedOperators(sharedQueryPlan,
                                                               globalExecutionPlan->getLockedExecutionNode(6),
                                                               globalExecutionPlan->getLockedExecutionNode(3),
                                                               topology);
    ASSERT_EQ(upstreamPinned.size(), 3);
    ASSERT_TRUE(upstreamPinned.contains(13));
    ASSERT_TRUE(upstreamPinned.contains(17));
    ASSERT_TRUE(upstreamPinned.contains(20));
    ASSERT_EQ(downStreamPinned.size(), 1);
    ASSERT_TRUE(downStreamPinned.contains(4));
}
}// namespace NES::RequestProcessor
