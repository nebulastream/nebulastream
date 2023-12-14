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
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Coordinator/OptimizerConfiguration.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <Operators/LogicalOperators/LogicalFilterOperator.hpp>
#include <Operators/LogicalOperators/LogicalProjectionOperator.hpp>
#include <Operators/LogicalOperators/LogicalUnionOperator.hpp>
#include <Operators/LogicalOperators/Sinks/LogicalSinkOperator.hpp>
#include <Operators/LogicalOperators/Sinks/NullOutputSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceOperator.hpp>
#include <Optimizer/Exceptions/GlobalQueryPlanUpdateException.hpp>
#include <Optimizer/Phases/QueryPlacementPhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/RequestTypes/QueryRequests/AddQueryRequest.hpp>
#include <Optimizer/RequestTypes/QueryRequests/StopQueryRequest.hpp>
#include <Optimizer/RequestTypes/TopologyRequests/RemoveTopologyLinkRequest.hpp>
#include <Optimizer/RequestTypes/TopologyRequests/RemoveTopologyNodeRequest.hpp>
#include <Phases/GlobalQueryPlanUpdatePhase.hpp>
#include <Plans/ChangeLog/ChangeLog.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Mobility/SpatialType.hpp>
#include <gtest/gtest.h>
#include <z3++.h>

namespace NES {

class GlobalQueryPlanUpdatePhaseTest : public Testing::BaseUnitTest {
  public:
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    Catalogs::Query::QueryCatalogPtr queryCatalog;
    QueryCatalogServicePtr queryCatalogService;
    Catalogs::UDF::UDFCatalogPtr udfCatalog;
    TopologyPtr topology;
    GlobalExecutionPlanPtr globalExecutionPlan;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("GlobalQueryPlanUpdatePhaseTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup GlobalQueryPlanUpdatePhaseTest test case.");
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
        std::map<std::string, std::any> properties;
        properties[NES::Worker::Properties::MAINTENANCE] = false;
        properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

        TopologyNodePtr rootNode = TopologyNode::create(1, "localhost", 123, 124, 100, properties);
        rootNode->addNodeProperty("slots", 100);
        topology->setAsRoot(rootNode);

        TopologyNodePtr nextNode = TopologyNode::create(2, "localhost", 123, 124, 100, properties);
        rootNode->addNodeProperty("slots", 100);
        topology->addNewTopologyNodeAsChild(rootNode, nextNode);

        TopologyNodePtr middleNode1 = TopologyNode::create(3, "localhost", 123, 124, 10, properties);
        middleNode1->addNodeProperty("slots", 10);
        topology->addNewTopologyNodeAsChild(nextNode, middleNode1);

        TopologyNodePtr middleNode2 = TopologyNode::create(4, "localhost", 123, 124, 10, properties);
        middleNode2->addNodeProperty("slots", 10);
        topology->addNewTopologyNodeAsChild(nextNode, middleNode2);

        TopologyNodePtr sourceNode1 = TopologyNode::create(5, "localhost", 123, 124, 1, properties);
        sourceNode1->addNodeProperty("slots", 1);
        topology->addNewTopologyNodeAsChild(middleNode1, sourceNode1);

        TopologyNodePtr sourceNode2 = TopologyNode::create(6, "localhost", 123, 124, 1, properties);
        sourceNode2->addNodeProperty("slots", 1);
        topology->addNewTopologyNodeAsChild(middleNode2, sourceNode2);

        //Prepare sources
        auto logicalSource1 = LogicalSource::create("source1", Schema::create());
        auto logicalSource2 = LogicalSource::create("source2", Schema::create());

        auto inputSchema = Schema::create();
        inputSchema->addField("f1", BasicType::INT32);
        inputSchema->addField("f2", BasicType::INT8);
        sourceCatalog->addLogicalSource("source1", inputSchema);
        sourceCatalog->addLogicalSource("source2", inputSchema);

        //Physical source1
        auto physicalSource1 = PhysicalSource::create(DefaultSourceType::create("source1", "physicalSource1"));
        Catalogs::Source::SourceCatalogEntryPtr sourceCatalogEntry1 =
            std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource1, logicalSource1, sourceNode1);

        //Physical source2
        auto physicalSource2 = PhysicalSource::create(DefaultSourceType::create("source2", "physicalSource2"));
        Catalogs::Source::SourceCatalogEntryPtr sourceCatalogEntry2 =
            std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource1, logicalSource2, sourceNode2);

        sourceCatalog->addPhysicalSource("source1", sourceCatalogEntry1);
        sourceCatalog->addPhysicalSource("source2", sourceCatalogEntry2);

        udfCatalog = Catalogs::UDF::UDFCatalog::create();
        globalExecutionPlan = GlobalExecutionPlan::create();
    }

    z3::ContextPtr context;
};

/**
 * @brief In this test we execute query merger phase on a single invalid query plan.
 */
TEST_F(GlobalQueryPlanUpdatePhaseTest, DISABLED_executeQueryMergerPhaseForSingleInvalidQueryPlan) {

    //Prepare
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create a new query without assigning it a query id.");
    auto q1 = Query::from("source1").sink(PrintSinkDescriptor::create());
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the query merger phase.");

    //Coordinator configuration
    const auto globalQueryPlan = GlobalQueryPlan::create();
    auto coordinatorConfig = Configurations::CoordinatorConfiguration::createDefault();
    auto optimizerConfiguration = Configurations::OptimizerConfiguration();
    optimizerConfiguration.queryMergerRule = Optimizer::QueryMergerRule::SyntaxBasedCompleteQueryMergerRule;
    coordinatorConfig->optimizer = optimizerConfiguration;

    auto phase = Optimizer::GlobalQueryPlanUpdatePhase::create(topology,
                                                               queryCatalogService,
                                                               sourceCatalog,
                                                               globalQueryPlan,
                                                               context,
                                                               coordinatorConfig,
                                                               udfCatalog,
                                                               globalExecutionPlan);
    auto catalogEntry1 = Catalogs::Query::QueryCatalogEntry(INVALID_QUERY_ID,
                                                            "",
                                                            Optimizer::PlacementStrategy::TopDown,
                                                            q1.getQueryPlan(),
                                                            QueryState::OPTIMIZING);
    auto request = AddQueryRequest::create(catalogEntry1.getInputQueryPlan(), catalogEntry1.getQueryPlacementStrategy());
    std::vector<NESRequestPtr> batchOfQueryRequests = {request};
    //Assert
    EXPECT_THROW(phase->execute(batchOfQueryRequests), GlobalQueryPlanUpdateException);
}

/**
 * @brief In this test we execute query merger phase on a single query plan.
 */
TEST_F(GlobalQueryPlanUpdatePhaseTest, executeQueryMergerPhaseForSingleQueryPlan) {

    //Prepare
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create a new query and assign it an id.");
    const auto* queryString = R"(Query::from("source1").sink(PrintSinkDescriptor::create()))";
    auto q1 = Query::from("source1").sink(PrintSinkDescriptor::create());
    q1.getQueryPlan()->setQueryId(1);
    queryCatalog->createNewEntry(queryString, q1.getQueryPlan(), Optimizer::PlacementStrategy::TopDown);
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the query merger phase.");
    const auto globalQueryPlan = GlobalQueryPlan::create();

    //Coordinator configuration
    auto coordinatorConfig = Configurations::CoordinatorConfiguration::createDefault();
    auto optimizerConfiguration = Configurations::OptimizerConfiguration();
    optimizerConfiguration.queryMergerRule = Optimizer::QueryMergerRule::SyntaxBasedCompleteQueryMergerRule;
    coordinatorConfig->optimizer = optimizerConfiguration;

    auto phase = Optimizer::GlobalQueryPlanUpdatePhase::create(topology,
                                                               queryCatalogService,
                                                               sourceCatalog,
                                                               globalQueryPlan,
                                                               context,
                                                               coordinatorConfig,
                                                               udfCatalog,
                                                               globalExecutionPlan);
    auto catalogEntry1 = queryCatalog->getQueryCatalogEntry(1);
    auto request = AddQueryRequest::create(catalogEntry1->getInputQueryPlan(), catalogEntry1->getQueryPlacementStrategy());
    std::vector<NESRequestPtr> batchOfQueryRequests = {request};
    auto resultPlan = phase->execute(batchOfQueryRequests);

    //Assert
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Should return 1 global query node with sink operator.");
    const auto& sharedQueryMetadataToDeploy = resultPlan->getSharedQueryPlansToDeploy();
    EXPECT_TRUE(sharedQueryMetadataToDeploy.size() == 1);
}

/**
 * @brief In this test we execute query merger phase on same valid query plan twice.
 */
TEST_F(GlobalQueryPlanUpdatePhaseTest, executeQueryMergerPhaseForDuplicateValidQueryPlan) {

    //Prepare
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create a new valid query.");
    const auto* queryString = R"(Query::from("source1").sink(PrintSinkDescriptor::create()))";
    auto q1 = Query::from("source1").sink(PrintSinkDescriptor::create());
    q1.getQueryPlan()->setQueryId(1);
    queryCatalog->createNewEntry(queryString, q1.getQueryPlan(), Optimizer::PlacementStrategy::TopDown);
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the query merger phase.");
    const auto globalQueryPlan = GlobalQueryPlan::create();

    //Coordinator configuration
    auto coordinatorConfig = Configurations::CoordinatorConfiguration::createDefault();
    auto optimizerConfiguration = Configurations::OptimizerConfiguration();
    optimizerConfiguration.queryMergerRule = Optimizer::QueryMergerRule::SyntaxBasedCompleteQueryMergerRule;
    coordinatorConfig->optimizer = optimizerConfiguration;

    auto phase = Optimizer::GlobalQueryPlanUpdatePhase::create(topology,
                                                               queryCatalogService,
                                                               sourceCatalog,
                                                               globalQueryPlan,
                                                               context,
                                                               coordinatorConfig,
                                                               udfCatalog,
                                                               globalExecutionPlan);
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the batch of query plan with duplicate query plans.");
    auto catalogEntry1 = queryCatalog->getQueryCatalogEntry(1);
    auto request = AddQueryRequest::create(catalogEntry1->getInputQueryPlan(), catalogEntry1->getQueryPlacementStrategy());
    std::vector<NESRequestPtr> nesRequests = {request, request};
    //Assert
    EXPECT_THROW(phase->execute(nesRequests), GlobalQueryPlanUpdateException);
}

/**
 * brief In this test we execute query merger phase on multiple valid query plans.
 */
TEST_F(GlobalQueryPlanUpdatePhaseTest, executeQueryMergerPhaseForMultipleValidQueryPlan) {
    //Prepare
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create two valid queryIdAndCatalogEntryMapping.");
    const auto* queryString1 = R"(Query::from("source1").sink(PrintSinkDescriptor::create()))";
    auto q1 = Query::from("source1").sink(PrintSinkDescriptor::create());
    q1.getQueryPlan()->setQueryId(1);
    const auto* queryString2 = R"(Query::from("source1").sink(PrintSinkDescriptor::create()))";
    auto q2 = Query::from("source1").sink(PrintSinkDescriptor::create());
    q2.getQueryPlan()->setQueryId(2);
    queryCatalog->createNewEntry(queryString1, q1.getQueryPlan(), Optimizer::PlacementStrategy::TopDown);
    queryCatalog->createNewEntry(queryString2, q2.getQueryPlan(), Optimizer::PlacementStrategy::TopDown);

    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the query merger phase.");
    const auto globalQueryPlan = GlobalQueryPlan::create();

    //Coordinator configuration
    auto coordinatorConfig = Configurations::CoordinatorConfiguration::createDefault();
    auto optimizerConfiguration = Configurations::OptimizerConfiguration();
    optimizerConfiguration.queryMergerRule = Optimizer::QueryMergerRule::SyntaxBasedCompleteQueryMergerRule;
    coordinatorConfig->optimizer = optimizerConfiguration;

    auto phase = Optimizer::GlobalQueryPlanUpdatePhase::create(topology,
                                                               queryCatalogService,
                                                               sourceCatalog,
                                                               globalQueryPlan,
                                                               context,
                                                               coordinatorConfig,
                                                               udfCatalog,
                                                               globalExecutionPlan);

    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the batch of query plan with duplicate query plans.");
    auto catalogEntry1 = queryCatalog->getQueryCatalogEntry(1);
    auto catalogEntry2 = queryCatalog->getQueryCatalogEntry(2);
    auto request1 = AddQueryRequest::create(catalogEntry1->getInputQueryPlan(), catalogEntry1->getQueryPlacementStrategy());
    auto request2 = AddQueryRequest::create(catalogEntry2->getInputQueryPlan(), catalogEntry2->getQueryPlacementStrategy());
    std::vector<NESRequestPtr> requests = {request1, request2};
    auto resultPlan = phase->execute(requests);

    //Assert
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Should return 1 global query node with sink operator.");
    const auto& sharedQueryMetadataToDeploy = resultPlan->getSharedQueryPlansToDeploy();
    EXPECT_TRUE(sharedQueryMetadataToDeploy.size() == 1);
}

/**
*  @brief In this test we execute query merger phase on a valid query plan with invalid status.
*/
TEST_F(GlobalQueryPlanUpdatePhaseTest, DISABLED_executeQueryMergerPhaseForAValidQueryPlanInInvalidState) {

    //Prepare
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create a new valid query.");
    auto q1 = Query::from("source1").sink(PrintSinkDescriptor::create());
    int queryId = 1;
    q1.getQueryPlan()->setQueryId(queryId);
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the query merger phase.");
    auto catalogEntry1 = queryCatalog->createNewEntry("", q1.getQueryPlan(), Optimizer::PlacementStrategy::TopDown);
    //Explicitly fail the query
    queryCatalogService->updateQueryStatus(queryId, QueryState::FAILED, "Random reason");

    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the query merger phase.");
    const auto globalQueryPlan = GlobalQueryPlan::create();

    //Coordinator configuration
    auto coordinatorConfig = Configurations::CoordinatorConfiguration::createDefault();
    auto optimizerConfiguration = Configurations::OptimizerConfiguration();
    optimizerConfiguration.queryMergerRule = Optimizer::QueryMergerRule::SyntaxBasedCompleteQueryMergerRule;
    coordinatorConfig->optimizer = optimizerConfiguration;

    auto phase = Optimizer::GlobalQueryPlanUpdatePhase::create(topology,
                                                               queryCatalogService,
                                                               sourceCatalog,
                                                               globalQueryPlan,
                                                               context,
                                                               coordinatorConfig,
                                                               udfCatalog,
                                                               globalExecutionPlan);

    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the batch of query plan with duplicate query plans.");
    auto nesRequest1 = AddQueryRequest::create(catalogEntry1->getInputQueryPlan(), catalogEntry1->getQueryPlacementStrategy());
    std::vector<NESRequestPtr> batchOfQueryRequests = {nesRequest1};

    //Assert
    EXPECT_THROW(phase->execute(batchOfQueryRequests), GlobalQueryPlanUpdateException);
}

/**
 * @brief  In this test we execute query merger phase on multiple query requests with add and removal.
 */
TEST_F(GlobalQueryPlanUpdatePhaseTest, executeQueryMergerPhaseForMultipleValidQueryRequestsWithAddAndRemoval) {
    //Prepare
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create two valid queryIdAndCatalogEntryMapping.");
    const auto* queryString1 = R"(Query::from("source1").sink(PrintSinkDescriptor::create()))";
    auto q1 = Query::from("source1").sink(PrintSinkDescriptor::create());
    q1.getQueryPlan()->setQueryId(1);
    const auto* queryString2 = R"(Query::from("source1").sink(PrintSinkDescriptor::create()))";
    auto q2 = Query::from("source1").sink(PrintSinkDescriptor::create());
    q2.getQueryPlan()->setQueryId(2);
    queryCatalog->createNewEntry(queryString1, q1.getQueryPlan(), Optimizer::PlacementStrategy::TopDown);
    queryCatalog->createNewEntry(queryString2, q2.getQueryPlan(), Optimizer::PlacementStrategy::TopDown);
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the query merger phase.");
    const auto globalQueryPlan = GlobalQueryPlan::create();

    //Coordinator configuration
    auto coordinatorConfig = Configurations::CoordinatorConfiguration::createDefault();
    auto optimizerConfiguration = Configurations::OptimizerConfiguration();
    optimizerConfiguration.queryMergerRule = Optimizer::QueryMergerRule::SyntaxBasedCompleteQueryMergerRule;
    coordinatorConfig->optimizer = optimizerConfiguration;

    auto phase = Optimizer::GlobalQueryPlanUpdatePhase::create(topology,
                                                               queryCatalogService,
                                                               sourceCatalog,
                                                               globalQueryPlan,
                                                               context,
                                                               coordinatorConfig,
                                                               udfCatalog,
                                                               globalExecutionPlan);

    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the batch of query plan with duplicate query plans.");
    auto catalogEntry1 = queryCatalog->getQueryCatalogEntry(1);
    auto catalogEntry2 = queryCatalog->getQueryCatalogEntry(2);
    queryCatalogService->checkAndMarkForHardStop(2);

    auto nesRequest1 = AddQueryRequest::create(catalogEntry1->getInputQueryPlan(), catalogEntry1->getQueryPlacementStrategy());
    auto nesRequest2 = AddQueryRequest::create(catalogEntry2->getInputQueryPlan(), catalogEntry1->getQueryPlacementStrategy());
    auto nesRequest3 = StopQueryRequest::create(catalogEntry2->getInputQueryPlan()->getQueryId());

    std::vector<NESRequestPtr> batchOfQueryRequests = {nesRequest1, nesRequest2, nesRequest3};
    auto resultPlan = phase->execute(batchOfQueryRequests);
    resultPlan->removeFailedOrStoppedSharedQueryPlans();

    //Assert
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Should return 1 global query node with sink operator.");
    const auto& sharedQueryMetadataToDeploy = resultPlan->getSharedQueryPlansToDeploy();
    ASSERT_EQ(sharedQueryMetadataToDeploy.size(), 1u);
}

/**
 * @brief In this test we execute query merger phase on a single query plan.
 */
TEST_F(GlobalQueryPlanUpdatePhaseTest, queryMergerPhaseForSingleQueryPlan) {

    //Prepare
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create a new query and assign it an id.");
    const auto* queryString = R"(Query::from("source1").sink(PrintSinkDescriptor::create()))";

    for (int i = 1; i <= 10; i++) {
        NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the query merger phase.");
        auto q1 = Query::from("source1").sink(PrintSinkDescriptor::create());
        q1.getQueryPlan()->setQueryId(i);
        queryCatalog->createNewEntry(queryString, q1.getQueryPlan(), Optimizer::PlacementStrategy::TopDown);
    }

    std::vector<NESRequestPtr> batchOfNesRequests;
    auto allQueries = queryCatalog->getAllQueryCatalogEntries();
    for (auto& [key, value] : allQueries) {
        auto nesRequest = AddQueryRequest::create(value->getInputQueryPlan(), value->getQueryPlacementStrategy());
        batchOfNesRequests.emplace_back(nesRequest);
    }

    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the query merger phase.");
    const auto globalQueryPlan = GlobalQueryPlan::create();

    //Coordinator configuration
    auto coordinatorConfig = Configurations::CoordinatorConfiguration::createDefault();
    auto optimizerConfiguration = Configurations::OptimizerConfiguration();
    optimizerConfiguration.queryMergerRule = Optimizer::QueryMergerRule::Z3SignatureBasedCompleteQueryMergerRule;
    coordinatorConfig->optimizer = optimizerConfiguration;

    auto phase = Optimizer::GlobalQueryPlanUpdatePhase::create(topology,
                                                               queryCatalogService,
                                                               sourceCatalog,
                                                               globalQueryPlan,
                                                               context,
                                                               coordinatorConfig,
                                                               udfCatalog,
                                                               globalExecutionPlan);

    auto resultPlan = phase->execute(batchOfNesRequests);
    //Assert
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Should return 1 global query node with sink operator.");
    globalQueryPlan->removeFailedOrStoppedSharedQueryPlans();
    const auto& sharedQueryMetadataToDeploy = resultPlan->getSharedQueryPlansToDeploy();
    EXPECT_TRUE(sharedQueryMetadataToDeploy.size() == 1);
}

/**
 * @brief In this test we execute query merger phase on a single query plan.
 */
TEST_F(GlobalQueryPlanUpdatePhaseTest, queryMergerPhaseForSingleComplexQueryPlan) {

    //Prepare
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create a new query and assign it an id.");
    const auto* queryString = R"(Query::from("source1").sink(PrintSinkDescriptor::create()))";

    for (int i = 1; i <= 1; i++) {
        NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the query merger phase.");
        auto q1 = Query::from("example")
                      .filter(Attribute("X") <= Attribute("Y"))
                      .map(Attribute("id") = Attribute("id") / 1)
                      .map(Attribute("Y") = Attribute("Y") - 2)
                      .map(Attribute("NEW_id2") = Attribute("Y") / Attribute("Y"))
                      .filter(Attribute("val") < 36)
                      .filter(Attribute("Y") >= 49)
                      .unionWith(Query::from("example")
                                     .filter(Attribute("X") <= Attribute("Y"))
                                     .map(Attribute("id") = Attribute("id") / 1)
                                     .map(Attribute("Y") = Attribute("Y") - 2)
                                     .map(Attribute("NEW_id2") = Attribute("Y") / Attribute("Y"))
                                     .filter(Attribute("val") < 36)
                                     .filter(Attribute("Y") >= 49))
                      .sink(NullOutputSinkDescriptor::create());

        q1.getQueryPlan()->setQueryId(i);
        queryCatalog->createNewEntry(queryString, q1.getQueryPlan(), Optimizer::PlacementStrategy::TopDown);
    }

    std::vector<NESRequestPtr> batchOfNesRequests;
    auto allQueries = queryCatalog->getAllQueryCatalogEntries();
    for (const auto& [key, value] : allQueries) {
        auto nesRequest = AddQueryRequest::create(value->getInputQueryPlan(), value->getQueryPlacementStrategy());
        batchOfNesRequests.emplace_back(nesRequest);
    }

    //Setup source catalog
    auto sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
    NES::SchemaPtr schema = NES::Schema::create()
                                ->addField("id", BasicType::UINT64)
                                ->addField("val", BasicType::UINT64)
                                ->addField("X", BasicType::UINT64)
                                ->addField("Y", BasicType::UINT64);
    sourceCatalog->addLogicalSource("example", schema);
    auto logicalSource = sourceCatalog->getLogicalSource("example");

    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

    auto node = TopologyNode::create(0, "localhost", 4000, 5000, 14, properties);
    auto physicalSource = PhysicalSource::create("example", "test1");
    Catalogs::Source::SourceCatalogEntryPtr sourceCatalogEntry1 =
        std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, node);
    sourceCatalog->addPhysicalSource("example", sourceCatalogEntry1);

    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the query merger phase.");
    const auto globalQueryPlan = GlobalQueryPlan::create();

    //Coordinator configuration
    auto coordinatorConfig = Configurations::CoordinatorConfiguration::createDefault();
    auto optimizerConfiguration = Configurations::OptimizerConfiguration();
    optimizerConfiguration.queryMergerRule = Optimizer::QueryMergerRule::Z3SignatureBasedCompleteQueryMergerRule;
    coordinatorConfig->optimizer = optimizerConfiguration;

    auto phase = Optimizer::GlobalQueryPlanUpdatePhase::create(topology,
                                                               queryCatalogService,
                                                               sourceCatalog,
                                                               globalQueryPlan,
                                                               context,
                                                               coordinatorConfig,
                                                               udfCatalog,
                                                               globalExecutionPlan);

    auto resultPlan = phase->execute(batchOfNesRequests);
    //Assert
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Should return 1 global query node with sink operator.");
    globalQueryPlan->removeFailedOrStoppedSharedQueryPlans();
    const auto& sharedQueryMetadataToDeploy = resultPlan->getSharedQueryPlansToDeploy();
    EXPECT_TRUE(sharedQueryMetadataToDeploy.size() == 1);
}

/**
 * @brief  In this test we execute query merger phase on multiple query requests with add and later remove the shared query plan by id
 */
TEST_F(GlobalQueryPlanUpdatePhaseTest, executeQueryMergerPhaseForMultipleValidQueryRequestsWithRemovalById) {
    //Setup source catalog
    NES::SchemaPtr schema = NES::Schema::create()
                                ->addField("id", BasicType::UINT64)
                                ->addField("val", BasicType::UINT64)
                                ->addField("X", BasicType::UINT64)
                                ->addField("Y", BasicType::UINT64);
    sourceCatalog->addLogicalSource("example", schema);
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;
    auto node = TopologyNode::create(0, "localhost", 4000, 5000, 14, properties);
    auto physicalSource = PhysicalSource::create("example", "test1");
    auto logicalSource = sourceCatalog->getLogicalSource("example");
    Catalogs::Source::SourceCatalogEntryPtr sourceCatalogEntry1 =
        std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, node);
    sourceCatalog->addPhysicalSource("example", sourceCatalogEntry1);

    //Prepare
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create two valid queryIdAndCatalogEntryMapping.");
    const auto* queryString1 = R"(Query::from("source1").sink(PrintSinkDescriptor::create()))";
    auto q1 = Query::from("source1").sink(PrintSinkDescriptor::create());
    q1.getQueryPlan()->setQueryId(1);
    const auto* queryString2 = R"(Query::from("example").sink(PrintSinkDescriptor::create()))";
    auto q2 = Query::from("example").sink(PrintSinkDescriptor::create());
    q2.getQueryPlan()->setQueryId(2);
    queryCatalog->createNewEntry(queryString1, q1.getQueryPlan(), Optimizer::PlacementStrategy::TopDown);
    queryCatalog->createNewEntry(queryString2, q2.getQueryPlan(), Optimizer::PlacementStrategy::TopDown);

    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the query merger phase.");
    const auto globalQueryPlan = GlobalQueryPlan::create();

    //Coordinator configuration
    auto coordinatorConfig = Configurations::CoordinatorConfiguration::createDefault();
    auto optimizerConfiguration = Configurations::OptimizerConfiguration();
    optimizerConfiguration.queryMergerRule = Optimizer::QueryMergerRule::SyntaxBasedCompleteQueryMergerRule;
    coordinatorConfig->optimizer = optimizerConfiguration;

    auto phase = Optimizer::GlobalQueryPlanUpdatePhase::create(topology,
                                                               queryCatalogService,
                                                               sourceCatalog,
                                                               globalQueryPlan,
                                                               context,
                                                               coordinatorConfig,
                                                               udfCatalog,
                                                               globalExecutionPlan);

    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the batch of query plan with duplicate query plans.");
    auto catalogEntry1 = queryCatalog->getQueryCatalogEntry(1);
    auto catalogEntry2 = queryCatalog->getQueryCatalogEntry(2);
    queryCatalogService->checkAndMarkForHardStop(2);

    auto nesRequest1 = AddQueryRequest::create(catalogEntry1->getInputQueryPlan(), catalogEntry1->getQueryPlacementStrategy());
    auto nesRequest2 = AddQueryRequest::create(catalogEntry2->getInputQueryPlan(), catalogEntry1->getQueryPlacementStrategy());
    auto nesRequest3 = StopQueryRequest::create(catalogEntry2->getInputQueryPlan()->getQueryId());

    std::vector<NESRequestPtr> batchOfQueryRequests = {nesRequest1, nesRequest2};
    auto resultPlan = phase->execute(batchOfQueryRequests);
    const auto& sharedQueryMetadataToDeploy = resultPlan->getSharedQueryPlansToDeploy();

    EXPECT_EQ(sharedQueryMetadataToDeploy.size(), 2u);

    auto sharedQueryPlanId = globalQueryPlan->getSharedQueryId(catalogEntry2->getInputQueryPlan()->getQueryId());
    auto sharedQueryPlanId1 = globalQueryPlan->getSharedQueryId(catalogEntry1->getInputQueryPlan()->getQueryId());
    NES_INFO("Id2 {}, id1 {}", sharedQueryPlanId, sharedQueryPlanId1);
    resultPlan = phase->execute({nesRequest3});
    resultPlan->removeSharedQueryPlan(sharedQueryPlanId);

    //Assert
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Should return 1 global query node with sink operator.");
    const auto& sharedQueryMetadataToDeploy2 = resultPlan->getSharedQueryPlansToDeploy();
    EXPECT_EQ(sharedQueryMetadataToDeploy2.size(), 1u);
}

/**
 * @brief In this test we execute a link removal request over a topology link that is not used by and placed query for data transfer.
 */
TEST_F(GlobalQueryPlanUpdatePhaseTest, testLinkRemovalRequestForUnusedLink) {

    //Prepare
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create a new query and assign it an id.");
    const auto* queryString = R"(Query::from("source1").sink(PrintSinkDescriptor::create()))";

    int queryId = 1;
    auto q1 = Query::from("source1").sink(PrintSinkDescriptor::create());
    q1.getQueryPlan()->setQueryId(queryId);
    queryCatalog->createNewEntry(queryString, q1.getQueryPlan(), Optimizer::PlacementStrategy::TopDown);
    auto nesAddQueryRequest = AddQueryRequest::create(q1.getQueryPlan(), Optimizer::PlacementStrategy::TopDown);

    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the query merger phase.");
    const auto globalQueryPlan = GlobalQueryPlan::create();

    //Coordinator configuration
    auto coordinatorConfig = Configurations::CoordinatorConfiguration::createDefault();
    coordinatorConfig->enableQueryReconfiguration = true;
    auto optimizerConfiguration = Configurations::OptimizerConfiguration();
    optimizerConfiguration.queryMergerRule = Optimizer::QueryMergerRule::Z3SignatureBasedCompleteQueryMergerRule;
    coordinatorConfig->optimizer = optimizerConfiguration;

    auto globalQueryPlanUpdatePhase = Optimizer::GlobalQueryPlanUpdatePhase::create(topology,
                                                                                    queryCatalogService,
                                                                                    sourceCatalog,
                                                                                    globalQueryPlan,
                                                                                    context,
                                                                                    coordinatorConfig,
                                                                                    udfCatalog,
                                                                                    globalExecutionPlan);

    //Execute Add query request
    globalQueryPlanUpdatePhase->execute({nesAddQueryRequest});

    //Fetch the updated shared query plans and assert on the change logs
    auto updatedSharedQueryPlans = globalQueryPlan->getSharedQueryPlansToDeploy();
    EXPECT_EQ(updatedSharedQueryPlans.size(), 1);
    // Get current time stamp
    uint64_t nowInMicroSec =
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    auto changelogs = updatedSharedQueryPlans[0]->getChangeLogEntries(nowInMicroSec);
    EXPECT_EQ(changelogs.size(), 1);

    //Check state of the operators in the changelog entry
    auto changeLogEntry = changelogs[0].second;
    for (const auto& downstreamOperator : changeLogEntry->downstreamOperators) {
        EXPECT_EQ(OperatorState::TO_BE_PLACED, downstreamOperator->getOperatorState());
    }

    for (const auto& upstreamOperator : changeLogEntry->upstreamOperators) {
        EXPECT_EQ(OperatorState::TO_BE_PLACED, upstreamOperator->getOperatorState());
    }

    //Perform query placement
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, coordinatorConfig);
    ASSERT_TRUE(queryPlacementPhase->execute(updatedSharedQueryPlans[0]));

    //Execute remove topology link request
    auto removeTopologyLinkRequest = Experimental::RemoveTopologyLinkRequest::create(4, 6);
    globalQueryPlanUpdatePhase->execute({removeTopologyLinkRequest});

    //Fetch updated shared query plans and assert on the change logs
    updatedSharedQueryPlans = globalQueryPlan->getSharedQueryPlansToDeploy();
    EXPECT_EQ(updatedSharedQueryPlans.size(), 1);
    // Get current time stamp
    nowInMicroSec =
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    changelogs = updatedSharedQueryPlans[0]->getChangeLogEntries(nowInMicroSec);
    EXPECT_TRUE(changelogs.empty());
}

/**
 * @brief In this test we execute a link removal request over a topology link that is used by and placed query for data transfer.
 */
TEST_F(GlobalQueryPlanUpdatePhaseTest, testLinkRemovalRequestForUsedLink) {

    //Prepare
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create a new query and assign it an id.");
    const auto* queryString = R"(Query::from("source1").sink(PrintSinkDescriptor::create()))";

    int queryId = 1;
    auto q1 = Query::from("source1").sink(PrintSinkDescriptor::create());
    q1.getQueryPlan()->setQueryId(queryId);
    queryCatalog->createNewEntry(queryString, q1.getQueryPlan(), Optimizer::PlacementStrategy::TopDown);
    auto nesAddQueryRequest = AddQueryRequest::create(q1.getQueryPlan(), Optimizer::PlacementStrategy::TopDown);

    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the query merger phase.");
    const auto globalQueryPlan = GlobalQueryPlan::create();

    //Coordinator configuration
    auto coordinatorConfig = Configurations::CoordinatorConfiguration::createDefault();
    coordinatorConfig->enableQueryReconfiguration = true;
    auto optimizerConfiguration = Configurations::OptimizerConfiguration();
    optimizerConfiguration.queryMergerRule = Optimizer::QueryMergerRule::Z3SignatureBasedCompleteQueryMergerRule;
    coordinatorConfig->optimizer = optimizerConfiguration;

    auto globalQueryPlanUpdatePhase = Optimizer::GlobalQueryPlanUpdatePhase::create(topology,
                                                                                    queryCatalogService,
                                                                                    sourceCatalog,
                                                                                    globalQueryPlan,
                                                                                    context,
                                                                                    coordinatorConfig,
                                                                                    udfCatalog,
                                                                                    globalExecutionPlan);

    //Execute Add query request
    globalQueryPlanUpdatePhase->execute({nesAddQueryRequest});

    //Fetch the updated shared query plans and assert on the change logs
    auto updatedSharedQueryPlans = globalQueryPlan->getSharedQueryPlansToDeploy();
    EXPECT_EQ(updatedSharedQueryPlans.size(), 1);
    // Get current time stamp
    uint64_t nowInMicroSec =
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    auto changelogs = updatedSharedQueryPlans[0]->getChangeLogEntries(nowInMicroSec);
    EXPECT_EQ(changelogs.size(), 1);

    //Check state of the operators in the changelog entry
    auto changeLogEntry = changelogs[0].second;
    for (const auto& downstreamOperator : changeLogEntry->downstreamOperators) {
        EXPECT_EQ(OperatorState::TO_BE_PLACED, downstreamOperator->getOperatorState());
    }

    for (const auto& upstreamOperator : changeLogEntry->upstreamOperators) {
        EXPECT_EQ(OperatorState::TO_BE_PLACED, upstreamOperator->getOperatorState());
    }

    //Perform query placement
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, coordinatorConfig);
    ASSERT_TRUE(queryPlacementPhase->execute(updatedSharedQueryPlans[0]));

    //Execute remove topology link request
    auto removeTopologyLinkRequest = Experimental::RemoveTopologyLinkRequest::create(3, 5);
    globalQueryPlanUpdatePhase->execute({removeTopologyLinkRequest});

    //Fetch updated shared query plans and assert on the change logs
    updatedSharedQueryPlans = globalQueryPlan->getSharedQueryPlansToDeploy();
    EXPECT_EQ(updatedSharedQueryPlans.size(), 1);
    // Get current time stamp
    nowInMicroSec =
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    changelogs = updatedSharedQueryPlans[0]->getChangeLogEntries(nowInMicroSec);
    EXPECT_EQ(changelogs.size(), 1);
    auto changelogEntry = changelogs[0].second;

    //Check downstream operators
    auto expectedDownstreamOperators = updatedSharedQueryPlans[0]->getQueryPlan()->getRootOperators();
    EXPECT_EQ(changelogEntry->downstreamOperators.size(), 1);
    EXPECT_EQ(expectedDownstreamOperators.size(), 1);
    EXPECT_EQ((*changelogEntry->downstreamOperators.begin())->getId(), expectedDownstreamOperators[0]->getId());
    for (const auto& downstreamOperator : changeLogEntry->downstreamOperators) {
        EXPECT_EQ(OperatorState::PLACED, downstreamOperator->getOperatorState());
    }

    auto expectedUpstreamOperators = updatedSharedQueryPlans[0]->getQueryPlan()->getLeafOperators();
    EXPECT_EQ(changelogEntry->upstreamOperators.size(), 1);
    EXPECT_EQ(expectedUpstreamOperators.size(), 1);
    EXPECT_EQ((*changelogEntry->upstreamOperators.begin())->getId(), expectedUpstreamOperators[0]->getId());
    for (const auto& upstreamOperator : changeLogEntry->upstreamOperators) {
        EXPECT_EQ(OperatorState::PLACED, upstreamOperator->getOperatorState());
    }
}

/**
 * @brief In this test we execute a link removal request over a topology link that is used by and placed query for data transfer.
 */
TEST_F(GlobalQueryPlanUpdatePhaseTest, testLinkRemovalRequestForUsedLinkWithFilterQuery) {

    //Prepare
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create a new query and assign it an id.");
    const auto* queryString = R"(Query::from("source1").filter(Attribute("f1") < 1000).sink(PrintSinkDescriptor::create()))";

    int queryId = 1;
    auto q1 = Query::from("source1").filter(Attribute("f1") < 1000).sink(PrintSinkDescriptor::create());
    q1.getQueryPlan()->setQueryId(queryId);
    queryCatalog->createNewEntry(queryString, q1.getQueryPlan(), Optimizer::PlacementStrategy::BottomUp);
    auto nesAddQueryRequest = AddQueryRequest::create(q1.getQueryPlan(), Optimizer::PlacementStrategy::BottomUp);

    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the query merger phase.");
    const auto globalQueryPlan = GlobalQueryPlan::create();

    //Coordinator configuration
    auto coordinatorConfig = Configurations::CoordinatorConfiguration::createDefault();
    coordinatorConfig->enableQueryReconfiguration = true;
    auto optimizerConfiguration = Configurations::OptimizerConfiguration();
    optimizerConfiguration.queryMergerRule = Optimizer::QueryMergerRule::Z3SignatureBasedCompleteQueryMergerRule;
    coordinatorConfig->optimizer = optimizerConfiguration;

    auto globalQueryPlanUpdatePhase = Optimizer::GlobalQueryPlanUpdatePhase::create(topology,
                                                                                    queryCatalogService,
                                                                                    sourceCatalog,
                                                                                    globalQueryPlan,
                                                                                    context,
                                                                                    coordinatorConfig,
                                                                                    udfCatalog,
                                                                                    globalExecutionPlan);

    //Execute Add query request
    globalQueryPlanUpdatePhase->execute({nesAddQueryRequest});

    //Fetch the updated shared query plans and assert on the change logs
    auto updatedSharedQueryPlans = globalQueryPlan->getSharedQueryPlansToDeploy();
    EXPECT_EQ(updatedSharedQueryPlans.size(), 1);
    // Get current time stamp
    uint64_t nowInMicroSec =
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    auto changelogs = updatedSharedQueryPlans[0]->getChangeLogEntries(nowInMicroSec);
    EXPECT_EQ(changelogs.size(), 1);

    //Perform query placement
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, coordinatorConfig);
    ASSERT_TRUE(queryPlacementPhase->execute(updatedSharedQueryPlans[0]));

    //Execute remove topology link request
    auto removeTopologyLinkRequest = Experimental::RemoveTopologyLinkRequest::create(3, 5);
    globalQueryPlanUpdatePhase->execute({removeTopologyLinkRequest});

    //Fetch updated shared query plans and assert on the change logs
    updatedSharedQueryPlans = globalQueryPlan->getSharedQueryPlansToDeploy();
    EXPECT_EQ(updatedSharedQueryPlans.size(), 1);
    // Get current time stamp
    nowInMicroSec =
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    changelogs = updatedSharedQueryPlans[0]->getChangeLogEntries(nowInMicroSec);
    EXPECT_EQ(changelogs.size(), 1);
    auto changelogEntry = changelogs[0].second;

    //Check downstream operators of updated change log entry
    auto expectedDownstreamOperators = updatedSharedQueryPlans[0]->getQueryPlan()->getOperatorByType<FilterLogicalOperatorNode>();
    EXPECT_EQ(changelogEntry->downstreamOperators.size(), 1);
    EXPECT_EQ(expectedDownstreamOperators.size(), 1);
    EXPECT_EQ((*changelogEntry->downstreamOperators.begin())->getId(), expectedDownstreamOperators[0]->getId());
    for (const auto& downstreamOperator : changelogEntry->downstreamOperators) {
        EXPECT_EQ(OperatorState::PLACED, downstreamOperator->getOperatorState());
    }

    //Check upstream operators of updated change log entry
    auto expectedUpstreamOperators = updatedSharedQueryPlans[0]->getQueryPlan()->getOperatorByType<SourceLogicalOperatorNode>();
    EXPECT_EQ(changelogEntry->upstreamOperators.size(), 1);
    EXPECT_EQ(expectedUpstreamOperators.size(), 1);
    EXPECT_EQ((*changelogEntry->upstreamOperators.begin())->getId(), expectedUpstreamOperators[0]->getId());
    for (const auto& upstreamOperator : changelogEntry->upstreamOperators) {
        EXPECT_EQ(OperatorState::PLACED, upstreamOperator->getOperatorState());
    }
}

/**
 * @brief In this test we execute a link removal request over a topology link that is used by and placed query for data transfer.
 */
TEST_F(GlobalQueryPlanUpdatePhaseTest, testLinkRemovalRequestForUsedLinkWithUnionQuery) {

    //Prepare
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create a new query and assign it an id.");
    const auto* queryString = R"(Query::from("source1").unionWith(Query::from("source2")).sink(PrintSinkDescriptor::create()))";

    int queryId = 1;
    auto q1 = Query::from("source1").unionWith(Query::from("source2")).sink(PrintSinkDescriptor::create());
    q1.getQueryPlan()->setQueryId(queryId);
    queryCatalog->createNewEntry(queryString, q1.getQueryPlan(), Optimizer::PlacementStrategy::TopDown);
    auto nesAddQueryRequest = AddQueryRequest::create(q1.getQueryPlan(), Optimizer::PlacementStrategy::TopDown);

    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the query merger phase.");
    const auto globalQueryPlan = GlobalQueryPlan::create();

    //Coordinator configuration
    auto coordinatorConfig = Configurations::CoordinatorConfiguration::createDefault();
    coordinatorConfig->enableQueryReconfiguration = true;
    auto optimizerConfiguration = Configurations::OptimizerConfiguration();
    optimizerConfiguration.queryMergerRule = Optimizer::QueryMergerRule::Z3SignatureBasedCompleteQueryMergerRule;
    coordinatorConfig->optimizer = optimizerConfiguration;

    auto globalQueryPlanUpdatePhase = Optimizer::GlobalQueryPlanUpdatePhase::create(topology,
                                                                                    queryCatalogService,
                                                                                    sourceCatalog,
                                                                                    globalQueryPlan,
                                                                                    context,
                                                                                    coordinatorConfig,
                                                                                    udfCatalog,
                                                                                    globalExecutionPlan);

    //Execute Add query request
    globalQueryPlanUpdatePhase->execute({nesAddQueryRequest});

    //Fetch the updated shared query plans and assert on the change logs
    auto updatedSharedQueryPlans = globalQueryPlan->getSharedQueryPlansToDeploy();
    EXPECT_EQ(updatedSharedQueryPlans.size(), 1);
    // Get current time stamp
    uint64_t nowInMicroSec =
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    auto changelogs = updatedSharedQueryPlans[0]->getChangeLogEntries(nowInMicroSec);
    EXPECT_EQ(changelogs.size(), 1);

    //Perform query placement
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, coordinatorConfig);
    ASSERT_TRUE(queryPlacementPhase->execute(updatedSharedQueryPlans[0]));

    //Execute remove topology link request
    auto removeTopologyLinkRequest = Experimental::RemoveTopologyLinkRequest::create(1, 2);
    globalQueryPlanUpdatePhase->execute({removeTopologyLinkRequest});

    //Fetch updated shared query plans and assert on the change logs
    updatedSharedQueryPlans = globalQueryPlan->getSharedQueryPlansToDeploy();
    EXPECT_EQ(updatedSharedQueryPlans.size(), 1);
    // Get current time stamp
    nowInMicroSec =
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    changelogs = updatedSharedQueryPlans[0]->getChangeLogEntries(nowInMicroSec);
    EXPECT_EQ(changelogs.size(), 1);
    auto changelogEntry = changelogs[0].second;
    auto sharedQueryPlan = updatedSharedQueryPlans[0]->getQueryPlan();
    auto expectedUpstreamOperators = sharedQueryPlan->getSourceOperators();
    EXPECT_EQ(changelogEntry->upstreamOperators.size(), 2);
    EXPECT_EQ(expectedUpstreamOperators.size(), 2);
    //Actual upstream operator should have same operator id as one of the two expected operators
    auto actualUpstreamOperatorIterator = changelogEntry->upstreamOperators.begin();
    EXPECT_TRUE((*actualUpstreamOperatorIterator)->getId() == expectedUpstreamOperators[0]->getId()
                || (*actualUpstreamOperatorIterator)->getId() == expectedUpstreamOperators[1]->getId());
    ++actualUpstreamOperatorIterator;
    EXPECT_TRUE((*actualUpstreamOperatorIterator)->getId() == expectedUpstreamOperators[0]->getId()
                || (*actualUpstreamOperatorIterator)->getId() == expectedUpstreamOperators[1]->getId());
    for (const auto& upstreamOperator : changelogEntry->upstreamOperators) {
        EXPECT_EQ(OperatorState::PLACED, upstreamOperator->getOperatorState());
    }

    auto expectedUnionOperators = sharedQueryPlan->getOperatorByType<UnionLogicalOperatorNode>();
    auto expectedProjectionOperators = sharedQueryPlan->getOperatorByType<ProjectionLogicalOperatorNode>();
    EXPECT_EQ(changelogEntry->downstreamOperators.size(), 2);
    EXPECT_EQ(expectedUnionOperators.size(), 1);
    EXPECT_EQ(expectedProjectionOperators.size(), 1);
    //Actual downstream operator should have same operator id as one of the two expected operators
    auto actualDownstreamOperatorIterator = changelogEntry->downstreamOperators.begin();
    EXPECT_TRUE((*actualDownstreamOperatorIterator)->getId() == expectedProjectionOperators[0]->getId()
                || (*actualDownstreamOperatorIterator)->getId() == expectedUnionOperators[0]->getId());
    ++actualDownstreamOperatorIterator;
    EXPECT_TRUE((*actualDownstreamOperatorIterator)->getId() == expectedProjectionOperators[0]->getId()
                || (*actualDownstreamOperatorIterator)->getId() == expectedUnionOperators[0]->getId());
    for (const auto& downstreamOperator : changelogEntry->downstreamOperators) {
        EXPECT_EQ(OperatorState::PLACED, downstreamOperator->getOperatorState());
    }
}

/**
 * @brief In this test we execute a node removal request over a topology link that is used by and placed query for data transfer.
 */
TEST_F(GlobalQueryPlanUpdatePhaseTest, testNodeRemovalRequestForUnusedNodeWithFilterQuery) {

    //Prepare
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create a new query and assign it an id.");
    const auto* queryString = R"(Query::from("source1").filter(Attribute("f1") < 1000).sink(PrintSinkDescriptor::create()))";

    int queryId = 1;
    auto q1 = Query::from("source1").filter(Attribute("f1") < 1000).sink(PrintSinkDescriptor::create());
    q1.getQueryPlan()->setQueryId(queryId);
    queryCatalog->createNewEntry(queryString, q1.getQueryPlan(), Optimizer::PlacementStrategy::BottomUp);
    auto nesAddQueryRequest = AddQueryRequest::create(q1.getQueryPlan(), Optimizer::PlacementStrategy::BottomUp);

    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the query merger phase.");
    const auto globalQueryPlan = GlobalQueryPlan::create();

    //Coordinator configuration
    auto coordinatorConfig = Configurations::CoordinatorConfiguration::createDefault();
    coordinatorConfig->enableQueryReconfiguration = true;
    auto optimizerConfiguration = Configurations::OptimizerConfiguration();
    optimizerConfiguration.queryMergerRule = Optimizer::QueryMergerRule::Z3SignatureBasedCompleteQueryMergerRule;
    coordinatorConfig->optimizer = optimizerConfiguration;

    auto globalQueryPlanUpdatePhase = Optimizer::GlobalQueryPlanUpdatePhase::create(topology,
                                                                                    queryCatalogService,
                                                                                    sourceCatalog,
                                                                                    globalQueryPlan,
                                                                                    context,
                                                                                    coordinatorConfig,
                                                                                    udfCatalog,
                                                                                    globalExecutionPlan);

    //Execute Add query request
    globalQueryPlanUpdatePhase->execute({nesAddQueryRequest});

    //Fetch the updated shared query plans and assert on the change logs
    auto updatedSharedQueryPlans = globalQueryPlan->getSharedQueryPlansToDeploy();
    EXPECT_EQ(updatedSharedQueryPlans.size(), 1);
    // Get current time stamp
    uint64_t nowInMicroSec =
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    auto changelogs = updatedSharedQueryPlans[0]->getChangeLogEntries(nowInMicroSec);
    EXPECT_EQ(changelogs.size(), 1);

    //Check state of the operators in the changelog entry
    auto changeLogEntry = changelogs[0].second;
    for (const auto& downstreamOperator : changeLogEntry->downstreamOperators) {
        EXPECT_EQ(OperatorState::TO_BE_PLACED, downstreamOperator->getOperatorState());
    }

    for (const auto& upstreamOperator : changeLogEntry->upstreamOperators) {
        EXPECT_EQ(OperatorState::TO_BE_PLACED, upstreamOperator->getOperatorState());
    }

    //Perform query placement
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, coordinatorConfig);
    ASSERT_TRUE(queryPlacementPhase->execute(updatedSharedQueryPlans[0]));

    //Check state of the operators in the changelog entry
    changeLogEntry = changelogs[0].second;
    for (const auto& downstreamOperator : changeLogEntry->downstreamOperators) {
        EXPECT_EQ(OperatorState::PLACED, downstreamOperator->getOperatorState());
    }

    for (const auto& upstreamOperator : changeLogEntry->upstreamOperators) {
        EXPECT_EQ(OperatorState::PLACED, upstreamOperator->getOperatorState());
    }

    //Execute remove topology link request
    auto removeTopologyLinkRequest = Experimental::RemoveTopologyNodeRequest::create(4);
    globalQueryPlanUpdatePhase->execute({removeTopologyLinkRequest});

    //Fetch updated shared query plans and assert on the change logs
    updatedSharedQueryPlans = globalQueryPlan->getSharedQueryPlansToDeploy();
    EXPECT_EQ(updatedSharedQueryPlans.size(), 1);
    // Get current time stamp
    nowInMicroSec =
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    changelogs = updatedSharedQueryPlans[0]->getChangeLogEntries(nowInMicroSec);
    EXPECT_TRUE(changelogs.empty());
}

/**
 * @brief In this test we execute a node removal request over a topology link that is used by and placed query for data transfer.
 */
TEST_F(GlobalQueryPlanUpdatePhaseTest, testNodeRemovalRequestForUsedNodeWithFilterQuery) {

    //Prepare
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create a new query and assign it an id.");
    const auto* queryString = R"(Query::from("source1").filter(Attribute("f1") < 1000).sink(PrintSinkDescriptor::create()))";

    int queryId = 1;
    auto q1 = Query::from("source1").filter(Attribute("f1") < 1000).sink(PrintSinkDescriptor::create());
    q1.getQueryPlan()->setQueryId(queryId);
    queryCatalog->createNewEntry(queryString, q1.getQueryPlan(), Optimizer::PlacementStrategy::BottomUp);
    auto nesAddQueryRequest = AddQueryRequest::create(q1.getQueryPlan(), Optimizer::PlacementStrategy::BottomUp);

    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the query merger phase.");
    const auto globalQueryPlan = GlobalQueryPlan::create();

    //Coordinator configuration
    auto coordinatorConfig = Configurations::CoordinatorConfiguration::createDefault();
    coordinatorConfig->enableQueryReconfiguration = true;
    auto optimizerConfiguration = Configurations::OptimizerConfiguration();
    optimizerConfiguration.queryMergerRule = Optimizer::QueryMergerRule::Z3SignatureBasedCompleteQueryMergerRule;
    coordinatorConfig->optimizer = optimizerConfiguration;

    auto globalQueryPlanUpdatePhase = Optimizer::GlobalQueryPlanUpdatePhase::create(topology,
                                                                                    queryCatalogService,
                                                                                    sourceCatalog,
                                                                                    globalQueryPlan,
                                                                                    context,
                                                                                    coordinatorConfig,
                                                                                    udfCatalog,
                                                                                    globalExecutionPlan);

    //Execute Add query request
    globalQueryPlanUpdatePhase->execute({nesAddQueryRequest});

    //Fetch the updated shared query plans and assert on the change logs
    auto updatedSharedQueryPlans = globalQueryPlan->getSharedQueryPlansToDeploy();
    EXPECT_EQ(updatedSharedQueryPlans.size(), 1);
    // Get current time stamp
    uint64_t nowInMicroSec =
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    auto changelogs = updatedSharedQueryPlans[0]->getChangeLogEntries(nowInMicroSec);
    EXPECT_EQ(changelogs.size(), 1);
    //Check state of the operators in the changelog entry
    auto changeLogEntry = changelogs[0].second;
    for (const auto& downstreamOperator : changeLogEntry->downstreamOperators) {
        EXPECT_EQ(OperatorState::TO_BE_PLACED, downstreamOperator->getOperatorState());
    }

    for (const auto& upstreamOperator : changeLogEntry->upstreamOperators) {
        EXPECT_EQ(OperatorState::TO_BE_PLACED, upstreamOperator->getOperatorState());
    }

    //Perform query placement
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, coordinatorConfig);
    ASSERT_TRUE(queryPlacementPhase->execute(updatedSharedQueryPlans[0]));

    //Check state of the operators in the changelog entry
    changeLogEntry = changelogs[0].second;
    for (const auto& downstreamOperator : changeLogEntry->downstreamOperators) {
        EXPECT_EQ(OperatorState::PLACED, downstreamOperator->getOperatorState());
    }

    for (const auto& upstreamOperator : changeLogEntry->upstreamOperators) {
        EXPECT_EQ(OperatorState::PLACED, upstreamOperator->getOperatorState());
    }

    //Execute remove topology link request
    auto removeTopologyLinkRequest = Experimental::RemoveTopologyNodeRequest::create(2);
    globalQueryPlanUpdatePhase->execute({removeTopologyLinkRequest});

    //Fetch updated shared query plans and assert on the change logs
    updatedSharedQueryPlans = globalQueryPlan->getSharedQueryPlansToDeploy();
    EXPECT_EQ(updatedSharedQueryPlans.size(), 1);
    // Get current time stamp
    nowInMicroSec =
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    changelogs = updatedSharedQueryPlans[0]->getChangeLogEntries(nowInMicroSec);
    EXPECT_EQ(changelogs.size(), 1);

    auto changelogEntry = changelogs[0].second;
    auto expectedUpstreamOperators = updatedSharedQueryPlans[0]->getQueryPlan()->getOperatorByType<FilterLogicalOperatorNode>();
    EXPECT_EQ(changelogEntry->upstreamOperators.size(), 1);
    EXPECT_EQ(expectedUpstreamOperators.size(), 1);
    EXPECT_EQ((*changelogEntry->upstreamOperators.begin())->getId(), expectedUpstreamOperators[0]->getId());
    auto expectedDownstreamOperators = updatedSharedQueryPlans[0]->getQueryPlan()->getRootOperators();
    EXPECT_EQ(changelogEntry->downstreamOperators.size(), 1);
    EXPECT_EQ(expectedDownstreamOperators.size(), 1);
    EXPECT_EQ((*changelogEntry->downstreamOperators.begin())->getId(), expectedDownstreamOperators[0]->getId());
}

/**
 * @brief In this test we execute a node removal request over a topology link that is used by and placed query for data transfer.
 */
TEST_F(GlobalQueryPlanUpdatePhaseTest, testNodeRemovalRequestForUsedNodeWithUnionQuery) {

    //Prepare
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create a new query and assign it an id.");
    const auto* queryString = R"(Query::from("source1").unionWith(Query::from("source2")).sink(PrintSinkDescriptor::create()))";

    int queryId = 1;
    auto q1 = Query::from("source1").unionWith(Query::from("source2")).sink(PrintSinkDescriptor::create());
    q1.getQueryPlan()->setQueryId(queryId);
    queryCatalog->createNewEntry(queryString, q1.getQueryPlan(), Optimizer::PlacementStrategy::BottomUp);
    auto nesAddQueryRequest = AddQueryRequest::create(q1.getQueryPlan(), Optimizer::PlacementStrategy::BottomUp);

    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the query merger phase.");
    const auto globalQueryPlan = GlobalQueryPlan::create();

    //Coordinator configuration
    auto coordinatorConfig = Configurations::CoordinatorConfiguration::createDefault();
    coordinatorConfig->enableQueryReconfiguration = true;
    auto optimizerConfiguration = Configurations::OptimizerConfiguration();
    optimizerConfiguration.queryMergerRule = Optimizer::QueryMergerRule::Z3SignatureBasedCompleteQueryMergerRule;
    coordinatorConfig->optimizer = optimizerConfiguration;

    auto globalQueryPlanUpdatePhase = Optimizer::GlobalQueryPlanUpdatePhase::create(topology,
                                                                                    queryCatalogService,
                                                                                    sourceCatalog,
                                                                                    globalQueryPlan,
                                                                                    context,
                                                                                    coordinatorConfig,
                                                                                    udfCatalog,
                                                                                    globalExecutionPlan);

    //Execute Add query request
    globalQueryPlanUpdatePhase->execute({nesAddQueryRequest});

    //Fetch the updated shared query plans and assert on the change logs
    auto updatedSharedQueryPlans = globalQueryPlan->getSharedQueryPlansToDeploy();
    EXPECT_EQ(updatedSharedQueryPlans.size(), 1);
    // Get current time stamp
    uint64_t nowInMicroSec =
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    auto changelogs = updatedSharedQueryPlans[0]->getChangeLogEntries(nowInMicroSec);
    EXPECT_EQ(changelogs.size(), 1);

    //Check state of the operators in the changelog entry
    auto changeLogEntry = changelogs[0].second;
    for (const auto& downstreamOperator : changeLogEntry->downstreamOperators) {
        EXPECT_EQ(OperatorState::TO_BE_PLACED, downstreamOperator->getOperatorState());
    }

    for (const auto& upstreamOperator : changeLogEntry->upstreamOperators) {
        EXPECT_EQ(OperatorState::TO_BE_PLACED, upstreamOperator->getOperatorState());
    }

    //Perform query placement
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, coordinatorConfig);
    ASSERT_TRUE(queryPlacementPhase->execute(updatedSharedQueryPlans[0]));

    //Check state of the operators in the changelog entry
    changeLogEntry = changelogs[0].second;
    for (const auto& downstreamOperator : changeLogEntry->downstreamOperators) {
        EXPECT_EQ(OperatorState::PLACED, downstreamOperator->getOperatorState());
    }

    for (const auto& upstreamOperator : changeLogEntry->upstreamOperators) {
        EXPECT_EQ(OperatorState::PLACED, upstreamOperator->getOperatorState());
    }

    //Execute remove topology link request
    auto removeTopologyLinkRequest = Experimental::RemoveTopologyNodeRequest::create(2);
    globalQueryPlanUpdatePhase->execute({removeTopologyLinkRequest});

    //Fetch updated shared query plans and assert on the change logs
    updatedSharedQueryPlans = globalQueryPlan->getSharedQueryPlansToDeploy();
    EXPECT_EQ(updatedSharedQueryPlans.size(), 1);
    // Get current time stamp
    nowInMicroSec =
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    changelogs = updatedSharedQueryPlans[0]->getChangeLogEntries(nowInMicroSec);
    EXPECT_EQ(changelogs.size(), 1);
    auto changelogEntry = changelogs[0].second;

    const QueryPlanPtr& sharedQueryPlan = updatedSharedQueryPlans[0]->getQueryPlan();
    auto expectedUpstreamOperators = sharedQueryPlan->getSourceOperators();
    auto expectedProjectionOperators = sharedQueryPlan->getOperatorByType<ProjectionLogicalOperatorNode>();
    EXPECT_EQ(changelogEntry->upstreamOperators.size(), 2);
    EXPECT_EQ(expectedProjectionOperators.size(), 1);
    EXPECT_EQ(expectedUpstreamOperators.size(), 2);
    //Actual upstream operator should have same operator id as one of the two expected operators
    auto actualUpstreamOperatorIterator = changelogEntry->upstreamOperators.begin();
    EXPECT_TRUE((*actualUpstreamOperatorIterator)->getId() == expectedUpstreamOperators[0]->getId()
                || (*actualUpstreamOperatorIterator)->getId() == expectedUpstreamOperators[1]->getId()
                || (*actualUpstreamOperatorIterator)->getId() == expectedProjectionOperators[0]->getId());
    ++actualUpstreamOperatorIterator;
    EXPECT_TRUE((*actualUpstreamOperatorIterator)->getId() == expectedUpstreamOperators[0]->getId()
                || (*actualUpstreamOperatorIterator)->getId() == expectedUpstreamOperators[1]->getId()
                || (*actualUpstreamOperatorIterator)->getId() == expectedProjectionOperators[0]->getId());

    auto expectedSinkOperators = sharedQueryPlan->getOperatorByType<SinkLogicalOperatorNode>();
    EXPECT_EQ(changelogEntry->downstreamOperators.size(), 1);
    EXPECT_EQ(expectedSinkOperators.size(), 1);
    //Actual upstream operator should have same operator id as one of the two expected operators
    auto actualDownstreamOperatorIterator = changelogEntry->downstreamOperators.begin();
    EXPECT_TRUE((*actualDownstreamOperatorIterator)->getId() == expectedSinkOperators[0]->getId());

    //Check state of the operators in the changelog entry
    for (const auto& downstreamOperator : changelogEntry->downstreamOperators) {
        EXPECT_EQ(OperatorState::PLACED, downstreamOperator->getOperatorState());
    }

    for (const auto& upstreamOperator : changelogEntry->upstreamOperators) {
        EXPECT_EQ(OperatorState::PLACED, upstreamOperator->getOperatorState());
    }

    //Check state of operator to be replaced
    auto expectedOperatorsToBeReplaced = sharedQueryPlan->getOperatorByType<UnionLogicalOperatorNode>();
    EXPECT_EQ(expectedOperatorsToBeReplaced.size(), 1);
    EXPECT_EQ(expectedOperatorsToBeReplaced[0]->getOperatorState(), OperatorState::TO_BE_REPLACED);
}

}// namespace NES
