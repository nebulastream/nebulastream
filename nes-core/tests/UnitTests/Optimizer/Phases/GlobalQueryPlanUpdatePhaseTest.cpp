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
#include <Catalogs/Query/QueryCatalog.hpp>
#include <Catalogs/Query/QueryCatalogEntry.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/UDF/UdfCatalog.hpp>
#include <Configurations/Coordinator/OptimizerConfiguration.hpp>
#include <Exceptions/GlobalQueryPlanUpdateException.hpp>
#include <NesBaseTest.hpp>
#include <Operators/LogicalOperators/Sinks/NullOutputSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Optimizer/Phases/GlobalQueryPlanUpdatePhase.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Services/QueryCatalogService.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <WorkQueues/RequestTypes/RunQueryRequest.hpp>
#include <WorkQueues/RequestTypes/StopQueryRequest.hpp>
#include <gtest/gtest.h>
#include <z3++.h>

namespace NES {

class GlobalQueryPlanUpdatePhaseTest : public Testing::TestWithErrorHandling<testing::Test> {
  public:
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    Catalogs::Query::QueryCatalogPtr queryCatalog;
    QueryCatalogServicePtr queryCatalogService;
    Catalogs::UDF::UdfCatalogPtr udfCatalog;
    TopologyPtr topology;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("GlobalQueryPlanUpdatePhaseTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup GlobalQueryPlanUpdatePhaseTest test case.");
    }

    /* Will be called before a  test is executed. */
    void SetUp() override {
        Testing::TestWithErrorHandling<testing::Test>::SetUp();
        context = std::make_shared<z3::context>();
        queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
        queryCatalogService = std::make_shared<QueryCatalogService>(queryCatalog);
        topology = Topology::create();
        //Setup source catalog
        sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
        auto node = TopologyNode::create(0, "localhost", 4000, 5000, 14);
        auto defaultSourceType = DefaultSourceType::create();
        auto physicalSource = PhysicalSource::create("default_logical", "test1", defaultSourceType);
        auto logicalSource = LogicalSource::create("default_logical", Schema::create());
        Catalogs::Source::SourceCatalogEntryPtr sourceCatalogEntry1 =
            std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, node);
        sourceCatalog->addPhysicalSource("default_logical", sourceCatalogEntry1);
        udfCatalog = Catalogs::UDF::UdfCatalog::create();
    }

    z3::ContextPtr context;
};

/**
 * @brief In this test we execute query merger phase on a single invalid query plan.
 */
TEST_F(GlobalQueryPlanUpdatePhaseTest, DISABLED_executeQueryMergerPhaseForSingleInvalidQueryPlan) {

    //Prepare
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create a new query without assigning it a query id.");
    auto q1 = Query::from("default_logical").sink(PrintSinkDescriptor::create());
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the query merger phase.");

    const auto globalQueryPlan = GlobalQueryPlan::create();
    auto optimizerConfiguration = Configurations::OptimizerConfiguration();
    optimizerConfiguration.queryMergerRule = Optimizer::QueryMergerRule::SyntaxBasedCompleteQueryMergerRule;
    auto phase = Optimizer::GlobalQueryPlanUpdatePhase::create(topology,
                                                               queryCatalogService,
                                                               sourceCatalog,
                                                               globalQueryPlan,
                                                               context,
                                                               optimizerConfiguration,
                                                               udfCatalog);
    auto catalogEntry1 =
        Catalogs::Query::QueryCatalogEntry(INVALID_QUERY_ID, "", "topdown", q1.getQueryPlan(), QueryStatus::Optimizing);
    auto request = RunQueryRequest::create(catalogEntry1.getInputQueryPlan(), catalogEntry1.getQueryPlacementStrategy());
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
    const auto* queryString = R"(Query::from("default_logical").sink(PrintSinkDescriptor::create()))";
    auto q1 = Query::from("default_logical").sink(PrintSinkDescriptor::create());
    q1.getQueryPlan()->setQueryId(1);
    queryCatalog->createNewEntry(queryString, q1.getQueryPlan(), "TopDown");
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the query merger phase.");
    auto optimizerConfiguration = Configurations::OptimizerConfiguration();
    optimizerConfiguration.queryMergerRule = Optimizer::QueryMergerRule::SyntaxBasedCompleteQueryMergerRule;
    const auto globalQueryPlan = GlobalQueryPlan::create();
    auto phase = Optimizer::GlobalQueryPlanUpdatePhase::create(topology,
                                                               queryCatalogService,
                                                               sourceCatalog,
                                                               globalQueryPlan,
                                                               context,
                                                               optimizerConfiguration,
                                                               udfCatalog);
    auto catalogEntry1 = queryCatalog->getQueryCatalogEntry(1);
    auto request = RunQueryRequest::create(catalogEntry1->getInputQueryPlan(), catalogEntry1->getQueryPlacementStrategy());
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
TEST_F(GlobalQueryPlanUpdatePhaseTest, DISABLED_executeQueryMergerPhaseForDuplicateValidQueryPlan) {

    //Prepare
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create a new valid query.");
    const auto* queryString = R"(Query::from("default_logical").sink(PrintSinkDescriptor::create()))";
    auto q1 = Query::from("default_logical").sink(PrintSinkDescriptor::create());
    q1.getQueryPlan()->setQueryId(1);
    queryCatalog->createNewEntry(queryString, q1.getQueryPlan(), "TopDown");
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the query merger phase.");
    auto optimizerConfiguration = Configurations::OptimizerConfiguration();
    optimizerConfiguration.queryMergerRule = Optimizer::QueryMergerRule::SyntaxBasedCompleteQueryMergerRule;
    const auto globalQueryPlan = GlobalQueryPlan::create();
    auto phase = Optimizer::GlobalQueryPlanUpdatePhase::create(topology,
                                                               queryCatalogService,
                                                               sourceCatalog,
                                                               globalQueryPlan,
                                                               context,
                                                               optimizerConfiguration,
                                                               udfCatalog);
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the batch of query plan with duplicate query plans.");
    auto catalogEntry1 = queryCatalog->getQueryCatalogEntry(1);
    auto request = RunQueryRequest::create(catalogEntry1->getInputQueryPlan(), catalogEntry1->getQueryPlacementStrategy());
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
    const auto* queryString1 = R"(Query::from("default_logical").sink(PrintSinkDescriptor::create()))";
    auto q1 = Query::from("default_logical").sink(PrintSinkDescriptor::create());
    q1.getQueryPlan()->setQueryId(1);
    const auto* queryString2 = R"(Query::from("default_logical").sink(PrintSinkDescriptor::create()))";
    auto q2 = Query::from("default_logical").sink(PrintSinkDescriptor::create());
    q2.getQueryPlan()->setQueryId(2);
    queryCatalog->createNewEntry(queryString1, q1.getQueryPlan(), "TopDown");
    queryCatalog->createNewEntry(queryString2, q2.getQueryPlan(), "TopDown");
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the query merger phase.");
    auto optimizerConfiguration = Configurations::OptimizerConfiguration();
    optimizerConfiguration.queryMergerRule = Optimizer::QueryMergerRule::SyntaxBasedCompleteQueryMergerRule;
    const auto globalQueryPlan = GlobalQueryPlan::create();
    auto phase = Optimizer::GlobalQueryPlanUpdatePhase::create(topology,
                                                               queryCatalogService,
                                                               sourceCatalog,
                                                               globalQueryPlan,
                                                               context,
                                                               optimizerConfiguration,
                                                               udfCatalog);
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the batch of query plan with duplicate query plans.");
    auto catalogEntry1 = queryCatalog->getQueryCatalogEntry(1);
    auto catalogEntry2 = queryCatalog->getQueryCatalogEntry(2);
    auto request1 = RunQueryRequest::create(catalogEntry1->getInputQueryPlan(), catalogEntry1->getQueryPlacementStrategy());
    auto request2 = RunQueryRequest::create(catalogEntry2->getInputQueryPlan(), catalogEntry2->getQueryPlacementStrategy());
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
    auto q1 = Query::from("default_logical").sink(PrintSinkDescriptor::create());
    int queryId = 1;
    q1.getQueryPlan()->setQueryId(queryId);
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the query merger phase.");
    auto catalogEntry1 = queryCatalog->createNewEntry("", q1.getQueryPlan(), "topdown");
    //Explicitly fail the query
    queryCatalogService->updateQueryStatus(queryId, QueryStatus::Failed, "Random reason");
    auto optimizerConfiguration = Configurations::OptimizerConfiguration();
    optimizerConfiguration.queryMergerRule = Optimizer::QueryMergerRule::SyntaxBasedCompleteQueryMergerRule;
    const auto globalQueryPlan = GlobalQueryPlan::create();
    auto phase = Optimizer::GlobalQueryPlanUpdatePhase::create(topology,
                                                               queryCatalogService,
                                                               sourceCatalog,
                                                               globalQueryPlan,
                                                               context,
                                                               optimizerConfiguration,
                                                               udfCatalog);
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the batch of query plan with duplicate query plans.");
    auto nesRequest1 = RunQueryRequest::create(catalogEntry1->getInputQueryPlan(), catalogEntry1->getQueryPlacementStrategy());
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
    const auto* queryString1 = R"(Query::from("default_logical").sink(PrintSinkDescriptor::create()))";
    auto q1 = Query::from("default_logical").sink(PrintSinkDescriptor::create());
    q1.getQueryPlan()->setQueryId(1);
    const auto* queryString2 = R"(Query::from("default_logical").sink(PrintSinkDescriptor::create()))";
    auto q2 = Query::from("default_logical").sink(PrintSinkDescriptor::create());
    q2.getQueryPlan()->setQueryId(2);
    queryCatalog->createNewEntry(queryString1, q1.getQueryPlan(), "TopDown");
    queryCatalog->createNewEntry(queryString2, q2.getQueryPlan(), "TopDown");
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the query merger phase.");
    auto optimizerConfiguration = Configurations::OptimizerConfiguration();
    optimizerConfiguration.queryMergerRule = Optimizer::QueryMergerRule::SyntaxBasedCompleteQueryMergerRule;
    const auto globalQueryPlan = GlobalQueryPlan::create();
    auto phase = Optimizer::GlobalQueryPlanUpdatePhase::create(topology,
                                                               queryCatalogService,
                                                               sourceCatalog,
                                                               globalQueryPlan,
                                                               context,
                                                               optimizerConfiguration,
                                                               udfCatalog);
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the batch of query plan with duplicate query plans.");
    auto catalogEntry1 = queryCatalog->getQueryCatalogEntry(1);
    auto catalogEntry2 = queryCatalog->getQueryCatalogEntry(2);
    queryCatalogService->checkAndMarkForHardStop(2);
    auto catalogEntry3 = queryCatalog->getQueryCatalogEntry(2);

    auto nesRequest1 = RunQueryRequest::create(catalogEntry1->getInputQueryPlan(), catalogEntry1->getQueryPlacementStrategy());
    auto nesRequest2 = RunQueryRequest::create(catalogEntry2->getInputQueryPlan(), catalogEntry1->getQueryPlacementStrategy());
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
    const auto* queryString = R"(Query::from("default_logical").sink(PrintSinkDescriptor::create()))";

    for (int i = 1; i <= 10; i++) {
        NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the query merger phase.");
        auto q1 = Query::from("default_logical").sink(PrintSinkDescriptor::create());
        q1.getQueryPlan()->setQueryId(i);
        queryCatalog->createNewEntry(queryString, q1.getQueryPlan(), "TopDown");
    }

    std::vector<NESRequestPtr> batchOfNesRequests;
    auto allQueries = queryCatalog->getAllQueryCatalogEntries();
    for (auto& [key, value] : allQueries) {
        auto nesRequest = RunQueryRequest::create(value->getInputQueryPlan(), value->getQueryPlacementStrategy());
        batchOfNesRequests.emplace_back(nesRequest);
    }
    auto optimizerConfiguration = Configurations::OptimizerConfiguration();
    optimizerConfiguration.queryMergerRule = Optimizer::QueryMergerRule::Z3SignatureBasedCompleteQueryMergerRule;
    const auto globalQueryPlan = GlobalQueryPlan::create();
    auto phase = Optimizer::GlobalQueryPlanUpdatePhase::create(topology,
                                                               queryCatalogService,
                                                               sourceCatalog,
                                                               globalQueryPlan,
                                                               context,
                                                               optimizerConfiguration,
                                                               udfCatalog);
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
TEST_F(GlobalQueryPlanUpdatePhaseTest, queryMergerPhaseForSingleQueryPlan1) {

    //Prepare
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create a new query and assign it an id.");
    const auto* queryString = R"(Query::from("default_logical").sink(PrintSinkDescriptor::create()))";

    //    auto queryCatalogService = std::make_shared<Catalogs::Query::QueryCatalog>();
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
        queryCatalog->createNewEntry(queryString, q1.getQueryPlan(), "TopDown");
    }

    std::vector<NESRequestPtr> batchOfNesRequests;
    auto allQueries = queryCatalog->getAllQueryCatalogEntries();
    for (auto& [key, value] : allQueries) {
        auto nesRequest = RunQueryRequest::create(value->getInputQueryPlan(), value->getQueryPlacementStrategy());
        batchOfNesRequests.emplace_back(nesRequest);
    }

    //Setup source catalog
    auto sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    NES::SchemaPtr schema = NES::Schema::create()
                                ->addField("id", NES::UINT64)
                                ->addField("val", NES::UINT64)
                                ->addField("X", NES::UINT64)
                                ->addField("Y", NES::UINT64);
    sourceCatalog->addLogicalSource("example", schema);
    auto logicalSource = sourceCatalog->getLogicalSource("example");

    auto node = TopologyNode::create(0, "localhost", 4000, 5000, 14);
    auto physicalSource = PhysicalSource::create("example", "test1");
    Catalogs::Source::SourceCatalogEntryPtr sourceCatalogEntry1 =
        std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, node);
    sourceCatalog->addPhysicalSource("example", sourceCatalogEntry1);
    auto optimizerConfiguration = Configurations::OptimizerConfiguration();
    optimizerConfiguration.queryMergerRule = Optimizer::QueryMergerRule::Z3SignatureBasedCompleteQueryMergerRule;
    const auto globalQueryPlan = GlobalQueryPlan::create();
    auto phase = Optimizer::GlobalQueryPlanUpdatePhase::create(topology,
                                                               queryCatalogService,
                                                               sourceCatalog,
                                                               globalQueryPlan,
                                                               context,
                                                               optimizerConfiguration,
                                                               udfCatalog);

    auto resultPlan = phase->execute(batchOfNesRequests);
    //Assert
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Should return 1 global query node with sink operator.");
    globalQueryPlan->removeFailedOrStoppedSharedQueryPlans();
    const auto& sharedQueryMetadataToDeploy = resultPlan->getSharedQueryPlansToDeploy();
    EXPECT_TRUE(sharedQueryMetadataToDeploy.size() == 1);
}
}// namespace NES
