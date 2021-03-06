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
#include <Catalogs/QueryCatalog.hpp>
#include <Catalogs/QueryCatalogEntry.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Exceptions/GlobalQueryPlanUpdateException.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Phases/GlobalQueryPlanUpdatePhase.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger.hpp>
#include <gtest/gtest.h>

namespace NES {

class GlobalQueryPlanUpdatePhaseTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::setupLogging("GlobalQueryPlanUpdatePhaseTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup GlobalQueryPlanUpdatePhaseTest test case.");
    }

    /* Will be called before a  test is executed. */
    void SetUp() {}

    /* Will be called before a test is executed. */
    void TearDown() { NES_INFO("Tear down GlobalQueryPlanUpdatePhaseTest test case."); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down GlobalQueryPlanUpdatePhaseTest test class."); }
};

/**
 * @brief In this test we execute query merger phase on a single invalid query plan.
 */
TEST_F(GlobalQueryPlanUpdatePhaseTest, executeQueryMergerPhaseForSingleInvalidQueryPlan) {

    //Prepare
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create a new query without assigning it a query id.");
    auto q1 = Query::from("default_logical").sink(PrintSinkDescriptor::create());
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the query merger phase.");
    auto queryCatalog = std::make_shared<QueryCatalog>();
    auto streamCatalog = std::make_shared<StreamCatalog>();
    const auto globalQueryPlan = GlobalQueryPlan::create();
    auto phase = GlobalQueryPlanUpdatePhase::create(queryCatalog, streamCatalog, globalQueryPlan, true);
    auto catalogEntry1 = QueryCatalogEntry(-1, "", "topdown", q1.getQueryPlan(), Scheduling);
    std::vector<QueryCatalogEntry> batchOfQueryRequests = {catalogEntry1};
    //Assert
    EXPECT_THROW(phase->execute(batchOfQueryRequests), GlobalQueryPlanUpdateException);
}

/**
 * @brief In this test we execute query merger phase on a single query plan.
 */
TEST_F(GlobalQueryPlanUpdatePhaseTest, executeQueryMergerPhaseForSingleQueryPlan) {

    //Prepare
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create a new query and assign it an id.");
    auto queryString = R"(Query::from("default_logical").sink(PrintSinkDescriptor::create()))";
    auto q1 = Query::from("default_logical").sink(PrintSinkDescriptor::create());
    q1.getQueryPlan()->setQueryId(1);
    auto queryCatalog = std::make_shared<QueryCatalog>();
    queryCatalog->addNewQueryRequest(queryString, q1.getQueryPlan(), "TopDown");
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the query merger phase.");
    auto streamCatalog = std::make_shared<StreamCatalog>();
    const auto globalQueryPlan = GlobalQueryPlan::create();
    auto phase = GlobalQueryPlanUpdatePhase::create(queryCatalog, streamCatalog, globalQueryPlan, true);
    auto catalogEntry1 = queryCatalog->getQueryCatalogEntry(1);
    std::vector<QueryCatalogEntry> batchOfQueryRequests = {catalogEntry1->copy()};
    auto resultPlan = phase->execute(batchOfQueryRequests);

    //Assert
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Should return 1 global query node with sink operator.");
    const auto& sharedQueryMetadataToDeploy = resultPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(sharedQueryMetadataToDeploy.size() == 1);
}

/**
 * @brief In this test we execute query merger phase on same valid query plan twice.
 */
TEST_F(GlobalQueryPlanUpdatePhaseTest, executeQueryMergerPhaseForDuplicateValidQueryPlan) {

    //Prepare
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create a new valid query.");
    auto queryString = R"(Query::from("default_logical").sink(PrintSinkDescriptor::create()))";
    auto q1 = Query::from("default_logical").sink(PrintSinkDescriptor::create());
    q1.getQueryPlan()->setQueryId(1);
    auto queryCatalog = std::make_shared<QueryCatalog>();
    queryCatalog->addNewQueryRequest(queryString, q1.getQueryPlan(), "TopDown");
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the query merger phase.");
    auto streamCatalog = std::make_shared<StreamCatalog>();
    const auto globalQueryPlan = GlobalQueryPlan::create();
    auto phase = GlobalQueryPlanUpdatePhase::create(queryCatalog, streamCatalog, globalQueryPlan, true);
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the batch of query plan with duplicate query plans.");
    auto catalogEntry1 = queryCatalog->getQueryCatalogEntry(1);
    std::vector<QueryCatalogEntry> batchOfQueryRequests = {catalogEntry1->copy(), catalogEntry1->copy()};
    //Assert
    EXPECT_THROW(phase->execute(batchOfQueryRequests), GlobalQueryPlanUpdateException);
}

/**
 * @brief In this test we execute query merger phase on multiple valid query plans.
 */
TEST_F(GlobalQueryPlanUpdatePhaseTest, executeQueryMergerPhaseForMultipleValidQueryPlan) {
    //Prepare
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create two valid queries.");
    auto queryString1 = R"(Query::from("default_logical").sink(PrintSinkDescriptor::create()))";
    auto q1 = Query::from("default_logical").sink(PrintSinkDescriptor::create());
    q1.getQueryPlan()->setQueryId(1);
    auto queryString2 = R"(Query::from("default_logical").sink(PrintSinkDescriptor::create()))";
    auto q2 = Query::from("default_logical").sink(PrintSinkDescriptor::create());
    q2.getQueryPlan()->setQueryId(2);
    auto queryCatalog = std::make_shared<QueryCatalog>();
    queryCatalog->addNewQueryRequest(queryString1, q1.getQueryPlan(), "TopDown");
    queryCatalog->addNewQueryRequest(queryString2, q2.getQueryPlan(), "TopDown");
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the query merger phase.");
    auto streamCatalog = std::make_shared<StreamCatalog>();
    const auto globalQueryPlan = GlobalQueryPlan::create();
    auto phase = GlobalQueryPlanUpdatePhase::create(queryCatalog, streamCatalog, globalQueryPlan, true);
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the batch of query plan with duplicate query plans.");
    auto catalogEntry1 = queryCatalog->getQueryCatalogEntry(1);
    auto catalogEntry2 = queryCatalog->getQueryCatalogEntry(2);
    std::vector<QueryCatalogEntry> batchOfQueryRequests = {catalogEntry1->copy(), catalogEntry2->copy()};
    auto resultPlan = phase->execute(batchOfQueryRequests);

    //Assert
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Should return 1 global query node with sink operator.");
    const auto& sharedQueryMetadataToDeploy = resultPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(sharedQueryMetadataToDeploy.size() == 1);
}

/**
 * @brief In this test we execute query merger phase on a valid query plan with invalid status.
 */
TEST_F(GlobalQueryPlanUpdatePhaseTest, executeQueryMergerPhaseForAValidQueryPlanInInvalidState) {

    //Prepare
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create a new valid query.");
    auto q1 = Query::from("default_logical").sink(PrintSinkDescriptor::create());
    q1.getQueryPlan()->setQueryId(1);
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the query merger phase.");
    auto queryCatalog = std::make_shared<QueryCatalog>();
    auto streamCatalog = std::make_shared<StreamCatalog>();
    const auto globalQueryPlan = GlobalQueryPlan::create();
    auto phase = GlobalQueryPlanUpdatePhase::create(queryCatalog, streamCatalog, globalQueryPlan, true);
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the batch of query plan with duplicate query plans.");
    auto catalogEntry1 = QueryCatalogEntry(1, "", "topdown", q1.getQueryPlan(), Failed);
    std::vector<QueryCatalogEntry> batchOfQueryRequests = {catalogEntry1};

    //Assert
    EXPECT_THROW(phase->execute(batchOfQueryRequests), GlobalQueryPlanUpdateException);
}

/**
 * @brief In this test we execute query merger phase on multiple query requests with add and removal.
 */
TEST_F(GlobalQueryPlanUpdatePhaseTest, executeQueryMergerPhaseForMultipleValidQueryRequestsWithAddAndRemoval) {
    //Prepare
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create two valid queries.");
    auto queryString1 = R"(Query::from("default_logical").sink(PrintSinkDescriptor::create()))";
    auto q1 = Query::from("default_logical").sink(PrintSinkDescriptor::create());
    q1.getQueryPlan()->setQueryId(1);
    auto queryString2 = R"(Query::from("default_logical").sink(PrintSinkDescriptor::create()))";
    auto q2 = Query::from("default_logical").sink(PrintSinkDescriptor::create());
    q2.getQueryPlan()->setQueryId(2);
    auto queryCatalog = std::make_shared<QueryCatalog>();
    queryCatalog->addNewQueryRequest(queryString1, q1.getQueryPlan(), "TopDown");
    queryCatalog->addNewQueryRequest(queryString2, q2.getQueryPlan(), "TopDown");
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the query merger phase.");
    auto streamCatalog = std::make_shared<StreamCatalog>();
    const auto globalQueryPlan = GlobalQueryPlan::create();
    auto phase = GlobalQueryPlanUpdatePhase::create(queryCatalog, streamCatalog, globalQueryPlan, true);
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the batch of query plan with duplicate query plans.");
    auto catalogEntry1 = queryCatalog->getQueryCatalogEntry(1)->copy();
    auto catalogEntry2 = queryCatalog->getQueryCatalogEntry(2)->copy();
    queryCatalog->addQueryStopRequest(2);
    auto catalogEntry3 = queryCatalog->getQueryCatalogEntry(2)->copy();
    std::vector<QueryCatalogEntry> batchOfQueryRequests = {catalogEntry1, catalogEntry2, catalogEntry3};
    auto resultPlan = phase->execute(batchOfQueryRequests);
    resultPlan->removeEmptySharedQueryMetaData();

    //Assert
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Should return 1 global query node with sink operator.");
    const auto& sharedQueryMetadataToDeploy = resultPlan->getSharedQueryMetaDataToDeploy();
    ASSERT_EQ(sharedQueryMetadataToDeploy.size(), 1);
}

}// namespace NES
