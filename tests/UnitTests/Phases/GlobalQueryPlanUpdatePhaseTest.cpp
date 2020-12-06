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
#include <Catalogs/QueryCatalogEntry.hpp>
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
    const auto globalQueryPlan = GlobalQueryPlan::create();
    auto phase = GlobalQueryPlanUpdatePhase::create(globalQueryPlan);
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
    auto q1 = Query::from("default_logical").sink(PrintSinkDescriptor::create());
    q1.getQueryPlan()->setQueryId(1);
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the query merger phase.");
    const auto globalQueryPlan = GlobalQueryPlan::create();
    auto phase = GlobalQueryPlanUpdatePhase::create(globalQueryPlan);
    auto catalogEntry1 = QueryCatalogEntry(1, "", "topdown", q1.getQueryPlan(), Scheduling);
    std::vector<QueryCatalogEntry> batchOfQueryRequests = {catalogEntry1};
    auto resultPlan = phase->execute(batchOfQueryRequests);

    //Assert
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Should return 1 global query node with sink operator.");
    const auto& globalQueryMetadataToDeploy = resultPlan->getGlobalQueryMetaDataToDeploy();
    ASSERT_TRUE(globalQueryMetadataToDeploy.size() == 1);
}

/**
 * @brief In this test we execute query merger phase on same valid query plan twice.
 */
TEST_F(GlobalQueryPlanUpdatePhaseTest, executeQueryMergerPhaseForDuplicateValidQueryPlan) {

    //Prepare
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create a new valid query.");
    auto q1 = Query::from("default_logical").sink(PrintSinkDescriptor::create());
    q1.getQueryPlan()->setQueryId(1);
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the query merger phase.");
    const auto globalQueryPlan = GlobalQueryPlan::create();
    auto phase = GlobalQueryPlanUpdatePhase::create(globalQueryPlan);
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the batch of query plan with duplicate query plans.");
    auto catalogEntry1 = QueryCatalogEntry(1, "", "topdown", q1.getQueryPlan(), Scheduling);
    std::vector<QueryCatalogEntry> batchOfQueryRequests = {catalogEntry1, catalogEntry1};
    //Assert
    EXPECT_THROW(phase->execute(batchOfQueryRequests), GlobalQueryPlanUpdateException);
}

/**
 * @brief In this test we execute query merger phase on multiple valid query plans.
 */
TEST_F(GlobalQueryPlanUpdatePhaseTest, executeQueryMergerPhaseForMultipleValidQueryPlan) {
    //Prepare
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create two valid queries.");
    auto q1 = Query::from("default_logical").sink(PrintSinkDescriptor::create());
    q1.getQueryPlan()->setQueryId(1);
    auto q2 = Query::from("default_logical").sink(PrintSinkDescriptor::create());
    q2.getQueryPlan()->setQueryId(2);
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the query merger phase.");
    const auto globalQueryPlan = GlobalQueryPlan::create();
    auto phase = GlobalQueryPlanUpdatePhase::create(globalQueryPlan);
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the batch of query plan with duplicate query plans.");
    auto catalogEntry1 = QueryCatalogEntry(1, "", "topdown", q1.getQueryPlan(), Scheduling);
    auto catalogEntry2 = QueryCatalogEntry(2, "", "topdown", q2.getQueryPlan(), Scheduling);
    std::vector<QueryCatalogEntry> batchOfQueryRequests = {catalogEntry1, catalogEntry2};
    auto resultPlan = phase->execute(batchOfQueryRequests);

    //Assert
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Should return 2 global query node with sink operator.");
    const auto& globalQueryMetadataToDeploy = resultPlan->getGlobalQueryMetaDataToDeploy();
    ASSERT_TRUE(globalQueryMetadataToDeploy.size() == 2);
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
    const auto globalQueryPlan = GlobalQueryPlan::create();
    auto phase = GlobalQueryPlanUpdatePhase::create(globalQueryPlan);
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
    auto q1 = Query::from("default_logical").sink(PrintSinkDescriptor::create());
    q1.getQueryPlan()->setQueryId(1);
    auto q2 = Query::from("default_logical").sink(PrintSinkDescriptor::create());
    q2.getQueryPlan()->setQueryId(2);
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the query merger phase.");
    const auto globalQueryPlan = GlobalQueryPlan::create();
    auto phase = GlobalQueryPlanUpdatePhase::create(globalQueryPlan);
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Create the batch of query plan with duplicate query plans.");
    auto catalogEntry1 = QueryCatalogEntry(1, "", "topdown", q1.getQueryPlan(), Scheduling);
    auto catalogEntry2 = QueryCatalogEntry(2, "", "topdown", q2.getQueryPlan(), Scheduling);
    auto catalogEntry3 = QueryCatalogEntry(2, "", "topdown", q2.getQueryPlan(), MarkedForStop);
    std::vector<QueryCatalogEntry> batchOfQueryRequests = {catalogEntry1, catalogEntry2, catalogEntry3};
    auto resultPlan = phase->execute(batchOfQueryRequests);

    //Assert
    NES_INFO("GlobalQueryPlanUpdatePhaseTest: Should return 1 global query node with sink operator.");
    const auto& globalQueryMetadataToDeploy = resultPlan->getGlobalQueryMetaDataToDeploy();
    ASSERT_TRUE(globalQueryMetadataToDeploy.size() == 1);
}

}// namespace NES
