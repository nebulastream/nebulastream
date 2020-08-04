#include <API/Query.hpp>
#include <Exceptions/QueryMergerException.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Phases/QueryMergerPhase.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger.hpp>
#include <gtest/gtest.h>
#include <iostream>
#include <memory>

namespace NES {

class QueryMergerPhaseTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::setupLogging("QueryMergerPhaseTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup QueryMergerPhaseTest test case.");
    }

    /* Will be called before a  test is executed. */
    void SetUp() {}

    /* Will be called before a test is executed. */
    void TearDown() {
        NES_INFO("Tear down QueryMergerPhaseTest test case.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() {
        NES_INFO("Tear down QueryMergerPhaseTest test class.");
    }
};

/**
 * @brief In this test we execute query merger phase on a single invalid query plan.
 */
TEST_F(QueryMergerPhaseTest, executeQueryMergerPhaseForSingleInvalidQueryPlan) {

    try {
        //Prepare
        NES_INFO("QueryMergerPhaseTest: Create a new query without assigning it a query id.");
        auto q1 = Query::from("default_logical").sink(PrintSinkDescriptor::create());
        NES_INFO("QueryMergerPhaseTest: Create the query merger phase.");
        auto phase = QueryMergerPhase::create();
        std::vector<QueryPlanPtr> batchOfQueryPlan = {q1.getQueryPlan()};
        auto resultPlan = phase->execute(batchOfQueryPlan);
        //Assert
        FAIL();
    } catch (QueryMergerException ex) {
        //Assert
        NES_INFO("QueryMergerPhaseTest: received QueryMergerException exception as expected with message: " << ex.what());
        SUCCEED();
    }
}

/**
 * @brief In this test we execute query merger phase on a single query plan.
 */
TEST_F(QueryMergerPhaseTest, executeQueryMergerPhaseForSingleQueryPlan) {

    //Prepare
    NES_INFO("QueryMergerPhaseTest: Create a new query and assign it an id.");
    auto q1 = Query::from("default_logical").sink(PrintSinkDescriptor::create());
    q1.getQueryPlan()->setQueryId("Q1");
    NES_INFO("QueryMergerPhaseTest: Create the query merger phase.");
    auto phase = QueryMergerPhase::create();
    std::vector<QueryPlanPtr> batchOfQueryPlan = {q1.getQueryPlan()};
    auto resultPlan = phase->execute(batchOfQueryPlan);

    //Assert
    NES_INFO("QueryMergerPhaseTest: Should return 1 global query node with sink operator.");
    const auto& globalQueryNodes = resultPlan->getAllNewGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();
    ASSERT_TRUE(globalQueryNodes.size() == 1);
}

/**
 * @brief In this test we execute query merger phase on same valid query plan twice.
 */
TEST_F(QueryMergerPhaseTest, executeQueryMergerPhaseForDuplicateValidQueryPlan) {

    try {
        //Prepare
        NES_INFO("QueryMergerPhaseTest: Create a new valid query.");
        auto q1 = Query::from("default_logical").sink(PrintSinkDescriptor::create());
        q1.getQueryPlan()->setQueryId("Q1");
        NES_INFO("QueryMergerPhaseTest: Create the query merger phase.");
        auto phase = QueryMergerPhase::create();
        NES_INFO("QueryMergerPhaseTest: Create the batch of query plan with duplicate query plans.");
        std::vector<QueryPlanPtr> batchOfQueryPlan = {q1.getQueryPlan(), q1.getQueryPlan()};
        auto resultPlan = phase->execute(batchOfQueryPlan);
        //Assert
        FAIL();
    } catch (QueryMergerException ex) {
        //Assert
        NES_INFO("QueryMergerPhaseTest: received QueryMergerException exception as expected with message: " << ex.what());
        SUCCEED();
    }
}

/**
 * @brief In this test we execute query merger phase on multiple valid query plans.
 */
TEST_F(QueryMergerPhaseTest, executeQueryMergerPhaseForMultipleValidQueryPlan) {
    //Prepare
    NES_INFO("QueryMergerPhaseTest: Create two valid queries.");
    auto q1 = Query::from("default_logical").sink(PrintSinkDescriptor::create());
    q1.getQueryPlan()->setQueryId("Q1");
    auto q2 = Query::from("default_logical").sink(PrintSinkDescriptor::create());
    q2.getQueryPlan()->setQueryId("Q2");
    NES_INFO("QueryMergerPhaseTest: Create the query merger phase.");
    auto phase = QueryMergerPhase::create();
    NES_INFO("QueryMergerPhaseTest: Create the batch of query plan with duplicate query plans.");
    std::vector<QueryPlanPtr> batchOfQueryPlan = {q1.getQueryPlan(), q2.getQueryPlan()};
    auto resultPlan = phase->execute(batchOfQueryPlan);

    //Assert
    NES_INFO("QueryMergerPhaseTest: Should return 2 global query node with sink operator.");
    const auto& globalQueryNodes = resultPlan->getAllNewGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();
    ASSERT_TRUE(globalQueryNodes.size() == 2);
}

}// namespace NES
