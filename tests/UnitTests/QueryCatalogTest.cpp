#include "gtest/gtest.h"
#include <Catalogs/QueryCatalog.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <Util/UtilityFunctions.hpp>
#include <iostream>
#include <Exceptions/InvalidArgumentException.hpp>

using namespace NES;

std::string ip = "127.0.0.1";
std::string host = "localhost";
uint16_t publish_port = 4711;

class QueryCatalogTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        std::cout << "Setup QueryCatalogTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() {
        NES::setupLogging("QueryCatalogTest.log", NES::LOG_DEBUG);
        NES_DEBUG("FINISHED ADDING 5 Nodes to topology");
        std::cout << "Setup QueryCatalogTest test case." << std::endl;
    }

    /* Will be called before a test is executed. */
    void TearDown() {
        std::cout << "Tear down QueryCatalogTest test case." << std::endl;
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() {
        std::cout << "Tear down QueryCatalogTest test class." << std::endl;
    }
};


TEST_F(QueryCatalogTest, testAddNewQuery) {

    //Prepare
    std::string queryString = "Query::from(\"default_logical\").filter(Attribute(\"value\") < 42).sink(PrintSinkDescriptor::create()); ";
    QueryPtr query = UtilityFunctions::createQueryFromCodeString(queryString);
    std::string queryId = UtilityFunctions::generateIdString();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    auto catalogEntry = queryCatalog->addNewQueryRequest(queryString, queryPlan, "BottomUp");

    //Assert
    EXPECT_TRUE(catalogEntry);
    map<string, QueryCatalogEntryPtr> reg = queryCatalog->getAllQueryCatalogEntries();
    EXPECT_TRUE(reg.size() == 1);
    map<string, QueryCatalogEntryPtr> run = queryCatalog->getQueries(QueryStatus::Registered);
    EXPECT_TRUE(run.size() == 1);
}

TEST_F(QueryCatalogTest, testAddNewQueryAndStop) {

    //Prepare
    std::string queryString = "Query::from(\"default_logical\").filter(Attribute(\"value\") < 42).sink(PrintSinkDescriptor::create()); ";
    QueryPtr query = UtilityFunctions::createQueryFromCodeString(queryString);
    std::string queryId = UtilityFunctions::generateIdString();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();

    auto catalogEntry = queryCatalog->addNewQueryRequest(queryString, queryPlan, "BottomUp");

    //Assert
    EXPECT_TRUE(catalogEntry);
    map<string, QueryCatalogEntryPtr> reg = queryCatalog->getAllQueryCatalogEntries();
    EXPECT_TRUE(reg.size() == 1);
    map<string, QueryCatalogEntryPtr> registeredQueries = queryCatalog->getQueries(QueryStatus::Registered);
    EXPECT_TRUE(registeredQueries.size() == 1);

    //SendStop request
    catalogEntry = queryCatalog->addQueryStopRequest(queryId);

    //Assert
    EXPECT_TRUE(catalogEntry);
    registeredQueries = queryCatalog->getQueries(QueryStatus::Registered);
    EXPECT_TRUE(registeredQueries.size() == 0);
    map<string, QueryCatalogEntryPtr> queriesMarkedForStop = queryCatalog->getQueries(QueryStatus::MarkedForStop);
    EXPECT_TRUE(queriesMarkedForStop.size() == 1);
}

TEST_F(QueryCatalogTest, testPrintQuery) {

    //Prepare
    std::string queryString = "Query::from(\"default_logical\").filter(Attribute(\"value\") < 42).sink(PrintSinkDescriptor::create()); ";
    QueryPtr query = UtilityFunctions::createQueryFromCodeString(queryString);
    std::string queryId = UtilityFunctions::generateIdString();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    auto catalogEntry = queryCatalog->addNewQueryRequest(queryString, queryPlan, "BottomUp");

    //Assert
    EXPECT_TRUE(catalogEntry);
    map<string, QueryCatalogEntryPtr> reg = queryCatalog->getAllQueryCatalogEntries();
    EXPECT_TRUE(reg.size() == 1);
    std::string ret = queryCatalog->printQueries();
    cout << "ret=" << ret << endl;
}

TEST_F(QueryCatalogTest, getAllQueriesWithoutAnyQueryRegistration) {
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    std::map<std::string, std::string> allRegisteredQueries = queryCatalog->getAllQueries();
    EXPECT_TRUE(allRegisteredQueries.empty());
}

TEST_F(QueryCatalogTest, getAllQueriesAfterQueryRegistration) {

    //Prepare
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    std::string queryString = "Query::from(\"default_logical\").filter(Attribute(\"value\") < 42).sink(PrintSinkDescriptor::create()); ";
    QueryPtr query = UtilityFunctions::createQueryFromCodeString(queryString);
    std::string queryId = UtilityFunctions::generateIdString();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    auto catalogEntry = queryCatalog->addNewQueryRequest(queryString, queryPlan, "BottomUp");

    //Assert
    EXPECT_TRUE(catalogEntry);
    std::map<std::string, std::string> allRegisteredQueries = queryCatalog->getAllQueries();
    EXPECT_EQ(allRegisteredQueries.size(), 1);
    EXPECT_TRUE(allRegisteredQueries.find(queryId) != allRegisteredQueries.end());
}

TEST_F(QueryCatalogTest, getAllRunningQueries) {

    //Prepare
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
    std::string queryString = "Query::from(\"default_logical\").filter(Attribute(\"value\") < 42).sink(PrintSinkDescriptor::create()); ";
    QueryPtr query = UtilityFunctions::createQueryFromCodeString(queryString);
    std::string queryId = UtilityFunctions::generateIdString();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    queryCatalog->addNewQueryRequest(queryString, queryPlan, "BottomUp");
    queryCatalog->markQueryAs(queryId, QueryStatus::Running);

    //Assert
    std::map<std::string, std::string> queries = queryCatalog->getQueriesWithStatus("running");
    EXPECT_EQ(queries.size(), 1);
    EXPECT_TRUE(queries.find(queryId) != queries.end());
}

TEST_F(QueryCatalogTest, throInvalidArgumentExceptionWhenQueryStatusIsUnknown) {
    try {
        //Prepare
        QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>();
        std::map<std::string, std::string> queries = queryCatalog->getQueriesWithStatus("something_random");
        NES_WARNING("Should have thrown invalid argument exception");
        //Assert for Failure
        FAIL();
    } catch (InvalidArgumentException& e) {
        //Assert for Success
        NES_INFO(e.what());
        SUCCEED();
    }
}