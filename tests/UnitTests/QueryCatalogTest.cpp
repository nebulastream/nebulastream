#include "gtest/gtest.h"
#include <Catalogs/QueryCatalog.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <GRPC/CoordinatorRPCServer.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Topology/TopologyManager.hpp>
#include <Util/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <iostream>

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

void setupTests(StreamCatalogPtr streamCatalog, TopologyManagerPtr topologyManager) {
    const auto& kCoordinatorNode = topologyManager->createNESWorkerNode(0, "127.0.0.1", CPUCapacity::HIGH);
    for (int i = 1; i < 5; i++) {
        //FIXME: add node properties
        PhysicalStreamConfig streamConf;
        std::string address = ip + ":" + std::to_string(publish_port);

        auto entry = TestUtils::registerTestNode(i, address, 2, "",
                                                 streamConf, NESNodeType::Sensor, streamCatalog, topologyManager);
    }
}

TEST_F(QueryCatalogTest, testAddQuery) {
    std::string queryString =
        "Query::from(\"default_logical\").filter(Attribute(\"value\") < 42).sink(PrintSinkDescriptor::create()); ";

    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    TopologyManagerPtr topologyManager = std::make_shared<TopologyManager>();
    GlobalExecutionPlanPtr executionPlan = GlobalExecutionPlan::create();
    setupTests(streamCatalog, topologyManager);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>(topologyManager, streamCatalog, executionPlan);
    string queryId = queryCatalog->registerQuery(queryString, "BottomUp");
    map<string, QueryCatalogEntryPtr> reg = queryCatalog->getRegisteredQueries();
    EXPECT_TRUE(reg.size() == 1);

    map<string, QueryCatalogEntryPtr> run = queryCatalog->getQueries(
        QueryStatus::Running);
    EXPECT_TRUE(run.size() == 0);

    EXPECT_TRUE(queryCatalog->queryExists(queryId));
}

TEST_F(QueryCatalogTest, testAddQueryAndStartStop) {
    std::string queryString =
        "Query::from(\"default_logical\").filter(Attribute(\"value\") < 42).sink(PrintSinkDescriptor::create()); ";

    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    TopologyManagerPtr topologyManager = std::make_shared<TopologyManager>();
    GlobalExecutionPlanPtr executionPlan = GlobalExecutionPlan::create();
    setupTests(streamCatalog, topologyManager);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>(topologyManager, streamCatalog, executionPlan);

    string queryId = queryCatalog->registerQuery(queryString,
                                                 "BottomUp");
    map<string, QueryCatalogEntryPtr> reg = queryCatalog->getRegisteredQueries();
    EXPECT_TRUE(reg.size() == 1);

    map<string, QueryCatalogEntryPtr> run = queryCatalog->getQueries(
        QueryStatus::Running);
    EXPECT_TRUE(run.size() == 0);

    queryCatalog->markQueryAs(queryId, QueryStatus::Running);
    map<string, QueryCatalogEntryPtr> run_new = queryCatalog->getQueries(QueryStatus::Running);
    EXPECT_TRUE(run_new.size() == 1);

    EXPECT_TRUE(queryCatalog->queryExists(queryId));
    EXPECT_TRUE(queryCatalog->isQueryRunning(queryId));

    queryCatalog->markQueryAs(queryId, QueryStatus::Stopped);
    map<string, QueryCatalogEntryPtr> run_new_stop = queryCatalog->getQueries(QueryStatus::Running);
    EXPECT_TRUE(run_new_stop.size() == 0);

    EXPECT_FALSE(queryCatalog->isQueryRunning(queryId));
}

TEST_F(QueryCatalogTest, testAddRemoveQuery) {
    std::string queryString =
        "Query::from(\"default_logical\").filter(Attribute(\"value\") < 42).sink(PrintSinkDescriptor::create()); ";
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    TopologyManagerPtr topologyManager = std::make_shared<TopologyManager>();
    GlobalExecutionPlanPtr executionPlan = GlobalExecutionPlan::create();
    setupTests(streamCatalog, topologyManager);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>(topologyManager, streamCatalog, executionPlan);

    string queryId = queryCatalog->registerQuery(queryString,
                                                 "BottomUp");
    map<string, QueryCatalogEntryPtr> reg = queryCatalog->getRegisteredQueries();
    EXPECT_TRUE(reg.size() == 1);

    map<string, QueryCatalogEntryPtr> run = queryCatalog->getQueries(
        QueryStatus::Running);
    EXPECT_TRUE(run.size() == 0);

    queryCatalog->deleteQuery(queryId);
    EXPECT_FALSE(queryCatalog->queryExists(queryId));
}

TEST_F(QueryCatalogTest, testPrintQuery) {
    std::string queryString =
        "Query::from(\"default_logical\").filter(Attribute(\"value\") < 42).sink(PrintSinkDescriptor::create()); ";
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    TopologyManagerPtr topologyManager = std::make_shared<TopologyManager>();
    GlobalExecutionPlanPtr executionPlan = GlobalExecutionPlan::create();
    setupTests(streamCatalog, topologyManager);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>(topologyManager, streamCatalog, executionPlan);

    string queryId = queryCatalog->registerQuery(queryString,
                                                 "BottomUp");
    map<string, QueryCatalogEntryPtr> reg = queryCatalog->getRegisteredQueries();
    EXPECT_TRUE(reg.size() == 1);

    std::string ret = queryCatalog->printQueries();
    cout << "ret=" << ret << endl;
}

//

TEST_F(QueryCatalogTest, get_all_registered_queries_without_query_registration) {
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    TopologyManagerPtr topologyManager = std::make_shared<TopologyManager>();
    GlobalExecutionPlanPtr executionPlan = GlobalExecutionPlan::create();
    setupTests(streamCatalog, topologyManager);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>(topologyManager, streamCatalog, executionPlan);

    std::map<std::string, std::string> allRegisteredQueries =
        queryCatalog->getAllRegisteredQueries();
    EXPECT_TRUE(allRegisteredQueries.empty());
}

TEST_F(QueryCatalogTest, get_all_registered_queries_after_query_registration) {
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    TopologyManagerPtr topologyManager = std::make_shared<TopologyManager>();
    GlobalExecutionPlanPtr executionPlan = GlobalExecutionPlan::create();
    setupTests(streamCatalog, topologyManager);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>(topologyManager, streamCatalog, executionPlan);

    std::string queryString =
        "Query::from(\"default_logical\").filter(Attribute(\"value\") < 42).sink(PrintSinkDescriptor::create()); ";

    const string queryId = queryCatalog->registerQuery(queryString,
                                                       "BottomUp");

    std::map<std::string, std::string> allRegisteredQueries =
        queryCatalog->getAllRegisteredQueries();
    EXPECT_EQ(allRegisteredQueries.size(), 1);
    EXPECT_TRUE(allRegisteredQueries.find(queryId) != allRegisteredQueries.end());
}

TEST_F(QueryCatalogTest, get_all_running_queries) {
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    TopologyManagerPtr topologyManager = std::make_shared<TopologyManager>();
    GlobalExecutionPlanPtr executionPlan = GlobalExecutionPlan::create();
    setupTests(streamCatalog, topologyManager);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>(topologyManager, streamCatalog, executionPlan);

    std::string queryString =
        "Query::from(\"default_logical\").filter(Attribute(\"value\") < 42).sink(PrintSinkDescriptor::create()); ";

    const string queryId = queryCatalog->registerQuery(queryString,
                                                       "BottomUp");

    queryCatalog->markQueryAs(queryId, QueryStatus::Running);

    std::map<std::string, std::string> queries = queryCatalog->getQueriesWithStatus("running");
    EXPECT_EQ(queries.size(), 1);
    EXPECT_TRUE(queries.find(queryId) != queries.end());
}

TEST_F(QueryCatalogTest, throw_exception_when_query_status_is_unknown) {
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    TopologyManagerPtr topologyManager = std::make_shared<TopologyManager>();
    GlobalExecutionPlanPtr executionPlan = GlobalExecutionPlan::create();
    setupTests(streamCatalog, topologyManager);
    QueryCatalogPtr queryCatalog = std::make_shared<QueryCatalog>(topologyManager, streamCatalog, executionPlan);

    try {
        std::map<std::string, std::string> queries = queryCatalog->getQueriesWithStatus("something_random");
        NES_DEBUG("Should have thrown invalid argument exception");
        FAIL();
    } catch (std::invalid_argument e) {
        SUCCEED();
    }
}