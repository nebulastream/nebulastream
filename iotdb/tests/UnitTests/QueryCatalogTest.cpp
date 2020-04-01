#include "gtest/gtest.h"

#include <iostream>

#include <Catalogs/QueryCatalog.hpp>
#include <Services/CoordinatorService.hpp>

#include <Topology/NESTopologyManager.hpp>

#include <Util/Logger.hpp>

using namespace NES;

class QueryCatalogTest : public testing::Test {
 public:
  /* Will be called before any test in this class are executed. */
  static void SetUpTestCase() {
    std::cout << "Setup QueryCatalogTest test class." << std::endl;
  }

  /* Will be called before a test is executed. */
  void SetUp() {
    NES::setupLogging("QueryCatalogTest.log", NES::LOG_DEBUG);
    QueryCatalog::instance().clearQueries();
    NESTopologyManager::getInstance().resetNESTopologyPlan();
    const auto &kCoordinatorNode = NESTopologyManager::getInstance()
        .createNESCoordinatorNode(0, "127.0.0.1", CPUCapacity::HIGH);
    kCoordinatorNode->setPublishPort(4711);
    kCoordinatorNode->setReceivePort(4815);

    std::string ip = "127.0.0.1";
    uint16_t receive_port = 0;
    std::string host = "localhost";
    uint16_t publish_port = 4711;

    CoordinatorServicePtr coordinatorServicePtr =
        CoordinatorService::getInstance();
    coordinatorServicePtr->clearQueryCatalogs();
    for (int i = 1; i < 5; i++) {
      //FIXME: add node properties
      PhysicalStreamConfig streamConf;
      auto entry = coordinatorServicePtr->register_sensor(i, ip, publish_port,
                                                          receive_port, 2, "",
                                                          streamConf);
    }
    NES_DEBUG("FINISHED ADDING 5 Nodes to topology")
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

TEST_F(QueryCatalogTest, add_query) {
  std::string queryString =
      "InputQuery::from(default_logical).filter(default_logical[\"value\"] > 42).print(std::cout); ";

  string queryId = QueryCatalog::instance().registerQuery(queryString,
                                                          "BottomUp");
  map<string, QueryCatalogEntryPtr> reg = QueryCatalog::instance()
      .getRegisteredQueries();
  EXPECT_TRUE(reg.size() == 1);

  map<string, QueryCatalogEntryPtr> run = QueryCatalog::instance().getQueries(
      QueryStatus::Running);
  EXPECT_TRUE(run.size() == 0);

  EXPECT_TRUE(QueryCatalog::instance().queryExists(queryId));
}

TEST_F(QueryCatalogTest, add_query_and_start_stop) {
  std::string queryString =
      "InputQuery::from(default_logical).filter(default_logical[\"value\"] > 42).print(std::cout); ";

  string queryId = QueryCatalog::instance().registerQuery(queryString,
                                                          "BottomUp");
  map<string, QueryCatalogEntryPtr> reg = QueryCatalog::instance()
      .getRegisteredQueries();
  EXPECT_TRUE(reg.size() == 1);

  map<string, QueryCatalogEntryPtr> run = QueryCatalog::instance().getQueries(
      QueryStatus::Running);
  EXPECT_TRUE(run.size() == 0);

  QueryCatalog::instance().markQueryAs(queryId, QueryStatus::Running);
  map<string, QueryCatalogEntryPtr> run_new = QueryCatalog::instance()
      .getQueries(QueryStatus::Running);
  EXPECT_TRUE(run_new.size() == 1);

  EXPECT_TRUE(QueryCatalog::instance().queryExists(queryId));
  EXPECT_TRUE(QueryCatalog::instance().isQueryRunning(queryId));

  QueryCatalog::instance().markQueryAs(queryId, QueryStatus::Stopped);
  map<string, QueryCatalogEntryPtr> run_new_stop = QueryCatalog::instance()
      .getQueries(QueryStatus::Running);
  EXPECT_TRUE(run_new_stop.size() == 0);

  EXPECT_FALSE(QueryCatalog::instance().isQueryRunning(queryId));
}

TEST_F(QueryCatalogTest, add_remove_query) {
  std::string queryString =
      "InputQuery::from(default_logical).filter(default_logical[\"value\"] > 42).print(std::cout); ";

  string queryId = QueryCatalog::instance().registerQuery(queryString,
                                                          "BottomUp");
  map<string, QueryCatalogEntryPtr> reg = QueryCatalog::instance()
      .getRegisteredQueries();
  EXPECT_TRUE(reg.size() == 1);

  map<string, QueryCatalogEntryPtr> run = QueryCatalog::instance().getQueries(
      QueryStatus::Running);
  EXPECT_TRUE(run.size() == 0);

  QueryCatalog::instance().deleteQuery(queryId);
  EXPECT_FALSE(QueryCatalog::instance().queryExists(queryId));
}

TEST_F(QueryCatalogTest, print_query) {
  std::string queryString =
      "InputQuery::from(default_logical).filter(default_logical[\"value\"] > 42).print(std::cout); ";

  string queryId = QueryCatalog::instance().registerQuery(queryString,
                                                          "BottomUp");
  map<string, QueryCatalogEntryPtr> reg = QueryCatalog::instance()
      .getRegisteredQueries();
  EXPECT_TRUE(reg.size() == 1);

  std::string ret = QueryCatalog::instance().printQueries();
  cout << "ret=" << ret << endl;
}

