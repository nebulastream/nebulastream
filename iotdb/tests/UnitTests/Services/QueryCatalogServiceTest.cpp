#include <gtest/gtest.h>
#include <Catalogs/QueryCatalog.hpp>
#include <Services/QueryCatalogService.hpp>
#include <Util/Logger.hpp>
#include <Topology/NESTopologyManager.hpp>
#include <Catalogs/PhysicalStreamConfig.hpp>
#include <Services/CoordinatorService.hpp>

using namespace NES;

class QueryCatalogServiceTest : public testing::Test {
 public:

  std::string testSchema =
      "SchemaPtr schema = Schema::create()->addField(\"id\", BasicType::UINT32)"
          "->addField(\"value\", BasicType::UINT64);";
  const std::string defaultLogicalStreamName = "default_logical";

  /* Will be called before any test in this class are executed. */
  static void SetUpTestCase() {
    std::cout << "Setup QueryCatalogService test class." << std::endl;
  }

  /* Will be called before a test is executed. */
  void SetUp() {
    NES::setupLogging("QueryCatalogServiceTest.log", NES::LOG_DEBUG);

    QueryCatalog::instance().clearQueries();
    NESTopologyManager::getInstance().resetNESTopologyPlan();
    const auto &kCoordinatorNode = NESTopologyManager::getInstance()
        .createNESWorkerNode(0, "127.0.0.1", CPUCapacity::HIGH);
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
      PhysicalStreamConfig streamConf;
        std::string address = ip + ":" + std::to_string(publish_port);
      auto entry = coordinatorServicePtr->registerNode(i, address, 2, "",
                                                       streamConf, NESNodeType::Sensor);
    }
    NES_DEBUG("FINISHED ADDING 5 Nodes to topology");
    std::cout << "Setup QueryCatalogService test case." << std::endl;
  }

  /* Will be called before a test is executed. */
  void TearDown() {
    std::cout << "Setup QueryCatalogService case." << std::endl;
  }

  /* Will be called after all tests in this class are finished. */
  static void TearDownTestCase() {
    std::cout << "Tear down QueryCatalogService class." << std::endl;
  }
};

TEST_F(QueryCatalogServiceTest, get_all_registered_queries_without_query_registration) {

  QueryCatalogServicePtr queryCatalogServicePtr =
      QueryCatalogService::getInstance();
  std::map<std::string, std::string> allRegisteredQueries =
      queryCatalogServicePtr->getAllRegisteredQueries();
  EXPECT_TRUE(allRegisteredQueries.empty());
}

TEST_F(QueryCatalogServiceTest, get_all_registered_queries_after_query_registration) {

  std::string queryString =
      "InputQuery::from(default_logical).filter(default_logical[\"value\"] < 42).print(std::cout); ";

  const string queryId = QueryCatalog::instance().registerQuery(queryString,
                                                                "BottomUp");

  QueryCatalogServicePtr queryCatalogServicePtr =
      QueryCatalogService::getInstance();
  std::map<std::string, std::string> allRegisteredQueries =
      queryCatalogServicePtr->getAllRegisteredQueries();
  EXPECT_EQ(allRegisteredQueries.size(), 1);
  EXPECT_TRUE(allRegisteredQueries.find(queryId) != allRegisteredQueries.end());
}

TEST_F(QueryCatalogServiceTest, get_all_running_queries) {

  std::string queryString =
      "InputQuery::from(default_logical).filter(default_logical[\"value\"] < 42).print(std::cout); ";

  const string queryId = QueryCatalog::instance().registerQuery(queryString,
                                                                "BottomUp");

  QueryCatalog::instance().markQueryAs(queryId, QueryStatus::Running);

  QueryCatalogServicePtr queryCatalogServicePtr =
      QueryCatalogService::getInstance();
  std::map<std::string, std::string> queries = queryCatalogServicePtr
      ->getQueriesWithStatus("running");
  EXPECT_EQ(queries.size(), 1);
  EXPECT_TRUE(queries.find(queryId) != queries.end());
}

TEST_F(QueryCatalogServiceTest, throw_exception_when_query_status_is_unknown) {

  try {
    QueryCatalogServicePtr queryCatalogServicePtr =
        QueryCatalogService::getInstance();
    std::map<std::string, std::string> queries = queryCatalogServicePtr
        ->getQueriesWithStatus("something_random");
    NES_DEBUG("Should have thrown invalid argument exception");
    FAIL();
  } catch (std::invalid_argument e) {
    SUCCEED();
  }
}

