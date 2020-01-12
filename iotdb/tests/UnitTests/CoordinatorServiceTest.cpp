#include <gtest/gtest.h>
#include <SourceSink/PrintSink.hpp>
#include <Services/CoordinatorService.hpp>
#include <SourceSink/ZmqSource.hpp>

#include <QueryCompiler/GeneratedQueryExecutionPlan.hpp>
#include <QueryCompiler/QueryCompiler.hpp>
#include <Util/Logger.hpp>

using namespace NES;

class CoordinatorCafTest : public testing::Test {
 public:
  std::string queryString =
//      "Schema schema = Schema::create()"
//          ".addField(\"id\", BasicType::UINT32)"
//          ".addField(\"value\", BasicType::UINT64);"
//          "Stream stream = Stream(\"default_logical\", schema);"
          "return InputQuery::from(default_logical).filter(default_logical[\"value\"] > 42).print(std::cout); ";

  /* Will be called before any test in this class are executed. */
  static void SetUpTestCase() {
    std::cout << "Setup NES Coordinator test class." << std::endl;
  }

  /* Will be called before a test is executed. */
  void SetUp() {
    std::cout << "Setup NES Coordinator test case." << std::endl;
    setupLogging();

    NESTopologyManager::getInstance().resetNESTopologyPlan();
    const auto& kCoordinatorNode = NESTopologyManager::getInstance()
        .createNESCoordinatorNode("127.0.0.1", CPUCapacity::HIGH);
    kCoordinatorNode->setPublishPort(4711);
    kCoordinatorNode->setReceivePort(4815);

    coordinatorServicePtr = CoordinatorService::getInstance();
    coordinatorServicePtr->clearQueryCatalogs();
    for (int i = 0; i < 5; i++) {
      //FIXME: add node properties
      PhysicalStreamConfig streamConf;
    auto entry = coordinatorServicePtr->register_sensor(ip, publish_port,
        receive_port, 2
        ,"", streamConf);
  }
    IOTDB_DEBUG("FINISHED ADDING 5 Nodes to topology")
}

/* Will be called before a test is executed. */
void TearDown() {
  std::cout << "Setup NES Coordinator test case." << std::endl;
}

/* Will be called after all tests in this class are finished. */
static void TearDownTestCase() {
  std::cout << "Tear down NES Coordinator test class." << std::endl;
}
 protected:
  static void setupLogging() {
      // create PatternLayout
      log4cxx::LayoutPtr layoutPtr(
          new log4cxx::PatternLayout(
              "%d{MMM dd yyyy HH:mm:ss} %c:%L [%-5t] [%p] : %m%n"));

      // create FileAppender
      LOG4CXX_DECODE_CHAR(fileName, "CoordinatorServiceTest.log");
      log4cxx::FileAppenderPtr file(
          new log4cxx::FileAppender(layoutPtr, fileName));

      // create ConsoleAppender
      log4cxx::ConsoleAppenderPtr console(
          new log4cxx::ConsoleAppender(layoutPtr));

      // set log level
      iotdbLogger->setLevel(log4cxx::Level::getDebug());
  //    logger->setLevel(log4cxx::Level::getInfo());

  // add appenders and other will inherit the settings
      iotdbLogger->addAppender(file);
      iotdbLogger->addAppender(console);
    }


CoordinatorServicePtr coordinatorServicePtr;
std::string ip = "127.0.0.1";
uint16_t receive_port = 0;
std::string host = "localhost";
uint16_t publish_port = 4711;
//std::string sensor_type = "default";
};

/* Test serialization for Schema  */
TEST_F(CoordinatorCafTest, test_registration_and_topology) {
  string topo = coordinatorServicePtr->getTopologyPlanString();
  std::cout << "current topo=" << topo << std::endl;
  string expectedTopo = "graph G {\n"
      "0[label=\"0 type=Coordinator\"];\n"
      "1[label=\"1 type=Sensor(default_physical)\"];\n"
      "2[label=\"2 type=Sensor(default_physical)\"];\n"
      "3[label=\"3 type=Sensor(default_physical)\"];\n"
      "4[label=\"4 type=Sensor(default_physical)\"];\n"
      "5[label=\"5 type=Sensor(default_physical)\"];\n"
      "1--0 [label=\"0\"];\n"
      "2--0 [label=\"1\"];\n"
      "3--0 [label=\"2\"];\n"
      "4--0 [label=\"3\"];\n"
      "5--0 [label=\"4\"];\n"
      "}\n";
  EXPECT_EQ(topo, expectedTopo);
}

TEST_F(CoordinatorCafTest, test_node_properties) {
  //FIXME:provide own properties
  string nodeProps =
      "{\"cpus\":[{\"guest\":0,\"guest_nice\":0,\"idle\":1777049,\"iowait\":3074,\"irq\":0,\"name\":\"cpu\",\"nice\":186,\"softirq\":1276,\"steal\":0,\"system\":15308,\"user\":122112},{\"guest\":0,\"guest_nice\":0,\"idle\":226210,\"iowait\":284,\"irq\":0,\"name\":\"cpu0\",\"nice\":11,\"softirq\":392,\"steal\":0,\"system\":1588,\"user\":11438},{\"guest\":0,\"guest_nice\":0,\"idle\":220835,\"iowait\":307,\"irq\":0,\"name\":\"cpu1\",\"nice\":15,\"softirq\":255,\"steal\":0,\"system\":1902,\"user\":16182},{\"guest\":0,\"guest_nice\":0,\"idle\":220454,\"iowait\":324,\"irq\":0,\"name\":\"cpu2\",\"nice\":14,\"softirq\":126,\"steal\":0,\"system\":1974,\"user\":17030},{\"guest\":0,\"guest_nice\":0,\"idle\":221382,\"iowait\":332,\"irq\":0,\"name\":\"cpu3\",\"nice\":21,\"softirq\":130,\"steal\":0,\"system\":2240,\"user\":15955},{\"guest\":0,\"guest_nice\":0,\"idle\":218898,\"iowait\":564,\"irq\":0,\"name\":\"cpu4\",\"nice\":7,\"softirq\":88,\"steal\":0,\"system\":1957,\"user\":18461},{\"guest\":0,\"guest_nice\":0,\"idle\":222420,\"iowait\":442,\"irq\":0,\"name\":\"cpu5\",\"nice\":103,\"softirq\":90,\"steal\":0,\"system\":1931,\"user\":15007},{\"guest\":0,\"guest_nice\":0,\"idle\":223872,\"iowait\":402,\"irq\":0,\"name\":\"cpu6\",\"nice\":6,\"softirq\":84,\"steal\":0,\"system\":1811,\"user\":13855},{\"guest\":0,\"guest_nice\":0,\"idle\":222973,\"iowait\":415,\"irq\":0,\"name\":\"cpu7\",\"nice\":7,\"softirq\":108,\"steal\":0,\"system\":1901,\"user\":14182}],\"fs\":{\"f_bavail\":8782124,\"f_bfree\":12246175,\"f_blocks\":68080498,\"f_bsize\":4096,\"f_frsize\":4096},\"mem\":{\"bufferram\":457990144,\"freehigh\":0,\"freeram\":6954029056,\"freeswap\":16893603840,\"loads_15min\":46432,\"loads_1min\":87456,\"loads_5min\":69536,\"mem_unit\":1,\"procs\":1512,\"sharedram\":820908032,\"totalhigh\":0,\"totalram\":16534970368,\"totalswap\":16893603840},\"nets\":[{\"hostname\":\"\",\"port\":\"\"},{\"enp0s31f6\":{\"rx_bytes\":0,\"rx_dropped\":0,\"rx_packets\":0,\"tx_bytes\":0,\"tx_dropped\":0,\"tx_packets\":0},\"wlp3s0\":{\"host\":\"192.168.178.37\",\"host6\":\"fe80::52ca:ec35:fc8d:2954%wlp3s0\"}},{\"vmnet1\":{\"host\":\"172.16.158.1\",\"host6\":\"fe80::250:56ff:fec0:1%vmnet1\"},\"wlp3s0\":{\"rx_bytes\":28886252,\"rx_dropped\":0,\"rx_packets\":27532,\"tx_bytes\":3055394,\"tx_dropped\":0,\"tx_packets\":15798}},{\"vmnet1\":{\"rx_bytes\":0,\"rx_dropped\":0,\"rx_packets\":0,\"tx_bytes\":0,\"tx_dropped\":0,\"tx_packets\":95},\"vmnet8\":{\"host\":\"192.168.74.1\",\"host6\":\"fe80::250:56ff:fec0:8%vmnet8\"}},{\"virbr0\":{\"host\":\"192.168.122.1\"},\"vmnet8\":{\"rx_bytes\":0,\"rx_dropped\":0,\"rx_packets\":0,\"tx_bytes\":0,\"tx_dropped\":0,\"tx_packets\":95}},{\"docker0\":{\"host\":\"172.17.0.1\"},\"virbr0\":{\"rx_bytes\":0,\"rx_dropped\":0,\"rx_packets\":0,\"tx_bytes\":0,\"tx_dropped\":0,\"tx_packets\":0}},{\"docker0\":{\"rx_bytes\":0,\"rx_dropped\":0,\"rx_packets\":0,\"tx_bytes\":0,\"tx_dropped\":0,\"tx_packets\":0}}]}";
  PhysicalStreamConfig streamConf;
  streamConf.physicalStreamName = "default_test_node_props";
  auto entry = coordinatorServicePtr->register_sensor(
      ip, publish_port, receive_port, 2, nodeProps, streamConf);

  string expectedProperties = coordinatorServicePtr->getNodePropertiesAsString(
      entry);
  EXPECT_EQ(nodeProps, expectedProperties);

}

TEST_F(CoordinatorCafTest, test_deregistration_and_topology) {
  //FIXME:provide own properties
  PhysicalStreamConfig streamConf;
  streamConf.physicalStreamName = "default_delete_me";
  streamConf.logicalStreamName = "default_delete_me";

  StreamCatalog::instance().addLogicalStream("default_delete_me", std::make_shared<Schema>(Schema()));
  auto entry = coordinatorServicePtr->register_sensor(
      ip, publish_port, receive_port, 2, "", streamConf);

  EXPECT_NE(entry, nullptr);

  string expectedTopo1 = "graph G {\n"
      "0[label=\"0 type=Coordinator\"];\n"
      "1[label=\"1 type=Sensor(default_physical)\"];\n"
      "2[label=\"2 type=Sensor(default_physical)\"];\n"
      "3[label=\"3 type=Sensor(default_physical)\"];\n"
      "4[label=\"4 type=Sensor(default_physical)\"];\n"
      "5[label=\"5 type=Sensor(default_physical)\"];\n"
      "6[label=\"6 type=Sensor(default_delete_me)\"];\n"
      "1--0 [label=\"0\"];\n"
      "2--0 [label=\"1\"];\n"
      "3--0 [label=\"2\"];\n"
      "4--0 [label=\"3\"];\n"
      "5--0 [label=\"4\"];\n"
      "6--0 [label=\"5\"];\n"
      "}\n";
  EXPECT_EQ(coordinatorServicePtr->getTopologyPlanString(), expectedTopo1);

  coordinatorServicePtr->deregister_sensor(entry);
  string expectedTopo2 = "graph G {\n"
      "0[label=\"0 type=Coordinator\"];\n"
      "1[label=\"1 type=Sensor(default_physical)\"];\n"
      "2[label=\"2 type=Sensor(default_physical)\"];\n"
      "3[label=\"3 type=Sensor(default_physical)\"];\n"
      "4[label=\"4 type=Sensor(default_physical)\"];\n"
      "5[label=\"5 type=Sensor(default_physical)\"];\n"
      "1--0 [label=\"0\"];\n"
      "2--0 [label=\"1\"];\n"
      "3--0 [label=\"2\"];\n"
      "4--0 [label=\"3\"];\n"
      "5--0 [label=\"4\"];\n"
      "}\n";
  EXPECT_EQ(coordinatorServicePtr->getTopologyPlanString(), expectedTopo2);
}

TEST_F(CoordinatorCafTest, test_register_query) {
  string queryId = coordinatorServicePtr->register_query(queryString,
                                                         "BottomUp");
  EXPECT_TRUE(coordinatorServicePtr->getRegisteredQueries().size() == 1);

  string expectedPlacement =
      "graph G {\n"
          "0[label=\"1 operatorName=SOURCE(OP-1)=>FILTER(OP-2)=>SINK(SYS) nodeName=1\"];\n"
          "1[label=\"0 operatorName=SOURCE(SYS)=>SINK(OP-3) nodeName=0\"];\n"
          "2[label=\"2 operatorName=empty nodeName=2\"];\n"
          "3[label=\"3 operatorName=empty nodeName=3\"];\n"
          "4[label=\"4 operatorName=empty nodeName=4\"];\n"
          "5[label=\"5 operatorName=empty nodeName=5\"];\n"
          "0--1 [label=\"1\"];\n"
          "2--1 [label=\"2\"];\n"
          "3--1 [label=\"3\"];\n"
          "4--1 [label=\"4\"];\n"
          "5--1 [label=\"5\"];\n"
          "}\n";
  const NESExecutionPlan* kExecutionPlan = coordinatorServicePtr
      ->getRegisteredQuery(queryId);
  EXPECT_EQ(kExecutionPlan->getTopologyPlanString(), expectedPlacement);
}

TEST_F(CoordinatorCafTest, test_register_deregister_query) {
  string queryId = coordinatorServicePtr->register_query(queryString,
                                                         "BottomUp");
  EXPECT_TRUE(coordinatorServicePtr->getRegisteredQueries().size() == 1);
  coordinatorServicePtr->deregister_query(queryId);
  EXPECT_TRUE(coordinatorServicePtr->getRegisteredQueries().empty());
}

TEST_F(CoordinatorCafTest, test_make_deployment) {
  string queryId = coordinatorServicePtr->register_query(queryString,
                                                         "BottomUp");
  EXPECT_TRUE(coordinatorServicePtr->getRegisteredQueries().size() == 1);
  unordered_map<NESTopologyEntryPtr, ExecutableTransferObject> etos =
      coordinatorServicePtr->make_deployment(queryId);
  EXPECT_TRUE(etos.size() == 2);

  for (auto& x : etos) {
    EXPECT_TRUE(x.first);
    string s_eto = SerializationTools::ser_eto(x.second);
    EXPECT_TRUE(!s_eto.empty());
    SerializationTools::parse_eto(s_eto);
  }
  EXPECT_TRUE(coordinatorServicePtr->getRegisteredQueries().empty());
  EXPECT_TRUE(coordinatorServicePtr->getRunningQueries().size() == 1);
}

TEST_F(CoordinatorCafTest, test_run_deregister_query) {
  string queryId = coordinatorServicePtr->register_query(queryString,
                                                         "BottomUp");
  EXPECT_TRUE(coordinatorServicePtr->getRegisteredQueries().size() == 1);
  unordered_map<NESTopologyEntryPtr, ExecutableTransferObject> etos =
      coordinatorServicePtr->make_deployment(queryId);
  EXPECT_TRUE(etos.size() == 2);
  EXPECT_TRUE(coordinatorServicePtr->getRegisteredQueries().empty());
  EXPECT_TRUE(coordinatorServicePtr->getRunningQueries().size() == 1);

  coordinatorServicePtr->deregister_query(queryId);
  EXPECT_TRUE(coordinatorServicePtr->getRegisteredQueries().empty());
  EXPECT_TRUE(coordinatorServicePtr->getRunningQueries().empty());
}

TEST_F(CoordinatorCafTest, test_compile_deployment) {
  string queryId = coordinatorServicePtr->register_query(queryString,
                                                         "BottomUp");
  EXPECT_TRUE(coordinatorServicePtr->getRegisteredQueries().size() == 1);
  unordered_map<NESTopologyEntryPtr, ExecutableTransferObject> etos =
      coordinatorServicePtr->make_deployment(queryId);
  EXPECT_TRUE(etos.size() == 2);

  for (auto& x : etos) {
    ExecutableTransferObject eto = x.second;
    QueryExecutionPlanPtr qep = eto.toQueryExecutionPlan();
    EXPECT_TRUE(qep);
  }
  EXPECT_TRUE(coordinatorServicePtr->getRegisteredQueries().empty());
  EXPECT_TRUE(coordinatorServicePtr->getRunningQueries().size() == 1);
}

TEST_F(CoordinatorCafTest, test_code_gen) {
  auto* engine = new NodeEngine();
  engine->start();

  Schema schema = Schema::create().addField("id", BasicType::UINT32).addField(
      "value", BasicType::UINT64);

  Stream def = Stream("default", schema);

  InputQuery& query = InputQuery::from(def).filter(def["value"] > 42).print(
      std::cout);

  auto queryCompiler = createDefaultQueryCompiler();
  QueryExecutionPlanPtr qep = queryCompiler->compile(query.getRoot());
  // Create new Source and Sink
  DataSourcePtr source = createTestDataSourceWithSchema(schema);
  source->setNumBuffersToProcess(10);
  qep->addDataSource(source);

  DataSinkPtr sink = createPrintSinkWithSchema(schema, std::cout);
  qep->addDataSink(sink);

  engine->deployQuery(qep);
  sleep(2);
  engine->stopWithUndeploy();
}

//FIXME: this test times out
TEST_F(CoordinatorCafTest, DISABLED_test_local_distributed_deployment) {
  auto* engine = new NodeEngine();
  engine->start();
  string queryId = coordinatorServicePtr->register_query(queryString,
                                                         "BottomUp");
  EXPECT_EQ(coordinatorServicePtr->getRegisteredQueries().size(), 1);
  unordered_map<NESTopologyEntryPtr, ExecutableTransferObject> etos =
      coordinatorServicePtr->make_deployment(queryId);
  EXPECT_TRUE(etos.size() == 2);

  vector<QueryExecutionPlanPtr> qeps;
  for (auto& x : etos) {
    NESTopologyEntryPtr v = x.first;
    cout << "Deploying QEP for " << v->getEntryTypeString() << endl;
    ExecutableTransferObject eto = x.second;
    QueryExecutionPlanPtr qep = eto.toQueryExecutionPlan();
    EXPECT_TRUE(qep);
    engine->deployQuery(qep);
    qeps.push_back(qep);
  }
  EXPECT_TRUE(coordinatorServicePtr->getRegisteredQueries().empty());
  EXPECT_TRUE(coordinatorServicePtr->getRunningQueries().size() == 1);

  for (const QueryExecutionPlanPtr& qep : qeps) {
    engine->undeployQuery(qep);
  }
  engine->stopWithUndeploy();

  coordinatorServicePtr->deregister_query(queryId);
  EXPECT_TRUE(
      coordinatorServicePtr->getRegisteredQueries().empty()
          && coordinatorServicePtr->getRunningQueries().empty());
}

//TODO: Fixme, remove Thread.Sleep
TEST_F(CoordinatorCafTest, DISABLED_test_sequential_local_distributed_deployment) {
  auto* engine = new NodeEngine();
  engine->start();
  for (int i = 0; i < 15; i++) {
    string queryId = coordinatorServicePtr->register_query(queryString,
                                                           "BottomUp");
    EXPECT_EQ(coordinatorServicePtr->getRegisteredQueries().size(), 1);
    unordered_map<NESTopologyEntryPtr, ExecutableTransferObject> etos =
        coordinatorServicePtr->make_deployment(queryId);
    EXPECT_TRUE(etos.size() == 2);

    vector<QueryExecutionPlanPtr> qeps;
    for (auto& x : etos) {
      NESTopologyEntryPtr v = x.first;
      cout << "Deploying QEP for " << v->getEntryTypeString() << endl;
      ExecutableTransferObject eto = x.second;
      QueryExecutionPlanPtr qep = eto.toQueryExecutionPlan();
      EXPECT_TRUE(qep);
      engine->deployQuery(qep);
      qeps.push_back(qep);
    }
    EXPECT_TRUE(coordinatorServicePtr->getRegisteredQueries().empty());
    EXPECT_TRUE(coordinatorServicePtr->getRunningQueries().size() == 1);

    for (const QueryExecutionPlanPtr& qep : qeps) {
      engine->undeployQuery(qep);
    }

    coordinatorServicePtr->deregister_query(queryId);
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
  engine->stopWithUndeploy();
  EXPECT_TRUE(
      coordinatorServicePtr->getRegisteredQueries().empty()
          && coordinatorServicePtr->getRunningQueries().empty());
}
