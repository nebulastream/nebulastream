#include <gtest/gtest.h>
#include <SourceSink/PrintSink.hpp>
#include <Services/CoordinatorService.hpp>
#include <SourceSink/ZmqSource.hpp>

using namespace iotdb;

class CoordinatorCafTest : public testing::Test {
 public:
  /* Will be called before any test in this class are executed. */
  static void SetUpTestCase() {
    std::cout << "Setup NES Coordinator test class." << std::endl;
  }

  /* Will be called before a test is executed. */
  void SetUp() {
    std::cout << "Setup NES Coordinator test case." << std::endl;

    FogTopologyManager::getInstance().resetFogTopologyPlan();
    const auto &kCoordinatorNode = FogTopologyManager::getInstance()
        .createFogCoordinatorNode("127.0.0.1", CPUCapacity::HIGH);
    kCoordinatorNode->setPublishPort(4711);
    kCoordinatorNode->setReceivePort(4815);

    coordinatorServicePtr = CoordinatorService::getInstance();
    coordinatorServicePtr->clearQueryCatalogs();
    for (int i = 0; i < 5; i++) {
      auto entry = coordinatorServicePtr->register_sensor(ip, publish_port, receive_port, 2, sensor_type);
    }
  }

  /* Will be called before a test is executed. */
  void TearDown() { std::cout << "Setup NES Coordinator test case." << std::endl; }

  /* Will be called after all tests in this class are finished. */
  static void TearDownTestCase() { std::cout << "Tear down NES Coordinator test class." << std::endl; }

  CoordinatorServicePtr coordinatorServicePtr;
  std::string ip = "127.0.0.1";
  uint16_t receive_port = 0;
  std::string host = "localhost";
  uint16_t publish_port = 4711;
  std::string sensor_type = "cars";
};

/* Test serialization for Schema  */
TEST_F(CoordinatorCafTest, test_registration_and_topology) {
  string topo = coordinatorServicePtr->getTopologyPlanString();
  string expectedTopo = "graph G {\n"
                        "0[label=\"0 type=Coordinator\"];\n"
                        "1[label=\"1 type=Sensor(cars)\"];\n"
                        "2[label=\"2 type=Sensor(cars)\"];\n"
                        "3[label=\"3 type=Sensor(cars)\"];\n"
                        "4[label=\"4 type=Sensor(cars)\"];\n"
                        "5[label=\"5 type=Sensor(cars)\"];\n"
                        "1--0 [label=\"0\"];\n"
                        "2--0 [label=\"1\"];\n"
                        "3--0 [label=\"2\"];\n"
                        "4--0 [label=\"3\"];\n"
                        "5--0 [label=\"4\"];\n"
                        "}\n";
  EXPECT_EQ(topo, expectedTopo);
}

TEST_F(CoordinatorCafTest, test_deregistration_and_topology) {
  auto entry = coordinatorServicePtr->register_sensor(ip, publish_port, receive_port, 2, sensor_type + "_delete_me");
  string expectedTopo1 = "graph G {\n"
                         "0[label=\"0 type=Coordinator\"];\n"
                         "1[label=\"1 type=Sensor(cars)\"];\n"
                         "2[label=\"2 type=Sensor(cars)\"];\n"
                         "3[label=\"3 type=Sensor(cars)\"];\n"
                         "4[label=\"4 type=Sensor(cars)\"];\n"
                         "5[label=\"5 type=Sensor(cars)\"];\n"
                         "6[label=\"6 type=Sensor(cars_delete_me)\"];\n"
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
                         "1[label=\"1 type=Sensor(cars)\"];\n"
                         "2[label=\"2 type=Sensor(cars)\"];\n"
                         "3[label=\"3 type=Sensor(cars)\"];\n"
                         "4[label=\"4 type=Sensor(cars)\"];\n"
                         "5[label=\"5 type=Sensor(cars)\"];\n"
                         "1--0 [label=\"0\"];\n"
                         "2--0 [label=\"1\"];\n"
                         "3--0 [label=\"2\"];\n"
                         "4--0 [label=\"3\"];\n"
                         "5--0 [label=\"4\"];\n"
                         "}\n";
  EXPECT_EQ(coordinatorServicePtr->getTopologyPlanString(), expectedTopo2);
}

TEST_F(CoordinatorCafTest, test_register_query) {
  string queryId = coordinatorServicePtr->register_query("example", "BottomUp");
  EXPECT_TRUE(coordinatorServicePtr->getRegisteredQueries().size() == 1);

  string expectedPlacement = "graph G {\n"
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
  const FogExecutionPlan *kExecutionPlan = coordinatorServicePtr->getRegisteredQuery(queryId);
  EXPECT_EQ(kExecutionPlan->getTopologyPlanString(), expectedPlacement);
}

TEST_F(CoordinatorCafTest, test_register_deregister_query) {
  string queryId = coordinatorServicePtr->register_query("example", "BottomUp");
  EXPECT_TRUE(coordinatorServicePtr->getRegisteredQueries().size() == 1);
  coordinatorServicePtr->deregister_query(queryId);
  EXPECT_TRUE(coordinatorServicePtr->getRegisteredQueries().empty());
}

TEST_F(CoordinatorCafTest, test_make_deployment) {
  string queryId = coordinatorServicePtr->register_query("example", "BottomUp");
  EXPECT_TRUE(coordinatorServicePtr->getRegisteredQueries().size() == 1);
  unordered_map<FogTopologyEntryPtr, ExecutableTransferObject> etos = coordinatorServicePtr->make_deployment(queryId);
  EXPECT_TRUE(etos.size() == 2);

  for (auto &x : etos) {
    EXPECT_TRUE(x.first);
    string s_eto = SerializationTools::ser_eto(x.second);
    EXPECT_TRUE(!s_eto.empty());
    SerializationTools::parse_eto(s_eto);
  }
  EXPECT_TRUE(coordinatorServicePtr->getRegisteredQueries().empty());
  EXPECT_TRUE(coordinatorServicePtr->getRunningQueries().size() == 1);
}

TEST_F(CoordinatorCafTest, test_run_deregister_query) {
  string queryId = coordinatorServicePtr->register_query("example", "BottomUp");
  EXPECT_TRUE(coordinatorServicePtr->getRegisteredQueries().size() == 1);
  unordered_map<FogTopologyEntryPtr, ExecutableTransferObject> etos = coordinatorServicePtr->make_deployment(queryId);
  EXPECT_TRUE(etos.size() == 2);
  EXPECT_TRUE(coordinatorServicePtr->getRegisteredQueries().empty());
  EXPECT_TRUE(coordinatorServicePtr->getRunningQueries().size() == 1);

  coordinatorServicePtr->deregister_query(queryId);
  EXPECT_TRUE(coordinatorServicePtr->getRegisteredQueries().empty());
  EXPECT_TRUE(coordinatorServicePtr->getRunningQueries().empty());
}

TEST_F(CoordinatorCafTest, test_compile_deployment) {
  string queryId = coordinatorServicePtr->register_query("example", "BottomUp");
  EXPECT_TRUE(coordinatorServicePtr->getRegisteredQueries().size() == 1);
  unordered_map<FogTopologyEntryPtr, ExecutableTransferObject> etos = coordinatorServicePtr->make_deployment(queryId);
  EXPECT_TRUE(etos.size() == 2);

  for (auto &x : etos) {
    ExecutableTransferObject eto = x.second;
    QueryExecutionPlanPtr qep = eto.toQueryExecutionPlan();
    EXPECT_TRUE(qep);
  }
  EXPECT_TRUE(coordinatorServicePtr->getRegisteredQueries().empty());
  EXPECT_TRUE(coordinatorServicePtr->getRunningQueries().size() == 1);
}

TEST_F(CoordinatorCafTest, test_code_gen) {
  auto *engine = new NodeEngine();
  engine->start();

  Schema schema = Schema::create()
      .addField("id", BasicType::UINT32)
      .addField("value", BasicType::UINT64);

  Stream cars = Stream("cars", schema);

  InputQuery &query = InputQuery::from(cars)
      .filter(cars["value"] > 42)
      .print(std::cout);

  CodeGeneratorPtr code_gen = createCodeGenerator();
  PipelineContextPtr context = createPipelineContext();

  OperatorPtr queryOp = query.getRoot();

  queryOp->produce(code_gen, context, std::cout);
  PipelineStagePtr stage = code_gen->compile(CompilerArgs());

  GeneratedQueryExecutionPlanPtr qep(new GeneratedQueryExecutionPlan(nullptr, stage));

  // Create new Source and Sink
  DataSourcePtr source = createTestDataSourceWithSchema(schema);
  source->setNumBuffersToProcess(10);
  qep->addDataSource(source);

  DataSinkPtr sink = createPrintSinkWithSink(schema, std::cout);
  qep->addDataSink(sink);

  engine->deployQuery(qep);
  engine->stopWithUndeploy();
}

TEST_F(CoordinatorCafTest, test_local_distributed_deployment) {
  auto *engine = new NodeEngine();
  engine->start();
  string queryId = coordinatorServicePtr->register_query("example", "BottomUp");
  EXPECT_EQ(coordinatorServicePtr->getRegisteredQueries().size(), 1);
  unordered_map<FogTopologyEntryPtr, ExecutableTransferObject> etos = coordinatorServicePtr->make_deployment(queryId);
  EXPECT_TRUE(etos.size() == 2);

  vector<QueryExecutionPlanPtr> qeps;
  for (auto &x : etos) {
    FogTopologyEntryPtr v = x.first;
    cout << "Deploying QEP for " << v->getEntryTypeString() << endl;
    ExecutableTransferObject eto = x.second;
    QueryExecutionPlanPtr qep = eto.toQueryExecutionPlan();
    EXPECT_TRUE(qep);
    engine->deployQuery(qep);
    qeps.push_back(qep);
  }
  EXPECT_TRUE(coordinatorServicePtr->getRegisteredQueries().empty());
  EXPECT_TRUE(coordinatorServicePtr->getRunningQueries().size() == 1);

  for (const QueryExecutionPlanPtr &qep: qeps) {
    engine->undeployQuery(qep);
  }
  engine->stopWithUndeploy();

  coordinatorServicePtr->deregister_query("example");
  EXPECT_TRUE(coordinatorServicePtr->getRegisteredQueries().empty() && coordinatorServicePtr->getRunningQueries().empty());
}

TEST_F(CoordinatorCafTest, DISABLED_test_sequential_local_distributed_deployment) {
  auto *engine = new NodeEngine();
  engine->start();
  for (int i=0; i<15; i++) {
    string queryId = coordinatorServicePtr->register_query("example", "BottomUp");
    EXPECT_EQ(coordinatorServicePtr->getRegisteredQueries().size(), 1);
    unordered_map<FogTopologyEntryPtr, ExecutableTransferObject> etos = coordinatorServicePtr->make_deployment(queryId);
    EXPECT_TRUE(etos.size() == 2);

    vector<QueryExecutionPlanPtr> qeps;
    for (auto &x : etos) {
      FogTopologyEntryPtr v = x.first;
      cout << "Deploying QEP for " << v->getEntryTypeString() << endl;
      ExecutableTransferObject eto = x.second;
      QueryExecutionPlanPtr qep = eto.toQueryExecutionPlan();
      EXPECT_TRUE(qep);
      engine->deployQuery(qep);
      qeps.push_back(qep);
    }
    EXPECT_TRUE(coordinatorServicePtr->getRegisteredQueries().empty());
    EXPECT_TRUE(coordinatorServicePtr->getRunningQueries().size() == 1);

    for (const QueryExecutionPlanPtr &qep: qeps) {
      engine->undeployQuery(qep);
    }

    coordinatorServicePtr->deregister_query("example");
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
  engine->stopWithUndeploy();
  EXPECT_TRUE(coordinatorServicePtr->getRegisteredQueries().empty() && coordinatorServicePtr->getRunningQueries().empty());
}