#include <gtest/gtest.h>
#include <SourceSink/PrintSink.hpp>
#include <Actors/NesCoordinator.hpp>
#include <SourceSink/ZmqSource.hpp>

using namespace iotdb;

class CoordinatorCafTest : public testing::Test {
 public:
  /* Will be called before any test in this class are executed. */
  static void SetUpTestCase() {
    std::cout << "Setup SerializationToolsTest test class." << std::endl;
  }

  /* Will be called before a test is executed. */
  void SetUp() {
    std::cout << "Setup SerializationToolsTest test case." << std::endl;
    coordinatorPtr = std::make_shared<NesCoordinator>(NesCoordinator("127.0.0.1", 4711, 4815));
    for (int i = 0; i < 5; i++) {
      auto entry = coordinatorPtr->register_sensor(ip, publish_port, receive_port, 2, sensor_type + std::to_string(i));
    }
  }

  /* Will be called before a test is executed. */
  void TearDown() { std::cout << "Setup SerializationToolsTest test case." << std::endl; }

  /* Will be called after all tests in this class are finished. */
  static void TearDownTestCase() { std::cout << "Tear down SerializationToolsTest test class." << std::endl; }

  std::shared_ptr<NesCoordinator> coordinatorPtr;
  std::string ip = "127.0.0.1";
  uint16_t receive_port = 0;
  std::string host = "localhost";
  uint16_t publish_port = 4711;
  std::string sensor_type = "cars";
};

/* Test serialization for Schema  */
TEST_F(CoordinatorCafTest, test_registration_and_topology) {
  string topo = coordinatorPtr->getTopologyPlanString();
  string expectedTopo = "graph G {\n"
                        "0[label=\"0 type=Coordinator\"];\n"
                        "1[label=\"1 type=Sensor(cars0)\"];\n"
                        "2[label=\"2 type=Sensor(cars1)\"];\n"
                        "3[label=\"3 type=Sensor(cars2)\"];\n"
                        "4[label=\"4 type=Sensor(cars3)\"];\n"
                        "5[label=\"5 type=Sensor(cars4)\"];\n"
                        "1--0 [label=\"0\"];\n"
                        "2--0 [label=\"1\"];\n"
                        "3--0 [label=\"2\"];\n"
                        "4--0 [label=\"3\"];\n"
                        "5--0 [label=\"4\"];\n"
                        "}\n";
  EXPECT_EQ(topo, expectedTopo);
}

TEST_F(CoordinatorCafTest, test_deregistration_and_topology) {
  auto entry = coordinatorPtr->register_sensor(ip, publish_port, receive_port, 2, sensor_type + "_delete_me");
  string expectedTopo1 = "graph G {\n"
                         "0[label=\"0 type=Coordinator\"];\n"
                         "1[label=\"1 type=Sensor(cars0)\"];\n"
                         "2[label=\"2 type=Sensor(cars1)\"];\n"
                         "3[label=\"3 type=Sensor(cars2)\"];\n"
                         "4[label=\"4 type=Sensor(cars3)\"];\n"
                         "5[label=\"5 type=Sensor(cars4)\"];\n"
                         "6[label=\"6 type=Sensor(cars_delete_me)\"];\n"
                         "1--0 [label=\"0\"];\n"
                         "2--0 [label=\"1\"];\n"
                         "3--0 [label=\"2\"];\n"
                         "4--0 [label=\"3\"];\n"
                         "5--0 [label=\"4\"];\n"
                         "6--0 [label=\"5\"];\n"
                         "}\n";
  EXPECT_EQ(coordinatorPtr->getTopologyPlanString(), expectedTopo1);

  coordinatorPtr->deregister_sensor(entry);
  string expectedTopo2 = "graph G {\n"
                         "0[label=\"0 type=Coordinator\"];\n"
                         "1[label=\"1 type=Sensor(cars0)\"];\n"
                         "2[label=\"2 type=Sensor(cars1)\"];\n"
                         "3[label=\"3 type=Sensor(cars2)\"];\n"
                         "4[label=\"4 type=Sensor(cars3)\"];\n"
                         "5[label=\"5 type=Sensor(cars4)\"];\n"
                         "1--0 [label=\"0\"];\n"
                         "2--0 [label=\"1\"];\n"
                         "3--0 [label=\"2\"];\n"
                         "4--0 [label=\"3\"];\n"
                         "5--0 [label=\"4\"];\n"
                         "}\n";
  EXPECT_EQ(coordinatorPtr->getTopologyPlanString(), expectedTopo2);
}

TEST_F(CoordinatorCafTest, test_register_query) {
  FogExecutionPlan execPlan = coordinatorPtr->register_query("example", "cars1", "BottomUp");
  EXPECT_TRUE(coordinatorPtr->getRegisteredQueries().size() == 1);

  string expectedPlacement = "graph G {\n"
                             "0[label=\"2 operatorName=SOURCE(OP-1)=>FILTER(OP-2)=>SINK(SYS) nodeName=2\"];\n"
                             "1[label=\"0 operatorName=SOURCE(SYS)=>SINK(OP-3) nodeName=0\"];\n"
                             "2[label=\"1 operatorName=empty nodeName=1\"];\n"
                             "3[label=\"3 operatorName=empty nodeName=3\"];\n"
                             "4[label=\"4 operatorName=empty nodeName=4\"];\n"
                             "5[label=\"5 operatorName=empty nodeName=5\"];\n"
                             "2--1 [label=\"1\"];\n"
                             "0--1 [label=\"2\"];\n"
                             "3--1 [label=\"3\"];\n"
                             "4--1 [label=\"4\"];\n"
                             "5--1 [label=\"5\"];\n"
                             "}\n";
  EXPECT_EQ(execPlan.getTopologyPlanString(), expectedPlacement);
}

TEST_F(CoordinatorCafTest, test_register_deregister_query) {
  FogExecutionPlan execPlan = coordinatorPtr->register_query("example", "cars1", "BottomUp");
  EXPECT_TRUE(coordinatorPtr->getRegisteredQueries().size() == 1);
  coordinatorPtr->deregister_query("example");
  EXPECT_TRUE(coordinatorPtr->getRegisteredQueries().empty());
}

TEST_F(CoordinatorCafTest, test_make_deployment) {
  FogExecutionPlan execPlan = coordinatorPtr->register_query("example", "cars1", "BottomUp");
  EXPECT_TRUE(coordinatorPtr->getRegisteredQueries().size() == 1);
  unordered_map<FogTopologyEntryPtr, ExecutableTransferObject> etos = coordinatorPtr->make_deployment("example");
  EXPECT_TRUE(etos.size() == 2);

  for (auto &x : etos) {
    EXPECT_TRUE(x.first);
    string s_eto = SerializationTools::ser_eto(x.second);
    EXPECT_TRUE(!s_eto.empty());
    SerializationTools::parse_eto(s_eto);
  }
  EXPECT_TRUE(coordinatorPtr->getRegisteredQueries().empty());
  EXPECT_TRUE(coordinatorPtr->getRunningQueries().size() == 1);
}

TEST_F(CoordinatorCafTest, test_run_deregister_query) {
  FogExecutionPlan execPlan = coordinatorPtr->register_query("example", "cars1", "BottomUp");
  EXPECT_TRUE(coordinatorPtr->getRegisteredQueries().size() == 1);
  unordered_map<FogTopologyEntryPtr, ExecutableTransferObject> etos = coordinatorPtr->make_deployment("example");
  EXPECT_TRUE(etos.size() == 2);
  EXPECT_TRUE(coordinatorPtr->getRegisteredQueries().empty());
  EXPECT_TRUE(coordinatorPtr->getRunningQueries().size() == 1);

  coordinatorPtr->deregister_query("example");
  EXPECT_TRUE(coordinatorPtr->getRegisteredQueries().empty());
  EXPECT_TRUE(coordinatorPtr->getRunningQueries().empty());
}

TEST_F(CoordinatorCafTest, test_compile_deployment) {
  FogExecutionPlan execPlan = coordinatorPtr->register_query("example", "cars1", "BottomUp");
  EXPECT_TRUE(coordinatorPtr->getRegisteredQueries().size() == 1);
  unordered_map<FogTopologyEntryPtr, ExecutableTransferObject> etos = coordinatorPtr->make_deployment("example");
  EXPECT_TRUE(etos.size() == 2);

  for (auto &x : etos) {
    ExecutableTransferObject eto = x.second;
    QueryExecutionPlanPtr qep = eto.toQueryExecutionPlan();
    EXPECT_TRUE(qep);
  }
  EXPECT_TRUE(coordinatorPtr->getRegisteredQueries().empty());
  EXPECT_TRUE(coordinatorPtr->getRunningQueries().size() == 1);
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
}

TEST_F(CoordinatorCafTest, test_local_distributed_deployment) {
  auto *engine = new NodeEngine();
  engine->start();
  FogExecutionPlan execPlan = coordinatorPtr->register_query("example", "cars1", "BottomUp");
  EXPECT_TRUE(coordinatorPtr->getRegisteredQueries().size() == 1);
  unordered_map<FogTopologyEntryPtr, ExecutableTransferObject> etos = coordinatorPtr->make_deployment("example");
  EXPECT_TRUE(etos.size() == 2);

  for (auto &x : etos) {
    FogTopologyEntryPtr v = x.first;
    cout << "Deploying QEP for " << v->getEntryTypeString() << endl;
    ExecutableTransferObject eto = x.second;
    QueryExecutionPlanPtr qep = eto.toQueryExecutionPlan();
    EXPECT_TRUE(qep);
    engine->deployQuery(qep);
  }
  EXPECT_TRUE(coordinatorPtr->getRegisteredQueries().empty());
  EXPECT_TRUE(coordinatorPtr->getRunningQueries().size() == 1);
}
