#include <gtest/gtest.h>
#include <SourceSink/PrintSink.hpp>
#include <Actors/NesCoordinator.hpp>

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

