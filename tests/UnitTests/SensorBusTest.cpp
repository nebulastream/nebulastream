#include <NodeEngine/NodeEngine.hpp>
#include <Util/SensorBus.hpp>
#include <Util/Logger.hpp>

#include <gtest/gtest.h>

using namespace NES;

namespace NES {

const size_t buffersManaged = 1024;
const size_t bufferSize = 4*1024;

class SensorBusTests : public testing::Test {

  public:
    NodeEnginePtr nodeEngine;
    BufferManagerPtr bufferManager;
    std::string path_to_bus;
    char path_to_bus_str[10];
    int bus_file_descriptor;
    int sensor_address_in_bus;
    int bus_file_allocated_id;

  static void SetUpTestCase() {
      NES::setupLogging("SensorBusTest.log", NES::LOG_DEBUG);
      NES_INFO("Setup SourceBusTest test class.");
  }

  static void TearDownTestCase() {
      std::cout << "Tear down NetworkStackTest class." << std::endl;
  }

  void SetUp() override {
      std::cout << "Setup SourceBusTest test case." << std::endl;
      std::cout << "Setup SinkTest test case." << std::endl;
      nodeEngine = std::make_shared<NodeEngine>();
      nodeEngine->createBufferManager(bufferSize, buffersManaged);

      bus_file_allocated_id = 1;
      snprintf(path_to_bus_str, 19,
               "/dev/i2c-%d",
               bus_file_allocated_id);
      sensor_address_in_bus = 0x1c;
  }

  void TearDown() override {
      std::cout << "Tear down SourceBusTest test case." << std::endl;
  }

  TEST_F(SensorBusTests, busMustStartAndSensorControllable) {
      ASSERT_EQ(SensorBus::init_bus(bus_file_descriptor, path_to_bus_str, sensor_address_in_bus), true);
  }
};

}// namespace NES