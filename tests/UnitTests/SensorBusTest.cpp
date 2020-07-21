#include <NodeEngine/NodeEngine.hpp>
#include <Sensors/SensorBus.hpp>
#include <Util/Logger.hpp>

#include <chrono>

#include <gtest/gtest.h>

using namespace NES;

namespace NES {

/**
 * Tests for sensor buses. We start with the I2C bus.
 *
 * For these tests to run, one has to load the i2c-stub
 * kernel module with: `sudo modprobe i2c-stub chip_addr=0x1c`.
 * Additionally, since reading and writing require sudo access,
 * the user that runs the tests should be added to the I2C
 * group with: `sudo adduser $USER i2c`.
 *
 * They are currently disabled but tested locally.
 * Ideally, these all should be mocked.
 */
class SensorBusTest : public testing::Test {

  public:
    NodeEnginePtr nodeEngine;
    BufferManagerPtr bufferManager;
    std::string path_to_bus;
    char path_to_bus_str[10];
    int bus_file_descriptor;
    int sensor_address_in_bus;
    int bus_file_allocated_id;
    int data_size;
    unsigned char data_buffer[5] = {0};
    int64_t timeStamp;

  static void SetUpTestCase() {
      NES::setupLogging("SensorBusTest.log", NES::LOG_DEBUG);
      NES_INFO("Setup SourceBusTest test class.");
  }

  static void TearDownTestCase() {
      std::cout << "Tear down NetworkStackTest class." << std::endl;
  }

  /**
   * Assume file descriptor will be in /dev/i2c-1.
   * This is allocated randomly in a real test.
   *
   * Create the desired file path and descriptor.
   * Assume the sensor will be using registers after 0x1c address.
   * Assume data size to read or write is 4 bytes.
   * Create an int64_t value to write and later read.
   */
  void SetUp() override {
      NES_DEBUG("Setup SourceBusTest test case.");
      bus_file_allocated_id = 1;
      snprintf(path_to_bus_str, 19,
               "/dev/i2c-%d",
               bus_file_allocated_id);
      sensor_address_in_bus = 0x1c;
      data_size = 4;
      timeStamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
  }

  void TearDown() override {
      NES_DEBUG("Tear down SourceBusTest test case.");
  }
};

/**
 * @brief probe the bus file descriptor and sensor address
 * @component Util function for initializing a bus and sensor IO
 * @result true, if ioctl in bus and sensor level succeeds
 */
TEST_F(SensorBusTest, busMustStartAndSensorControllable) {
    bool result = SensorBus::initBus(bus_file_descriptor, path_to_bus_str, sensor_address_in_bus);
    EXPECT_TRUE(result);
}

/**
 * @brief probe the bus file descriptor and sensor address and write a value
 * @component Util function for writing to a bus and sensor
 * @result true, if ioctl in bus and sensor level succeeds writing the timestamp
 */
TEST_F(SensorBusTest, sensorAddressMustBeWriteable) {
    SensorBus::initBus(bus_file_descriptor, path_to_bus_str, sensor_address_in_bus);
    bool result = SensorBus::write(bus_file_descriptor, sensor_address_in_bus, data_size, reinterpret_cast<unsigned char*>(&timeStamp));
    EXPECT_TRUE(result);
}

/**
 * @brief probe the bus file descriptor and sensor address and read a value
 * @component Util function for reading from a sensor attached to a bus
 * @result true, if ioctl in bus and sensor level succeeds reading the timestamp
 */
TEST_F(SensorBusTest, sensorAddressMustBeReadable) {
    SensorBus::initBus(bus_file_descriptor, path_to_bus_str, sensor_address_in_bus);
    bool result = SensorBus::read(bus_file_descriptor, sensor_address_in_bus, data_size, data_buffer);
    EXPECT_TRUE(result);
}

/**
 * @brief probe the bus and sensor address and read a value, after writing
 * @component Util function for reading and util for writing to a bus
 * @result true, if ioctl in bus and sensor level succeeds reading the timestamp
 */
TEST_F(SensorBusTest, dataMustBeSameReadAfterWrite) {
    SensorBus::initBus(bus_file_descriptor, path_to_bus_str, sensor_address_in_bus);
    SensorBus::write(bus_file_descriptor, sensor_address_in_bus, data_size, reinterpret_cast<unsigned char*>(&timeStamp));
    SensorBus::read(bus_file_descriptor, sensor_address_in_bus, data_size, data_buffer);
    int64_t timestampFromBus;
    std::memcpy(&timestampFromBus, data_buffer, sizeof(int));
    ASSERT_EQ(timestampFromBus, timeStamp);
}

}// namespace NES