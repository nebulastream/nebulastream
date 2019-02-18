#include <array>
#include <gtest/gtest.h>
#include <iostream>
#include <memory>
#include <thread>
#include <zmq.hpp>

#include <API/APIDataTypes.hpp>
#include <API/Field.hpp>
#include <API/Schema.hpp>
#include <Runtime/ZmqSource.hpp>

using namespace iotdb;

#ifndef LOCAL_HOST
#define LOCAL_HOST "127.0.0.1"
#endif

#ifndef LOCAL_PORT
#define LOCAL_PORT 38938
#endif

class RuntimeDataSourceTest : public testing::Test {
public:
  /* Will be called before any test in this class are executed. */
  static void SetUpTestCase() { std::cout << "Setup RuntimeDataSourceTest test class." << std::endl; }

  /* Will be called before a test is executed. */
  void SetUp() { std::cout << "Setup RuntimeDataSourceTest test case." << std::endl; }

  /* Will be called before a test is executed. */
  void TearDown() { std::cout << "Setup RuntimeDataSourceTest test case." << std::endl; }

  /* Will be called after all tests in this class are finished. */
  static void TearDownTestCase() { std::cout << "Tear down RuntimeDataSourceTest test class." << std::endl; }
};

/* ------------------------------------------------------------------------- */
/* - ZeroMQ Data Source ---------------------------------------------------- */
TEST_F(RuntimeDataSourceTest, ZmqSourceReceiveData) {

  // Create ZeroMQ Data Source.
  auto schema = Schema::create()
                    .addVarSizeField("KEY", APIDataType(APIDataType::Uint), 4)
                    .addVarSizeField("VALUE", APIDataType(APIDataType::Uint), 4);
  auto zmq_source = std::make_shared<ZmqSource>(schema, LOCAL_HOST, LOCAL_PORT, "TOPIC");
  std::cout << zmq_source->toString() << std::endl;

  // Define test data.
  std::array<uint32_t, 8> test_data = {{0, 100, 1, 99, 2, 98, 3, 97}};
  auto test_data_size = test_data.size() * sizeof(uint32_t);

  // Open Publisher
  zmq::context_t context(1);
  zmq::socket_t socket(context, ZMQ_PUB);
  auto address = std::string("tcp://") + std::string(LOCAL_HOST) + std::string(":") + std::to_string(LOCAL_PORT);
  socket.bind(address.c_str());

  // Start thread for receiving the data.
  bool receiving_finished = false;
  auto receiving_thread = std::thread([&]() {
    // Receive data.
    auto tuple_buffer = zmq_source->receiveData();

    // Test received data.
    uint32_t *tuple = (uint32_t *)tuple_buffer.buffer;
    for (size_t i = 0; i != tuple_buffer.num_tuples; ++i) {
      EXPECT_EQ(*(tuple++), i);
      EXPECT_EQ(*(tuple++), 100 - i);
    }

    receiving_finished = true;
  });

  // Wait until receiving is complete.
  while (!receiving_finished) {
    // Send data from here.
    auto string = std::string("TOPIC");
    zmq::message_t message_topic(string.size());
    memcpy(message_topic.data(), string.data(), string.size());
    socket.send(message_topic, ZMQ_SNDMORE);

    zmq::message_t message_data(test_data_size);
    memcpy(message_data.data(), test_data.data(), test_data_size);
    socket.send(message_data);
  }
  receiving_thread.join();
}
