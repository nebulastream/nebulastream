#include <array>
#include <gtest/gtest.h>
#include <iostream>
#include <memory>
#include <thread>
#include <zmq.hpp>

#include <API/Schema.hpp>
#include <Core/TupleBuffer.hpp>
#include <Runtime/DataSink.hpp>
#include <Runtime/DataSource.hpp>

using namespace iotdb;

#ifndef LOCAL_HOST
#define LOCAL_HOST "127.0.0.1"
#endif

#ifndef LOCAL_PORT
#define LOCAL_PORT 38938
#endif

class RuntimeDataSourceSinkTest : public testing::Test {
public:
  /* Will be called before any test in this class are executed. */
  static void SetUpTestCase() { std::cout << "Setup RuntimeDataSourceSinkTest test class." << std::endl; }

  /* Will be called before a test is executed. */
  void SetUp() {
    std::cout << "Setup RuntimeDataSourceSinkTest test case." << std::endl;

    topic = std::string("TOPIC");
    address = std::string("tcp://") + std::string(LOCAL_HOST) + std::string(":") + std::to_string(LOCAL_PORT);

    test_data = {{0, 100, 1, 99, 2, 98, 3, 97}};
    test_data_size = test_data.size() * sizeof(uint32_t);
    test_schema = Schema::create().addField("KEY", UINT32).addField("VALUE", UINT32);
  }

  /* Will be called before a test is executed. */
  void TearDown() { std::cout << "Setup RuntimeDataSourceSinkTest test case." << std::endl; }

  /* Will be called after all tests in this class are finished. */
  static void TearDownTestCase() { std::cout << "Tear down RuntimeDataSourceSinkTest test class." << std::endl; }

  std::string topic;
  std::string address;

  Schema test_schema;
  size_t test_data_size;
  std::array<uint32_t, 8> test_data;
};

/* - ZeroMQ Data Source ---------------------------------------------------- */
TEST_F(RuntimeDataSourceSinkTest, ZmqSourceReceiveData) {

  // Create ZeroMQ Data Source.
  auto test_schema = Schema::create().addField("KEY", UINT32).addField("VALUE", UINT32);
  auto zmq_source = createZmqSource(test_schema, LOCAL_HOST, LOCAL_PORT, topic);
  std::cout << zmq_source->toString() << std::endl;

  // Open Publisher
  zmq::context_t context(1);
  zmq::socket_t socket(context, ZMQ_PUB);
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
    zmq::message_t message_topic(topic.size());
    memcpy(message_topic.data(), topic.data(), topic.size());
    socket.send(message_topic, ZMQ_SNDMORE);

    zmq::message_t message_data(test_data_size);
    memcpy(message_data.data(), test_data.data(), test_data_size);
    socket.send(message_data);
  }
  receiving_thread.join();
}

/* - ZeroMQ Data Sink ------------------------------------------------------ */
TEST_F(RuntimeDataSourceSinkTest, ZmqSinkSendData) {

  // Create ZeroMQ Data Sink.
  auto test_schema = Schema::create().addField("KEY", UINT32).addField("VALUE", UINT32);
  auto zmq_sink = createZmqSink(test_schema, LOCAL_HOST, LOCAL_PORT, topic);
  std::cout << zmq_sink->toString() << std::endl;

  // Put test data into a TupleBuffer vector.
  void *buffer = new char[test_data_size];
  auto tuple_buffer = TupleBuffer(buffer, test_data_size, sizeof(uint32_t) * 2, test_data.size() / 2);
  std::memcpy(tuple_buffer.buffer, &test_data, test_data_size);
  auto tuple_buffer_vec = std::vector<TupleBuffer *>();
  tuple_buffer_vec.push_back(&tuple_buffer);

  // Start thread for receiving the data.
  bool receiving_finished = false;
  auto receiving_thread = std::thread([&]() {
    // Starting receiving enviroment.
    int linger = 0;
    auto context = zmq::context_t(1);
    auto socket = zmq::socket_t(context, ZMQ_SUB);
    socket.connect(address.c_str());
    socket.setsockopt(ZMQ_SUBSCRIBE, topic.c_str(), topic.size());
    socket.setsockopt(ZMQ_LINGER, &linger, sizeof(linger));

    // Receive message.
    zmq::message_t new_data;
    socket.recv(&new_data); // envelope - not needed at the moment
    socket.recv(&new_data); // actual data

    // Test received data.
    uint32_t *tuple = (uint32_t *)new_data.data();
    for (size_t i = 0; i != new_data.size() / test_schema.getSchemaSize(); ++i) {
      EXPECT_EQ(*(tuple++), i);
      EXPECT_EQ(*(tuple++), 100 - i);
    }

    socket.close();
    receiving_finished = true;
  });

  // Wait until receiving is complete.
  while (!receiving_finished) {
    zmq_sink->writeData(tuple_buffer_vec);
  }
  receiving_thread.join();

  // Release buffer memory.
  delete[](char *) buffer;
}

/* - ZeroMQ Data Sink to ZeroMQ Data Source  ------------------------------- */
TEST_F(RuntimeDataSourceSinkTest, ZmqSinkToSource) {

  // Put test data into a TupleBuffer vector.
  void *buffer = new char[test_data_size];
  auto tuple_buffer = TupleBuffer(buffer, test_data_size, sizeof(uint32_t) * 2, test_data.size() / 2);
  std::memcpy(tuple_buffer.buffer, &test_data, test_data_size);
  auto tuple_buffer_vec = std::vector<TupleBuffer *>();
  tuple_buffer_vec.push_back(&tuple_buffer);

  // Create ZeroMQ Data Sink.
  auto zmq_sink = createZmqSink(test_schema, LOCAL_HOST, LOCAL_PORT, "TOPIC");
  std::cout << zmq_sink->toString() << std::endl;

  // Create ZeroMQ Data Source.
  auto zmq_source = createZmqSource(test_schema, LOCAL_HOST, LOCAL_PORT, "TOPIC");
  std::cout << zmq_source->toString() << std::endl;

  // Start thread for receiving the data.
  bool receiving_finished = false;
  auto receiving_thread = std::thread([&]() {
    // Receive data.
    auto new_data = zmq_source->receiveData();

    // Test received data.
    uint32_t *tuple = (uint32_t *)new_data.buffer;
    for (size_t i = 0; i != new_data.num_tuples; ++i) {
      EXPECT_EQ(*(tuple++), i);
      EXPECT_EQ(*(tuple++), 100 - i);
    }

    receiving_finished = true;
  });

  // Wait until receiving is complete.
  while (!receiving_finished) {
    zmq_sink->writeData(tuple_buffer_vec);
  }
  receiving_thread.join();

  // Release buffer memory.
  delete[](char *) buffer;
}