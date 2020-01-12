#include <array>
#include <gtest/gtest.h>
#include <iostream>
#include <memory>
#include <thread>
#include <zmq.hpp>

#include <API/Schema.hpp>
#include <NodeEngine/BufferManager.hpp>
#include <Util/Logger.hpp>
#include <SourceSink/DataSink.hpp>
#include <SourceSink/DataSource.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <SourceSink/SinkCreator.hpp>
#include <SourceSink/SourceCreator.hpp>
using namespace NES;

#ifndef LOCAL_HOST
#define LOCAL_HOST "127.0.0.1"
#endif

#ifndef LOCAL_PORT
#define LOCAL_PORT 38938
#endif

void setupLogging() {
  // create PatternLayout
  log4cxx::LayoutPtr layoutPtr(
      new log4cxx::PatternLayout(
          "%d{MMM dd yyyy HH:mm:ss} %c:%L [%-5t] [%p] : %m%n"));

  // create FileAppender
  LOG4CXX_DECODE_CHAR(fileName, "iotdb.log");
  log4cxx::FileAppenderPtr file(new log4cxx::FileAppender(layoutPtr, fileName));

  // create ConsoleAppender
  log4cxx::ConsoleAppenderPtr console(new log4cxx::ConsoleAppender(layoutPtr));

  // set log level
  // logger->setLevel(log4cxx::Level::getTrace());
  iotdbLogger->setLevel(log4cxx::Level::getDebug());
  //	logger->setLevel(log4cxx::Level::getInfo());
  //	logger->setLevel(log4cxx::Level::getWarn());
  // logger->setLevel(log4cxx::Level::getError());
  //	logger->setLevel(log4cxx::Level::getFatal());

  // add appenders and other will inherit the settings
  iotdbLogger->addAppender(file);
  iotdbLogger->addAppender(console);
}

class RuntimeDataSourceSinkTest : public testing::Test {
 public:
  /* Will be called before any test in this class are executed. */
  static void SetUpTestCase() {
    std::cout << "Setup RuntimeDataSourceSinkTest test class." << std::endl;
  }

  /* Will be called before a test is executed. */
  void SetUp() {
    std::cout << "Setup RuntimeDataSourceSinkTest test case." << std::endl;
    setupLogging();
    address = std::string("tcp://") + std::string(LOCAL_HOST) + std::string(":")
        + std::to_string(LOCAL_PORT);

    test_data = { {0, 100, 1, 99, 2, 98, 3, 97}};
    test_data_size = test_data.size() * sizeof(uint32_t);
    tupleCnt = 8;
    //    test_data_size = 4096;
    test_schema = Schema::create().addField("KEY", UINT32).addField("VALUE",
                                                                    UINT32);
    BufferManager::instance();
    //    BufferManager::instance().setBufferSize(test_data_size);
  }

  /* Will be called before a test is executed. */
  void TearDown() {
    std::cout << "Setup RuntimeDataSourceSinkTest test case." << std::endl;
  }

  /* Will be called after all tests in this class are finished. */
  static void TearDownTestCase() {
    std::cout << "Tear down RuntimeDataSourceSinkTest test class." << std::endl;
  }

  size_t tupleCnt;
  std::string address;

  Schema test_schema;
  size_t test_data_size;
  std::array<uint32_t, 8> test_data;
};

/* - ZeroMQ Data Source ---------------------------------------------------- */
TEST_F(RuntimeDataSourceSinkTest, ZmqSourceReceiveData) {

  // Create ZeroMQ Data Source.
  auto test_schema = Schema::create().addField("KEY", UINT32).addField("VALUE",
                                                                       UINT32);
  auto zmq_source = createZmqSource(test_schema, LOCAL_HOST, LOCAL_PORT);
  std::cout << zmq_source->toString() << std::endl;
  BufferManager::instance().setBufferSize(test_data_size);

  // Start thread for receiving the data.
  bool receiving_finished = false;
  auto receiving_thread = std::thread([&]() {
    // Receive data.
      auto tuple_buffer = zmq_source->receiveData();

      // Test received data.
      size_t sum = 0;
      uint32_t* tuple = (uint32_t*)tuple_buffer->getBuffer();
      for (size_t i = 0; i != 8; ++i) {
        sum += *(tuple++);
      }
      size_t expected = 400;
      EXPECT_EQ(sum, expected);

      BufferManager::instance().releaseBuffer(tuple_buffer);
      receiving_finished = true;
    });
  size_t tupCnt = 8;
  // Wait until receiving is complete.

  // Open Publisher
  zmq::context_t context(1);
  zmq::socket_t socket(context, ZMQ_PUSH);
  socket.connect(address.c_str());
  while (!receiving_finished) {

    // Send data from here.
    zmq::message_t message_tupleCnt(8);
    memcpy(message_tupleCnt.data(), &tupCnt, 8);
    socket.send(message_tupleCnt, ZMQ_SNDMORE);

    zmq::message_t message_data(test_data_size);
    memcpy(message_data.data(), test_data.data(), test_data_size);
    socket.send(message_data);
  }
  receiving_thread.join();
}

/* - ZeroMQ Data Sink ------------------------------------------------------ */
TEST_F(RuntimeDataSourceSinkTest, ZmqSinkSendData) {

  return;
  //FIXME: this test makes no sense, redo it

  // Create ZeroMQ Data Sink.
  auto test_schema = Schema::create().addField("KEY", UINT32).addField("VALUE",
                                                                       UINT32);
  auto zmq_sink = createZmqSink(test_schema, LOCAL_HOST, LOCAL_PORT);
  std::cout << zmq_sink->toString() << std::endl;

  // Put test data into a TupleBuffer vector.
  void* buffer = new char[test_data_size];
  //  auto tuple_buffer = TupleBuffer(buffer, test_data_size, sizeof(uint32_t) * 2, test_data.size() / 2);
  auto tuple_buffer = BufferManager::instance().getBuffer();

  std::memcpy(tuple_buffer->getBuffer(), &test_data, test_data_size);
  auto tuple_buffer_vec = std::vector<TupleBufferPtr>();
  tuple_buffer_vec.push_back(tuple_buffer);

  // Start thread for receiving the data.
  bool receiving_finished = false;
  auto receiving_thread =
      std::thread([&]() {
        // Starting receiving enviroment.
          int linger = 0;
          auto context = zmq::context_t(1);
          auto socket = zmq::socket_t(context, ZMQ_SUB);
          socket.connect(address.c_str());
          socket.setsockopt(ZMQ_SUBSCRIBE, "", 0);
          socket.setsockopt(ZMQ_LINGER, &linger, sizeof(linger));

          // Receive message.
          zmq::message_t new_data;
          socket.recv(&new_data);// envelope - not needed at the moment
          socket.recv(&new_data);// actual data

          // Test received data.
          uint32_t* tuple = (uint32_t*)new_data.data();
          for (size_t i = 0; i != new_data.size() / test_schema.getSchemaSize(); ++i) {
            EXPECT_EQ(*(tuple++), i);
            size_t expected = 100 - i;
            EXPECT_EQ(*(tuple++), expected);
          }

          socket.close();
          receiving_finished = true;
        });

  // Wait until receiving is complete.
  while (!receiving_finished) {
    zmq_sink->writeDataInBatch(tuple_buffer_vec);
  }
  receiving_thread.join();

  // Release buffer memory.
  delete[] (char*) buffer;
}

/* - ZeroMQ Data Sink to ZeroMQ Data Source  ------------------------------- */
TEST_F(RuntimeDataSourceSinkTest, ZmqSinkToSource) {

  return;
  //FIXME: this test makes no sense, redo it
  // Put test data into a TupleBuffer vector.
  void* buffer = new char[test_data_size];
  //  auto tuple_buffer = TupleBuffer(buffer, test_data_size, sizeof(uint32_t) * 2, test_data.size() / 2);
  auto tuple_buffer = BufferManager::instance().getBuffer();

  std::memcpy(tuple_buffer->getBuffer(), &test_data, test_data_size);
  auto tuple_buffer_vec = std::vector<TupleBufferPtr>();
  tuple_buffer_vec.push_back(tuple_buffer);

  // Create ZeroMQ Data Sink.
  auto zmq_sink = createZmqSink(test_schema, LOCAL_HOST, LOCAL_PORT);
  std::cout << zmq_sink->toString() << std::endl;

  // Create ZeroMQ Data Source.
  auto zmq_source = createZmqSource(test_schema, LOCAL_HOST, LOCAL_PORT);
  std::cout << zmq_source->toString() << std::endl;

  // Start thread for receivingh the data.
  bool receiving_finished = false;
  auto receiving_thread = std::thread([&]() {
    // Receive data.
      auto new_data = zmq_source->receiveData();

      // Test received data.
      uint32_t* tuple = (uint32_t*)new_data->getBuffer();
      for (size_t i = 0; i != new_data->getNumberOfTuples(); ++i) {
        EXPECT_EQ(*(tuple++), i);
        size_t expected = 100 - i;
        EXPECT_EQ(*(tuple++), expected);
      }
      BufferManager::instance().releaseBuffer(new_data);
      receiving_finished = true;
    });

  // Wait until receiving is complete.
  while (!receiving_finished) {
    zmq_sink->writeDataInBatch(tuple_buffer_vec);
  }
  receiving_thread.join();

  // Release buffer memory.
  delete[] (char*) buffer;
}
