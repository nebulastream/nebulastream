#include <array>
#include <gtest/gtest.h>
#include <iostream>
#include <memory>
#include <thread>
#include <zmq.hpp>

#include <API/Schema.hpp>
#include <Util/Logger.hpp>
#include <SourceSink/SourceCreator.hpp>

#include <NodeEngine/NodeEngine.hpp>

using namespace NES;

#ifndef LOCAL_HOST
#define LOCAL_HOST "127.0.0.1"
#endif

#ifndef LOCAL_PORT
#define LOCAL_PORT 38938
#endif

class ZMQTest : public testing::Test {
public:

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES_DEBUG("Setup ZMQTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() {
        NES_DEBUG("Setup ZMQTest test case.");
        NES::setupLogging("ZMQTest.log", NES::LOG_DEBUG);

        address = std::string("tcp://") + std::string(LOCAL_HOST) + std::string(":")
                  + std::to_string(LOCAL_PORT);

        test_data = {{0, 100, 1, 99, 2, 98, 3, 97}};
        test_data_size = test_data.size() * sizeof(uint32_t);
        tupleCnt = 8;
        //    test_data_size = 4096;
        test_schema = Schema::create()->addField("KEY", UINT32)->addField("VALUE",
                                                                          UINT32);

    }

    /* Will be called before a test is executed. */
    void TearDown() {
        NES_DEBUG("Setup ZMQTest test case.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() {
        NES_DEBUG("Tear down ZMQTest test class." );
    }

    size_t tupleCnt;
    std::string address;

    SchemaPtr test_schema;
    size_t test_data_size;
    std::array<uint32_t, 8> test_data;
};

/* - ZeroMQ Data Source ---------------------------------------------------- */
TEST_F(ZMQTest, ZmqSourceReceiveData) {
    NodeEnginePtr nodeEngine = std::make_shared<NodeEngine>();
    nodeEngine->start();

    // Create ZeroMQ Data Source.
    auto test_schema = Schema::create()->addField("KEY", UINT32)->addField("VALUE",
                                                                           UINT32);
    auto zmq_source = createZmqSource(test_schema, nodeEngine->getBufferManager(), nodeEngine->getQueryManager(), LOCAL_HOST, LOCAL_PORT);
    std::cout << zmq_source->toString() << std::endl;
    // bufferManager->resizeFixedBufferSize(test_data_size);

    // Start thread for receiving the data.
    bool receiving_finished = false;
    auto receiving_thread = std::thread([&]() {
        // Receive data.
        auto tuple_buffer = zmq_source->receiveData();

        // Test received data.
        size_t sum = 0;
        uint32_t *tuple = (uint32_t *) tuple_buffer->getBuffer();
        for (size_t i = 0; i != 8; ++i) {
            sum += *(tuple++);
        }
        size_t expected = 400;
        EXPECT_EQ(sum, expected);

        // bufferManager->releaseFixedSizeBuffer(tuple_buffer);
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
TEST_F(ZMQTest, ZmqSinkSendData) {

    return;
    //FIXME: this test makes no sense, redo it
/**
  // Create ZeroMQ Data Sink.
  auto test_schema = Schema::create()->addField("KEY", UINT32)->addField("VALUE",
                                                                       UINT32);
  auto zmq_sink = createZmqSink(test_schema, LOCAL_HOST, LOCAL_PORT);
  std::cout << zmq_sink->toString() << std::endl;

  // Put test data into a TupleBuffer vector.
  void *buffer = new char[test_data_size];
  //  auto tuple_buffer = TupleBuffer(buffer, test_data_size, sizeof(uint32_t) * 2, test_data.size() / 2);
  auto tuple_buffer = bufferManager->getFixedSizeBuffer();

  std::memcpy(tuple_buffer->getBuffer(), &test_data, test_data_size);
  auto tuple_buffer_vec = std::vector<TupleBufferPtr>();
  tuple_buffer_vec.push_back(tuple_buffer);

  // Start thread for receiving the data.
  bool receiving_finished = false;
  auto receiving_thread = std::thread([&]() {
    // Starting receiving enviroment.
    int linger = 0;
    auto context = zmq::context_t(1);
    auto socket = zmq::socket_t(context, ZMQ_SUB);
    socket.connect(address.c_str());
    socket.setsockopt(ZMQ_SUBSCRIBE, "", 0);
    socket.setsockopt(ZMQ_LINGER, &linger, sizeof(linger));

    // Receive message.
    zmq::message_t new_data;
    socket.recv(&new_data);  // envelope - not needed at the moment
    socket.recv(&new_data);  // actual data

    // Test received data.
    uint32_t *tuple = (uint32_t*) new_data.data();
    for (size_t i = 0; i != new_data.size() / test_schema->getSchemaSizeInBytes(); ++i) {
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
  **/
}

/* - ZeroMQ Data Sink to ZeroMQ Data Source  ------------------------------- */
TEST_F(ZMQTest, ZmqSinkToSource) {

/**
  //FIXME: this test makes no sense, redo it
  // Put test data into a TupleBuffer vector.
  void *buffer = new char[test_data_size];
  //  auto tuple_buffer = TupleBuffer(buffer, test_data_size, sizeof(uint32_t) * 2, test_data.size() / 2);
  auto tuple_buffer = bufferManager->getFixedSizeBuffer();

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
    uint32_t *tuple = (uint32_t*) new_data->getBuffer();
    for (size_t i = 0; i != new_data->getNumberOfTuples(); ++i) {
      EXPECT_EQ(*(tuple++), i);
      size_t expected = 100 - i;
      EXPECT_EQ(*(tuple++), expected);
    }
   // bufferManager->releaseFixedSizeBuffer(new_data);
    receiving_finished = true;
  });

  // Wait until receiving is complete.
  while (!receiving_finished) {
    zmq_sink->writeDataInBatch(tuple_buffer_vec);
  }
  receiving_thread.join();

  // Release buffer memory.
  delete[] (char*) buffer;
  */

}
