/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <array>
#include <gtest/gtest.h>
#include <iostream>
#include <memory>
#include <thread>
#include <zmq.hpp>

#include <API/Schema.hpp>
#include <Catalogs/PhysicalStreamConfig.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineFactory.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/Logger.hpp>
#include <gtest/gtest.h>

using namespace NES;

#ifndef LOCAL_ADDRESS
#define LOCAL_ADDRESS "127.0.0.1"
#endif

#ifndef LOCAL_PORT
#define LOCAL_PORT 38938
#endif

class ZMQTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::setupLogging("ZMQTest.log", NES::LOG_DEBUG);
        NES_DEBUG("Setup ZMQTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        NES_DEBUG("Setup ZMQTest test case.");
        PhysicalSourcePtr conf = PhysicalStreamConfig::createEmpty();
        nodeEngine = Runtime::NodeEngineFactory::createDefaultNodeEngine("127.0.0.1", 3001, conf);

        address = std::string("tcp://") + std::string(LOCAL_ADDRESS) + std::string(":") + std::to_string(LOCAL_PORT);

        test_data = {{0, 100, 1, 99, 2, 98, 3, 97}};
        test_data_size = test_data.size() * sizeof(uint32_t);
        tupleCnt = 8;
        //    testDataSize = 4096;
        test_schema = Schema::create()->addField("KEY", UINT32)->addField("VALUE", UINT32);
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        ASSERT_TRUE(nodeEngine->stop());
        NES_DEBUG("Setup ZMQTest test case.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_DEBUG("Tear down ZMQTest test class."); }

    uint64_t tupleCnt{};
    std::string address;

    SchemaPtr test_schema;
    uint64_t test_data_size{};
    std::array<uint32_t, 8> test_data{};

    Runtime::NodeEnginePtr nodeEngine{nullptr};
};

/* - ZeroMQ Data Source ---------------------------------------------------- */
TEST_F(ZMQTest, testZmqSourceReceiveData) {
    // Create ZeroMQ Data Source.
    auto test_schema = Schema::create()->addField("KEY", UINT32)->addField("VALUE", UINT32);
    auto zmq_source = createZmqSource(test_schema,
                                      nodeEngine->getBufferManager(),
                                      nodeEngine->getQueryManager(),
                                      LOCAL_ADDRESS,
                                      LOCAL_PORT,
                                      1,
                                      12,
                                      std::vector<Runtime::Execution::SuccessorExecutablePipeline>());
    std::cout << zmq_source->toString() << std::endl;
    // bufferManager->resizeFixedBufferSize(testDataSize);

    // Start thread for receiving the data.
    bool receiving_finished = false;
    auto receiving_thread = std::thread([&]() {
        // Receive data.
        zmq_source->open();
        auto tuple_buffer = zmq_source->receiveData();

        // Test received data.
        uint64_t sum = 0;
        auto* tuple = (uint32_t*) tuple_buffer->getBuffer();
        for (uint64_t i = 0; i != 8; ++i) {
            sum += *(tuple++);
        }
        uint64_t expected = 400;
        EXPECT_EQ(sum, expected);

        receiving_finished = true;
    });
    uint64_t tupCnt = 8;
    // Wait until receiving is complete.

    // Open Publisher
    zmq::context_t context(1);
    zmq::socket_t socket(context, ZMQ_PUSH);
    socket.connect(address.c_str());

    while (!receiving_finished) {

        // Send data from here.
        auto const envelopeSizeBytes = 16;
        zmq::message_t message_tupleCnt(envelopeSizeBytes);
        memcpy(message_tupleCnt.data(), &tupCnt, envelopeSizeBytes);
        static_cast<uint64_t*>(message_tupleCnt.data())[1] = static_cast<uint64_t>(0ull);
        if (auto const sentEnvelope = socket.send(message_tupleCnt, zmq::send_flags::sndmore).value_or(0);
            sentEnvelope != envelopeSizeBytes) {
            NES_ERROR("ZMQ Test Error: Sending message metadata failed!" << sentEnvelope << message_tupleCnt.size());
        }

        zmq::message_t message_data(test_data_size);
        memcpy(message_data.data(), test_data.data(), test_data_size);
        if (auto const sentPayload = socket.send(message_data, zmq::send_flags::none); sentPayload != test_data_size) {
            NES_ERROR("ZMQ Test Error: Sending message payload failed!");
        }
    }
    receiving_thread.join();
}

/* - ZeroMQ Data Sink ------------------------------------------------------ */
TEST_F(ZMQTest, DISABLED_testZmqSinkSendData) {

    //FIXME: this test makes no sense, redo it
    /**
  // Create ZeroMQ Data Sink.
  auto testSchema = Schema::create()->addField("KEY", UINT32)->addField("VALUE",
                                                                       UINT32);
  auto zmq_sink = createBinaryZmqSink(testSchema, LOCAL_ADDRESS, LOCAL_PORT);
  std::cout << zmq_sink->toString() << std::endl;

  // Put test data into a TupleBuffer vector.
  void *buffer = new char[testDataSize];
  //  auto tuple_buffer = TupleBuffer(buffer, testDataSize, sizeof(uint32_t) * 2, test_data.size() / 2);
  auto tuple_buffer = bufferManager->getFixedSizeBuffer();

  std::memcpy(tuple_buffer->getBuffer(), &test_data, testDataSize);
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
    for (uint64_t i = 0; i != new_data.size() / testSchema->getSchemaSizeInBytes(); ++i) {
  EXPECT_EQ(*(tuple++), i);
  uint64_t expected = 100 - i;
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
TEST_F(ZMQTest, DISABLED_testZmqSinkToSource) {

    /**
  //FIXME: this test makes no sense, redo it
  // Put test data into a TupleBuffer vector.
  void *buffer = new char[testDataSize];
  //  auto tuple_buffer = TupleBuffer(buffer, testDataSize, sizeof(uint32_t) * 2, test_data.size() / 2);
  auto tuple_buffer = bufferManager->getFixedSizeBuffer();

  std::memcpy(tuple_buffer->getBuffer(), &test_data, testDataSize);
  auto tuple_buffer_vec = std::vector<TupleBufferPtr>();
  tuple_buffer_vec.push_back(tuple_buffer);

  // Create ZeroMQ Data Sink.
  auto zmq_sink = createBinaryZmqSink(testSchema, LOCAL_ADDRESS, LOCAL_PORT);
  std::cout << zmq_sink->toString() << std::endl;

  // Create ZeroMQ Data Source.
  auto zmq_source = createZmqSource(testSchema, LOCAL_ADDRESS, LOCAL_PORT);
  std::cout << zmq_source->toString() << std::endl;

  // Start thread for receivingh the data.
  bool receiving_finished = false;
  auto receiving_thread = std::thread([&]() {
    // Receive data.
    auto new_data = zmq_source->receiveData();

    // Test received data.
    uint32_t *tuple = (uint32_t*) new_data->getBuffer();
    for (uint64_t i = 0; i != new_data->getNumberOfTuples(); ++i) {
      EXPECT_EQ(*(tuple++), i);
      uint64_t expected = 100 - i;
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