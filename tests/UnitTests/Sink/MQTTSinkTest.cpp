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

#ifdef ENABLE_MQTT_BUILD
#include <array>
#include <gtest/gtest.h>
#include <iostream>
#include <memory>

#include <API/Schema.hpp>
#include <Catalogs/PhysicalStreamConfig.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <NodeEngine/WorkerContext.hpp>
#include <Sinks/Mediums/MQTTSink.hpp>
#include <Sinks/SinkCreator.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/Logger.hpp>

using namespace NES;

#ifndef LOCAL_ADDRESS
#define LOCAL_ADDRESS "127.0.0.1:1883"
#endif

#ifndef CLIENT_ID
#define CLIENT_ID "nes-mqtt-test-client"
#endif

#ifndef TOPIC
#define TOPIC "v1/devices/me/telemetry"
#endif

#ifndef USER
#define USER "rfRqLGZRChg8eS30PEeR"
#endif

#ifndef QUALITY_OF_SERVICE
#define QUALITY_OF_SERVICE 1
#endif

#ifndef MAX_BUFFERED_MESSAGES
#define MAX_BUFFERED_MESSAGES 120
#endif

#ifndef SEND_PERIOD
#define SEND_PERIOD 1
#endif

#ifndef TIME_TYPE
#define TIME_TYPE 1 //milliseconds
#endif

/**
 * @brief this class implements tests for the MQTTSink class
 * Due to a LACK OF BROKER in the most common MQTT-CPP libraries, the broker must be set up manually.
 * An easy way is to use the Mosquitto broker. On Ubuntu: 'sudo apt install mosquitto'.
 * In order to start the broker with information for every received payload: 'mosquitto -v' (uses default port 1883).
 * BE AWARE that some of the tests require the tester to manually kill/disconnect the broker.
 * ALSO: Not all tests are meant to succeed, but might produce wanted errors.
 */

//FIXME tests right now rely on setting up a broker manually. Moreover, they intentionally fail.
// - find a way to fully automate tests (e.g. using redBoltz c++ MQTT library, which offers a broker
// - fix tests, so they do not intentionally fail, but always succeed, if right behaviour is shown

class DISABLED_MQTTTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::setupLogging("DISABLED_MQTTTest.log", NES::LOG_DEBUG);
        NES_DEBUG("Setup DISABLED_MQTTTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() {
        NES_DEBUG("Setup DISABLED_MQTTTest test case.");
        PhysicalStreamConfigPtr conf = PhysicalStreamConfig::createEmpty();
        nodeEngine = NodeEngine::create("127.0.0.1", 3111, conf);

        address = std::string("tcp://") + std::string(LOCAL_ADDRESS);

        test_data = {{0, 100, 1, 99, 2, 98, 3, 97}};
        test_data_size = test_data.size() * sizeof(uint32_t);
        tupleCnt = 4;
        //    test_data_size = 4096;
        test_schema = Schema::create()->addField("KEY", UINT32)->addField("VALUE", UINT32);
    }

    /* Will be called before a test is executed. */
    void TearDown() {
        nodeEngine->stop();
        nodeEngine.reset();
        NES_DEBUG("Setup MQTT test case.");
    }

    NES::NodeEngine::TupleBuffer createBuff(uint64_t bufferSize, std::shared_ptr<NodeEngine::BufferManager> buffMgr) {
        auto buffer = buffMgr->getBufferBlocking();
        std::mt19937 rnd;
        std::uniform_int_distribution gen(1, 100);
        for (uint32_t j = 0; j < bufferSize / sizeof(uint32_t); ++j) {
            buffer.getBuffer<uint32_t>()[j] = j/2;
            ++j;
            buffer.getBuffer<uint32_t>()[j] = (uint32_t)gen(rnd);
        }
        buffer.setNumberOfTuples(bufferSize / sizeof(uint64_t));

        return buffer;
    }

    bool createMQTTSink_ConnectToBroker_WriteData(uint64_t numTuples=10, uint32_t maxBufferedMSGs=1, char timeUnit='m',
                                                  uint64_t msgDelay=500, bool asynchronousClient=1, bool printBuffer=0) {

        /// Create MQTT Sink
        NodeEngine::WorkerContext wctx(NodeEngine::NesThread::getId());
        auto testSchema = Schema::create()->addField("KEY", UINT32)->addField("VALUE", UINT32);
        auto mqttSink = createMQTTSink(testSchema, 0, nodeEngine, LOCAL_ADDRESS, CLIENT_ID, TOPIC, USER,
                                       maxBufferedMSGs, timeUnit, msgDelay, asynchronousClient);

        /// Create Buffer
        const uint64_t bufferSize = 8 * numTuples;
        auto buffMgr = std::make_shared<NodeEngine::BufferManager>(bufferSize, 1);
        auto inputBuffer = createBuff(bufferSize, buffMgr);
        if (printBuffer) {
            NES_DEBUG("bufferContent before write=" << UtilityFunctions::prettyPrintTupleBuffer(inputBuffer, testSchema)
                                                    << '\n');
        }

        /// Connect to Broker
        MQTTSink* testSink = dynamic_cast<MQTTSink*>(mqttSink.get());
        testSink->connect();
        return testSink->writeData(inputBuffer, wctx);
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_DEBUG("Tear down MQTT test class."); }

    uint64_t tupleCnt;
    std::string address;

    SchemaPtr test_schema;
    uint64_t test_data_size;
    std::array<uint32_t, 8> test_data;

    NodeEngine::NodeEnginePtr nodeEngine{nullptr};
};


/* ------------------------------------------------------------------------------ */
/* ------------------------ ASYNCHRONOUS CLIENT TESTS --------------------------- */
/* ------------------------------------------------------------------------------ */
TEST_F(DISABLED_MQTTTest, testMQTTClientCreation) {
    auto testSchema = Schema::create()->addField("KEY", UINT32)->addField("VALUE", UINT32);
    auto mqttSink = createMQTTSink(testSchema, 0, nodeEngine, LOCAL_ADDRESS, CLIENT_ID,
                                   TOPIC, USER);
    std::cout << mqttSink->toString() << '\n';
}

/* - MQTT Client SetUp / Connect to Broker ---------------------------------------------------- */
TEST_F(DISABLED_MQTTTest, testMQTTConnectToBrokerAsynchronous) {
    auto testSchema = Schema::create()->addField("KEY", UINT32)->addField("VALUE", UINT32);
    auto mqttSink = createMQTTSink(testSchema, 0, nodeEngine, LOCAL_ADDRESS, CLIENT_ID,
                                   TOPIC, USER);
    MQTTSink* testSink = dynamic_cast<MQTTSink*>(mqttSink.get());

    bool connectedToBroker = testSink->connect();
    ASSERT_EQ(true, connectedToBroker);
}

/* - MQTT Client send a finite amount of Data to Broker ---------------------------------------------------- */
TEST_F(DISABLED_MQTTTest, testMQTTsendFiniteDataToBrokerAsynchronous) {
    bool bufferDataSuccessfullyWrittenToBroker = createMQTTSink_ConnectToBroker_WriteData();
    ASSERT_EQ(true, bufferDataSuccessfullyWrittenToBroker);
}

/* - MQTT Client kill broker, does client stop? ---------------------------------------------------- */
TEST_F(DISABLED_MQTTTest, testMQTTbrokerDeathToClientStopAsynchronous) {
    NES_DEBUG("\n\nThis test is meant to be done manually and to crash. Kill the client during the sending process. "
              "The MQTTSink writeData() call should fail and log: 'No more messages can be buffered'\n If the test is "
              "not stopped, it RUNS FOR AN HOUR");
    bool bufferDataSuccessfullyWrittenToBroker = createMQTTSink_ConnectToBroker_WriteData(3600, 1,
                                                                                          's', 1);
    NES_DEBUG("testMQTTbrokerDeathToClientStopAsynchronous result: " << bufferDataSuccessfullyWrittenToBroker);
}

/* - MQTT Client kill disconnect and reconnect to broker, payloads lost? ---------------------------------------------------- */
TEST_F(DISABLED_MQTTTest, testMQTTsendMassiveDataQuicklyAsynchronous) {
    bool bufferDataSuccessfullyWrittenToBroker = createMQTTSink_ConnectToBroker_WriteData(50000, 1000,
                                                                                          'n', 100000);
    ASSERT_EQ(true, bufferDataSuccessfullyWrittenToBroker);
}

TEST_F(DISABLED_MQTTTest, testMQTTUnsentMessagesAsynchronous) {
    NES_DEBUG("\n\nThis test comprises two different tests. The first is not meant to succeed automatically but requires the "
              "tester to evaluate the result.\n TEST 1:\n Kill the broker during the sending process. The writeData() function "
              "should return false and the MQTTSink should log: 'Unsent messages'. "
              "\n\n TEST2:\n Kill the broker during the sending process, but this time reconnect before the client is "
              "finished sending messages. The test should succeed.\n ");

    bool bufferDataSuccessfullyWrittenToBroker = createMQTTSink_ConnectToBroker_WriteData(20, 20);
    ASSERT_EQ(true, bufferDataSuccessfullyWrittenToBroker);
}

/* ------------------------------------------------------------------------------ */
/* ------------------------- SYNCHRONOUS CLIENT TESTS --------------------------- */
/* ------------------------------------------------------------------------------ */
/* - MQTT Client SetUp / Connect to Broker ---------------------------------------------------- */
TEST_F(DISABLED_MQTTTest, testMQTTConnectToBrokerSynchronously) {
    auto testSchema = Schema::create()->addField("KEY", UINT32)->addField("VALUE", UINT32);
    auto mqttSink = createMQTTSink(testSchema, 0, nodeEngine, LOCAL_ADDRESS, CLIENT_ID,
                                   TOPIC, USER, 1, 'm', 500, false);
    MQTTSink* testSink = dynamic_cast<MQTTSink*>(mqttSink.get());

    bool connectedToBroker = testSink->connect();
    ASSERT_EQ(true, connectedToBroker);
}

/* - MQTT Synchronous Client send a finite amount of Data to Broker --------------------------------------------- */
TEST_F(DISABLED_MQTTTest, testMQTTsendFiniteDataToBrokerSynchronously) {
    NES_DEBUG("\n\nThis test comprises two different tests. The first is not meant to succeed automatically but requires the "
              "tester to evaluate the result.\n TEST 1:\n Kill the broker during the sending process. The writeData() function "
              "should return false and the MQTTSink should log: 'Connection Lost'. "
              "\n\n TEST2:\n Do not kill the broker during the sending process. The client should send all messages successfully "
              "and the test should pass.\n");

    bool bufferDataSuccessfullyWrittenToBroker =
        createMQTTSink_ConnectToBroker_WriteData(20, 1, 'm', 500, 0);
    ASSERT_EQ(true, bufferDataSuccessfullyWrittenToBroker);
}


#endif
