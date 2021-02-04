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
#include <thread>

#include <API/Schema.hpp>
#include <Catalogs/PhysicalStreamConfig.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <Sinks/SinkCreator.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/Logger.hpp>
#include <gtest/gtest.h>



using namespace NES;

#ifndef LOCAL_HOST
#define LOCAL_HOST "127.0.0.1"
#endif

#ifndef LOCAL_PORT
#define LOCAL_PORT 1883
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

class MQTTTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::setupLogging("MQTTTest.log", NES::LOG_DEBUG);
        NES_DEBUG("Setup MQTTTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() {
        NES_DEBUG("Setup MQTTTest test case.");
        PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create();
        nodeEngine = NodeEngine::create("127.0.0.1", 3111, conf);

        address = std::string("tcp://") + std::string(LOCAL_HOST) + std::string(":") + std::to_string(LOCAL_PORT);

        test_data = {{0, 100, 1, 99, 2, 98, 3, 97}};
        test_data_size = test_data.size() * sizeof(uint32_t);
        tupleCnt = 8;
        //    test_data_size = 4096;
        test_schema = Schema::create()->addField("KEY", UINT32)->addField("VALUE", UINT32);
    }

    /* Will be called before a test is executed. */
    void TearDown() {
        nodeEngine->stop();
        nodeEngine.reset();
        NES_DEBUG("Setup MQTT test case.");
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

/* - ZeroMQ Data Source ---------------------------------------------------- */
TEST_F(MQTTTest, testMQTTClientCreation) {
    auto testSchema = Schema::create()->addField("KEY", UINT32)->addField("VALUE", UINT32);
    auto mqttSink = createMQTTSink(testSchema, 0, nodeEngine, LOCAL_HOST, LOCAL_PORT, CLIENT_ID,
                                      TOPIC, USER);
//    auto zmqSink =
//        createCSVZmqSink(test_schema, 0, nodeEngine, CLIENT_ID, LOCAL_PORT);
//    std::cout << zmqSink->toString() << std::endl;
    std::cout << "Start MQTT_SINK_PRINT - " << mqttSink->toString() << " - End MQTT_SINK_PRINT" << '\n';

    //TODO write further tests - only test done: create MQTTClient
    // Possible tests
    //  - connect to mqtt broker -> IMPLEMENT CONNECT() method in MQTTSink
    //  - send data to mqtt broker -> send a finite amount of data, then stop
    //  - connection disruption -> send infinite amount of data to broker, then manually stop broker -> mqtt client should stop
    //  - disconnect - reconnect - destroy
//    mqttSink->
}
#endif
