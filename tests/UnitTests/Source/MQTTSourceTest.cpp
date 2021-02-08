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

#include <gtest/gtest.h>
#ifdef ENABLE_MQTT_BUILD
#include <Catalogs/PhysicalStreamConfig.hpp>
#include <cstring>
#include <future>
#include <iostream>
#include <string>

#include <API/Schema.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/Logger.hpp>
#include <thread>

#ifndef SERVERADDRESS
#define SERVERADDRESS "127.0.0.1:1883"
#endif

#ifndef CLIENTID
#define CLIENTID "nes-mqtt-test-client"
#endif

#ifndef TOPIC
#define TOPIC "v1/devices/me/telemetry"
#endif

#ifndef USER
#define USER "rfRqLGZRChg8eS30PEeR"
#endif

namespace NES {

class MQTTSourceTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::setupLogging("MQTTSourceTest.log", NES::LOG_DEBUG);
        NES_DEBUG("MQTTSOURCETEST::SetUpTestCase()");
    }

    void SetUp() {
        NES_DEBUG("MQTTSOURCETEST::SetUp() MQTTSourceTest cases set up.");

        test_schema = Schema::create()->addField("var", UINT32);

        PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create();
        nodeEngine = NodeEngine::create("127.0.0.1", 31337, conf);

        bufferManager = nodeEngine->getBufferManager();
        queryManager = nodeEngine->getQueryManager();

        buffer_size = bufferManager->getBufferSize();

        ASSERT_GT(buffer_size, 0);
    }

    /* Will be called after a test is executed. */
    void TearDown() {
        nodeEngine->stop();
        nodeEngine.reset();
        NES_DEBUG("MQTTSOURCETEST::TearDown() Tear down MQTTSourceTest");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_DEBUG("MQTTSOURCETEST::TearDownTestCases() Tear down MQTTSourceTest test class."); }

    NodeEngine::NodeEnginePtr nodeEngine{nullptr};
    NodeEngine::BufferManagerPtr bufferManager;
    NodeEngine::QueryManagerPtr queryManager;
    SchemaPtr test_schema;
    uint64_t buffer_size;

};

/**
 * Tests basic set up of MQTT source
 */
TEST_F(MQTTSourceTest, MQTTSourceInit) {

    auto mqttSource = createMQTTSource(test_schema, bufferManager, queryManager, SERVERADDRESS, CLIENTID, USER, TOPIC, 1);

    SUCCEED();
}

/**
 * Test if schema, OPC server url, and node index are the same
 */
TEST_F(MQTTSourceTest, MQTTSourcePrint) {

    auto mqttSource = createMQTTSource(test_schema, bufferManager, queryManager, SERVERADDRESS, CLIENTID, USER, TOPIC, 1);

    std::string expected = "MQTTSOURCE(SCHEMA(var:INTEGER ), SERVERADDRESS=127.0.0.1:1883, CLIENTID=nes-mqtt-test-client, "
                           "USER=rfRqLGZRChg8eS30PEeR, TOPIC=v1/devices/me/telemetry. ";

    EXPECT_EQ(mqttSource->toString(), expected);

    std::cout << mqttSource->toString() << std::endl;

    SUCCEED();
}

}// namespace NES
#endif
