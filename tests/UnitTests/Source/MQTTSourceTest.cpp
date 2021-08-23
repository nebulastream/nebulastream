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
#include <gtest/gtest.h>
#include <future>


#include <API/Schema.hpp>
#include <Catalogs/PhysicalStreamConfig.hpp>
#include <Mobility/LocationHTTPClient.h>
#include <NodeEngine/NodeEngine.hpp>
#include <Sources/SourceCreator.hpp>

#include <Util/Logger.hpp>
#include <thread>

#ifndef SERVERADDRESS
#define SERVERADDRESS "tcp://127.0.0.1:1883"
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

    void SetUp() override {
        NES_DEBUG("MQTTSOURCETEST::SetUp() MQTTSourceTest cases set up.");

        test_schema = Schema::create()->addField("var", UINT32);

        LocationHTTPClientPtr client = LocationHTTPClient::create("test", 1, "test");
        PhysicalStreamConfigPtr conf = PhysicalStreamConfig::createEmpty();
        nodeEngine = NodeEngine::NodeEngine::create("127.0.0.1", 31337, client, conf, 1, 4096, 1024, 12, 12);

        bufferManager = nodeEngine->getBufferManager();
        queryManager = nodeEngine->getQueryManager();

        buffer_size = bufferManager->getBufferSize();
//        long expectedSize = 0;

        ASSERT_GT(buffer_size, 0U);
    }

    /* Will be called after a test is executed. */
    void TearDown() override {
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
    uint64_t buffer_size{};
};

/**
 * Tests basic set up of MQTT source
 */
TEST_F(MQTTSourceTest, MQTTSourceInit) {

    auto mqttSource = createMQTTSource(test_schema, bufferManager, queryManager, SERVERADDRESS, CLIENTID, USER, TOPIC, 1, 12, {});

    SUCCEED();
}

/**
 * Test if schema, MQTT server address, clientId, user, and topic are the same
 */
TEST_F(MQTTSourceTest, MQTTSourcePrint) {

    auto mqttSource = createMQTTSource(test_schema, bufferManager, queryManager, SERVERADDRESS, CLIENTID, USER, TOPIC, 1, 12, {});

    std::string expected = "MQTTSOURCE(SCHEMA(var:INTEGER ), SERVERADDRESS=tcp://127.0.0.1:1883, CLIENTID=nes-mqtt-test-client, "
                           "USER=rfRqLGZRChg8eS30PEeR, TOPIC=v1/devices/me/telemetry. ";

    EXPECT_EQ(mqttSource->toString(), expected);

    std::cout << mqttSource->toString() << std::endl;

    SUCCEED();
}

/**
 * Tests if obtained value is valid.
 */
TEST_F(MQTTSourceTest, DISABLED_MQTTSourceValue) {

    auto test_schema = Schema::create()->addField("var", UINT32);
    auto mqttSource = createMQTTSource(test_schema, bufferManager, queryManager, SERVERADDRESS, CLIENTID, USER, TOPIC, 1, 12, {});
    auto tuple_buffer = mqttSource->receiveData();
    EXPECT_TRUE(tuple_buffer.has_value());
    uint64_t value = 0;
    auto* tuple = (uint32_t*) tuple_buffer->getBuffer();
    value = *tuple;
    uint64_t expected = 43;
    NES_DEBUG("MQTTSOURCETEST::TEST_F(MQTTSourceTest, MQTTSourceValue) expected value is: " << expected
                                                                                            << ". Received value is: " << value);
    EXPECT_EQ(value, expected);
}
}// namespace NES
#endif
