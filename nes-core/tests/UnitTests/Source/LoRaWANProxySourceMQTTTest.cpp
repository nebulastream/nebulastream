// /*
//     Licensed under the Apache License, Version 2.0 (the "License");
//     you may not use this file except in compliance with the License.
//     You may obtain a copy of the License at
//         https://www.apache.org/licenses/LICENSE-2.0
//     Unless required by applicable law or agreed to in writing, software
//     distributed under the License is distributed on an "AS IS" BASIS,
//     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//     See the License for the specific language governing permissions and
//     limitations under the License.
// *

// /*
//     Licensed under the Apache License, Version 2.0 (the "License");
//     you may not use this file except in compliance with the License.
//     You may obtain a copy of the License at
//         https://www.apache.org/licenses/LICENSE-2.0
//     Unless required by applicable law or agreed to in writing, software
//     distributed under the License is distributed on an "AS IS" BASIS,
//     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//     See the License for the specific language governing permissions and
//     limitations under the License.
// *

//
// Created by Kasper Hjort Berthelsen on 28/11/2022.
//

#include <Catalogs/Source/PhysicalSourceTypes/LoRaWANProxySourceType.hpp>
#include <NesBaseTest.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/TestUtils.hpp>
#include <boost/process/spawn.hpp>
#include <gtest/gtest.h>
#include <mqtt/client.h>
constexpr int OPERATORID = 1;
constexpr int ORIGINID = 1;
constexpr int NUMSOURCELOCALBUFFERS = 12;

const std::string ADDRESS{"tcp://localhost:1883"};
const std::string PRODUCER_CLIENT_ID{"producer_client"};
const std::string CONSUMER_CLIENT_ID{"consumer_client"};
const std::string DEVICE_ID = "test_deviceid";
const std::string APP_ID = "test_appid";
const std::string TOPIC = "application/" + APP_ID + "/device/" + DEVICE_ID + "/event/up";


namespace NES {
class LoRaWANProxySourceMQTTTest : public Testing::NESBaseTest {
  public:
    mqtt::client client{ADDRESS, PRODUCER_CLIENT_ID};

    std::map<std::string, std::string> sourceConfig{
        {Configurations::LORAWAN_NETWORK_STACK_CONFIG, "ChirpStack"},
        {Configurations::URL_CONFIG, ADDRESS},
        {Configurations::USER_NAME_CONFIG, CONSUMER_CLIENT_ID},
        {Configurations::PASSWORD_CONFIG, "hellothere"},
        {Configurations::LORAWAN_APP_ID_CONFIG, APP_ID},
    };
    LoRaWANProxySourceTypePtr loRaWANProxySourceType;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        Testing::NESBaseTest::SetUpTestCase();
        NES::Logger::setupLogging("LoRaWANProxySourceMQTTTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("LORAWANPROXYSOURCEMQTTTEST::SetUpTestCase()");
    }

    void SetUp() override {
        Testing::NESBaseTest::SetUp();
        NES_DEBUG("LORAWANPROXYSOURCEMQTTTEST::SetUp() LoRaWANProxySource MQTT test cases set up.");
        client.connect();
        ASSERT_TRUE(client.is_connected()) << "client setup failed";

        schema = Schema::create()->addField("var", UINT32);
        loRaWANProxySourceType = LoRaWANProxySourceType::create(sourceConfig);
        auto workerConfigurations = WorkerConfiguration::create();
        nodeEngine = Runtime::NodeEngineBuilder::create(workerConfigurations)
                         .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                         .build();
        bufferManager = nodeEngine->getBufferManager();
        queryManager = nodeEngine->getQueryManager();
    }

    void TearDown() override {
        Testing::NESBaseTest::TearDown();
        client.disconnect();
        ASSERT_TRUE(nodeEngine->stop());
        NES_DEBUG("LORAWANPROXYSOURCEMQTTTEST::TearDown() Tear down LoRaWANProxySourceMQTTTest");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() {
        Testing::NESBaseTest::TearDownTestCase();
        NES_DEBUG("LORAWANPROXYSOURCEMQTTTEST::TearDownTestCases() Tear down LoRaWANProxySourceMQTTTest test class.");
    }

    Runtime::NodeEnginePtr nodeEngine{nullptr};
    Runtime::BufferManagerPtr bufferManager;
    Runtime::QueryManagerPtr queryManager;
    SchemaPtr schema;
    uint64_t buffer_size{};
};

TEST_F(LoRaWANProxySourceMQTTTest, LoRaWANProxySourceConnect) {
    auto init = LoRaWANProxySource(schema,
                                   bufferManager,
                                   queryManager,
                                   loRaWANProxySourceType,
                                   OPERATORID,
                                   ORIGINID,
                                   NUMSOURCELOCALBUFFERS,
                                   GatheringMode::INTERVAL_MODE,
                                   {});
    EXPECT_TRUE(init.connect());
};

TEST_F(LoRaWANProxySourceMQTTTest, LoRaWANProxySourceRecieveData) {
    auto init = LoRaWANProxySource(schema,
                                   bufferManager,
                                   queryManager,
                                   loRaWANProxySourceType,
                                   OPERATORID,
                                   ORIGINID,
                                   NUMSOURCELOCALBUFFERS,
                                   GatheringMode::INTERVAL_MODE,
                                   {});
    EXPECT_TRUE(init.connect());
    sleep(1);
    auto msg = mqtt::make_message(TOPIC, R"({"var":1})");
    client.publish(msg);
    sleep(1);
    auto data = init.receiveData();
    EXPECT_TRUE(data.has_value());
};
}