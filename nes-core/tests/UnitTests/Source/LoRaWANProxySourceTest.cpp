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

#include <Catalogs/Source/PhysicalSourceTypes/LoRaWANProxySourceType.hpp>
#include <BaseIntegrationTest.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/TestUtils.hpp>
//#include <boost/process/spawn.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Runtime/NodeEngine.hpp>
#include <gtest/gtest.h>
#include <mqtt/client.h>

constexpr int OPERATORID = 1;
constexpr int ORIGINID = 1;
constexpr int NUMSOURCELOCALBUFFERS = 12;

namespace NES {

class LoRaWANProxySourceTest : public Testing::BaseIntegrationTest {
  public:
    Runtime::NodeEnginePtr nodeEngine{nullptr};
    Runtime::BufferManagerPtr bufferManager;
    Runtime::QueryManagerPtr queryManager;
    SchemaPtr schema;
    uint64_t buffer_size{};
    std::map<std::string, std::string> sourceConfig{
        {Configurations::LORAWAN_NETWORK_STACK_CONFIG, "ChirpStack"},
        {Configurations::URL_CONFIG, "tcp://localhost:1883"},
        {Configurations::LORAWAN_CA_PATH, "notneeded"},
        {Configurations::LORAWAN_CERT_PATH, "alsonotneeded"},
        {Configurations::LORAWAN_KEY_PATH, "includingthis"},
        {Configurations::LORAWAN_DEVICE_EUIS, "yaba daba doo"},
        {Configurations::LORAWAN_SENSOR_FIELDS, "hehe"},
        {Configurations::USER_NAME_CONFIG, "hellothere"},
        {Configurations::PASSWORD_CONFIG, "General Grevious"},
        {Configurations::LORAWAN_APP_ID_CONFIG, "testing"},
    };
    LoRaWANProxySourceTypePtr loRaWANProxySourceType;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        Testing::BaseIntegrationTest::SetUpTestCase();
        NES::Logger::setupLogging("LoRaWANProxySourceTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("LORAWANPROXYSOURCETEST::SetUpTestCase()");
    }

    void SetUp() override {
        Testing::BaseIntegrationTest::SetUp();
        NES_DEBUG("LORAWANPROXYSOURCETEST::SetUp() LoRaWANProxySource test cases set up.");
        schema = Schema::create()->addField("var", BasicType::UINT32);
        loRaWANProxySourceType = LoRaWANProxySourceType::create(sourceConfig);
        auto workerConfigurations = WorkerConfiguration::create();
        nodeEngine = Runtime::NodeEngineBuilder::create(workerConfigurations)
                         .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                         .build();
        bufferManager = nodeEngine->getBufferManager();
        queryManager = nodeEngine->getQueryManager();
    }

    /* Will be called after a test is executed. */
    void TearDown() override {
        Testing::BaseIntegrationTest::TearDown();
        ASSERT_TRUE(nodeEngine->stop());
        NES_DEBUG("LORAWANPROXYSOURCETEST::TearDown() Tear down LoRaWANProxySourceTest");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() {
        NES_DEBUG("LORAWANPROXYSOURCETEST::TearDownTestCases() Tear down LoRaWANProxySourceTest test class.");
    }
};
/// TESTS

TEST_F(LoRaWANProxySourceTest, LoRaWANProxySourceInit) {
    auto init = createLoRaWANProxySource(schema,
                                         bufferManager,
                                         queryManager,
                                         loRaWANProxySourceType,
                                         OPERATORID,
                                         ORIGINID,
                                         NUMSOURCELOCALBUFFERS,
                                         {});
    EXPECT_EQ(init->getSchema(), schema);
    EXPECT_EQ(init->getOperatorId(), OPERATORID);
    std::cout << init->toString();
    SUCCEED();
}

TEST_F(LoRaWANProxySourceTest, LoRaWANProxySourceInitCorrectValues) {
    auto init = createLoRaWANProxySource(schema,
                                         bufferManager,
                                         queryManager,
                                         loRaWANProxySourceType,
                                         OPERATORID,
                                         ORIGINID,
                                         NUMSOURCELOCALBUFFERS,
                                         {});
    auto expected = "LoRaWANProxySource(SCHEMA(var:INTEGER(32 bits)), CONFIG(LoRaWANProxySourceType =>  "
                    "{\nnetworkStack: ChirpStack\n"
                    "url: tcp://localhost:1883\n"
                    "userName: hellothere\n"
                    "password: General Grevious\n"
                    "appId: testing\n"
                    "CAPath: notneeded\n"
                    "certPath: alsonotneeded\n"
                    "keyPath: includingthis\n"
                    "sensorFields: [ hehe, ]})).";
    EXPECT_EQ(expected, init->toString());
}

}// namespace NES