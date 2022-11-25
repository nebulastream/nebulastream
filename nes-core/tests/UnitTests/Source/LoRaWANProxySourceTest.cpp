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
#include <NesBaseTest.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/TestUtils.hpp>
#include <gtest/gtest.h>

constexpr int OPERATORID = 1;
constexpr int ORIGINID = 1;
constexpr int NUMSOURCELOCALBUFFERS = 12;

namespace NES {

class LoRaWANProxySourceTest : public Testing::NESBaseTest {
  public:
    Runtime::NodeEnginePtr nodeEngine{nullptr};
    Runtime::BufferManagerPtr bufferManager;
    Runtime::QueryManagerPtr queryManager;
    SchemaPtr schema;
    uint64_t buffer_size{};
    LoRaWANProxySourceTypePtr loRaWANProxySourceType;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("LoRaWANProxySourceTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("LORAWANPROXYSOURCETEST::SetUpTestCase()");
    }

    void SetUp() override {
        Testing::NESBaseTest::SetUp();
        NES_DEBUG("LORAWANPROXYSOURCETEST::SetUp() LoRaWANProxySource test cases set up.");
        schema = Schema::create()->addField("var", UINT32);
        loRaWANProxySourceType = LoRaWANProxySourceType::create();
        auto workerConfigurations = WorkerConfiguration::create();
        nodeEngine = Runtime::NodeEngineBuilder::create(workerConfigurations)
                         .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                         .build();
        bufferManager = nodeEngine->getBufferManager();
        queryManager = nodeEngine->getQueryManager();
    }

    /* Will be called after a test is executed. */
    void TearDown() override {
        Testing::NESBaseTest::TearDown();
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
    SUCCEED();
}

}// namespace NES