/*
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

#include <API/Schema.hpp>
#include <BaseIntegrationTest.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/TCPSourceType.hpp>
#include <Operators/LogicalOperators/Sources/TCPSourceDescriptor.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <iostream>
#include <string>

#include <Catalogs/Query/QueryCatalog.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Identifiers.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Services/RequestHandlerService.hpp>
#include <Util/TestUtils.hpp>
#include <netinet/in.h>

#ifndef OPERATORID
#define OPERATORID 1
#endif

#ifndef ORIGINID
#define ORIGINID 1
#endif

#ifndef NUMSOURCELOCALBUFFERS
#define NUMSOURCELOCALBUFFERS 12
#endif

#ifndef SUCCESSORS
#define SUCCESSORS                                                                                                               \
    {}
#endif

#ifndef INPUTFORMAT
#define INPUTFORMAT InputFormat::JSON
#endif

namespace NES {

class TCPSourceTest : public Testing::BaseIntegrationTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("TCPSourceTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("TCPSOURCETEST::SetUpTestCase()");
    }

    void SetUp() override {
        Testing::BaseIntegrationTest::SetUp();
        NES_DEBUG("TCPSOURCETEST::SetUp() TCPSourceTest cases set up.");
        test_schema = Schema::create()->addField("var", BasicType::UINT32);
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
        NES_DEBUG("TCPSOURCETEST::TearDown() Tear down TCPSOURCETEST");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_DEBUG("TCPSOURCETEST::TearDownTestCases() Tear down TCPSOURCETEST test class."); }

    Runtime::NodeEnginePtr nodeEngine{nullptr};
    Runtime::BufferManagerPtr bufferManager;
    Runtime::QueryManagerPtr queryManager;
    SchemaPtr test_schema;
    uint64_t buffer_size{};
    TCPSourceTypePtr tcpSourceType;
};

/**
 * Test TCPSource Construction via PhysicalSourceFactory from YAML
 */
TEST_F(TCPSourceTest, TCPSourceUserSpecifiedTupleSizePrint) {

    Yaml::Node sourceConfiguration;
    std::string yaml = R"(
logicalSourceName: tcpsource
physicalSourceName: tcpsource_1
type: TCP_SOURCE
configuration:
    socketDomain: AF_INET
    socketType: SOCK_STREAM
    socketPort: 9080
    socketHost: 192.168.1.2
    inputFormat: CSV
    socketBufferSize: 100
    decideMessageSize: USER_SPECIFIED_BUFFER_SIZE
    flushIntervalMS: 10
    )";
    Yaml::Parse(sourceConfiguration, yaml);

    auto sourceType = PhysicalSourceTypeFactory::createFromYaml(sourceConfiguration);
    auto tcpSourceType = std::dynamic_pointer_cast<TCPSourceType>(sourceType);
    ASSERT_NE(tcpSourceType, nullptr);

    ASSERT_EQ(tcpSourceType->getSocketDomain()->getValue(), AF_INET);
    ASSERT_EQ(tcpSourceType->getSocketType()->getValue(), SOCK_STREAM);
    ASSERT_EQ(tcpSourceType->getSocketPort()->getValue(), 9080);
    ASSERT_EQ(tcpSourceType->getSocketHost()->getValue(), "192.168.1.2");
    ASSERT_EQ(tcpSourceType->getInputFormat()->getValue(), InputFormat::CSV);
    ASSERT_EQ(tcpSourceType->getDecideMessageSize()->getValue(), TCPDecideMessageSize::USER_SPECIFIED_BUFFER_SIZE);
    ASSERT_EQ(tcpSourceType->getSocketBufferSize()->getValue(), 100);
    ASSERT_EQ(tcpSourceType->getFlushIntervalMS()->getValue(), 10);

    auto tcpSource = createTCPSource(test_schema,
                                     bufferManager,
                                     queryManager,
                                     tcpSourceType,
                                     OPERATORID,
                                     ORIGINID,
                                     NUMSOURCELOCALBUFFERS,
                                     "tcp-source",
                                     SUCCESSORS);

    std::string expected =
        R"(TCPSOURCE(SCHEMA(var:INTEGER(32 bits)), TCPSourceType => {
socketHost: 192.168.1.2
socketPort: 9080
socketDomain: 2
socketType: 1
flushIntervalMS: 10
inputFormat: CSV
decideMessageSize: USER_SPECIFIED_BUFFER_SIZE
tupleSeparator:)";
    std::string expected_part_2 = " \n";
    std::string expected_part_3 = R"(
socketBufferSize: 100
bytesUsedForSocketBufferSizeTransfer: 0
})";
    EXPECT_EQ(tcpSource->toString(), expected + expected_part_2 + expected_part_3);
}

/**
 * Test TCPSource Construction via PhysicalSourceFactory from YAML
 */
TEST_F(TCPSourceTest, TCPSourceTupleSeparatorPrint) {

    Yaml::Node sourceConfiguration;
    std::string yaml = R"(
logicalSourceName: tcpsource
physicalSourceName: tcpsource_1
type: TCP_SOURCE
configuration:
    socketDomain: AF_INET
    socketType: SOCK_STREAM
    socketPort: 9090
    socketHost: 127.0.0.1
    inputFormat: JSON
    decideMessageSize: TUPLE_SEPARATOR
    #defaults to tupleSeparator: '\n'
    flushIntervalMS: 10
    )";
    Yaml::Parse(sourceConfiguration, yaml);

    auto sourceType = PhysicalSourceTypeFactory::createFromYaml(sourceConfiguration);
    auto tcpSourceType = std::dynamic_pointer_cast<TCPSourceType>(sourceType);
    ASSERT_NE(tcpSourceType, nullptr);

    ASSERT_EQ(tcpSourceType->getSocketDomain()->getValue(), AF_INET);
    ASSERT_EQ(tcpSourceType->getSocketType()->getValue(), SOCK_STREAM);
    ASSERT_EQ(tcpSourceType->getSocketPort()->getValue(), 9090);
    ASSERT_EQ(tcpSourceType->getSocketHost()->getValue(), "127.0.0.1");
    ASSERT_EQ(tcpSourceType->getInputFormat()->getValue(), INPUTFORMAT);
    ASSERT_EQ(tcpSourceType->getTupleSeparator()->getValue(), '\n');
    ASSERT_EQ(tcpSourceType->getDecideMessageSize()->getValue(), TCPDecideMessageSize::TUPLE_SEPARATOR);
    ASSERT_EQ(tcpSourceType->getFlushIntervalMS()->getValue(), 10);

    auto tcpSource = createTCPSource(test_schema,
                                     bufferManager,
                                     queryManager,
                                     tcpSourceType,
                                     OPERATORID,
                                     ORIGINID,
                                     NUMSOURCELOCALBUFFERS,
                                     "tcp-source",
                                     SUCCESSORS);

    std::string expected =
        R"(TCPSOURCE(SCHEMA(var:INTEGER(32 bits)), TCPSourceType => {
socketHost: 127.0.0.1
socketPort: 9090
socketDomain: 2
socketType: 1
flushIntervalMS: 10
inputFormat: JSON
decideMessageSize: TUPLE_SEPARATOR
tupleSeparator:)";
    std::string expected_part_2 = " \n";
    std::string expected_part_3 = R"(
socketBufferSize: 0
bytesUsedForSocketBufferSizeTransfer: 0
})";
    EXPECT_EQ(tcpSource->toString(), expected + expected_part_2 + expected_part_3);
}
/**
 * Test TCPSource Construction via PhysicalSourceFactory from YAML
 */
TEST_F(TCPSourceTest, TCPSourceBufferSizeFromSocketPrint) {

    Yaml::Node sourceConfiguration;
    std::string yaml = R"(
logicalSourceName: tcpsource
physicalSourceName: tcpsource_1
type: TCP_SOURCE
configuration:
    socketDomain: AF_INET
    socketType: SOCK_STREAM
    socketPort: 9090
    socketHost: 127.0.0.1
    inputFormat: JSON
    decideMessageSize: BUFFER_SIZE_FROM_SOCKET
    bytesUsedForSocketBufferSizeTransfer: 8
    flushIntervalMS: 100
    )";
    Yaml::Parse(sourceConfiguration, yaml);

    auto sourceType = PhysicalSourceTypeFactory::createFromYaml(sourceConfiguration);
    auto tcpSourceType = std::dynamic_pointer_cast<TCPSourceType>(sourceType);
    ASSERT_NE(tcpSourceType, nullptr);

    ASSERT_EQ(tcpSourceType->getSocketDomain()->getValue(), AF_INET);
    ASSERT_EQ(tcpSourceType->getSocketType()->getValue(), SOCK_STREAM);
    ASSERT_EQ(tcpSourceType->getSocketPort()->getValue(), 9090);
    ASSERT_EQ(tcpSourceType->getSocketHost()->getValue(), "127.0.0.1");
    ASSERT_EQ(tcpSourceType->getInputFormat()->getValue(), INPUTFORMAT);
    ASSERT_EQ(tcpSourceType->getDecideMessageSize()->getValue(), TCPDecideMessageSize::BUFFER_SIZE_FROM_SOCKET);
    ASSERT_EQ(tcpSourceType->getFlushIntervalMS()->getValue(), 100);

    auto tcpSource = createTCPSource(test_schema,
                                     bufferManager,
                                     queryManager,
                                     tcpSourceType,
                                     OPERATORID,
                                     ORIGINID,
                                     NUMSOURCELOCALBUFFERS,
                                     "tcp-source",
                                     SUCCESSORS);

    std::string expected =
        R"(TCPSOURCE(SCHEMA(var:INTEGER(32 bits)), TCPSourceType => {
socketHost: 127.0.0.1
socketPort: 9090
socketDomain: 2
socketType: 1
flushIntervalMS: 100
inputFormat: JSON
decideMessageSize: BUFFER_SIZE_FROM_SOCKET
tupleSeparator:)";
    std::string expected_part_2 = " \n";
    std::string expected_part_3 = R"(
socketBufferSize: 0
bytesUsedForSocketBufferSizeTransfer: 8
})";
    EXPECT_EQ(tcpSource->toString(), expected + expected_part_2 + expected_part_3);
}

}// namespace NES
