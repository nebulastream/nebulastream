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
#include <Runtime/HardwareManager.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/QueryManager.hpp>
#include <Services/RequestHandlerService.hpp>
#include <Util/TestUtils.hpp>
#include <arpa/inet.h>
#include <netinet/in.h>

#ifndef OPERATORID
#define OPERATORID 1
#endif

#ifndef ORIGINID
#define ORIGINID 1
#endif

#ifndef STATISTICID
#define STATISTICID 1
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

static thread_local struct {
    bool called_socket_create = false;
    int domain = 0;
    int type = 0;
    int protocol = 0;
    bool called_connect = false;
    size_t connect_fd = 0;
    sockaddr_in addr_in = {};
    socklen_t socklen = 0;
    size_t numbers_of_reads = 0;
    std::vector<size_t> read_sizes;
    std::vector<const void*> read_ptr;
} record_socket_parameters;
static thread_local struct {
    int fd = 0;
    int connection_fd = 0;
    std::vector<char> data;
    size_t index = 0;
} socket_mock_data;

void setup_read_from_buffer(std::vector<char> data) {
    socket_mock_data.fd = 9234;
    socket_mock_data.data = std::move(data);
    socket_mock_data.index = 0;

    record_socket_parameters = {};
}
std::string get_ip_address() {
    char ip_string[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &(record_socket_parameters.addr_in.sin_addr), ip_string, INET_ADDRSTRLEN);
    return {ip_string, strnlen(ip_string, INET_ADDRSTRLEN)};
}

short get_port() { return ntohs(record_socket_parameters.addr_in.sin_port); }

// Mock Network Stack
// We can override socket and connect within the TCPSourceTest binary
// both socket and create are defined weakly and can be overriden
// since neither functions are called anywhere else within the test it is fine to completly mock them
extern "C" int socket(int domain, int type, int protocol) {
    assert(!record_socket_parameters.called_socket_create && "Unexpected multiple create socket calls");
    record_socket_parameters.called_socket_create = true;
    record_socket_parameters.domain = domain;
    record_socket_parameters.type = type;
    record_socket_parameters.protocol = protocol;

    return socket_mock_data.fd;
}
extern "C" int connect(int fd, const sockaddr* addr, [[maybe_unused]] socklen_t addrlen) {
    assert(!record_socket_parameters.called_connect && "Unexpected multiple connect calls");
    record_socket_parameters.connect_fd = fd;
    assert(addrlen == sizeof(sockaddr_in) && "Unexpected size of sockaddr_in parameter");
    record_socket_parameters.addr_in = *reinterpret_cast<const sockaddr_in*>(addr);
    return socket_mock_data.connection_fd;
}

// read however is more tricky as it is used everywhere in the system. We can wrap the read function
// see nes-runtime cmake script ('-Wl,--wrap,read') this means calls to read a instead calling __wrap_read
// and __wrap_read can delegate all real calls to __real_read.
extern "C" ssize_t __real_read(int fd, void* data, size_t size);
extern "C" ssize_t __wrap_read(int fd, void* data, size_t size) {
    if (fd != socket_mock_data.fd) {
        return __real_read(fd, data, size);
    }

    record_socket_parameters.numbers_of_reads++;
    record_socket_parameters.read_sizes.push_back(size);
    record_socket_parameters.read_ptr.push_back(data);

    const auto read_size = std::min(size, socket_mock_data.data.size() - socket_mock_data.index);
    std::memcpy(data, socket_mock_data.data.data() + socket_mock_data.index, read_size);

    socket_mock_data.index += read_size;
    return static_cast<ssize_t>(read_size);
}

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
        bufferManager = std::make_shared<Runtime::BufferManager>();
        queryManager = std::make_shared<Runtime::DynamicQueryManager>(nullptr,
                                                                      std::vector{bufferManager},
                                                                      1,
                                                                      1,
                                                                      std::make_shared<Runtime::HardwareManager>(),
                                                                      1);
        auto workerConfigurations = WorkerConfiguration::create();
    }

    /* Will be called after a test is executed. */
    void TearDown() override {
        Testing::BaseIntegrationTest::TearDown();
        queryManager->destroy();
        NES_DEBUG("TCPSOURCETEST::TearDown() Tear down TCPSOURCETEST");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_DEBUG("TCPSOURCETEST::TearDownTestCases() Tear down TCPSOURCETEST test class."); }

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
    socketBufferSize: 20
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
    ASSERT_EQ(tcpSourceType->getSocketBufferSize()->getValue(), 20);
    ASSERT_EQ(tcpSourceType->getFlushIntervalMS()->getValue(), 10);

    auto tcpSource = createTCPSource(test_schema,
                                     bufferManager,
                                     queryManager,
                                     tcpSourceType,
                                     OPERATORID,
                                     ORIGINID,
                                     STATISTICID,
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
socketBufferSize: 20
bytesUsedForSocketBufferSizeTransfer: 0
})";
    EXPECT_EQ(tcpSource->toString(), expected + expected_part_2 + expected_part_3);

    std::stringstream ss;
    for (int i = 0; i < 1000; i++) {
        ss << fmt::format("{:20}", i);
    }

    auto as_string = ss.str();
    size_t total_number_of_bytes = as_string.size();
    setup_read_from_buffer(std::vector(as_string.begin(), as_string.end()));
    read(socket_mock_data.connection_fd, nullptr, 0);

    tcpSource->open();

    ASSERT_EQ(get_ip_address(), "192.168.1.2");
    ASSERT_EQ(get_port(), 9080);

    uint32_t i = 0;
    while (socket_mock_data.index < total_number_of_bytes) {
        auto buffer = bufferManager->getBufferBlocking();
        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(
            Runtime::MemoryLayouts::RowLayout::create(test_schema, bufferManager->getBufferSize()),
            buffer);
        std::dynamic_pointer_cast<TCPSource>(tcpSource)->fillBuffer(dynamicBuffer);

        for (const auto& tuple : dynamicBuffer) {
            ASSERT_EQ(tuple["var"].read<uint32_t>(), i++);
        }
    }

    ASSERT_EQ(i, 1000);
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
                                     STATISTICID,
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

    std::stringstream ss;
    for (int i = 0; i < 1000; i++) {
        ss << "{ \"var\": " << i << "}" << '\n';
    }
    std::string as_string = ss.str();
    setup_read_from_buffer(std::vector(as_string.begin(), as_string.end()));
    read(socket_mock_data.connection_fd, nullptr, 0);

    tcpSource->open();

    ASSERT_EQ(get_ip_address(), "127.0.0.1");
    ASSERT_EQ(get_port(), 9090);

    uint32_t i = 0;
    while (socket_mock_data.index < as_string.size()) {
        auto buffer = bufferManager->getBufferBlocking();
        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(
            Runtime::MemoryLayouts::RowLayout::create(test_schema, bufferManager->getBufferSize()),
            buffer);
        std::dynamic_pointer_cast<TCPSource>(tcpSource)->fillBuffer(dynamicBuffer);

        for (const auto& tuple : dynamicBuffer) {
            ASSERT_EQ(tuple["var"].read<uint32_t>(), i++);
        }
    }
    ASSERT_EQ(i, 1000);
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
                                     STATISTICID,
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

    std::stringstream ss;
    for (int i = 0; i < 1000; i++) {
        std::string tuple = fmt::format("{{\"var\":{}}}", i);
        ss << fmt::format("{:8}{}", tuple.size(), tuple);
    }

    auto as_string = ss.str();
    size_t total_number_of_bytes = as_string.size();
    setup_read_from_buffer(std::vector(as_string.begin(), as_string.end()));
    read(socket_mock_data.connection_fd, nullptr, 0);

    tcpSource->open();

    ASSERT_EQ(get_ip_address(), "127.0.0.1");
    ASSERT_EQ(get_port(), 9090);

    uint32_t i = 0;
    while (socket_mock_data.index < total_number_of_bytes) {
        auto buffer = bufferManager->getBufferBlocking();
        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(
            Runtime::MemoryLayouts::RowLayout::create(test_schema, bufferManager->getBufferSize()),
            buffer);
        std::dynamic_pointer_cast<TCPSource>(tcpSource)->fillBuffer(dynamicBuffer);

        for (const auto& tuple : dynamicBuffer) {
            ASSERT_EQ(tuple["var"].read<uint32_t>(), i++);
        }
    }
    ASSERT_EQ(i, 1000);
}

/**
 * Test TCPSource Construction via PhysicalSourceFactory from YAML
 */
TEST_F(TCPSourceTest, TCPSourceBufferSizeFromSocketNesFormatPrint) {

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
    inputFormat: NES
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
    ASSERT_EQ(tcpSourceType->getInputFormat()->getValue(), InputFormat::NES);
    ASSERT_EQ(tcpSourceType->getDecideMessageSize()->getValue(), TCPDecideMessageSize::BUFFER_SIZE_FROM_SOCKET);
    ASSERT_EQ(tcpSourceType->getFlushIntervalMS()->getValue(), 100);

    auto schema = Schema::create()
                      ->addField("timestamp", BasicType::UINT64)
                      ->addField("bidder", BasicType::UINT64)
                      ->addField("auction", BasicType::UINT64)
                      ->addField("price", BasicType::FLOAT64);
    auto tcpSource = createTCPSource(schema,
                                     bufferManager,
                                     queryManager,
                                     tcpSourceType,
                                     OPERATORID,
                                     ORIGINID,
                                     NUMSOURCELOCALBUFFERS,
                                     "tcp-source",
                                     SUCCESSORS);

    std::string expected =
        R"(TCPSOURCE(SCHEMA(timestamp:INTEGER(64 bits) bidder:INTEGER(64 bits) auction:INTEGER(64 bits) price:Float(64 bits)), TCPSourceType => {
socketHost: 127.0.0.1
socketPort: 9090
socketDomain: 2
socketType: 1
flushIntervalMS: 100
inputFormat: NES
decideMessageSize: BUFFER_SIZE_FROM_SOCKET
tupleSeparator:)";
    std::string expected_part_2 = " \n";
    std::string expected_part_3 = R"(
socketBufferSize: 0
bytesUsedForSocketBufferSizeTransfer: 8
})";
    EXPECT_EQ(tcpSource->toString(), expected + expected_part_2 + expected_part_3);

    size_t total_number_of_bytes = 0;
    std::vector<char> data;
    for (size_t number_of_buffers = 0; number_of_buffers < 10; number_of_buffers++) {
        auto buffer = bufferManager->getBufferBlocking();
        auto dynBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer::createDynamicTupleBuffer(buffer, schema);
        buffer.setNumberOfTuples(dynBuffer.getCapacity());
        uint64_t i = 0;
        for (auto tuple : dynBuffer) {
            tuple[0].write<uint64_t>(i);
            tuple[1].write<uint64_t>(i);
            tuple[2].write<uint64_t>(i);
            tuple[3].write<double>(i * 0.9);
            i++;
        }

        std::copy(reinterpret_cast<char*>(&i), reinterpret_cast<char*>(&i) + 8, std::back_inserter(data));
        std::copy(buffer.getBuffer(), buffer.getBuffer() + i * schema->getSchemaSizeInBytes(), std::back_inserter(data));
    }
    total_number_of_bytes += data.size();
    setup_read_from_buffer(std::move(data));
    read(socket_mock_data.connection_fd, nullptr, 0);

    tcpSource->open();

    ASSERT_EQ(get_ip_address(), "127.0.0.1");
    ASSERT_EQ(get_port(), 9090);

    uint32_t i = 0;
    while (socket_mock_data.index < total_number_of_bytes) {
        auto buffer = bufferManager->getBufferBlocking();
        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(
            Runtime::MemoryLayouts::RowLayout::create(schema, bufferManager->getBufferSize()),
            buffer);
        std::dynamic_pointer_cast<TCPSource>(tcpSource)->fillBuffer(dynamicBuffer);

        for (const auto& tuple : dynamicBuffer) {
            ASSERT_EQ(tuple[0].read<uint64_t>(), i % dynamicBuffer.getCapacity());
            ASSERT_EQ(tuple[1].read<uint64_t>(), i % dynamicBuffer.getCapacity());
            ASSERT_EQ(tuple[2].read<uint64_t>(), i % dynamicBuffer.getCapacity());
            i++;
        }
    }
    ASSERT_EQ(i, 10 * 256);
}

}// namespace NES
