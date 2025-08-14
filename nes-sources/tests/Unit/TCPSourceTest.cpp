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

#include <cstddef>
#include <cstdint>
#include <exception>
#include <future>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <variant>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/use_future.hpp>
#include <boost/asio/write.hpp>
#include <boost/system/detail/error_code.hpp>
#include <gtest/gtest.h>

#include <API/Schema.hpp>
#include <Async/Sources/TCPSource.hpp>
#include <Runtime/BufferManager.hpp>
#include <Sources/AsyncSource.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <BaseUnitTest.hpp>
#include <Common/DataTypes/BasicTypes.hpp>

namespace NES
{

namespace asio = boost::asio;

static constexpr size_t DEFAULT_BUFFER_SIZE = 32; // bytes
static constexpr size_t DEFAULT_NUM_BUFFERS = 4;
static constexpr uint32_t DEFAULT_PORT = 12345;

class MockTCPServer
{
public:
    explicit MockTCPServer(const size_t numInputTuplesToProduce)
        : acceptor(ioc, asio::ip::tcp::endpoint(asio::ip::tcp::v4(), DEFAULT_PORT)), numInputTuplesToProduce(numInputTuplesToProduce)
    {
        co_spawn(ioc, accept(), asio::detached);
        ioc.run();
    }

    asio::awaitable<void> writeTuples(asio::ip::tcp::socket socket) const
    {
        for (uint32_t i = 0; i < numInputTuplesToProduce; ++i)
        {
            std::string data = std::to_string(i) + "\n";
            co_await asio::async_write(socket, asio::const_buffer(data.data(), data.size()), asio::use_awaitable);
        }
        NES_DEBUG("Finished writing data. Shutting down server...");

        boost::system::error_code errorCode;
        socket.shutdown(asio::ip::tcp::socket::shutdown_both, errorCode);
        socket.close(errorCode);
    }

    asio::awaitable<void> accept()
    {
        NES_DEBUG("Waiting to accept connection request on localhost:{}...", DEFAULT_PORT);
        asio::ip::tcp::socket socket = co_await acceptor.async_accept(asio::use_awaitable);
        NES_DEBUG("Accepted connection.");
        asio::co_spawn(acceptor.get_executor(), writeTuples(std::move(socket)), asio::detached);
    }

private:
    asio::io_context ioc;
    asio::ip::tcp::acceptor acceptor;
    size_t numInputTuplesToProduce;
};


class TCPSourceTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("TCPSourceTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup TCPSourceTest class.");
    }

    static void TearDownTestSuite() { NES_INFO("Tear down TCPSourceTest class."); }

    void SetUp() override
    {
        BaseUnitTest::SetUp();

        bufferManager = Memory::BufferManager::create(DEFAULT_BUFFER_SIZE, DEFAULT_NUM_BUFFERS);
    }

    void TearDown() override { BaseUnitTest::TearDown(); }

protected:
    asio::io_context ioc;
    std::shared_ptr<Memory::BufferManager> bufferManager;
};


TEST_F(TCPSourceTest, FillBuffer)
{
    constexpr size_t numInputTuplesToProduce = 14;
    /// Server runs and awaits a single connection

    const std::jthread thread([] { MockTCPServer{numInputTuplesToProduce}; });

    const std::shared_ptr<Schema> schema = Schema::create()->addField("id", BasicType::INT32);
    const Sources::ParserConfig parserConfig = {.parserType = "CSV", .tupleDelimiter = "\n", .fieldDelimiter = ","};

    auto tcpSource = Sources::TCPSource{
        Sources::SourceDescriptor{schema, "tcpSource", "TCP", parserConfig, {{"socketHost", "localhost"}, {"socketPort", DEFAULT_PORT}}}};

    auto buf = bufferManager->getBufferBlocking();
    auto future = asio::co_spawn(
        ioc,
        [&]() -> asio::awaitable<Sources::AsyncSource::InternalSourceResult, asio::io_context::executor_type>
        {
            /// Open the connection to our mock server
            co_await tcpSource.open();
            auto result = co_await tcpSource.fillBuffer(buf);
            tcpSource.close();
            co_return result;
        },
        asio::use_future);

    ioc.run();
    auto sourceResult = future.get();

    std::ostringstream oss;
    for (size_t i = 0; i < numInputTuplesToProduce; ++i)
    {
        oss << i << "\n";
    }
    const std::string expected = oss.str();

    const auto actual = std::string{buf.getBuffer<const char>(), expected.size()};
    EXPECT_EQ(expected, actual);

    /// We expect to receive the Continue message since we have received exactly 32 bytes, which is equal to the buffer size specified
    EXPECT_TRUE(std::holds_alternative<Sources::AsyncSource::Continue>(sourceResult));
}
}
