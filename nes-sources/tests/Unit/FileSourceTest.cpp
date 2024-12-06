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

#include <future>
#include <memory>
#include <string>
#include <API/Schema.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>
#include <fmt/format.h>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <FileSource.hpp>
#include <Common/DataTypes/BasicTypes.hpp>
#include "ErrorHandling.hpp"
#include "Runtime/BufferManager.hpp"

namespace NES
{

namespace asio = boost::asio;

static constexpr auto TEST_DATA_DIR = "testdata";
static constexpr size_t DEFAULT_BUFFER_SIZE = 32; // bytes
static constexpr size_t DEFAULT_NUM_BUFFERS = 4;

struct FileSourceTestConfig
{
    std::string filePath;
    std::shared_ptr<Schema> schema = Schema::create()->addField("id", BasicType::INT32);
    Sources::ParserConfig parserConfig = {.parserType = "CSV", .tupleDelimiter = "\n", .fieldDelimiter = ","};

    explicit FileSourceTestConfig(std::string filePath) : filePath(std::move(filePath)) { }
};

class FileSourceTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("FileSourceTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup FileSourceTest class.");
    }

    static void TearDownTestSuite() { NES_INFO("Tear down FileSourceTest class."); }

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

TEST_F(FileSourceTest, FillBuffer)
{
    auto config = FileSourceTestConfig{fmt::format("{}/fileSourceTestSimple.csv", TEST_DATA_DIR)};

    auto fileSource = Sources::FileSource{
        Sources::SourceDescriptor{config.schema, "fileSource", "File", config.parserConfig, {{"filePath", config.filePath}}}};

    auto buf = bufferManager->getBufferBlocking();
    auto future = asio::co_spawn(
        ioc,
        [&]() -> asio::awaitable<Sources::Source::InternalSourceResult>
        {
            co_await fileSource.open(ioc);
            auto result = co_await fileSource.fillBuffer(buf);
            co_await fileSource.close(ioc);
            co_return result;
        },
        asio::use_future);

    ioc.run();

    auto sourceResult = future.get();

    EXPECT_TRUE(std::holds_alternative<Sources::Source::EoS>(sourceResult));

    const std::string expected = "0\n1\n2\n3\n4\n5\n6\n7\n8\n9";
    const std::string actual = std::string{buf.getBuffer<const char>(), expected.size()};

    EXPECT_EQ(expected, actual);
}


TEST_F(FileSourceTest, FileNotPresent)
{
    auto config = FileSourceTestConfig{fmt::format("{}/missingFile.csv", TEST_DATA_DIR)};

    Sources::FileSource fileSource{
        Sources::SourceDescriptor{config.schema, "fileSource", "File", config.parserConfig, {{"filePath", config.filePath}}}};

    try
    {
        auto future = asio::co_spawn(
            ioc,
            [&]() -> asio::awaitable<void> { co_await fileSource.open(ioc); },
            asio::use_future); // Use future to capture any exceptions

        ioc.run();
        future.get(); // Will throw if the coroutine throws
    }
    catch (const Exception& e)
    {
        EXPECT_EQ(e.code(), ErrorCode::CannotOpenSource);
    }
}


TEST_F(FileSourceTest, ReadIntoTwoBuffers)
{
    auto config = FileSourceTestConfig{fmt::format("{}/fileSourceTestTwoBuffers.csv", TEST_DATA_DIR)};

    auto fileSource = Sources::FileSource{
        Sources::SourceDescriptor{config.schema, "fileSource", "File", config.parserConfig, {{"filePath", config.filePath}}}};


    std::promise<Sources::Source::InternalSourceResult> resultAfterFirstBuffer;
    std::promise<Sources::Source::InternalSourceResult> resultAfterSecondBuffer;
    auto buf1 = bufferManager->getBufferBlocking();
    auto buf2 = bufferManager->getBufferBlocking();
    asio::co_spawn(
        ioc,
        [&]() -> asio::awaitable<void>
        {
            co_await fileSource.open(ioc);
            resultAfterFirstBuffer.set_value(co_await fileSource.fillBuffer(buf1));
            resultAfterSecondBuffer.set_value(co_await fileSource.fillBuffer(buf2));
            co_await fileSource.close(ioc);
        },
        asio::detached);

    ioc.run();

    auto sourceResult1 = resultAfterFirstBuffer.get_future().get();

    EXPECT_TRUE(std::holds_alternative<Sources::Source::Continue>(sourceResult1));

    const std::string expected1 = "0\n1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n11\n12\n13\n";
    const std::string actual1 = std::string{buf1.getBuffer<const char>(), expected1.size()};

    EXPECT_EQ(expected1, actual1);

    auto sourceResult2 = resultAfterSecondBuffer.get_future().get();

    EXPECT_TRUE(std::holds_alternative<Sources::Source::EoS>(sourceResult2));

    const std::string expected2 = "14\n15";
    const std::string actual2 = std::string{buf2.getBuffer<const char>(), expected2.size()};

    EXPECT_EQ(expected2, actual2);
}
}
