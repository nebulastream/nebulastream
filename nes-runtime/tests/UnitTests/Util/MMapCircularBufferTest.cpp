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
#include <Common/ExecutableType/Array.hpp>
#include <Runtime/BufferManager.hpp>
#include <Util/MMapCircularBuffer.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <numeric>
namespace NES {
using namespace std::literals;
class MMapCircularBufferTest : public Testing::BaseUnitTest {
  public:
    size_t pageSize = getpagesize();
    static void SetUpTestCase() {
        Logger::setupLogging("MMapCircularBufferTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup MMapCircularBufferTest test class.");
    }
    void SetUp() override { Testing::BaseUnitTest::SetUp(); }
};

TEST_F(MMapCircularBufferTest, TestConstruction) {
    MMapCircularBuffer buffer(pageSize);
    EXPECT_EQ(buffer.capacity(), pageSize);
    EXPECT_EQ(buffer.size(), 0);
    EXPECT_TRUE(buffer.empty());
    EXPECT_FALSE(buffer.full());
}

TEST_F(MMapCircularBufferTest, TestWrite) {
    MMapCircularBuffer buffer(pageSize);

    {
        auto writer = buffer.write();
        EXPECT_EQ(writer.size(), pageSize);
        EXPECT_EQ(SPAN_TYPE<char>(writer).size(), pageSize);

        std::memcpy(writer.data(), "SOME_DATA_HERE", 14);
        writer.consume(14);

        EXPECT_EQ(writer.size(), pageSize - 14);
        EXPECT_EQ(SPAN_TYPE<char>(writer).size(), pageSize - 14);
        EXPECT_EQ(buffer.size(), 0);
        EXPECT_EQ(buffer.capacity(), pageSize);
    }

    EXPECT_EQ(buffer.size(), 14);
    EXPECT_EQ(buffer.capacity(), pageSize);
    EXPECT_FALSE(buffer.empty());
    EXPECT_FALSE(buffer.full());
}

TEST_F(MMapCircularBufferTest, TestRead) {
    MMapCircularBuffer buffer(pageSize);
    {
        auto writer = buffer.write();
        std::memcpy(writer.data(), "SOME_DATA_HERE", 14);
        writer.consume(14);
    }

    {
        auto reader = buffer.read();
        EXPECT_EQ(reader.size(), 14);
        auto data = std::string_view(SPAN_TYPE<const char>(reader).data(), SPAN_TYPE<const char>(reader).size());
        EXPECT_EQ(data.size(), 14);
        EXPECT_EQ(data, "SOME_DATA_HERE"sv);
        reader.consume(14);
    }

    EXPECT_EQ(buffer.size(), 0);
    EXPECT_EQ(buffer.capacity(), pageSize);
    EXPECT_TRUE(buffer.empty());
    EXPECT_FALSE(buffer.full());
}

TEST_F(MMapCircularBufferTest, TestWrapAround) {
    MMapCircularBuffer buffer(pageSize);
    {
        // write pagesize - 2 bytes
        auto writer = buffer.write();
        std::vector<char> data(pageSize - 2);
        std::iota(data.begin(), data.end(), 0);
        std::memcpy(writer.data(), data.data(), data.size());
        writer.consume(data.size());
    }

    {
        // read and consume 100 bytes
        auto reader = buffer.read();
        auto data = std::string_view(SPAN_TYPE<const char>(reader).data(), SPAN_TYPE<const char>(reader).size());
        EXPECT_EQ(data[0], 0);
        reader.consume(100);
    }

    {
        // write 100 bytes which causes a wrap around
        auto writer = buffer.write();
        std::vector<char> data(100);
        std::iota(data.begin(), data.end(), 0);
        std::memcpy(writer.data(), data.data(), data.size());
        writer.consume(data.size());
    }

    {
        auto reader = buffer.read();
        auto data = std::string_view(SPAN_TYPE<const char>(reader).data(), SPAN_TYPE<const char>(reader).size());
        EXPECT_EQ(data[0], 100);
        reader.consume(pageSize - 2 - 100);

        data = std::string_view(SPAN_TYPE<const char>(reader).data(), SPAN_TYPE<const char>(reader).size());
        EXPECT_EQ(data[0], 0);
    }

    EXPECT_EQ(buffer.size(), 100);
    EXPECT_EQ(buffer.capacity(), pageSize);
    EXPECT_FALSE(buffer.empty());
    EXPECT_FALSE(buffer.full());
}
}// namespace NES