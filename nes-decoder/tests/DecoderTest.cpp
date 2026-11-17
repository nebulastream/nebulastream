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

#include <chrono>
#include <cstddef>
#include <cstring>
#include <memory>
#include <optional>
#include <span>
#include <stop_token>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <Decoders/Decoder.hpp>
#include <Decoders/DecoderStage.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>

/// Chain mechanics of the Decoder, independent of any codec: stage order, n:m buffers, flush propagation.
namespace NES
{
namespace
{
constexpr uint32_t BUFFER_SIZE = 256;

void append(const std::string_view bytes, const DecoderStage::BufferProvider& provideBuffer, std::vector<TupleBuffer>& output)
{
    auto buffer = provideBuffer();
    ASSERT_TRUE(buffer.has_value());
    ASSERT_LE(bytes.size(), buffer->getBufferSize());
    std::memcpy(buffer->getAvailableMemoryArea<char>().data(), bytes.data(), bytes.size());
    buffer->setNumberOfTuples(bytes.size());
    output.push_back(std::move(*buffer));
}

std::string_view asText(const std::span<const std::byte> bytes)
{
    return {reinterpret_cast<const char*>(bytes.data()), bytes.size()};
}

/// n:m and held-back state: emits one buffer per complete line, keeps an incomplete line until it is
/// completed or the stream ends.
class LineStage final : public DecoderStage
{
public:
    void process(const std::span<const std::byte> input, const BufferProvider& provideBuffer, std::vector<TupleBuffer>& output) override
    {
        pending += asText(input);
        for (auto end = pending.find('\n'); end != std::string::npos; end = pending.find('\n'))
        {
            append(std::string_view{pending}.substr(0, end + 1), provideBuffer, output);
            pending.erase(0, end + 1);
        }
    }

    void flush(const BufferProvider& provideBuffer, std::vector<TupleBuffer>& output) override
    {
        if (!pending.empty())
        {
            append(pending, provideBuffer, output);
            pending.clear();
        }
    }

private:
    std::string pending;
};

/// 1:1 and stateless: prefixes every buffer it gets with '>'. Makes the chain order visible.
class TagStage final : public DecoderStage
{
public:
    void process(const std::span<const std::byte> input, const BufferProvider& provideBuffer, std::vector<TupleBuffer>& output) override
    {
        append(">" + std::string{asText(input)}, provideBuffer, output);
    }
};

class DecoderTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("DecoderTest.log", LogLevel::LOG_DEBUG); }

protected:
    std::shared_ptr<BufferManager> bufferManager
        = BufferManager::create(1024 * BUFFER_SIZE, 0.5, BufferAlignment{64}, BUFFER_SIZE, std::make_shared<NesDefaultMemoryAllocator>());
    std::stop_source stopSource;

    /// Feeds the chunks through the decoder, flushes, and returns every emitted buffer as a string.
    std::vector<std::string> run(Decoder& decoder, const std::vector<std::string>& chunks)
    {
        std::vector<std::string> emitted;
        const auto collect = [&](std::vector<TupleBuffer> buffers)
        {
            for (const auto& buffer : buffers)
            {
                emitted.emplace_back(buffer.getAvailableMemoryArea<char>().data(), buffer.getNumberOfTuples());
            }
        };
        for (const auto& chunk : chunks)
        {
            collect(decoder.decode(std::as_bytes(std::span{chunk}), *bufferManager, stopSource.get_token()));
        }
        collect(decoder.flush(*bufferManager, stopSource.get_token()));
        return emitted;
    }

    static Decoder chain(std::unique_ptr<DecoderStage> first, std::unique_ptr<DecoderStage> second)
    {
        std::vector<std::unique_ptr<DecoderStage>> stages;
        stages.push_back(std::move(first));
        stages.push_back(std::move(second));
        return Decoder{std::move(stages)};
    }
};

TEST_F(DecoderTest, StageOrderDeterminesResult)
{
    const std::vector<std::string> chunks{"a\nb", "c\nd"};

    auto linesThenTag = chain(std::make_unique<LineStage>(), std::make_unique<TagStage>());
    EXPECT_EQ(run(linesThenTag, chunks), (std::vector<std::string>{">a\n", ">bc\n", ">d"}));

    auto tagThenLines = chain(std::make_unique<TagStage>(), std::make_unique<LineStage>());
    EXPECT_EQ(run(tagThenLines, chunks), (std::vector<std::string>{">a\n", "b>c\n", "d"}));
}

TEST_F(DecoderTest, StageMayEmitNothingForAnInput)
{
    auto decoder = chain(std::make_unique<LineStage>(), std::make_unique<TagStage>());
    EXPECT_TRUE(
        decoder.decode(std::as_bytes(std::span{std::string_view{"no newline yet"}}), *bufferManager, stopSource.get_token()).empty());
}

TEST_F(DecoderTest, FlushedDataPassesThroughLaterStages)
{
    /// The held-back "tail" of the first stage is flushed at end of stream and still gets tagged by the second.
    auto decoder = chain(std::make_unique<LineStage>(), std::make_unique<TagStage>());
    EXPECT_EQ(run(decoder, {"head\nta", "il"}), (std::vector<std::string>{">head\n", ">tail"}));
}

TEST_F(DecoderTest, FlushOrderIsFirstToLast)
{
    /// Both stages hold data back; the first stage's remainder must reach the second before the second flushes.
    auto decoder = chain(std::make_unique<LineStage>(), std::make_unique<LineStage>());
    EXPECT_EQ(run(decoder, {"x\ny"}), (std::vector<std::string>{"x\n", "y"}));
}

}
}
