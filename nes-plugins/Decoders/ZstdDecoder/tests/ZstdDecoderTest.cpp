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


#include <algorithm>
#include <chrono>
#include <cstddef>
#include <memory>
#include <span>
#include <stop_token>
#include <string>
#include <utility>
#include <vector>

#include <zstd.h>
#include <Decoders/Decoder.hpp>
#include <Decoders/DecoderCatalog.hpp>
#include <Decoders/DecoderChain.hpp>
#include <Decoders/DecoderStage.hpp>
#include <Identifiers/Identifier.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ZstdDecoder.hpp>
#include <lz4frame.h>

namespace NES
{
namespace
{
constexpr uint32_t BUFFER_SIZE = 4096;
constexpr size_t NUMBER_OF_BUFFERS = 4096;

std::string csvLines(const size_t lines)
{
    std::string text;
    for (size_t i = 0; i < lines; ++i)
    {
        text += std::to_string(i) + "," + std::to_string(i * 7919 % 1000003) + ",sensor_" + std::to_string(i % 13) + "\n";
    }
    return text;
}

std::vector<std::byte> zstdCompress(const std::span<const std::byte> data)
{
    std::vector<std::byte> frame(ZSTD_compressBound(data.size()));
    const auto size = ZSTD_compress(frame.data(), frame.size(), data.data(), data.size(), 3);
    EXPECT_FALSE(ZSTD_isError(size)) << ZSTD_getErrorName(size);
    frame.resize(size);
    return frame;
}

std::vector<std::byte> lz4Compress(const std::span<const std::byte> data)
{
    LZ4F_preferences_t preferences{};
    preferences.frameInfo.blockSizeID = LZ4F_max64KB;
    std::vector<std::byte> frame(LZ4F_compressFrameBound(data.size(), &preferences));
    const auto size = LZ4F_compressFrame(frame.data(), frame.size(), data.data(), data.size(), &preferences);
    EXPECT_FALSE(LZ4F_isError(size)) << LZ4F_getErrorName(size);
    frame.resize(size);
    return frame;
}

std::string text(const std::vector<TupleBuffer>& buffers)
{
    std::string result;
    for (const auto& buffer : buffers)
    {
        const auto bytes = buffer.getAvailableMemoryArea<char>();
        EXPECT_LE(buffer.getNumberOfTuples(), bytes.size());
        result.append(bytes.data(), buffer.getNumberOfTuples());
    }
    return result;
}

class ZstdDecoderTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("ZstdDecoderTest.log", LogLevel::LOG_DEBUG); }

protected:
    std::shared_ptr<BufferManager> bufferManager = BufferManager::create(
        NUMBER_OF_BUFFERS * BUFFER_SIZE * 2, 0.5, BufferAlignment{64}, BUFFER_SIZE, std::make_shared<NesDefaultMemoryAllocator>());
    DecoderStage::BufferProvider provideBuffer = [this] { return bufferManager->getBufferWithTimeout(std::chrono::seconds(1)); };

    std::string decodeInChunks(DecoderStage& stage, const std::span<const std::byte> input, const size_t chunkSize)
    {
        std::vector<TupleBuffer> output;
        for (size_t offset = 0; offset < input.size(); offset += chunkSize)
        {
            stage.process(input.subspan(offset, std::min(chunkSize, input.size() - offset)), provideBuffer, output);
        }
        stage.flush(provideBuffer, output);
        return text(output);
    }

    /// Runs a configured chain as the source thread does: fixed-size reads, then flush.
    std::string decodeWithChain(const std::vector<Identifier>& stages, const std::span<const std::byte> input)
    {
        DecoderCatalog catalog;
        const auto created = catalog.addDecoder(Identifier::parse("chain"), stages, {});
        EXPECT_TRUE(created.has_value()) << created.error();
        const auto decoder = DecoderChain::createDecoder(catalog.getDecoder(Identifier::parse("chain"))->stages);
        EXPECT_NE(decoder, nullptr);
        const std::stop_source stopSource;
        std::vector<TupleBuffer> output;
        for (size_t offset = 0; offset < input.size(); offset += BUFFER_SIZE)
        {
            for (auto& buffer : decoder->decode(
                     input.subspan(offset, std::min<size_t>(BUFFER_SIZE, input.size() - offset)), *bufferManager, stopSource.get_token()))
            {
                output.push_back(std::move(buffer));
            }
        }
        for (auto& buffer : decoder->flush(*bufferManager, stopSource.get_token()))
        {
            output.push_back(std::move(buffer));
        }
        return text(output);
    }
};

TEST_F(ZstdDecoderTest, DecodesFrameFedInAnyChunking)
{
    const auto original = csvLines(20000);
    const auto frame = zstdCompress(std::as_bytes(std::span{original}));
    for (const size_t chunkSize : {size_t{1}, size_t{7}, size_t{4096}, size_t{65536}, frame.size()})
    {
        ZstdDecoder stage{ZstdDecoderConfig{}};
        EXPECT_EQ(decodeInChunks(stage, frame, chunkSize), original) << "chunk size " << chunkSize;
    }
}

TEST_F(ZstdDecoderTest, DecodesConcatenatedFrames)
{
    const auto first = csvLines(3000);
    const auto second = csvLines(5000);
    auto stream = zstdCompress(std::as_bytes(std::span{first}));
    const auto secondFrame = zstdCompress(std::as_bytes(std::span{second}));
    stream.insert(stream.end(), secondFrame.begin(), secondFrame.end());
    ZstdDecoder stage{ZstdDecoderConfig{}};
    EXPECT_EQ(decodeInChunks(stage, stream, 1000), first + second);
}

TEST_F(ZstdDecoderTest, TruncatedStreamFailsOnFlush)
{
    const auto original = csvLines(5000);
    const auto frame = zstdCompress(std::as_bytes(std::span{original}));
    ZstdDecoder stage{ZstdDecoderConfig{}};
    std::vector<TupleBuffer> output;
    stage.process(std::span{frame}.first(frame.size() / 2), provideBuffer, output);
    EXPECT_ANY_THROW(stage.flush(provideBuffer, output));
}

TEST_F(ZstdDecoderTest, NotZstdFails)
{
    const std::string notCompressed = "1,2,3\n4,5,6\n";
    ZstdDecoder stage{ZstdDecoderConfig{}};
    EXPECT_ANY_THROW(decodeInChunks(stage, std::as_bytes(std::span{notCompressed}), 64));
}

/// Data compressed with Zstd, then with LZ4: the chain undoes it in reverse order (LZ4, then ZSTD).
TEST_F(ZstdDecoderTest, ChainLZ4ThenZstd)
{
    const auto original = csvLines(50000);
    const auto zstd = zstdCompress(std::as_bytes(std::span{original}));
    const auto zstdThenLz4 = lz4Compress(zstd);
    EXPECT_EQ(decodeWithChain({Identifier::parse("LZ4"), Identifier::parse("ZSTD")}, zstdThenLz4), original);
}

/// The reverse encoding order needs the reverse chain; the wrong order fails instead of producing garbage.
TEST_F(ZstdDecoderTest, ChainZstdThenLZ4AndWrongOrderFails)
{
    const auto original = csvLines(50000);
    const auto lz4 = lz4Compress(std::as_bytes(std::span{original}));
    const auto lz4ThenZstd = zstdCompress(lz4);
    EXPECT_EQ(decodeWithChain({Identifier::parse("ZSTD"), Identifier::parse("LZ4")}, lz4ThenZstd), original);
    EXPECT_ANY_THROW(decodeWithChain({Identifier::parse("LZ4"), Identifier::parse("ZSTD")}, lz4ThenZstd));
}

}
}
