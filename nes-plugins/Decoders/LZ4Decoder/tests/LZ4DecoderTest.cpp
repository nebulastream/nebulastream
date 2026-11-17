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
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <stop_token>
#include <string>
#include <utility>
#include <vector>

#include <Configurations/ConfigLiteral.hpp>
#include <Configurations/ConfigParsing.hpp>
#include <Decoders/Decoder.hpp>
#include <Decoders/DecoderCatalog.hpp>
#include <Decoders/DecoderChain.hpp>
#include <Decoders/DecoderStage.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <LZ4Decoder.hpp>
#include <lz4frame.h>

namespace NES
{
namespace
{
constexpr uint32_t BUFFER_SIZE = 4096;
constexpr size_t NUMBER_OF_BUFFERS = 2048;

/// Semi-compressible text, so frames hold several blocks and decoding spans many output buffers.
std::string csvLines(const size_t lines)
{
    std::string text;
    for (size_t i = 0; i < lines; ++i)
    {
        text += std::to_string(i) + "," + std::to_string(i * 7919 % 1000003) + ",sensor_" + std::to_string(i % 13) + "\n";
    }
    return text;
}

std::vector<std::byte> compress(const std::string& text, const bool checksums = false)
{
    LZ4F_preferences_t preferences{};
    preferences.frameInfo.blockSizeID = LZ4F_max64KB;
    preferences.frameInfo.contentChecksumFlag = checksums ? LZ4F_contentChecksumEnabled : LZ4F_noContentChecksum;
    preferences.frameInfo.blockChecksumFlag = checksums ? LZ4F_blockChecksumEnabled : LZ4F_noBlockChecksum;
    std::vector<std::byte> frame(LZ4F_compressFrameBound(text.size(), &preferences));
    const auto size = LZ4F_compressFrame(frame.data(), frame.size(), text.data(), text.size(), &preferences);
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

class LZ4DecoderTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("LZ4DecoderTest.log", LogLevel::LOG_DEBUG); }

protected:
    std::shared_ptr<BufferManager> bufferManager = BufferManager::create(
        NUMBER_OF_BUFFERS * BUFFER_SIZE * 2, 0.5, BufferAlignment{64}, BUFFER_SIZE, std::make_shared<NesDefaultMemoryAllocator>());
    DecoderStage::BufferProvider provideBuffer = [this] { return bufferManager->getBufferWithTimeout(std::chrono::seconds(1)); };

    /// Feeds input to the stage in chunks of chunkSize bytes, then flushes; returns the decoded text.
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
};

TEST_F(LZ4DecoderTest, DecodesFrameFedInAnyChunking)
{
    const auto original = csvLines(20000); /// ~0.5 MB: several 64 KB blocks, >100 output buffers
    const auto frame = compress(original);
    for (const size_t chunkSize : {size_t{1}, size_t{7}, size_t{4096}, size_t{65536}, frame.size()})
    {
        LZ4Decoder stage{LZ4DecoderConfig{}};
        EXPECT_EQ(decodeInChunks(stage, frame, chunkSize), original) << "chunk size " << chunkSize;
    }
}

TEST_F(LZ4DecoderTest, FillsOutputBuffersCompletelyExceptPerInput)
{
    const auto original = csvLines(20000);
    const auto frame = compress(original);
    LZ4Decoder stage{LZ4DecoderConfig{}};
    std::vector<TupleBuffer> output;
    stage.process(frame, provideBuffer, output);
    ASSERT_GT(output.size(), 1U);
    for (size_t i = 0; i + 1 < output.size(); ++i)
    {
        EXPECT_EQ(output[i].getNumberOfTuples(), BUFFER_SIZE) << "buffer " << i;
    }
    EXPECT_EQ(text(output), original);
}

TEST_F(LZ4DecoderTest, DecodesConcatenatedFrames)
{
    const auto first = csvLines(3000);
    const auto second = csvLines(5000);
    auto stream = compress(first);
    const auto secondFrame = compress(second, /*checksums*/ true);
    stream.insert(stream.end(), secondFrame.begin(), secondFrame.end());
    LZ4Decoder stage{LZ4DecoderConfig{}};
    EXPECT_EQ(decodeInChunks(stage, stream, 1000), first + second);
}

TEST_F(LZ4DecoderTest, EmptyInputProducesNothing)
{
    LZ4Decoder stage{LZ4DecoderConfig{}};
    std::vector<TupleBuffer> output;
    stage.process({}, provideBuffer, output);
    stage.flush(provideBuffer, output);
    EXPECT_TRUE(output.empty());
}

TEST_F(LZ4DecoderTest, TruncatedStreamFailsOnFlush)
{
    const auto frame = compress(csvLines(5000));
    LZ4Decoder stage{LZ4DecoderConfig{}};
    std::vector<TupleBuffer> output;
    stage.process(std::span{frame}.first(frame.size() / 2), provideBuffer, output);
    EXPECT_ANY_THROW(stage.flush(provideBuffer, output));
}

TEST_F(LZ4DecoderTest, CorruptInputFails)
{
    auto frame = compress(csvLines(5000), /*checksums*/ true);
    frame[frame.size() / 2] ^= std::byte{0xFF};
    LZ4Decoder stage{LZ4DecoderConfig{}};
    EXPECT_ANY_THROW(decodeInChunks(stage, frame, 4096));
}

TEST_F(LZ4DecoderTest, NotLZ4Fails)
{
    const std::string notCompressed = "1,2,3\n4,5,6\n";
    LZ4Decoder stage{LZ4DecoderConfig{}};
    EXPECT_ANY_THROW(decodeInChunks(stage, std::as_bytes(std::span{notCompressed}), 64));
}

/// The configured path: CREATE DECODER d AS (LZ4) -> catalog -> stage descriptors -> Decoder, as the source thread runs it.
TEST_F(LZ4DecoderTest, RunsAsNamedDecoder)
{
    DecoderCatalog catalog;
    const auto created = catalog.addDecoder(Identifier::parse("csv_lz4"), {Identifier::parse("lz4")}, {});
    ASSERT_TRUE(created.has_value()) << created.error();
    const auto named = catalog.getDecoder(Identifier::parse("CSV_LZ4"));
    ASSERT_TRUE(named.has_value());
    ASSERT_EQ(named->stages.size(), 1U);
    const auto decoder = DecoderChain::createDecoder(named->stages);
    ASSERT_NE(decoder, nullptr);

    const auto original = csvLines(10000);
    const auto frame = compress(original);
    const std::stop_source stopSource;
    std::vector<TupleBuffer> output;
    for (size_t offset = 0; offset < frame.size(); offset += BUFFER_SIZE)
    {
        for (auto& buffer : decoder->decode(
                 std::span{frame}.subspan(offset, std::min<size_t>(BUFFER_SIZE, frame.size() - offset)),
                 *bufferManager,
                 stopSource.get_token()))
        {
            output.push_back(std::move(buffer));
        }
    }
    for (auto& buffer : decoder->flush(*bufferManager, stopSource.get_token()))
    {
        output.push_back(std::move(buffer));
    }
    EXPECT_EQ(text(output), original);
}

TEST_F(LZ4DecoderTest, DecoderCatalogValidatesStagesAndOptions)
{
    DecoderCatalog catalog;
    const auto lz4 = Identifier::parse("LZ4");
    EXPECT_FALSE(catalog.addDecoder(Identifier::parse("empty"), {}, {}).has_value()) << "at least one stage";
    EXPECT_FALSE(catalog.addDecoder(Identifier::parse("twice"), {lz4, lz4}, {}).has_value()) << "a stage may appear only once";
    EXPECT_FALSE(catalog.addDecoder(Identifier::parse("unknown"), {Identifier::parse("NOT_A_DECODER")}, {}).has_value());
    EXPECT_FALSE(catalog
                     .addDecoder(
                         Identifier::parse("bad_option"),
                         {lz4},
                         createConfigLiteralSchema({LiteralConfigValue{QualifiedIdentifier::parse("LZ4.NO_SUCH_OPTION"), int64_t{1}}}))
                     .has_value())
        << "options must belong to a listed stage";
    EXPECT_TRUE(catalog.getAllDecoders().empty()) << "failed creations must not register anything";

    ASSERT_TRUE(catalog.addDecoder(Identifier::parse("ok"), {lz4}, {}).has_value());
    EXPECT_FALSE(catalog.addDecoder(Identifier::parse("ok"), {lz4}, {}).has_value()) << "names are unique";
    EXPECT_TRUE(catalog.removeDecoder(Identifier::parse("ok")));
    EXPECT_FALSE(catalog.removeDecoder(Identifier::parse("ok")));
}

}
}
