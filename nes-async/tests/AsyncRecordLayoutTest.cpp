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
#include <cstring>
#include <memory>
#include <optional>
#include <span>
#include <string>

#include <Async/AsyncRecordLayout.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <gtest/gtest.h>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
constexpr uint32_t POOLED_BUFFER_SIZE = 4096;
constexpr uint32_t NUMBER_OF_POOLED_BUFFERS = 64;
constexpr BufferAlignment BUFFER_ALIGNMENT{64};
constexpr double UNPOOLED_MEMORY_FRACTION = 0.5;
constexpr size_t TOTAL_MEMORY_IN_BYTES = 10 * static_cast<size_t>(NUMBER_OF_POOLED_BUFFERS) * POOLED_BUFFER_SIZE;

std::shared_ptr<BufferManager> makeBufferManager()
{
    return BufferManager::create(
        TOTAL_MEMORY_IN_BYTES, UNPOOLED_MEMORY_FRACTION, BUFFER_ALIGNMENT, POOLED_BUFFER_SIZE, std::make_shared<NesDefaultMemoryAllocator>());
}

UnqualifiedUnboundField field(const std::string& name, const DataType::Type type)
{
    return UnqualifiedUnboundField{Identifier::parse(name), type};
}

/// reviewId UINT64, reviewText VARSIZED — the shape of the movie review stream.
Schema<UnqualifiedUnboundField, Ordered> reviewSchema()
{
    return Schema<UnqualifiedUnboundField, Ordered>{field("reviewId", DataType::Type::UINT64), field("reviewText", DataType::Type::VARSIZED)};
}

/// The same plus the column an operator appends.
Schema<UnqualifiedUnboundField, Ordered> reviewSchemaWithSentiment()
{
    return Schema<UnqualifiedUnboundField, Ordered>{
        field("reviewId", DataType::Type::UINT64), field("reviewText", DataType::Type::VARSIZED), field("sentiment", DataType::Type::VARSIZED)};
}

std::span<const std::byte> bytesOf(const uint64_t& value)
{
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    return {reinterpret_cast<const std::byte*>(&value), sizeof(value)};
}
}

class AsyncRecordLayoutTest : public ::testing::Test
{
};

TEST_F(AsyncRecordLayoutTest, ComputesRowOffsetsAsPrefixSums)
{
    const AsyncRecordLayout layout{Schema<UnqualifiedUnboundField, Ordered>{
        field("a", DataType::Type::UINT64), field("b", DataType::Type::VARSIZED), field("c", DataType::Type::FLOAT32)}};

    EXPECT_EQ(layout.fieldCount(), 3U);
    EXPECT_EQ(layout.offsetOf(0), 0U);
    /// UINT64 occupies 8 bytes, VARSIZED 16 (child index, offset and size).
    EXPECT_EQ(layout.offsetOf(1), 8U);
    EXPECT_EQ(layout.offsetOf(2), 24U);
    EXPECT_EQ(layout.tupleSize(), 28U);
    EXPECT_EQ(layout.capacity(280), 10U);
}

TEST_F(AsyncRecordLayoutTest, FindsFieldsByCanonicalName)
{
    const AsyncRecordLayout layout{reviewSchema()};

    /// Unquoted identifiers are upper-cased, so that is how fields are addressed.
    ASSERT_TRUE(layout.indexOf("REVIEWTEXT").has_value());
    EXPECT_EQ(layout.indexOf("REVIEWTEXT").value(), 1U);
    EXPECT_EQ(layout.nameOf(0), "REVIEWID");
    EXPECT_FALSE(layout.indexOf("does_not_exist").has_value());
}

TEST_F(AsyncRecordLayoutTest, RejectsNullableFields)
{
    const Schema<UnqualifiedUnboundField, Ordered> schema{UnqualifiedUnboundField{
        Identifier::parse("maybe"), DataType{DataType::Type::FLOAT32, DataType::NULLABLE::IS_NULLABLE}}};

    EXPECT_THROW(AsyncRecordLayout{schema}, Exception);
}

TEST_F(AsyncRecordLayoutTest, WritesAndReadsBackARecord)
{
    const auto bufferManager = makeBufferManager();
    const AsyncRecordLayout layout{reviewSchema()};
    auto buffer = bufferManager->getBufferBlocking();

    constexpr uint64_t reviewId = 1145982;
    const std::string reviewText = "Timed to be just long enough for most youngsters' brief attention spans.";

    AsyncRecordWriter writer{layout, buffer, *bufferManager, 0};
    writer.writeRaw(0, bytesOf(reviewId));
    writer.writeText(1, reviewText);

    const AsyncRecordView view{layout, buffer, 0};
    EXPECT_EQ(view.readAsText(0), "1145982");
    EXPECT_EQ(view.readText(1), reviewText);
}

TEST_F(AsyncRecordLayoutTest, RendersEveryFixedSizeTypeAsText)
{
    const auto bufferManager = makeBufferManager();
    const AsyncRecordLayout layout{Schema<UnqualifiedUnboundField, Ordered>{
        field("i", DataType::Type::INT32), field("u", DataType::Type::UINT8), field("f", DataType::Type::FLOAT64),
        field("b", DataType::Type::BOOLEAN)}};
    auto buffer = bufferManager->getBufferBlocking();

    constexpr int32_t signedValue = -42;
    constexpr uint8_t smallValue = 7;
    constexpr double floatingValue = 0.5;
    constexpr bool booleanValue = true;

    AsyncRecordWriter writer{layout, buffer, *bufferManager, 0};
    /// NOLINTBEGIN(cppcoreguidelines-pro-type-reinterpret-cast)
    writer.writeRaw(0, std::span{reinterpret_cast<const std::byte*>(&signedValue), sizeof(signedValue)});
    writer.writeRaw(1, std::span{reinterpret_cast<const std::byte*>(&smallValue), sizeof(smallValue)});
    writer.writeRaw(2, std::span{reinterpret_cast<const std::byte*>(&floatingValue), sizeof(floatingValue)});
    writer.writeRaw(3, std::span{reinterpret_cast<const std::byte*>(&booleanValue), sizeof(booleanValue)});
    /// NOLINTEND(cppcoreguidelines-pro-type-reinterpret-cast)

    const AsyncRecordView view{layout, buffer, 0};
    EXPECT_EQ(view.readAsText(0), "-42");
    EXPECT_EQ(view.readAsText(1), "7");
    EXPECT_EQ(view.readAsText(2), "0.5");
    EXPECT_EQ(view.readAsText(3), "true");
}

TEST_F(AsyncRecordLayoutTest, AddressesSeveralRecordsInOneBuffer)
{
    const auto bufferManager = makeBufferManager();
    const AsyncRecordLayout layout{reviewSchema()};
    auto buffer = bufferManager->getBufferBlocking();
    constexpr uint64_t recordCount = 5;

    for (uint64_t index = 0; index < recordCount; ++index)
    {
        const uint64_t id = 1000 + index;
        AsyncRecordWriter writer{layout, buffer, *bufferManager, index};
        writer.writeRaw(0, bytesOf(id));
        writer.writeText(1, "review number " + std::to_string(index));
    }
    buffer.setNumberOfTuples(recordCount);

    for (uint64_t index = 0; index < recordCount; ++index)
    {
        const AsyncRecordView view{layout, buffer, index};
        EXPECT_EQ(view.readAsText(0), std::to_string(1000 + index));
        EXPECT_EQ(view.readText(1), "review number " + std::to_string(index));
    }
}

TEST_F(AsyncRecordLayoutTest, CopiesMatchingFieldsAndAppendsANewOne)
{
    const auto bufferManager = makeBufferManager();
    const AsyncRecordLayout inputLayout{reviewSchema()};
    const AsyncRecordLayout outputLayout{reviewSchemaWithSentiment()};

    auto outputBuffer = bufferManager->getBufferBlocking();
    const std::string reviewText = "It doesn't matter if a movie costs 300 million or only 300 dollars.";

    {
        /// The input buffer only lives inside this scope. Copying must not leave the output
        /// pointing into its child buffers.
        auto inputBuffer = bufferManager->getBufferBlocking();
        constexpr uint64_t reviewId = 1636744;

        AsyncRecordWriter inputWriter{inputLayout, inputBuffer, *bufferManager, 0};
        inputWriter.writeRaw(0, bytesOf(reviewId));
        inputWriter.writeText(1, reviewText);

        const AsyncRecordView inputView{inputLayout, inputBuffer, 0};
        AsyncRecordWriter outputWriter{outputLayout, outputBuffer, *bufferManager, 0};
        outputWriter.copyMatchingFields(inputView);
        outputWriter.writeText(*outputLayout.indexOf("SENTIMENT"), "NEGATIVE");
    }

    const AsyncRecordView outputView{outputLayout, outputBuffer, 0};
    EXPECT_EQ(outputView.readAsText(0), "1636744");
    EXPECT_EQ(outputView.readText(1), reviewText);
    EXPECT_EQ(outputView.readText(2), "NEGATIVE");
}

TEST_F(AsyncRecordLayoutTest, CopySkipsFieldsTheTargetDoesNotHave)
{
    const auto bufferManager = makeBufferManager();
    const AsyncRecordLayout inputLayout{reviewSchemaWithSentiment()};
    /// Target keeps only the id — a projection-like case.
    const AsyncRecordLayout outputLayout{Schema<UnqualifiedUnboundField, Ordered>{field("reviewId", DataType::Type::UINT64)}};

    auto inputBuffer = bufferManager->getBufferBlocking();
    auto outputBuffer = bufferManager->getBufferBlocking();
    constexpr uint64_t reviewId = 2590987;

    AsyncRecordWriter inputWriter{inputLayout, inputBuffer, *bufferManager, 0};
    inputWriter.writeRaw(0, bytesOf(reviewId));
    inputWriter.writeText(1, "some review");
    inputWriter.writeText(2, "POSITIVE");

    AsyncRecordWriter outputWriter{outputLayout, outputBuffer, *bufferManager, 0};
    EXPECT_NO_THROW(outputWriter.copyMatchingFields(AsyncRecordView{inputLayout, inputBuffer, 0}));

    const AsyncRecordView outputView{outputLayout, outputBuffer, 0};
    EXPECT_EQ(outputView.readAsText(0), "2590987");
}

}
