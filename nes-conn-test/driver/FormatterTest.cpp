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
#include <cstddef>
#include <cstdint>
#include <string>
#include <tuple>
#include <vector>

#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <fmt/format.h>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <Formatter.hpp>
#include <InputFormatterTestUtil.hpp>

namespace NES
{

namespace
{
constexpr uint32_t defaultBufferSize = 4096;
constexpr uint32_t defaultBufferCount = 64;

std::size_t countTuples(const std::vector<TupleBuffer>& buffers)
{
    std::size_t total = 0;
    for (const auto& buffer : buffers)
    {
        total += buffer.getNumberOfTuples();
    }
    return total;
}

/// Stamp the single-chunk identity that `Formatter::encodeToText` now reads off the buffer
/// instead of setting itself — in production `SourceThread::addBufferMetaData` provides it.
/// The encode's JSON output formatter orders by sequence number, so buffers encoded in
/// succession must carry monotonic sequence numbers (mirroring SourceThread).
void stampForEncode(TupleBuffer& buffer, std::uint64_t sequenceNumber)
{
    buffer.setSequenceNumber(SequenceNumber(sequenceNumber));
    buffer.setChunkNumber(INITIAL_CHUNK_NUMBER);
    buffer.setLastChunk(true);
}
}

class FormatterTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("FormatterTest.log", LogLevel::LOG_DEBUG); }

    void SetUp() override { BaseUnitTest::SetUp(); }

    void TearDown() override { BaseUnitTest::TearDown(); }
};

/// native -> JSON -> native must reproduce the original tuples exactly. One schema
/// drives both directions, so the JSON keys line up; the Formatter owns all buffer
/// plumbing, so the test only hands it the native buffer and the resulting text.
TEST_F(FormatterTest, roundTripNativeToJsonToNative)
{
    using enum InputFormatterTestUtil::TestDataTypes;
    using TestTuple = std::tuple<int32_t, int64_t, double>;

    auto bufferManager = BufferManager::create(defaultBufferSize, defaultBufferCount);
    const auto schema = InputFormatterTestUtil::createSchema({INT32, INT64, FLOAT64});
    ConnTest::Formatter formatter(schema, bufferManager);

    const std::vector<TestTuple> tuples{TestTuple(1, 100, 1.5), TestTuple(2, 200, 2.5), TestTuple(3, 300, 4.25)};
    auto nativeIn = InputFormatterTestUtil::createTupleBufferFromTuples<TestTuple>(schema, *bufferManager, tuples);
    stampForEncode(nativeIn, SequenceNumber::INITIAL);

    /// native -> JSON
    const auto json = formatter.encodeToText(nativeIn);
    ASSERT_FALSE(json.empty());
    /// one JSONL object (terminated by '}\n') per input tuple
    EXPECT_EQ(std::ranges::count(json, '\n'), static_cast<std::ptrdiff_t>(tuples.size()));

    /// JSON -> native
    auto roundTripped = formatter.decode(json);
    ASSERT_EQ(countTuples(roundTripped), tuples.size());
    std::vector<TupleBuffer> expected{nativeIn};
    EXPECT_TRUE(InputFormatterTestUtil::compareTestTupleBuffersOrderSensitive(roundTripped, expected, schema));
}

/// The encode stage persists across calls, so the source can re-encode buffer after
/// buffer (each `encodeToText` only executes, never stops the stage). Decoding the
/// concatenated JSONL recovers them all. (`decode` is one-shot — it flushes the
/// input formatter's tail — which is exactly how the native sink uses it.)
TEST_F(FormatterTest, encodesBuffersInSuccession)
{
    using enum InputFormatterTestUtil::TestDataTypes;
    using TestTuple = std::tuple<int32_t, int64_t, double>;

    auto bufferManager = BufferManager::create(defaultBufferSize, defaultBufferCount);
    const auto schema = InputFormatterTestUtil::createSchema({INT32, INT64, FLOAT64});
    ConnTest::Formatter formatter(schema, bufferManager);

    const std::vector<TestTuple> tuplesA{TestTuple(1, 100, 1.5), TestTuple(2, 200, 2.5)};
    const std::vector<TestTuple> tuplesB{TestTuple(3, 300, 3.5), TestTuple(4, 400, 4.5)};
    auto nativeA = InputFormatterTestUtil::createTupleBufferFromTuples<TestTuple>(schema, *bufferManager, tuplesA);
    auto nativeB = InputFormatterTestUtil::createTupleBufferFromTuples<TestTuple>(schema, *bufferManager, tuplesB);
    stampForEncode(nativeA, SequenceNumber::INITIAL);
    stampForEncode(nativeB, SequenceNumber::INITIAL + 1);

    /// encode, twice in succession (the source's per-buffer pattern)
    const auto jsonA = formatter.encodeToText(nativeA);
    const auto jsonB = formatter.encodeToText(nativeB);
    ASSERT_FALSE(jsonA.empty());
    ASSERT_FALSE(jsonB.empty());

    auto out = formatter.decode(jsonA + jsonB);
    EXPECT_EQ(countTuples(out), tuplesA.size() + tuplesB.size());
}

/// The native-sink path: a whole JSONL blob decodes into several native row-layout
/// buffers when the pool's buffer size only fits a few tuples each. `decodeJsonl`
/// sizes the pool so the emit packs `buffer_size / row_width` tuples per buffer.
TEST_F(FormatterTest, decodesOneInputIntoMultipleNativeBuffers)
{
    using enum InputFormatterTestUtil::TestDataTypes;

    /// Row width is 20 bytes (INT32 + INT64 + FLOAT64); a 64-byte buffer fits 3 tuples.
    constexpr std::size_t smallBufferSize = 64;
    const auto schema = InputFormatterTestUtil::createSchema({INT32, INT64, FLOAT64});

    /// JSON keys are the canonical (bare, lowercased) field identifiers the Formatter
    /// speaks — `Field_0` from createSchema is lowercased to `field_0`.
    constexpr int recordCount = 7;
    /// Spread field_1 values so the rows are not all-identical.
    constexpr int field1Stride = 10;
    std::string jsonl;
    for (int i = 0; i < recordCount; ++i)
    {
        jsonl += fmt::format(
            R"({{"field_0":{},"field_1":{},"field_2":{}.5}})"
            "\n",
            i,
            i * field1Stride,
            i);
    }

    const auto decoded = ConnTest::Formatter::decodeJsonl(schema, smallBufferSize, jsonl);

    EXPECT_GT(decoded.buffers.size(), 1U); /// 7 tuples at 3 per buffer -> 3 native buffers
    EXPECT_EQ(countTuples(decoded.buffers), static_cast<std::size_t>(recordCount));
}

}
