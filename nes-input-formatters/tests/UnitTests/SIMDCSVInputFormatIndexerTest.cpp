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

#include <cstdint>
#include <random>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatterTestUtil.hpp>
#include <InputFormatterValidationProvider.hpp>
#include <SIMDCSVKernel.hpp>
#include <SIMDCSVScan.hpp>

/// NOLINTBEGIN(readability-magic-numbers)
namespace NES
{
namespace
{

/// Captures every call the SIMDCSVScan driver makes, so two kernels (or a kernel vs. the oracle) can
/// be compared byte-for-byte.
struct RecordingSink
{
    std::vector<uint32_t> offsets;
    uint64_t numFields = 0;
    bool noTupleDelimiters = false;
    bool marked = false;
    uint32_t firstTuple = 0;
    uint32_t lastTuple = 0;

    void startSetup(const uint64_t fields, size_t /*delimiterSize*/)
    {
        offsets.clear();
        numFields = fields;
        noTupleDelimiters = false;
        marked = false;
        firstTuple = 0;
        lastTuple = 0;
    }

    void emplaceFieldOffset(const uint32_t offset) { offsets.push_back(offset); }

    void markNoTupleDelimiters() { noTupleDelimiters = true; }

    void markWithTupleDelimiters(const uint32_t first, const uint32_t last)
    {
        marked = true;
        firstTuple = first;
        lastTuple = last;
    }

    bool operator==(const RecordingSink&) const = default;
};

/// Independent, obviously-correct re-implementation of the scalar CSVInputFormatIndexer contract.
/// Used as the oracle the SIMD kernels must match exactly. Mirrors initializeIndexFunctionForTuple /
/// initializeIndexFunctionForTupleWithCommasInStrings from CSVInputFormatIndexer.cpp.
RecordingSink
oracle(const std::string_view view, const char fieldDelim, const char tupleDelim, const bool quoteAware, const uint64_t numFields)
{
    RecordingSink sink;
    sink.startSetup(numFields, 1);

    const auto firstNewline = view.find(tupleDelim);
    if (firstNewline == std::string_view::npos)
    {
        sink.markNoTupleDelimiters();
        return sink;
    }

    const auto first = static_cast<uint32_t>(firstNewline);
    auto last = first;
    auto startIdx = firstNewline + 1;
    auto end = view.find(tupleDelim, startIdx);
    while (end != std::string_view::npos)
    {
        const auto tuple = view.substr(startIdx, end - startIdx);
        sink.emplaceFieldOffset(static_cast<uint32_t>(startIdx));
        uint64_t fieldCount = 1;
        bool quoted = false;
        for (size_t i = 0; i < tuple.size(); ++i)
        {
            const char byte = tuple[i];
            if (!quoteAware)
            {
                if (byte == fieldDelim)
                {
                    sink.emplaceFieldOffset(static_cast<uint32_t>(startIdx + i + 1));
                    ++fieldCount;
                }
                continue;
            }
            if (!quoted && byte == fieldDelim)
            {
                sink.emplaceFieldOffset(static_cast<uint32_t>(startIdx + i + 1));
                ++fieldCount;
            }
            quoted = quoted != (byte == '"');
        }
        sink.emplaceFieldOffset(static_cast<uint32_t>(startIdx + tuple.size()));
        if (fieldCount != numFields)
        {
            throw CannotFormatSourceData("oracle field count mismatch (parsed {} vs {})", fieldCount, numFields);
        }
        last = static_cast<uint32_t>(end);
        startIdx = end + 1;
        end = view.find(tupleDelim, startIdx);
    }
    sink.markWithTupleDelimiters(first, last);
    return sink;
}

struct KernelOutcome
{
    RecordingSink sink;
    bool threw = false;
    bool operator==(const KernelOutcome&) const = default;
};

KernelOutcome runDriver(
    const SimdCsv::ComputeBlocksFn fn,
    const std::string_view view,
    const char fieldDelim,
    const char tupleDelim,
    const bool quoteAware,
    const uint64_t numFields)
{
    KernelOutcome outcome;
    try
    {
        SimdCsv::indexInto(outcome.sink, view, fieldDelim, tupleDelim, quoteAware, numFields, fn);
    }
    catch (const Exception&)
    {
        outcome.threw = true;
    }
    return outcome;
}

KernelOutcome
runOracle(const std::string_view view, const char fieldDelim, const char tupleDelim, const bool quoteAware, const uint64_t numFields)
{
    KernelOutcome outcome;
    try
    {
        outcome.sink = oracle(view, fieldDelim, tupleDelim, quoteAware, numFields);
    }
    catch (const Exception&)
    {
        outcome.threw = true;
    }
    return outcome;
}

class SIMDCSVInputFormatIndexerTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase()
    {
        Logger::setupLogging("SIMDCSVInputFormatIndexerTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup SIMDCSVInputFormatIndexerTest test class.");
    }
};

}

/// At least the scalar fallback is always available; on a SIMD-capable build there are more.
TEST_F(SIMDCSVInputFormatIndexerTest, kernelsAreAvailable)
{
    const auto kernels = SimdCsv::availableKernels();
    ASSERT_FALSE(kernels.empty());
    EXPECT_STREQ(kernels.front().name, "scalar");
}

/// Hand-checked emission: only the tuples strictly between newlines are indexed. The leading row
/// before the first newline and the trailing bytes after the last newline are spanning fragments.
TEST_F(SIMDCSVInputFormatIndexerTest, handCheckedEmissionMatchesContract)
{
    for (const auto& kernel : SimdCsv::availableKernels())
    {
        /// "1,2\n3,4\n": leading "1,2" skipped, one indexed tuple "3,4" at [4,6,7], delimiters at 3 and 7.
        const auto outcome = runDriver(kernel.fn, "1,2\n3,4\n", ',', '\n', true, 2);
        EXPECT_FALSE(outcome.threw) << kernel.name;
        EXPECT_TRUE(outcome.sink.marked) << kernel.name;
        EXPECT_EQ(outcome.sink.firstTuple, 3U) << kernel.name;
        EXPECT_EQ(outcome.sink.lastTuple, 7U) << kernel.name;
        EXPECT_EQ(outcome.sink.offsets, (std::vector<uint32_t>{4, 6, 7})) << kernel.name;

        /// single newline => no complete tuple, first == last == the only newline.
        const auto single = runDriver(kernel.fn, "x\n", ',', '\n', true, 2);
        EXPECT_TRUE(single.sink.marked) << kernel.name;
        EXPECT_EQ(single.sink.firstTuple, 1U) << kernel.name;
        EXPECT_EQ(single.sink.lastTuple, 1U) << kernel.name;
        EXPECT_TRUE(single.sink.offsets.empty()) << kernel.name;

        /// no newline at all => buffer is one spanning fragment.
        const auto none = runDriver(kernel.fn, "no-delimiter-here", ',', '\n', true, 2);
        EXPECT_TRUE(none.sink.noTupleDelimiters) << kernel.name;
        EXPECT_FALSE(none.sink.marked) << kernel.name;
    }
}

/// Commas inside quotes are not field separators (parity with allow_commas_in_strings), and quote
/// state does not bleed across rows.
TEST_F(SIMDCSVInputFormatIndexerTest, commasInsideQuotesAreMasked)
{
    /// header is the skipped leading fragment; the two indexed rows each have 3 fields, the quoted
    /// comma in row 1 must not split a field.
    const std::string_view view = "h\na,\"b,c\",d\nx,y,z\n";
    for (const auto& kernel : SimdCsv::availableKernels())
    {
        const auto quoteAware = runDriver(kernel.fn, view, ',', '\n', true, 3);
        const auto expected = runOracle(view, ',', '\n', true, 3);
        EXPECT_EQ(quoteAware, expected) << kernel.name;
        EXPECT_FALSE(quoteAware.threw) << kernel.name;

        /// With quote handling disabled the quoted comma splits the field, so row 1 has 4 fields and
        /// the count check fails (just like the scalar indexer).
        const auto notQuoteAware = runDriver(kernel.fn, view, ',', '\n', false, 3);
        EXPECT_TRUE(notQuoteAware.threw) << kernel.name;
    }
}

/// Every compiled+runnable kernel must produce exactly the same result as the scalar oracle on a wide
/// range of randomized, well-formed inputs (including inputs far larger than one 64-byte SIMD block
/// and every possible tail length).
TEST_F(SIMDCSVInputFormatIndexerTest, differentialFuzzAgainstOracle)
{
    const auto kernels = SimdCsv::availableKernels();
    std::mt19937 rng(0xC5'71'9A'2DU);
    constexpr int iterations = 4000;
    for (int iter = 0; iter < iterations; ++iter)
    {
        const uint64_t numFields = 1 + (rng() % 6);
        const bool quoteAware = (rng() % 2) == 0;
        const auto rows = rng() % 60;

        std::string csv;
        for (unsigned r = 0; r < rows; ++r)
        {
            for (uint64_t f = 0; f < numFields; ++f)
            {
                if (f != 0)
                {
                    csv += ',';
                }
                const auto len = rng() % 6;
                if (quoteAware && (rng() % 4) == 0)
                {
                    csv += '"';
                    static constexpr std::string_view quotedAlphabet = "ab,cd"; /// includes a comma
                    for (unsigned k = 0; k < len; ++k)
                    {
                        csv += quotedAlphabet[rng() % quotedAlphabet.size()];
                    }
                    csv += '"';
                }
                else
                {
                    for (unsigned k = 0; k < len; ++k)
                    {
                        csv += static_cast<char>('a' + (rng() % 5));
                    }
                }
            }
            csv += '\n';
        }
        /// Optionally append an unterminated trailing row to exercise the trailing-fragment path and a
        /// non-trivial scalar tail.
        if ((rng() % 2) == 0)
        {
            for (uint64_t f = 0; f < numFields; ++f)
            {
                if (f != 0)
                {
                    csv += ',';
                }
                csv += static_cast<char>('a' + (rng() % 5));
            }
        }

        const auto expected = runOracle(csv, ',', '\n', quoteAware, numFields);
        for (const auto& kernel : kernels)
        {
            const auto actual = runDriver(kernel.fn, csv, ',', '\n', quoteAware, numFields);
            ASSERT_EQ(actual, expected) << "kernel=" << kernel.name << " iter=" << iter << " quoteAware=" << quoteAware
                                        << " numFields=" << numFields << " size=" << csv.size();
        }
    }
}

/// The no-quote kernel specialization (`selectComputeBlocks(false)`, installed when the indexer runs
/// with allow_commas_in_strings=false) skips the quote SIMD compare and leaves the quote mask zero. On
/// quote-free data it must emit EXACTLY the offsets the scalar oracle and the quote-aware kernel produce
/// when driven with quoteAware=false. Fuzzed over sizes spanning many 64-byte SIMD blocks + every tail.
TEST_F(SIMDCSVInputFormatIndexerTest, noQuoteKernelMatchesOracleOnQuoteFreeData)
{
    const auto noQuoteFn = SimdCsv::selectComputeBlocks(false);
    const auto quoteAwareFn = SimdCsv::selectComputeBlocks(true);
    std::mt19937 rng(0x9E'37'79'B1U);
    constexpr int iterations = 4000;
    for (int iter = 0; iter < iterations; ++iter)
    {
        const uint64_t numFields = 1 + (rng() % 6);
        const auto rows = rng() % 60;
        std::string csv;
        for (unsigned r = 0; r < rows; ++r)
        {
            for (uint64_t f = 0; f < numFields; ++f)
            {
                if (f != 0)
                {
                    csv += ',';
                }
                const auto len = rng() % 6; /// includes 0 (empty field) to hit adjacent delimiters
                for (unsigned k = 0; k < len; ++k)
                {
                    csv += static_cast<char>('a' + (rng() % 5)); /// quote-free alphabet
                }
            }
            csv += '\n';
        }
        const auto expected = runOracle(csv, ',', '\n', false, numFields);
        const auto noQuote = runDriver(noQuoteFn, csv, ',', '\n', false, numFields);
        const auto quoteAware = runDriver(quoteAwareFn, csv, ',', '\n', false, numFields);
        ASSERT_EQ(noQuote, expected) << "no-quote kernel vs oracle iter=" << iter << " numFields=" << numFields << " size=" << csv.size();
        ASSERT_EQ(noQuote, quoteAware) << "no-quote vs quote-aware kernel iter=" << iter << " size=" << csv.size();
    }
}

/// End-to-end through the real InputFormatIndexer registry + InputFormatter pipeline: selecting
/// "SIMDCSV" must produce the same tuples the scalar "CSV" indexer would. Mirrors SpecificSequenceTest.
TEST_F(SIMDCSVInputFormatIndexerTest, pipelineOneTupleAcrossTwoBuffers)
{
    using namespace InputFormatterTestUtil;
    using enum TestDataTypes;
    using TestTuple = std::tuple<int32_t, int32_t>;
    runTest<TestTuple>(TestConfig<TestTuple>{
        .numRequiredBuffers = 3,
        .sizeOfRawBuffers = 16,
        .sizeOfFormattedBuffers = 20,
        .parserConfig
        = InputFormatterValidationProvider::provide("SIMDCSV", {{"type", "SIMDCSV"}, {"tuple_delimiter", "\n"}, {"field_delimiter", ","}})
              .value(),
        .testSchema = {INT32, INT32},
        .memoryLayoutType = MemoryLayoutType::ROW_LAYOUT,
        .expectedResults = {WorkerThreadResults<TestTuple>{{{TestTuple(123456789, 123456789)}}}},
        .rawBytesPerThread
        = {{.sequenceNumber = SequenceNumber(1), .rawBytes = "123456789,123456"},
           {.sequenceNumber = SequenceNumber(2), .rawBytes = "789\n"}}});
}

TEST_F(SIMDCSVInputFormatIndexerTest, pipelineTwoFullTuplesInTwoBuffers)
{
    using namespace InputFormatterTestUtil;
    using enum TestDataTypes;
    using TestTuple = std::tuple<int32_t, int32_t>;
    runTest<TestTuple>(TestConfig<TestTuple>{
        .numRequiredBuffers = 4,
        .sizeOfRawBuffers = 16,
        .sizeOfFormattedBuffers = 20,
        .parserConfig
        = InputFormatterValidationProvider::provide("SIMDCSV", {{"type", "SIMDCSV"}, {"tuple_delimiter", "\n"}, {"field_delimiter", ","}})
              .value(),
        .testSchema = {INT32, INT32},
        .memoryLayoutType = MemoryLayoutType::ROW_LAYOUT,
        .expectedResults = {WorkerThreadResults<TestTuple>{{{TestTuple(123456789, 12345)}, {TestTuple{12345, 123456789}}}}},
        .rawBytesPerThread
        = {{.sequenceNumber = SequenceNumber(1), .rawBytes = "123456789,12345\n"},
           {.sequenceNumber = SequenceNumber(2), .rawBytes = "12345,123456789\n"}}});
}

}

/// NOLINTEND(readability-magic-numbers)
