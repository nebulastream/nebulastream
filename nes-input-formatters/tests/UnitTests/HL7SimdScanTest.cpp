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

#include <array>
#include <cstddef>
#include <cstdint>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <Hl7Scan.hpp>
#include <Hl7SimdKernel.hpp>

/// Differential test for the SIMDHL7 scan: every compiled+runnable kernel driven through
/// SimdHl7::flattenHl7SimdInto must -- after reconstructing the per-message group emission from the
/// flattened event band with the same fixed-stride math the Hl7FieldIndex read side uses -- produce
/// EXACTLY the emission of the production scalar walk (SimdHl7::indexHl7ScalarInto): same offsets,
/// same spanning-fragment marks, same in-order arity throws (including the scalar's
/// partial-emission-before-throw). The reconstruction in runSimd IS the read-side contract, so the
/// differential covers both the flatten and the stride addressing.
///
/// Both production delimiter modes run under the same harness (ScanConfig):
/// - HL7/MLLP (2-byte "\x1C\r" delimiter): the interesting corners are all delimiter-pair
///   placements relative to 64-byte SIMD blocks and the 16-block chunk -- "\x1C\r" straddling a
///   block boundary (the carry), the matched <CR>'s suppression from the structural mask across
///   that boundary, and a lone trailing <FS>.
/// - packed XML (1-byte '\n' delimiter, class {<,>}): no pair logic exists to go wrong, so the
///   sweeps instead exercise the delimiter byte AT the boundary and the delete-only flatten path.
/// NOLINTBEGIN(readability-magic-numbers)
namespace NES
{
namespace
{

/// One production scan shape: the structural class (as both the kernel's fixed 4-char array --
/// disabled slots duplicate an enabled char, mirroring HL7MetaData::getStructuralChars -- and the
/// scalar walk's byte string) plus the message delimiter.
struct ScanConfig
{
    std::array<char, 4> structuralChars;
    std::string_view structuralBytes;
    std::string_view messageDelimiter;
};

constexpr ScanConfig kHl7Config{.structuralChars = {'\r', '|', '^', '&'}, .structuralBytes = "\r|^&", .messageDelimiter = "\x1C\r"};
/// The packed-XML config of the XML plan: splitting at every '<' and '>' makes tags/close-tags
/// junk leaves and element values standalone leaves; records are '\n'-terminated lines.
constexpr ScanConfig kXmlConfig{.structuralChars = {'<', '>', '<', '<'}, .structuralBytes = "<>", .messageDelimiter = "\n"};

/// Captures every call the scan drivers make, so the SIMD path can be compared to the scalar walk.
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

struct Outcome
{
    RecordingSink sink;
    bool threw = false;
    bool operator==(const Outcome&) const = default;
};

Outcome runScalar(const ScanConfig& config, const std::string_view view, const uint64_t numFields)
{
    Outcome outcome;
    try
    {
        SimdHl7::indexHl7ScalarInto(outcome.sink, view, config.messageDelimiter, config.structuralBytes, numFields);
    }
    catch (const Exception&)
    {
        outcome.threw = true;
    }
    return outcome;
}

Outcome runSimd(const ScanConfig& config, const SimdHl7::ComputeBlocksFn fn, const std::string_view view, const uint64_t numFields)
{
    Outcome outcome;
    outcome.sink.startSetup(numFields, /*sizeOfFieldDelimiter*/ 1);

    const auto delimiterSize = config.messageDelimiter.size();
    std::vector<uint32_t> band(view.size() + 16);
    std::vector<uint32_t> pairBandIdx((view.size() / 2) + 2);
    /// 1-byte mode ignores msgSecond; pass the duplicate exactly like the production indexer does.
    const auto flattened = delimiterSize == 2
        ? SimdHl7::flattenHl7SimdInto<2>(
              band.data(), pairBandIdx.data(), view, config.structuralChars, config.messageDelimiter[0], config.messageDelimiter[1], fn)
        : SimdHl7::flattenHl7SimdInto<1>(
              band.data(), pairBandIdx.data(), view, config.structuralChars, config.messageDelimiter[0], config.messageDelimiter[0], fn);
    if (flattened.numPairs == 0)
    {
        outcome.sink.markNoTupleDelimiters();
        return outcome;
    }

    /// The production validator must reject exactly when the per-message reconstruction below does.
    bool productionValidatorThrew = false;
    try
    {
        SimdHl7::validateHl7MessageArity(pairBandIdx.data(), flattened.numPairs, numFields);
    }
    catch (const Exception&)
    {
        productionValidatorThrew = true;
    }

    /// Reconstruct the scalar walk's group emission from the band; message k spans delimiters k and
    /// k+1. The offset math here mirrors the Hl7FieldIndex read side (the first leaf skips the
    /// message delimiter's bytes, later leaves their 1-byte structural delimiter, the closing
    /// offset is the terminating delimiter's first byte).
    try
    {
        for (size_t k = 0; k + 1 < flattened.numPairs; ++k)
        {
            const size_t groupBegin = pairBandIdx[k];
            const size_t groupEnd = pairBandIdx[k + 1];
            outcome.sink.emplaceFieldOffset(band[groupBegin] + static_cast<uint32_t>(delimiterSize));
            for (size_t event = groupBegin + 1; event < groupEnd; ++event)
            {
                outcome.sink.emplaceFieldOffset(band[event] + 1);
            }
            outcome.sink.emplaceFieldOffset(band[groupEnd]);
            /// same in-order rejection as the scalar walk: the bad message's offsets are emitted first
            if (const size_t leafCount = groupEnd - groupBegin; leafCount != numFields)
            {
                SimdHl7::throwHl7ArityMismatch(leafCount, numFields);
            }
        }
    }
    catch (const Exception&)
    {
        outcome.threw = true;
    }
    EXPECT_EQ(productionValidatorThrew, outcome.threw);
    if (outcome.threw)
    {
        return outcome;
    }
    outcome.sink.markWithTupleDelimiters(band[pairBandIdx[0]], band[pairBandIdx[flattened.numPairs - 1]]);
    return outcome;
}

/// One well-formed message body with `leaves` leaves ("a|b|c" style, first leaf may be empty),
/// joined by random bytes from the config's structural class.
std::string makeMessage(std::mt19937& rng, const ScanConfig& config, const uint64_t leaves)
{
    std::string body;
    for (uint64_t leaf = 0; leaf < leaves; ++leaf)
    {
        if (leaf > 0)
        {
            body += config.structuralBytes[rng() % config.structuralBytes.size()];
        }
        const auto len = rng() % 4;
        for (size_t i = 0; i < len; ++i)
        {
            body += static_cast<char>('a' + (rng() % 26));
        }
    }
    return body;
}

/// Randomized adversarial bytes (lone/doubled delimiter bytes, empty leaves, arity mismatches):
/// scalar and SIMD must agree byte-for-byte on the emitted offsets AND on whether they reject.
void differentialFuzzAdversarialOver(const ScanConfig& config, const std::string_view alphabet, const uint32_t seed)
{
    const auto kernels = SimdHl7::availableKernels();
    std::mt19937 rng(seed);
    constexpr int iterations = 2000;
    for (int iter = 0; iter < iterations; ++iter)
    {
        const size_t len = rng() % 1200;
        std::string view;
        view.reserve(len);
        for (size_t i = 0; i < len; ++i)
        {
            view += alphabet[rng() % alphabet.size()];
        }
        const uint64_t numFields = 1 + (rng() % 6);

        const auto scalar = runScalar(config, view, numFields);
        for (const auto& kernel : kernels)
        {
            const auto simd = runSimd(config, kernel.fn, view, numFields);
            ASSERT_EQ(scalar, simd) << kernel.name << " iter=" << iter << " len=" << len;
        }
    }
}

/// Well-formed randomized streams (fixed leaf count, random leaf lengths/splits, random leading and
/// trailing fragments): success-path differential across all kernels.
void differentialFuzzWellFormedOver(const ScanConfig& config, const uint32_t seed)
{
    const auto kernels = SimdHl7::availableKernels();
    std::mt19937 rng(seed);
    constexpr int iterations = 1500;
    for (int iter = 0; iter < iterations; ++iter)
    {
        const uint64_t leaves = 1 + (rng() % 8);
        const auto messages = rng() % 12;
        std::string view = makeMessage(rng, config, leaves); /// leading fragment (cut-off message)
        view += config.messageDelimiter;
        for (unsigned m = 0; m < messages; ++m)
        {
            view += makeMessage(rng, config, leaves);
            view += config.messageDelimiter;
        }
        view.append(rng() % 40, 'a'); /// trailing fragment

        const auto scalar = runScalar(config, view, leaves);
        ASSERT_FALSE(scalar.threw);
        for (const auto& kernel : kernels)
        {
            const auto simd = runSimd(config, kernel.fn, view, leaves);
            ASSERT_EQ(scalar, simd) << kernel.name << " iter=" << iter;
        }
    }
}

}

class HL7SimdScanTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase()
    {
        Logger::setupLogging("HL7SimdScanTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup HL7SimdScanTest test class.");
    }
};

/// At least the scalar fallback is always available; on a SIMD-capable build there are more.
TEST_F(HL7SimdScanTest, kernelsAreAvailable)
{
    const auto kernels = SimdHl7::availableKernels();
    ASSERT_FALSE(kernels.empty());
    EXPECT_STREQ(kernels.front().name, "scalar");
}

/// Hand-checked emission: only messages strictly between delimiters are indexed; bytes before the
/// first and after the last delimiter are spanning fragments.
TEST_F(HL7SimdScanTest, handCheckedEmissionMatchesContract)
{
    for (const auto& kernel : SimdHl7::availableKernels())
    {
        /// "x\x1C\r" leading fragment; one complete message "a|b" at [3,5,6]; delimiters at 1 and 6.
        const auto outcome = runSimd(kHl7Config, kernel.fn, "x\x1C\ra|b\x1C\r", 2);
        EXPECT_FALSE(outcome.threw) << kernel.name;
        EXPECT_TRUE(outcome.sink.marked) << kernel.name;
        EXPECT_EQ(outcome.sink.firstTuple, 1U) << kernel.name;
        EXPECT_EQ(outcome.sink.lastTuple, 6U) << kernel.name;
        EXPECT_EQ(outcome.sink.offsets, (std::vector<uint32_t>{3, 5, 6})) << kernel.name;

        /// A single delimiter => no complete message, first == last.
        const auto single = runSimd(kHl7Config, kernel.fn, "x\x1C\ry", 2);
        EXPECT_TRUE(single.sink.marked) << kernel.name;
        EXPECT_EQ(single.sink.firstTuple, 1U) << kernel.name;
        EXPECT_EQ(single.sink.lastTuple, 1U) << kernel.name;
        EXPECT_TRUE(single.sink.offsets.empty()) << kernel.name;

        /// No delimiter at all => one spanning fragment. A lone <FS> (no <CR>) is NOT a delimiter.
        const auto none = runSimd(kHl7Config, kernel.fn, "no|delimiter\x1Chere", 2);
        EXPECT_TRUE(none.sink.noTupleDelimiters) << kernel.name;
        EXPECT_FALSE(none.sink.marked) << kernel.name;
    }
}

/// XML-config hand-checked emission (1-byte '\n' delimiter, class {<,>}): the complete message
/// "<a>v</a>" between the delimiters at 1 and 10 splits into 5 leaves ("", "a", "v", "/a", "") --
/// element value standalone at slot 2 (the 4f+4 pattern for F=1 without a root element).
TEST_F(HL7SimdScanTest, xmlHandCheckedEmissionMatchesContract)
{
    for (const auto& kernel : SimdHl7::availableKernels())
    {
        /// "x" leading fragment; structural bytes at 2,4,6,9; leaf starts 2,3,5,7,10; body end 10.
        const auto outcome = runSimd(kXmlConfig, kernel.fn, "x\n<a>v</a>\n", 5);
        EXPECT_FALSE(outcome.threw) << kernel.name;
        EXPECT_TRUE(outcome.sink.marked) << kernel.name;
        EXPECT_EQ(outcome.sink.firstTuple, 1U) << kernel.name;
        EXPECT_EQ(outcome.sink.lastTuple, 10U) << kernel.name;
        EXPECT_EQ(outcome.sink.offsets, (std::vector<uint32_t>{2, 3, 5, 7, 10, 10})) << kernel.name;

        /// A single delimiter => no complete message, first == last.
        const auto single = runSimd(kXmlConfig, kernel.fn, "x\ny", 5);
        EXPECT_TRUE(single.sink.marked) << kernel.name;
        EXPECT_EQ(single.sink.firstTuple, 1U) << kernel.name;
        EXPECT_EQ(single.sink.lastTuple, 1U) << kernel.name;
        EXPECT_TRUE(single.sink.offsets.empty()) << kernel.name;

        /// No delimiter at all => one spanning fragment (structural bytes alone frame nothing).
        const auto none = runSimd(kXmlConfig, kernel.fn, "<no>delimiter</no>", 5);
        EXPECT_TRUE(none.sink.noTupleDelimiters) << kernel.name;
        EXPECT_FALSE(none.sink.marked) << kernel.name;

        /// The scalar walk agrees on all three (the differential in miniature).
        EXPECT_EQ(runScalar(kXmlConfig, "x\n<a>v</a>\n", 5), outcome) << kernel.name;
    }
}

/// Sweep the message delimiter across every alignment around the 64-byte block boundary and the
/// 1024-byte chunk boundary: the <FS>|<CR> split carry and the matched-<CR> structural suppression
/// must behave identically to the scalar walk at every offset.
TEST_F(HL7SimdScanTest, delimiterAtEveryBoundaryAlignment)
{
    const auto kernels = SimdHl7::availableKernels();
    for (const size_t boundary : {size_t{64}, size_t{128}, size_t{1024}})
    {
        for (size_t shift = 0; shift < 6; ++shift)
        {
            /// Place the first delimiter so its <FS> lands at (boundary - 3 + shift): covers FS/CR
            /// fully before, straddling, and fully after the boundary.
            const size_t fsPos = boundary - 3 + shift;
            std::string view(fsPos, 'a');
            view += kHl7Config.messageDelimiter;
            view += "x|y";
            view += kHl7Config.messageDelimiter;
            view += "u|v";
            view += kHl7Config.messageDelimiter;
            for (const auto& kernel : kernels)
            {
                const auto scalar = runScalar(kHl7Config, view, 2);
                const auto simd = runSimd(kHl7Config, kernel.fn, view, 2);
                EXPECT_EQ(scalar, simd) << kernel.name << " boundary=" << boundary << " shift=" << shift;
                EXPECT_FALSE(simd.threw) << kernel.name;
                EXPECT_EQ(simd.sink.offsets.size(), 6U) << kernel.name; /// two complete 2-leaf messages
            }
        }
    }
}

/// XML twin of the boundary sweep: a 1-byte delimiter cannot straddle a block boundary, so the
/// corner is simply the delimiter (and the structural bytes around it) landing on either side of
/// the 64-byte block and 16-block chunk edges through the delete-only flatten path.
TEST_F(HL7SimdScanTest, xmlDelimiterAtEveryBoundaryAlignment)
{
    const auto kernels = SimdHl7::availableKernels();
    for (const size_t boundary : {size_t{64}, size_t{128}, size_t{1024}})
    {
        for (size_t shift = 0; shift < 5; ++shift)
        {
            /// Place the first delimiter at (boundary - 2 + shift): fully before, at, and after.
            const size_t delimiterPos = boundary - 2 + shift;
            std::string view(delimiterPos, 'a');
            view += kXmlConfig.messageDelimiter;
            view += "<a>v</a>";
            view += kXmlConfig.messageDelimiter;
            view += "<b>u</b>";
            view += kXmlConfig.messageDelimiter;
            for (const auto& kernel : kernels)
            {
                const auto scalar = runScalar(kXmlConfig, view, 5);
                const auto simd = runSimd(kXmlConfig, kernel.fn, view, 5);
                EXPECT_EQ(scalar, simd) << kernel.name << " boundary=" << boundary << " shift=" << shift;
                EXPECT_FALSE(simd.threw) << kernel.name;
                EXPECT_EQ(simd.sink.offsets.size(), 12U) << kernel.name; /// two complete 5-leaf messages
            }
        }
    }
}

/// Well-formed randomized streams (fixed leaf count, random leaf lengths/splits, random leading and
/// trailing fragments): success-path differential across all kernels.
TEST_F(HL7SimdScanTest, differentialFuzzWellFormed)
{
    differentialFuzzWellFormedOver(kHl7Config, 0x48'4C'37'01U);
}

TEST_F(HL7SimdScanTest, xmlDifferentialFuzzWellFormed)
{
    differentialFuzzWellFormedOver(kXmlConfig, 0x58'4D'4C'01U);
}

/// Adversarial randomized bytes from the delimiter-heavy alphabet (lone <FS>, lone <CR>, doubled
/// delimiters, empty leaves, arity mismatches): scalar and SIMD must agree byte-for-byte on the
/// emitted offsets AND on whether they reject the buffer.
TEST_F(HL7SimdScanTest, differentialFuzzAdversarial)
{
    differentialFuzzAdversarialOver(kHl7Config, "a|^&\r\x1C\x0B", 0x48'4C'37'02U);
}

/// XML alphabet: structural '<'/'>', the '\n' delimiter, plus '&' and '\x0B' as non-structural
/// noise (no entity processing -- '&' must index as ordinary value bytes in this mode).
TEST_F(HL7SimdScanTest, xmlDifferentialFuzzAdversarial)
{
    differentialFuzzAdversarialOver(kXmlConfig, "a<>&\n\x0B", 0x58'4D'4C'02U);
}

}

/// NOLINTEND(readability-magic-numbers)
