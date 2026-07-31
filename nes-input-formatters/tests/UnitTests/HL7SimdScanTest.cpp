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
/// differential covers both the flatten and the stride addressing. The interesting corners are all
/// delimiter-pair placements relative to 64-byte SIMD blocks and the 16-block chunk: "\x1C\r"
/// straddling a block boundary (the carry), the matched <CR>'s suppression from the structural mask
/// across that boundary, and a lone trailing <FS>.
/// NOLINTBEGIN(readability-magic-numbers)
namespace NES
{
namespace
{

constexpr std::array<char, 4> kStructuralChars{'\r', '|', '^', '&'};
constexpr std::string_view kStructuralBytes = "\r|^&";
constexpr std::string_view kMessageDelimiter = "\x1C\r";

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

Outcome runScalar(const std::string_view view, const uint64_t numFields)
{
    Outcome outcome;
    try
    {
        SimdHl7::indexHl7ScalarInto(outcome.sink, view, kMessageDelimiter, kStructuralBytes, numFields);
    }
    catch (const Exception&)
    {
        outcome.threw = true;
    }
    return outcome;
}

Outcome runSimd(const SimdHl7::ComputeBlocksFn fn, const std::string_view view, const uint64_t numFields)
{
    Outcome outcome;
    outcome.sink.startSetup(numFields, /*sizeOfFieldDelimiter*/ 1);

    std::vector<uint32_t> band(view.size() + 16);
    std::vector<uint32_t> pairBandIdx((view.size() / 2) + 2);
    const auto flattened
        = SimdHl7::flattenHl7SimdInto(band.data(), pairBandIdx.data(), view, kStructuralChars, kMessageDelimiter[0], kMessageDelimiter[1], fn);
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

    /// Reconstruct the scalar walk's group emission from the band; message k spans pairs k and k+1.
    /// The offset math here mirrors the Hl7FieldIndex read side (first leaf skips the 2-byte
    /// delimiter, later leaves their 1-byte structural delimiter, the closing offset is the <FS>).
    try
    {
        for (size_t k = 0; k + 1 < flattened.numPairs; ++k)
        {
            const size_t groupBegin = pairBandIdx[k];
            const size_t groupEnd = pairBandIdx[k + 1];
            outcome.sink.emplaceFieldOffset(band[groupBegin] + 2);
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

/// One well-formed message body with `leaves` leaves ("a|b|c" style, first leaf may be empty).
std::string makeMessage(std::mt19937& rng, const uint64_t leaves)
{
    std::string body;
    for (uint64_t leaf = 0; leaf < leaves; ++leaf)
    {
        if (leaf > 0)
        {
            constexpr std::string_view splits = "\r|^&";
            body += splits[rng() % splits.size()];
        }
        const auto len = rng() % 4;
        for (size_t i = 0; i < len; ++i)
        {
            body += static_cast<char>('a' + (rng() % 26));
        }
    }
    return body;
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
        const auto outcome = runSimd(kernel.fn, "x\x1C\ra|b\x1C\r", 2);
        EXPECT_FALSE(outcome.threw) << kernel.name;
        EXPECT_TRUE(outcome.sink.marked) << kernel.name;
        EXPECT_EQ(outcome.sink.firstTuple, 1U) << kernel.name;
        EXPECT_EQ(outcome.sink.lastTuple, 6U) << kernel.name;
        EXPECT_EQ(outcome.sink.offsets, (std::vector<uint32_t>{3, 5, 6})) << kernel.name;

        /// A single delimiter => no complete message, first == last.
        const auto single = runSimd(kernel.fn, "x\x1C\ry", 2);
        EXPECT_TRUE(single.sink.marked) << kernel.name;
        EXPECT_EQ(single.sink.firstTuple, 1U) << kernel.name;
        EXPECT_EQ(single.sink.lastTuple, 1U) << kernel.name;
        EXPECT_TRUE(single.sink.offsets.empty()) << kernel.name;

        /// No delimiter at all => one spanning fragment. A lone <FS> (no <CR>) is NOT a delimiter.
        const auto none = runSimd(kernel.fn, "no|delimiter\x1Chere", 2);
        EXPECT_TRUE(none.sink.noTupleDelimiters) << kernel.name;
        EXPECT_FALSE(none.sink.marked) << kernel.name;
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
            view += kMessageDelimiter;
            view += "x|y";
            view += kMessageDelimiter;
            view += "u|v";
            view += kMessageDelimiter;
            for (const auto& kernel : kernels)
            {
                const auto scalar = runScalar(view, 2);
                const auto simd = runSimd(kernel.fn, view, 2);
                EXPECT_EQ(scalar, simd) << kernel.name << " boundary=" << boundary << " shift=" << shift;
                EXPECT_FALSE(simd.threw) << kernel.name;
                EXPECT_EQ(simd.sink.offsets.size(), 6U) << kernel.name; /// two complete 2-leaf messages
            }
        }
    }
}

/// Well-formed randomized streams (fixed leaf count, random leaf lengths/splits, random leading and
/// trailing fragments): success-path differential across all kernels.
TEST_F(HL7SimdScanTest, differentialFuzzWellFormed)
{
    const auto kernels = SimdHl7::availableKernels();
    std::mt19937 rng(0x48'4C'37'01U);
    constexpr int iterations = 1500;
    for (int iter = 0; iter < iterations; ++iter)
    {
        const uint64_t leaves = 1 + (rng() % 8);
        const auto messages = rng() % 12;
        std::string view = makeMessage(rng, leaves); /// leading fragment (cut-off message)
        view += kMessageDelimiter;
        for (unsigned m = 0; m < messages; ++m)
        {
            view += makeMessage(rng, leaves);
            view += kMessageDelimiter;
        }
        view.append(rng() % 40, 'a'); /// trailing fragment

        const auto scalar = runScalar(view, leaves);
        ASSERT_FALSE(scalar.threw);
        for (const auto& kernel : kernels)
        {
            const auto simd = runSimd(kernel.fn, view, leaves);
            ASSERT_EQ(scalar, simd) << kernel.name << " iter=" << iter;
        }
    }
}

/// Adversarial randomized bytes from the delimiter-heavy alphabet (lone <FS>, lone <CR>, doubled
/// delimiters, empty leaves, arity mismatches): scalar and SIMD must agree byte-for-byte on the
/// emitted offsets AND on whether they reject the buffer.
TEST_F(HL7SimdScanTest, differentialFuzzAdversarial)
{
    const auto kernels = SimdHl7::availableKernels();
    constexpr std::string_view alphabet = "a|^&\r\x1C\x0B";
    std::mt19937 rng(0x48'4C'37'02U);
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

        const auto scalar = runScalar(view, numFields);
        for (const auto& kernel : kernels)
        {
            const auto simd = runSimd(kernel.fn, view, numFields);
            ASSERT_EQ(scalar, simd) << kernel.name << " iter=" << iter << " len=" << len;
        }
    }
}

}

/// NOLINTEND(readability-magic-numbers)
