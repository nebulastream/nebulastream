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
#include <cstring>
#include <span>
#include <vector>
#include <Operators/Statistic/EquiWidthHistogramBlobLayout.hpp>
#include <Statistics/EquiWidthHistogramBlob.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES
{
namespace
{

/// A blob built the way the physical function builds one: header written through the nautilus accessors, counters
/// memcpy'd in behind it.
std::vector<int8_t>
makeBlob(const uint64_t numberOfBins, const uint64_t minValue, const uint64_t maxValue, const std::vector<uint64_t>& counters)
{
    std::vector<int8_t> blob(EquiWidthHistogramBlob::HEADER_SIZE + (counters.size() * EquiWidthHistogramBlob::COUNTER_SIZE), 0);
    writeEquiWidthHistogramBlobHeader(
        nautilus::val<int8_t*>{blob.data()},
        nautilus::val<uint64_t>{numberOfBins},
        nautilus::val<uint64_t>{minValue},
        nautilus::val<uint64_t>{maxValue});
    if (not counters.empty())
    {
        std::memcpy(
            blob.data() + EquiWidthHistogramBlob::HEADER_SIZE, counters.data(), counters.size() * EquiWidthHistogramBlob::COUNTER_SIZE);
    }
    return blob;
}

}

class EquiWidthHistogramBlobTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase() { Logger::setupLogging("EquiWidthHistogramBlobTest.log", LogLevel::LOG_DEBUG); }
};

/// The header round trips and the counters come back in the order they were written.
TEST_F(EquiWidthHistogramBlobTest, headerAndCountersRoundTrip)
{
    const std::vector<uint64_t> counters{7, 0, 3, 11, 5};
    auto blob = makeBlob(5, 0, 25, counters);
    const EquiWidthHistogramBlobBinReader reader{nautilus::val<int8_t*>{blob.data()}};

    ASSERT_EQ(reader.getNumberOfBins(), 5U);
    for (uint64_t bin = 0; bin < counters.size(); ++bin)
    {
        EXPECT_EQ(reader.getBinCounter(nautilus::val<uint64_t>{bin}), counters[bin]) << "bin " << bin;
    }
}

/// A range that divides evenly: every bin is w wide, and only the last one includes its end.
TEST_F(EquiWidthHistogramBlobTest, evenRangeGivesEquallyWideBins)
{
    /// w = (24 - 0) / 12 = 2, so the bins are [0,2), [2,4), ..., [20,22) and finally [22,24].
    auto blob = makeBlob(12, 0, 24, std::vector<uint64_t>(12, 0));
    const EquiWidthHistogramBlobBinReader reader{nautilus::val<int8_t*>{blob.data()}};

    for (uint64_t bin = 0; bin < 11; ++bin)
    {
        EXPECT_EQ(reader.getBinStart(nautilus::val<uint64_t>{bin}), bin * 2) << "bin " << bin;
        EXPECT_EQ(reader.getBinEnd(nautilus::val<uint64_t>{bin}), (bin + 1) * 2) << "bin " << bin;
    }
    EXPECT_EQ(reader.getBinStart(nautilus::val<uint64_t>{11}), 22U);
    /// The last bin would end at 24 either way; what makes it different is that 24 itself belongs to it.
    EXPECT_EQ(reader.getBinEnd(nautilus::val<uint64_t>{11}), 24U);
}

/// A range with a remainder: the integer width leaves the last bin wider by the remainder plus maxValue itself.
TEST_F(EquiWidthHistogramBlobTest, lastBinAbsorbsTheRemainderAndIsInclusive)
{
    /// w = (25 - 0) / 4 = 6, so [0,6), [6,12), [12,18) and [18,25] -- eight values instead of six in the last bin.
    auto blob = makeBlob(4, 0, 25, std::vector<uint64_t>(4, 0));
    const EquiWidthHistogramBlobBinReader reader{nautilus::val<int8_t*>{blob.data()}};

    EXPECT_EQ(reader.getBinStart(nautilus::val<uint64_t>{0}), 0U);
    EXPECT_EQ(reader.getBinEnd(nautilus::val<uint64_t>{0}), 6U);
    EXPECT_EQ(reader.getBinStart(nautilus::val<uint64_t>{2}), 12U);
    EXPECT_EQ(reader.getBinEnd(nautilus::val<uint64_t>{2}), 18U);
    EXPECT_EQ(reader.getBinStart(nautilus::val<uint64_t>{3}), 18U);
    EXPECT_EQ(reader.getBinEnd(nautilus::val<uint64_t>{3}), 25U);
}

/// A histogram over a non-zero minimum reports bounds relative to it.
TEST_F(EquiWidthHistogramBlobTest, binsStartAtTheMinimum)
{
    /// w = (130 - 100) / 3 = 10, so [100,110), [110,120) and [120,130].
    auto blob = makeBlob(3, 100, 130, std::vector<uint64_t>(3, 0));
    const EquiWidthHistogramBlobBinReader reader{nautilus::val<int8_t*>{blob.data()}};

    EXPECT_EQ(reader.getBinStart(nautilus::val<uint64_t>{0}), 100U);
    EXPECT_EQ(reader.getBinEnd(nautilus::val<uint64_t>{1}), 120U);
    EXPECT_EQ(reader.getBinEnd(nautilus::val<uint64_t>{2}), 130U);
}

TEST_F(EquiWidthHistogramBlobTest, aBudgetBuysTheBinsItPaysFor)
{
    /// Nothing below the header plus one counter buys a bin, and the subtraction must not wrap on the way there.
    EXPECT_EQ(equiWidthHistogramBinsForBudget(0), 0U);
    EXPECT_EQ(equiWidthHistogramBinsForBudget(EquiWidthHistogramBlob::HEADER_SIZE), 0U);
    EXPECT_EQ(equiWidthHistogramBinsForBudget(EquiWidthHistogramBlob::HEADER_SIZE + 7), 0U);

    EXPECT_EQ(equiWidthHistogramBinsForBudget(EquiWidthHistogramBlob::HEADER_SIZE + 8), 1U);
    /// The example from the plan: 128 bytes is the header plus 13 counters, with 8 bytes left over.
    EXPECT_EQ(equiWidthHistogramBinsForBudget(128), 13U);
    EXPECT_EQ(equiWidthHistogramBinsForBudget(120), 12U);
    /// 682 bins, the size the delta-compression benchmark used.
    EXPECT_EQ(equiWidthHistogramBinsForBudget(5480), 682U);
}

TEST_F(EquiWidthHistogramBlobTest, theBudgetForABinCountIsTheInverse)
{
    for (const uint64_t bins : {1U, 12U, 13U, 682U})
    {
        EXPECT_EQ(equiWidthHistogramBinsForBudget(equiWidthHistogramBudgetForBins(bins)), bins) << bins << " bins";
    }
    EXPECT_EQ(equiWidthHistogramBudgetForBins(682), 5480U);
}

TEST_F(EquiWidthHistogramBlobTest, aWellFormedBlobValidates)
{
    auto blob = makeBlob(5, 0, 25, std::vector<uint64_t>(5, 1));
    EXPECT_NO_THROW(validateEquiWidthHistogramBlob(std::span<const int8_t>{blob}));
}

/// Everything a probe could be handed under this type name that is not a histogram of the size its header claims.
TEST_F(EquiWidthHistogramBlobTest, aMalformedBlobIsRejected)
{
    const auto expectRejected = [](const std::vector<int8_t>& blob, const char* what)
    {
        try
        {
            validateEquiWidthHistogramBlob(std::span<const int8_t>{blob});
            ADD_FAILURE() << "expected " << what << " to be rejected";
        }
        catch (const Exception& exception)
        {
            EXPECT_EQ(exception.code(), ErrorCode::CannotProbeStatistic) << what;
        }
    };

    expectRejected(std::vector<int8_t>(EquiWidthHistogramBlob::HEADER_SIZE - 1, 0), "a payload shorter than the header");
    expectRejected(makeBlob(0, 0, 25, {}), "a header claiming no bins");
    expectRejected(makeBlob(5, 25, 25, std::vector<uint64_t>(5, 0)), "an empty range");
    expectRejected(makeBlob(5, 25, 0, std::vector<uint64_t>(5, 0)), "an inverted range");
    expectRejected(makeBlob(30, 0, 25, std::vector<uint64_t>(30, 0)), "more bins than the range holds values");
    expectRejected(makeBlob(5, 0, 25, std::vector<uint64_t>(4, 0)), "a payload shorter than its bin count");
    expectRejected(makeBlob(5, 0, 25, std::vector<uint64_t>(6, 0)), "a payload longer than its bin count");

    /// A header that would overflow numberOfBins * COUNTER_SIZE must be rejected on size, not wrap into accepting.
    /// The range is wide enough that the bin count clears the range check and the size check is what has to catch it.
    auto overflowing = makeBlob(5, 0, uint64_t{1} << 63U, std::vector<uint64_t>(5, 0));
    constexpr uint64_t binsThatOverflowTheByteCount = (uint64_t{1} << 62U) + 3;
    std::memcpy(overflowing.data() + EquiWidthHistogramBlob::NUMBER_OF_BINS_OFFSET, &binsThatOverflowTheByteCount, sizeof(uint64_t));
    expectRejected(overflowing, "a bin count whose byte count overflows");
}

}
