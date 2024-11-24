//
// Created by ls on 11/25/24.
//

#include <gtest/gtest.h>
#include <limits>
#include <API/DataType.hpp>
namespace NES
{
class RangeTest : public ::testing::Test {
protected:
    Range r;
};

// Test initialization
TEST_F(RangeTest, DefaultInitialization) {
    auto [min, minNeg] = r.getMin();
    auto [max, maxNeg] = r.getMax();

    EXPECT_EQ(min, 0);
    EXPECT_FALSE(minNeg);
    EXPECT_EQ(max, 0);
    EXPECT_FALSE(maxNeg);
}

// Test setting min adjusts to maintain invariant
TEST_F(RangeTest, SetMinMaintainsInvariant) {
    // Setting min higher than current max should adjust min down
    r.setMin(10, false);  // Should result in [0,10]

    auto [min, minNeg] = r.getMin();
    auto [max, maxNeg] = r.getMax();

    EXPECT_EQ(min, 0);
    EXPECT_FALSE(minNeg);
    EXPECT_EQ(max, 10);
    EXPECT_FALSE(maxNeg);

    // Now set max higher and try again
    r.setMax(20, false);  // [0,20]
    r.setMin(15, false);  // Should result in [15,20]

    auto [min2, minNeg2] = r.getMin();
    auto [max2, maxNeg2] = r.getMax();

    EXPECT_EQ(min2, 15);
    EXPECT_FALSE(minNeg2);
    EXPECT_EQ(max2, 20);
    EXPECT_FALSE(maxNeg2);
}

// Test setting max maintains invariant
TEST_F(RangeTest, SetMaxMaintainsInvariant) {
    r.setMax(20, false);  // Should be [0,20]

    auto [min, minNeg] = r.getMin();
    auto [max, maxNeg] = r.getMax();

    EXPECT_EQ(min, 0);
    EXPECT_FALSE(minNeg);
    EXPECT_EQ(max, 20);
    EXPECT_FALSE(maxNeg);
}

// Test negative ranges
TEST_F(RangeTest, NegativeRanges) {
    // Setting negative max should work
    r.setMax(10, true);  // Should be [-10,0]

    auto [min, minNeg] = r.getMin();
    auto [max, maxNeg] = r.getMax();

    EXPECT_EQ(min, 10);
    EXPECT_TRUE(minNeg);
    EXPECT_EQ(max, 0);
    EXPECT_FALSE(maxNeg);

    // Setting negative min less than negative max
    r.setMin(20, true);  // Should stay [-20,0]

    auto [min2, minNeg2] = r.getMin();
    auto [max2, maxNeg2] = r.getMax();

    EXPECT_EQ(min2, 20);
    EXPECT_TRUE(minNeg2);
    EXPECT_EQ(max2, 0);
    EXPECT_FALSE(maxNeg2);
}

// Test signed interface
TEST_F(RangeTest, SignedInterface) {
    r.setMax(20);     // Should be [0,20]
    r.setMin(-10);    // Should be [-10,20]

    auto [min, minNeg] = r.getMin();
    auto [max, maxNeg] = r.getMax();

    EXPECT_EQ(min, 10);
    EXPECT_TRUE(minNeg);
    EXPECT_EQ(max, 20);
    EXPECT_FALSE(maxNeg);
}

// Test abs() function
TEST_F(RangeTest, AbsoluteValueTests) {
    // Positive range
    r.setMax(20);     // [0,20]
    Range abs1 = r.abs();
    EXPECT_EQ(abs1.getMin(), std::make_pair(0ULL, false));
    EXPECT_EQ(abs1.getMax(), std::make_pair(20ULL, false));

    // Negative range
    r = Range();      // Reset to [0,0]
    r.setMax(-10);    // [-10,0]
    Range abs2 = r.abs();
    EXPECT_EQ(abs2.getMin(), std::make_pair(0ULL, false));
    EXPECT_EQ(abs2.getMax(), std::make_pair(10ULL, false));

    // Range crossing zero
    r = Range();      // Reset to [0,0]
    r.setMax(20);     // [0,20]
    r.setMin(-10);    // [-10,20]
    Range abs3 = r.abs();
    EXPECT_EQ(abs3.getMin(), std::make_pair(0ULL, false));
    EXPECT_EQ(abs3.getMax(), std::make_pair(20ULL, false));

    // Larger negative than positive
    r = Range();      // Reset to [0,0]
    r.setMin(-30);    // [-30,0]
    r.setMax(20);     // [-30,20]
    Range abs4 = r.abs();
    EXPECT_EQ(abs4.getMin(), std::make_pair(0ULL, false));
    EXPECT_EQ(abs4.getMax(), std::make_pair(30ULL, false));
}

// Test edge cases
TEST_F(RangeTest, EdgeCases) {
    // Test maximum size_t
    size_t maxVal = std::numeric_limits<size_t>::max();
    r.setMax(maxVal, false);  // [0,MAX]
    auto [max, maxNeg] = r.getMax();
    EXPECT_EQ(max, maxVal);
    EXPECT_FALSE(maxNeg);

    // Test with maximum negative value
    r = Range();  // Reset to [0,0]
    r.setMin(maxVal, true);  // [-MAX,0]
    auto [min, minNeg] = r.getMin();
    EXPECT_EQ(min, maxVal);
    EXPECT_TRUE(minNeg);

    // Test with signed max/min
    r = Range();  // Reset to [0,0]
    ssize_t maxSigned = std::numeric_limits<ssize_t>::max();
    ssize_t minSigned = std::numeric_limits<ssize_t>::min();

    r.setMax(maxSigned);  // [0,MAX_SSIZE_T]
    r.setMin(minSigned);  // [MIN_SSIZE_T,MAX_SSIZE_T]

    auto [min2, minNeg2] = r.getMin();
    auto [max2, maxNeg2] = r.getMax();

    EXPECT_EQ(max2, static_cast<size_t>(maxSigned));
    EXPECT_FALSE(maxNeg2);
    EXPECT_EQ(min2, static_cast<size_t>(-minSigned));  // Assuming two's complement
    EXPECT_TRUE(minNeg2);
}
}