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
#include <ranges>
#include <vector>
#include <SliceStore/Slice.hpp>
#include <SliceStore/SliceAssigner.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>

namespace NES
{


struct SlicesForTimestamp
{
    SlicesForTimestamp(const SliceStart sliceStart, const SliceEnd sliceEnd, const Timestamp timestamp)
        : slice(sliceStart, sliceEnd), timestamp(timestamp)
    {
    }
    Slice slice;
    Timestamp timestamp;
};

class SliceAssignerTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("SlideAssignerTest.log", LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup SlideAssignerTest class.");
    }

    void SetUp() override { BaseUnitTest::SetUp(); }

    /// We assume that slicesForTimestamps[0] corresponds to windows[0]
    static void runValidation(
        const std::vector<SlicesForTimestamp>& slicesForTimestamps,
        const std::vector<std::vector<WindowInfo>>& windows,
        const SliceAssigner& sliceAssigner)
    {
        PRECONDITION(
            slicesForTimestamps.size() == windows.size(),
            "Sizes of slicesForTimestamps({}) and windows({}) must be the same",
            slicesForTimestamps.size(),
            windows.size());

        /// Testing if the slice assigner correctly assigns the timestamps to the slices as well as to the correct windows
        for (const auto& [sliceForTimestamp, expectedWindows] : std::ranges::zip_view(slicesForTimestamps, windows))
        {
            const auto actualSliceStart = sliceAssigner.getSliceStartTs(sliceForTimestamp.timestamp);
            const auto actualSliceEnd = sliceAssigner.getSliceEndTs(sliceForTimestamp.timestamp);
            EXPECT_EQ(sliceForTimestamp.slice.getSliceStart(), actualSliceStart);
            EXPECT_EQ(sliceForTimestamp.slice.getSliceEnd(), actualSliceEnd);

            auto windowInfo = sliceAssigner.getAllWindowsForSlice(Slice{actualSliceStart, actualSliceEnd});
            ASSERT_EQ(windowInfo.size(), expectedWindows.size());
            for (size_t j = 0; j < windowInfo.size(); ++j)
            {
                EXPECT_EQ(windowInfo[j].windowStart, Timestamp(expectedWindows[j].windowStart));
                EXPECT_EQ(windowInfo[j].windowEnd, Timestamp(expectedWindows[j].windowEnd));
            }
        }
    }
};


TEST_F(SliceAssignerTest, getSliceDividerSize10Slide2)
{
    /// Creating a slice store with a particular size and slide
    constexpr auto windowSize = 10;
    constexpr auto windowSlide = 2;
    const SliceAssigner sliceAssigner(windowSize, windowSlide);


    /// Creating the expected slices for the given timestamps as well as the windows for each slice.
    const std::vector<SlicesForTimestamp> slicesForTimestamps
        = {{Timestamp(8), Timestamp(10), Timestamp(8)},
           {Timestamp(8), Timestamp(10), Timestamp(9)},
           {Timestamp(10), Timestamp(12), Timestamp(10)},
           {Timestamp(10), Timestamp(12), Timestamp(11)},
           {Timestamp(12), Timestamp(14), Timestamp(12)}};
    const std::vector<std::vector<WindowInfo>> windows = {
        {{0, 10}, {2, 12}, {4, 14}, {6, 16}, {8, 18}},
        {{0, 10}, {2, 12}, {4, 14}, {6, 16}, {8, 18}},
        {{2, 12}, {4, 14}, {6, 16}, {8, 18}, {10, 20}},
        {{2, 12}, {4, 14}, {6, 16}, {8, 18}, {10, 20}},
        {{4, 14}, {6, 16}, {8, 18}, {10, 20}, {12, 22}},
    };

    runValidation(slicesForTimestamps, windows, sliceAssigner);
}

TEST_F(SliceAssignerTest, getSliceWithGapsSize8Slide13)
{
    /// Creating a slice store with a particular size and slide
    constexpr auto windowSize = 8;
    constexpr auto windowSlide = 13;
    const SliceAssigner sliceAssigner(windowSize, windowSlide);

    /// Creating the expected slices for the given timestamps as well as the windows for each slice.
    const std::vector<SlicesForTimestamp> slicesForTimestamps = {
        {Timestamp(0), Timestamp(8), Timestamp(0)},
        {Timestamp(0), Timestamp(8), Timestamp(5)},
        {Timestamp(0), Timestamp(8), Timestamp(7)},
        {Timestamp(8), Timestamp(13), Timestamp(8)},
        {Timestamp(13), Timestamp(21), Timestamp(13)},
        {Timestamp(13), Timestamp(21), Timestamp(18)},
        {Timestamp(13), Timestamp(21), Timestamp(20)},
        {Timestamp(21), Timestamp(26), Timestamp(21)},
        {Timestamp(26), Timestamp(34), Timestamp(26)},
        {Timestamp(26), Timestamp(34), Timestamp(31)},
        {Timestamp(26), Timestamp(34), Timestamp(33)},
    };
    const std::vector<std::vector<WindowInfo>> windows = {
        {{0, 8}},
        {{0, 8}},
        {{0, 8}},
        {{}},
        {{13, 21}},
        {{13, 21}},
        {{13, 21}},
        {{}},
        {{26, 34}},
        {{26, 34}},
        {{26, 34}},

    };
}

TEST_F(SliceAssignerTest, getSliceWithGapsSize10Slide20)
{
    /// Creating a slice store with a particular size and slide
    constexpr auto windowSize = 10;
    constexpr auto windowSlide = 20;
    const SliceAssigner sliceAssigner(windowSize, windowSlide);

    /// Creating the expected slices for the given timestamps as well as the windows for each slice.
    const std::vector<SlicesForTimestamp> slicesForTimestamps
        = {{Timestamp(0), Timestamp(10), Timestamp(0)},
           {Timestamp(0), Timestamp(10), Timestamp(1)},
           {Timestamp(0), Timestamp(10), Timestamp(2)},
           {Timestamp(0), Timestamp(10), Timestamp(9)},
           {Timestamp(10), Timestamp(20), Timestamp(10)},
           {Timestamp(10), Timestamp(20), Timestamp(12)},
           {Timestamp(10), Timestamp(20), Timestamp(19)},
           {Timestamp(20), Timestamp(30), Timestamp(20)},
           {Timestamp(20), Timestamp(30), Timestamp(21)},
           {Timestamp(20), Timestamp(30), Timestamp(24)},
           {Timestamp(20), Timestamp(30), Timestamp(27)},
           {Timestamp(30), Timestamp(40), Timestamp(31)},
           {Timestamp(30), Timestamp(40), Timestamp(37)},
           {Timestamp(30), Timestamp(40), Timestamp(39)},
           {Timestamp(40), Timestamp(50), Timestamp(41)}};
    const std::vector<std::vector<WindowInfo>> windows
        = {{{0, 10}},
           {{0, 10}},
           {{0, 10}},
           {{0, 10}},
           {{}},
           {{}},
           {{}},
           {{20, 30}},
           {{20, 30}},
           {{20, 30}},
           {{20, 30}},
           {{}},
           {{}},
           {{}},
           {{40, 50}}};

    runValidation(slicesForTimestamps, windows, sliceAssigner);
}

TEST_F(SliceAssignerTest, getSliceNonDividerSize20Slide3)
{
    /// Creating a slice store with a particular size and slide
    constexpr auto windowSize = 20;
    constexpr auto windowSlide = 3;
    const SliceAssigner sliceAssigner(windowSize, windowSlide);

    /// Creating the expected slices for the given timestamps as well as the windows for each slice.
    const std::vector<SlicesForTimestamp> slicesForTimestamps
        = {{Timestamp(0), Timestamp(3), Timestamp(0)},
           {Timestamp(3), Timestamp(6), Timestamp(4)},
           {Timestamp(3), Timestamp(6), Timestamp(5)},
           {Timestamp(6), Timestamp(9), Timestamp(8)},
           {Timestamp(9), Timestamp(12), Timestamp(9)},
           {Timestamp(9), Timestamp(12), Timestamp(10)},
           {Timestamp(12), Timestamp(15), Timestamp(14)},
           {Timestamp(15), Timestamp(18), Timestamp(17)},
           {Timestamp(18), Timestamp(20), Timestamp(18)},
           {Timestamp(18), Timestamp(20), Timestamp(19)},
           {Timestamp(20), Timestamp(21), Timestamp(20)},
           {Timestamp(21), Timestamp(23), Timestamp(21)},
           {Timestamp(27), Timestamp(29), Timestamp(27)},
           {Timestamp(29), Timestamp(30), Timestamp(29)},
           {Timestamp(30), Timestamp(32), Timestamp(30)}};
    const std::vector<std::vector<WindowInfo>> windows
        = {{{0, 20}},
           {{0, 20}, {3, 23}},
           {{0, 20}, {3, 23}},
           {{0, 20}, {3, 23}, {6, 26}},
           {{0, 20}, {3, 23}, {6, 26}, {9, 29}},
           {{0, 20}, {3, 23}, {6, 26}, {9, 29}},
           {{0, 20}, {3, 23}, {6, 26}, {9, 29}, {12, 32}},
           {{0, 20}, {3, 23}, {6, 26}, {9, 29}, {12, 32}, {15, 35}},
           {{0, 20}, {3, 23}, {6, 26}, {9, 29}, {12, 32}, {15, 35}, {18, 38}},
           {{0, 20}, {3, 23}, {6, 26}, {9, 29}, {12, 32}, {15, 35}, {18, 38}},
           {{3, 23}, {6, 26}, {9, 29}, {12, 32}, {15, 35}, {18, 38}},
           {{3, 23}, {6, 26}, {9, 29}, {12, 32}, {15, 35}, {18, 38}, {21, 41}},
           {{9, 29}, {12, 32}, {15, 35}, {18, 38}, {21, 41}, {24, 44}, {27, 47}},
           {{12, 32}, {15, 35}, {18, 38}, {21, 41}, {24, 44}, {27, 47}},
           {{12, 32}, {15, 35}, {18, 38}, {21, 41}, {24, 44}, {27, 47}, {30, 50}}};

    runValidation(slicesForTimestamps, windows, sliceAssigner);
}

TEST_F(SliceAssignerTest, getSliceNonDividerSize7Slide3)
{
    /// Creating a slice store with a particular size and slide
    constexpr auto windowSize = 7;
    constexpr auto windowSlide = 3;
    const SliceAssigner sliceAssigner(windowSize, windowSlide);

    /// Creating the expected slices for the given timestamps as well as the windows for each slice.
    const std::vector<SlicesForTimestamp> slicesForTimestamps
        = {{Timestamp(3), Timestamp(6), Timestamp(5)},
           {Timestamp(6), Timestamp(7), Timestamp(6)},
           {Timestamp(7), Timestamp(9), Timestamp(7)},
           {Timestamp(7), Timestamp(9), Timestamp(8)},
           {Timestamp(9), Timestamp(10), Timestamp(9)},
           {Timestamp(10), Timestamp(12), Timestamp(10)},
           {Timestamp(10), Timestamp(12), Timestamp(11)},
           {Timestamp(12), Timestamp(13), Timestamp(12)},
           {Timestamp(13), Timestamp(15), Timestamp(13)},
           {Timestamp(13), Timestamp(15), Timestamp(14)}};
    const std::vector<std::vector<WindowInfo>> windows
        = {{{0, 7}, {3, 10}},
           {{0, 7}, {3, 10}, {6, 13}},
           {{3, 10}, {6, 13}},
           {{3, 10}, {6, 13}},
           {{3, 10}, {6, 13}, {9, 16}},
           {{6, 13}, {9, 16}},
           {{6, 13}, {9, 16}},
           {{6, 13}, {9, 16}, {12, 19}},
           {{9, 16}, {12, 19}},
           {{9, 16}, {12, 19}}};

    runValidation(slicesForTimestamps, windows, sliceAssigner);
}

TEST_F(SliceAssignerTest, getSliceNonDividerSize10Slide3)
{
    /// Creating a slice store with a particular size and slide
    constexpr auto windowSize = 10;
    constexpr auto windowSlide = 3;
    const SliceAssigner sliceAssigner(windowSize, windowSlide);

    /// Creating the expected slices for the given timestamps as well as the windows for each slice.
    const std::vector<SlicesForTimestamp> slicesForTimestamps
        = {{Timestamp(6), Timestamp(9), Timestamp(8)},
           {Timestamp(9), Timestamp(10), Timestamp(9)},
           {Timestamp(10), Timestamp(12), Timestamp(10)},
           {Timestamp(10), Timestamp(12), Timestamp(11)},
           {Timestamp(12), Timestamp(13), Timestamp(12)}};
    const std::vector<std::vector<WindowInfo>> windows
        = {{{0, 10}, {3, 13}, {6, 16}},
           {{0, 10}, {3, 13}, {6, 16}, {9, 19}},
           {{3, 13}, {6, 16}, {9, 19}},
           {{3, 13}, {6, 16}, {9, 19}},
           {{3, 13}, {6, 16}, {9, 19}, {12, 22}}};

    runValidation(slicesForTimestamps, windows, sliceAssigner);
}

TEST_F(SliceAssignerTest, getSliceNonDividerSize10Slide4)
{
    /// Creating a slice store with a particular size and slide
    constexpr auto windowSize = 10;
    constexpr auto windowSlide = 4;
    const SliceAssigner sliceAssigner(windowSize, windowSlide);

    /// Creating the expected slices for the given timestamps as well as the windows for each slice.
    const std::vector<SlicesForTimestamp> slicesForTimestamps = {
        {Timestamp(8), Timestamp(10), Timestamp(8)},
        {Timestamp(8), Timestamp(10), Timestamp(9)},
        {Timestamp(10), Timestamp(12), Timestamp(10)},
        {Timestamp(10), Timestamp(12), Timestamp(11)},
        {Timestamp(12), Timestamp(14), Timestamp(12)},
        {Timestamp(12), Timestamp(14), Timestamp(13)},
        {Timestamp(14), Timestamp(16), Timestamp(14)},
    };
    const std::vector<std::vector<WindowInfo>> windows
        = {{{0, 10}, {4, 14}, {8, 18}},
           {{0, 10}, {4, 14}, {8, 18}},
           {{4, 14}, {8, 18}},
           {{4, 14}, {8, 18}},
           {{4, 14}, {8, 18}, {12, 22}},
           {{4, 14}, {8, 18}, {12, 22}},
           {{8, 18}, {12, 22}}};
    runValidation(slicesForTimestamps, windows, sliceAssigner);
}

TEST_F(SliceAssignerTest, getSliceNonDividerSize10Slide7)
{
    /// Creating a slice store with a particular size and slide
    constexpr auto windowSize = 10;
    constexpr auto windowSlide = 7;
    const SliceAssigner sliceAssigner(windowSize, windowSlide);

    /// Creating the expected slices for the given timestamps as well as the windows for each slice.
    const std::vector<SlicesForTimestamp> slicesForTimestamps = {
        {Timestamp(7), Timestamp(10), Timestamp(8)},
        {Timestamp(7), Timestamp(10), Timestamp(9)},
        {Timestamp(10), Timestamp(14), Timestamp(10)},
        {Timestamp(10), Timestamp(14), Timestamp(11)},
        {Timestamp(10), Timestamp(14), Timestamp(12)},
        {Timestamp(10), Timestamp(14), Timestamp(13)},
        {Timestamp(14), Timestamp(17), Timestamp(14)},
    };
    const std::vector<std::vector<WindowInfo>> windows
        = {{{0, 10}, {7, 17}}, {{0, 10}, {7, 17}}, {{7, 17}}, {{7, 17}}, {{7, 17}}, {{7, 17}}, {{7, 17}, {14, 24}}};
    runValidation(slicesForTimestamps, windows, sliceAssigner);
}


}
