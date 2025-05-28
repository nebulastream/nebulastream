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
#include <memory>
#include <tuple>
#include <utility>
#include <vector>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <SliceStore/SliceAssigner.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <SliceStore/DefaultTimeBasedSliceStore.hpp>
#include "../../nes-common/tests/Util/include/BaseUnitTest.hpp"

namespace NES
{

class SlideAssignerTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("SlideAssignerTest.log", LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup SlideAssignerTest class.");
    }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
    }
};

TEST_F(SlideAssignerTest, getSlice_divider_10_2)
{
    int windowSize = 10;
    int windowSlide = 2;
    std::vector<int> timestamps = {8, 9, 10, 11, 12};
    std::vector<std::pair<int, int>> slices = {{8, 10}, {8, 10}, {10, 12}, {10, 12}, {12, 14}};
    SliceAssigner sliceAssigner = SliceAssigner(windowSize, windowSlide);
    DefaultTimeBasedSliceStore store = DefaultTimeBasedSliceStore(windowSize,windowSlide,2);

    for (size_t i = 0; i < timestamps.size(); ++i)
    {
        const auto sliceStart = sliceAssigner.getSliceStartTs(Timestamp(timestamps[i]));
        EXPECT_EQ(sliceStart, Timestamp(slices[i].first));
        const auto sliceEnd = sliceAssigner.getSliceEndTs(Timestamp(timestamps[i]));
        EXPECT_EQ(sliceEnd, Timestamp(slices[i].second));
        const std::shared_ptr<Slice> newSlice = std::make_shared<Slice>(sliceStart, sliceEnd);

        std::vector<std::vector<std::pair<int, int>>> windows = {
            {{0, 10}, {2, 12},  {4, 14}, {6, 16}, {8, 18}},
            {{0, 10}, {2, 12},  {4, 14}, {6, 16}, {8, 18}},
            {{2, 12},  {4, 14}, {6, 16}, {8, 18}, {10, 20}},
            {{2, 12},  {4, 14}, {6, 16}, {8, 18}, {10, 20}},
            {{4, 14}, {6, 16}, {8, 18}, {10, 20},{12, 22}  },

        };
        auto windowInfo = store.getAllWindowInfosForSlice(*newSlice);

        for (size_t j = 0; j < windowInfo.size(); ++j)
        {
            EXPECT_EQ(windowInfo[j].windowStart,  Timestamp(windows[i][j].first));
            EXPECT_EQ(windowInfo[j].windowEnd,  Timestamp(windows[i][j].second));
        }
    }

}

TEST_F(SlideAssignerTest, getSlice_non_divider_10_3)
{
    int windowSize = 10;
    int windowSlide = 3;
    std::vector<int> timestamps = {8, 9, 10, 11, 12};
    std::vector<std::pair<int, int>> slices = {{6, 9}, {9, 10}, {10, 12}, {10, 12}, {12, 13}};
    SliceAssigner sliceAssigner = SliceAssigner(windowSize, windowSlide);
    DefaultTimeBasedSliceStore store = DefaultTimeBasedSliceStore(windowSize,windowSlide,2);

    for (size_t i = 0; i < timestamps.size(); ++i)
    {
        const auto sliceStart = sliceAssigner.getSliceStartTs(Timestamp(timestamps[i]));
        EXPECT_EQ(sliceStart, Timestamp(slices[i].first));
        const auto sliceEnd = sliceAssigner.getSliceEndTs(Timestamp(timestamps[i]));
        EXPECT_EQ(sliceEnd, Timestamp(slices[i].second));
        const std::shared_ptr<Slice> newSlice = std::make_shared<Slice>(sliceStart, sliceEnd);

        std::vector<std::vector<std::pair<int, int>>> windows = {
            {{0, 10}, {3, 13}, {6, 16}},
            {{0, 10}, {3, 13}, {6, 16}, {9,19}},
            {{3, 13}, {6, 16}, {9, 19}},
            {{3, 13}, {6, 16}, {9, 19}},
            {{3, 13}, {6, 16}, {9,19}, {12, 22}}

        };
        auto windowInfo = store.getAllWindowInfosForSlice(*newSlice);

        for (size_t j = 0; j < windowInfo.size(); ++j)
        {
            EXPECT_EQ(windowInfo[j].windowStart,  Timestamp(windows[i][j].first));
            EXPECT_EQ(windowInfo[j].windowEnd,  Timestamp(windows[i][j].second));
        }
    }

}

TEST_F(SlideAssignerTest, getSlice_non_divider_10_4)
{
    int windowSize = 10;
    int windowSlide = 4;
    std::vector<int> timestamps = {8, 9, 10, 11, 12, 13};
    std::vector<std::pair<int, int>> slices = {{8, 10}, {8, 10}, {10, 12}, {10, 12}, {12, 14}, {12, 14}, {14, 16}};
    SliceAssigner sliceAssigner = SliceAssigner(windowSize, windowSlide);
    DefaultTimeBasedSliceStore store = DefaultTimeBasedSliceStore(windowSize,windowSlide,2);

    for (size_t i = 0; i < timestamps.size(); ++i)
    {
        const auto sliceStart = sliceAssigner.getSliceStartTs(Timestamp(timestamps[i]));
        EXPECT_EQ(sliceStart, Timestamp(slices[i].first));
        const auto sliceEnd = sliceAssigner.getSliceEndTs(Timestamp(timestamps[i]));
        EXPECT_EQ(sliceEnd, Timestamp(slices[i].second));
        const std::shared_ptr<Slice> newSlice = std::make_shared<Slice>(sliceStart, sliceEnd);

        std::vector<std::vector<std::pair<int, int>>> windows = {
            {{0, 10}, {4, 14}, {8, 18}},
            {{0, 10}, {4, 14}, {8, 18}},
            {{4, 14}, {8, 18}},
            {{4, 14}, {8, 18}},
            {{4, 14}, {8, 18}, {12, 22}},
            {{4, 14}, {8, 18}, {12, 22}},
            {{8, 18}, {12, 22}}
        };
        auto windowInfo = store.getAllWindowInfosForSlice(*newSlice);

        for (size_t j = 0; j < windowInfo.size(); ++j)
        {
            EXPECT_EQ(windowInfo[j].windowStart,  Timestamp(windows[i][j].first));
            EXPECT_EQ(windowInfo[j].windowEnd,  Timestamp(windows[i][j].second));
        }
    }

}


}