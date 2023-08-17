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
#include <NesBaseTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <Windowing/Experimental/SlidingWindowAssigner.hpp>
#include <algorithm>
#include <atomic>
#include <gtest/gtest.h>
#include <iostream>
using namespace std;
namespace NES::Windowing::Experimental {

class WindowAssignerTest : public Testing::TestWithErrorHandling {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("WindowAssignerTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup WindowAssignerTest test class.");
    }
};

/**
 * @brief Test slice assignment for tumbling windows:
 * Tumbling windows have same size and slide:
 * Example with size=100:
 *
 * Windows:
 * 0-100, 100-200, 200-300
 *
 * Slices:
 * 0-100, 100-200, 200-300
 */
TEST_F(WindowAssignerTest, assignTumblingWindow) {
    // note we model tumbling windows as sliding windows with same size and slide.
    auto windowAssigner = SlidingWindowAssigner(100, 100);

    ASSERT_EQ(windowAssigner.getSliceStartTs(0), 0);
    ASSERT_EQ(windowAssigner.getSliceEndTs(0), 100);

    ASSERT_EQ(windowAssigner.getSliceStartTs(50), 0);
    ASSERT_EQ(windowAssigner.getSliceEndTs(50), 100);

    // test window edge
    ASSERT_EQ(windowAssigner.getSliceStartTs(99), 0);
    ASSERT_EQ(windowAssigner.getSliceEndTs(99), 100);
    ASSERT_EQ(windowAssigner.getSliceStartTs(100), 100);
    ASSERT_EQ(windowAssigner.getSliceEndTs(100), 200);

    ASSERT_EQ(windowAssigner.getSliceStartTs(199), 100);
    ASSERT_EQ(windowAssigner.getSliceEndTs(199), 200);
    ASSERT_EQ(windowAssigner.getSliceStartTs(200), 200);
    ASSERT_EQ(windowAssigner.getSliceEndTs(200), 300);
}

/**
 * @brief Test slice assignment for a regular sliding windows:
 * A regular sliding window has a slide, which is a divider of the size.
 * Example with size=100 and slide = 50.
 *
 * Windows:
 * 0-100, 50-150, 100-200, 150-250, 200-250, 250-300, 300-350
 *
 * Slices:
 * 0-50, 50-100, 100-150, 150-200, 200-250, 250-300, 300-350
 */
TEST_F(WindowAssignerTest, assignRegularSlidingWindow) {
    // note we model tumbling windows as sliding windows with same size and slide.
    auto windowAssigner = SlidingWindowAssigner(100, 50);

    ASSERT_EQ(windowAssigner.getSliceStartTs(0), 0);
    ASSERT_EQ(windowAssigner.getSliceEndTs(0), 50);

    ASSERT_EQ(windowAssigner.getSliceStartTs(49), 0);
    ASSERT_EQ(windowAssigner.getSliceEndTs(49), 50);
    ASSERT_EQ(windowAssigner.getSliceStartTs(50), 50);
    ASSERT_EQ(windowAssigner.getSliceEndTs(50), 100);
    ASSERT_EQ(windowAssigner.getSliceStartTs(99), 50);
    ASSERT_EQ(windowAssigner.getSliceEndTs(99), 100);

    ASSERT_EQ(windowAssigner.getSliceStartTs(100), 100);
    ASSERT_EQ(windowAssigner.getSliceEndTs(100), 150);

    ASSERT_EQ(windowAssigner.getSliceStartTs(150), 150);
    ASSERT_EQ(windowAssigner.getSliceEndTs(150), 200);

    ASSERT_EQ(windowAssigner.getSliceStartTs(200), 200);
    ASSERT_EQ(windowAssigner.getSliceEndTs(200), 250);

    ASSERT_EQ(windowAssigner.getSliceStartTs(250), 250);
    ASSERT_EQ(windowAssigner.getSliceEndTs(250), 300);
}

/**
 * @brief Test slice assignment for a ir-regular sliding windows:
 * A ir-regular sliding window has a slide, which is a not divider of the size.
 * Example with size=100 and slide = 30.
 *
 * Windows:
 * 0-100, 30-130, 60-160, 90-190, 120-220, 150-250, 180-280
 *
 * Slices:
 * 0-30, 30-60, 60-90, 90-100, 100-120, 120-130, 130-150, 150-160, 160-180, 180-190
 */
TEST_F(WindowAssignerTest, assignIrRegularSlidingWindow) {
    // note we model tumbling windows as sliding windows with same size and slide.
    auto windowAssigner = SlidingWindowAssigner(100, 30);

    // Slice 1
    ASSERT_EQ(windowAssigner.getSliceStartTs(0), 0);
    ASSERT_EQ(windowAssigner.getSliceEndTs(0), 30);
    ASSERT_EQ(windowAssigner.getSliceStartTs(29), 0);
    ASSERT_EQ(windowAssigner.getSliceEndTs(29), 30);
    // Slice 2
    ASSERT_EQ(windowAssigner.getSliceStartTs(30), 30);
    ASSERT_EQ(windowAssigner.getSliceEndTs(30), 60);
    ASSERT_EQ(windowAssigner.getSliceStartTs(59), 30);
    ASSERT_EQ(windowAssigner.getSliceEndTs(59), 60);
    // Slice 3
    ASSERT_EQ(windowAssigner.getSliceStartTs(65), 60);
    ASSERT_EQ(windowAssigner.getSliceEndTs(65), 90);
    // Slice 4
    ASSERT_EQ(windowAssigner.getSliceStartTs(95), 90);
    ASSERT_EQ(windowAssigner.getSliceEndTs(95), 100);
    // Slice 5
    ASSERT_EQ(windowAssigner.getSliceStartTs(105), 100);
    ASSERT_EQ(windowAssigner.getSliceEndTs(105), 120);
    // Slice 6
    ASSERT_EQ(windowAssigner.getSliceStartTs(120), 120);
    ASSERT_EQ(windowAssigner.getSliceEndTs(120), 130);
    // Slice 7
    ASSERT_EQ(windowAssigner.getSliceStartTs(130), 130);
    ASSERT_EQ(windowAssigner.getSliceEndTs(130), 150);
    // Slice 8
    ASSERT_EQ(windowAssigner.getSliceStartTs(150), 150);
    ASSERT_EQ(windowAssigner.getSliceEndTs(150), 160);
    // Slice 9
    ASSERT_EQ(windowAssigner.getSliceStartTs(160), 160);
    ASSERT_EQ(windowAssigner.getSliceEndTs(160), 180);
    // Slice 10
    ASSERT_EQ(windowAssigner.getSliceStartTs(180), 180);
    ASSERT_EQ(windowAssigner.getSliceEndTs(180), 190);
}

}// namespace NES::Windowing::Experimental