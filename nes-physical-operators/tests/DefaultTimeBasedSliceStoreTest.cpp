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

#include <memory>
#include <vector>
#include <SliceStore/DefaultTimeBasedSliceStore.hpp>
#include <SliceStore/Slice.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>

namespace NES
{

class DefaultTimeBasedSliceStoreTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("DefaultTimeBasedSliceStoreTest.log", LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup DefaultTimeBasedSliceStoreTest class.");
    }
};

TEST_F(DefaultTimeBasedSliceStoreTest, ReturnsAllActiveSlicesInSliceOrder)
{
    DefaultTimeBasedSliceStore sliceStore(/*windowSize=*/10, /*windowSlide=*/5);
    auto createSlice = [](const SliceStart sliceStart, const SliceEnd sliceEnd)
    {
        return std::vector<std::shared_ptr<Slice>>{std::make_shared<Slice>(sliceStart, sliceEnd)};
    };

    sliceStore.getSlicesOrCreate(Timestamp(1), createSlice);
    sliceStore.getSlicesOrCreate(Timestamp(8), createSlice);
    sliceStore.getSlicesOrCreate(Timestamp(14), createSlice);
    sliceStore.getSlicesOrCreate(Timestamp(9), createSlice);

    const auto activeSlices = sliceStore.getActiveSlices();
    ASSERT_EQ(activeSlices.size(), 3U);
    EXPECT_EQ(activeSlices[0]->getSliceStart(), Timestamp(0));
    EXPECT_EQ(activeSlices[0]->getSliceEnd(), Timestamp(5));
    EXPECT_EQ(activeSlices[1]->getSliceStart(), Timestamp(5));
    EXPECT_EQ(activeSlices[1]->getSliceEnd(), Timestamp(10));
    EXPECT_EQ(activeSlices[2]->getSliceStart(), Timestamp(10));
    EXPECT_EQ(activeSlices[2]->getSliceEnd(), Timestamp(15));
}

}
