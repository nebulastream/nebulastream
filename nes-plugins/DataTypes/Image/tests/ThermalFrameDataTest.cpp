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

#include <ThermalFrameData.hpp>

#include <array>
#include <cstdint>
#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/FixedSizedData.hpp>
#include <Nautilus/DataTypes/StructData.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>

namespace NES
{

class ThermalFrameDataTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase()
    {
        Logger::setupLogging("ThermalFrameDataTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup ThermalFrameDataTest class.");
    }
    static void TearDownTestCase() { NES_INFO("Tear down ThermalFrameDataTest class."); }
};

TEST_F(ThermalFrameDataTest, GetPixelCountReturnsConstructorArg)
{
    std::array<uint16_t, 16> pixels{};
    const ThermalFrameData frame(nautilus::val<int8_t*>(reinterpret_cast<int8_t*>(pixels.data())), pixels.size());

    EXPECT_EQ(frame.getPixelCount(), 16U);
}

TEST_F(ThermalFrameDataTest, AsStructDataMatchesRegisteredLayout)
{
    /// The wrapper builds the layout so it should match the type registered by
    /// `RegisterThermalFrameDataType`: a single FIXEDSIZED<UINT16, N> field
    /// named "pixels". Mismatch here means runtime byte reads would drift from
    /// the schema — the most important invariant for the wrapper.
    constexpr uint32_t pixelCount = 8;
    std::array<uint16_t, pixelCount> pixels{};
    const ThermalFrameData frame(nautilus::val<int8_t*>(reinterpret_cast<int8_t*>(pixels.data())), pixelCount);

    const StructData& inner = frame.asStructData();
    ASSERT_EQ(inner.getNumFields(), 1U);
    EXPECT_EQ(inner.getFields()[0].first, "pixels");
    EXPECT_EQ(inner.getFields()[0].second.type, DataType::Type::FIXEDSIZED);
    EXPECT_EQ(inner.getFields()[0].second.elementType, DataType::Type::UINT16);
    EXPECT_EQ(inner.getFields()[0].second.count, pixelCount);
    EXPECT_EQ(inner.getTotalSizeInBytes(), pixelCount * sizeof(uint16_t));
}

TEST_F(ThermalFrameDataTest, GetPixelsReadsThroughFixedSizedView)
{
    /// Mono16 raw values mimicking a 4x4 thermal frame. The wrapper exposes
    /// these via a FixedSizedData; loading by index should read the same
    /// bytes the buffer was constructed from.
    std::array<uint16_t, 16> pixels
        = {30100, 30180, 30220, 30150, 30210, 41900, 52400, 30260, 30230, 53100, 65535, 30290, 30170, 30240, 30205, 30130};
    const ThermalFrameData frame(nautilus::val<int8_t*>(reinterpret_cast<int8_t*>(pixels.data())), pixels.size());

    const FixedSizedData view = frame.getPixels();
    ASSERT_EQ(view.getNumElements(), pixels.size());
    EXPECT_EQ(view.getElementType(), DataType::Type::UINT16);
    EXPECT_EQ(view.getTotalSizeInBytes(), pixels.size() * sizeof(uint16_t));

    for (uint64_t i = 0; i < pixels.size(); ++i)
    {
        EXPECT_EQ(view.at(nautilus::val<uint64_t>(i)).getRawValueAs<nautilus::val<uint16_t>>(), pixels[i]);
    }
}

TEST_F(ThermalFrameDataTest, GetPixelsAliasesUnderlyingBuffer)
{
    /// `getPixels()` should not copy: mutating the underlying buffer must be
    /// visible through the FixedSizedData view returned earlier.
    std::array<uint16_t, 4> pixels = {1, 2, 3, 4};
    const ThermalFrameData frame(nautilus::val<int8_t*>(reinterpret_cast<int8_t*>(pixels.data())), pixels.size());

    const FixedSizedData before = frame.getPixels();
    EXPECT_EQ(before.at(nautilus::val<uint64_t>(2)).getRawValueAs<nautilus::val<uint16_t>>(), 3);

    pixels[2] = 999;
    const FixedSizedData after = frame.getPixels();
    EXPECT_EQ(after.at(nautilus::val<uint64_t>(2)).getRawValueAs<nautilus::val<uint16_t>>(), 999);
}

}
