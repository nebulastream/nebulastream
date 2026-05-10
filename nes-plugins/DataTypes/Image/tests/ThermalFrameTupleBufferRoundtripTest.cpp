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
#include <memory>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Nautilus/DataTypes/FixedSizedData.hpp>
#include <Nautilus/DataTypes/StructData.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <nautilus/val.hpp>
#include <BaseUnitTest.hpp>

namespace NES
{

class ThermalFrameTupleBufferRoundtripTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("ThermalFrameTupleBufferRoundtripTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup ThermalFrameTupleBufferRoundtripTest class.");
    }
    static void TearDownTestSuite() { NES_INFO("Tear down ThermalFrameTupleBufferRoundtripTest class."); }

    void SetUp() override
    {
        Testing::BaseUnitTest::SetUp();
        bufferManager = BufferManager::create();
    }

    std::shared_ptr<BufferManager> bufferManager;
};

namespace
{
/// Builds the same `STRUCT { pixels: FIXEDSIZED<UINT16, count> }` layout the
/// `RegisterThermalFrameDataType` registrar produces. Defining it here too
/// keeps the test independent of the plugin's registration order.
DataType thermalFrameDataType(uint32_t pixelCount)
{
    DataType pixels{DataType::Type::FIXEDSIZED, DataType::NULLABLE::NOT_NULLABLE, DataType::Type::UINT16, pixelCount};
    std::vector<std::pair<std::string, DataType>> fields;
    fields.emplace_back("pixels", pixels);
    return DataType{DataType::Type::STRUCT, DataType::NULLABLE::NOT_NULLABLE, std::string{"ThermalFrame"}, std::move(fields)};
}
}

/// End-to-end: store a ThermalFrame as a STRUCT field, read it back through
/// `TupleBufferRef`, confirm pixel values survive byte-for-byte. This is the
/// integration the in-place STRUCT layout was designed for.
TEST_F(ThermalFrameTupleBufferRoundtripTest, RoundtripsThermalFrameThroughRowTupleBuffer)
{
    constexpr uint32_t pixelCount = 16;
    const auto frameType = thermalFrameDataType(pixelCount);
    const auto schema = Schema().addField("frame", frameType);

    auto buffer = bufferManager->getBufferBlocking();
    auto bufRef = LowerSchemaProvider::lowerSchema(buffer.getBufferSize(), schema, MemoryLayoutType::ROW_LAYOUT);

    /// Source pixels — chosen to look like real thermal samples (mono16 raw codes).
    std::array<uint16_t, pixelCount> srcPixels
        = {30100, 30180, 30220, 30150, 30210, 41900, 52400, 30260, 30230, 53100, 65535, 30290, 30170, 30240, 30205, 30130};
    const ThermalFrameData srcFrame(nautilus::val<int8_t*>(reinterpret_cast<int8_t*>(srcPixels.data())), pixelCount);

    /// Write tuple 0 with the frame.
    {
        auto recordIdx = nautilus::val<uint64_t>(0);
        auto bufPtr = nautilus::val<TupleBuffer*>(std::addressof(buffer));
        const RecordBuffer recordBuffer(bufPtr);
        auto bufProviderVal = nautilus::val<AbstractBufferProvider*>(bufferManager.get());

        Record record;
        record.write("frame", VarVal{srcFrame.asStructData()});
        const auto result = bufRef->writeRecord(recordIdx, recordBuffer, record, bufProviderVal);
        ASSERT_TRUE(static_cast<bool>(result.successful));
        buffer.setNumberOfTuples(buffer.getNumberOfTuples() + 1);
    }

    /// Read tuple 0 back.
    auto recordIdx = nautilus::val<uint64_t>(0);
    auto bufPtr = nautilus::val<TupleBuffer*>(std::addressof(buffer));
    const RecordBuffer recordBuffer(bufPtr);

    auto record = bufRef->readRecord({"frame"}, recordBuffer, recordIdx);
    const auto& varVal = record.read("frame");
    const auto readStruct = varVal.getRawValueAs<StructData>();

    ASSERT_EQ(readStruct.getNumFields(), 1U);
    ASSERT_EQ(readStruct.getFields()[0].first, "pixels");

    const auto pixelsView = readStruct.at("pixels").getRawValueAs<FixedSizedData>();
    ASSERT_EQ(pixelsView.getNumElements(), pixelCount);
    EXPECT_EQ(pixelsView.getElementType(), DataType::Type::UINT16);
    for (uint64_t i = 0; i < srcPixels.size(); ++i)
    {
        EXPECT_EQ(pixelsView.at(nautilus::val<uint64_t>(i)).getRawValueAs<nautilus::val<uint16_t>>(), srcPixels[i])
            << "pixel mismatch at index " << i;
    }
}

}
