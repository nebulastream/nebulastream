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

#include <V4L2Frame.hpp>
#include <V4L2Sink.hpp>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Interface/VariableSizedAccess.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Schema/Schema.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <gtest/gtest.h>
#include <linux/videodev2.h>
#include <BackpressureChannel.hpp>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{
using Frame = detail::V4L2FrameTuple;

Schema<UnqualifiedUnboundField, Ordered> frameSchema(const bool nullableImage = false)
{
    return {
        UnqualifiedUnboundField{Identifier::parse("TIMESTAMP"), DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}},
        UnqualifiedUnboundField{Identifier::parse("WIDTH"), DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}},
        UnqualifiedUnboundField{Identifier::parse("HEIGHT"), DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}},
        UnqualifiedUnboundField{Identifier::parse("PIXEL_FORMAT"), DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}},
        UnqualifiedUnboundField{
            Identifier::parse("IMAGE"),
            DataType{DataType::Type::VARSIZED, nullableImage ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE}}};
}

SinkDescriptor descriptor(const Schema<UnqualifiedUnboundField, Ordered>& schema, const std::string& format = "NATIVE")
{
    SinkCatalog catalog;
    return catalog
        .addSinkDescriptor(
            Identifier::parse("CAMERA"),
            schema,
            Identifier::parse("V4L2"),
            Host{"localhost"},
            {{Identifier::parse("output_format"), format}},
            {})
        .value();
}
}

class V4L2SinkTest : public Testing::BaseUnitTest
{
protected:
    std::shared_ptr<BufferManager> buffers
        = BufferManager::create(16384, 0.5, BufferAlignment{64}, 4096, std::make_shared<NesDefaultMemoryAllocator>());
};

TEST_F(V4L2SinkTest, AcceptsSourceSchemaAndDefaultNativeFormat)
{
    auto channel = createBackpressureChannel();
    EXPECT_NO_THROW(V4L2Sink(std::move(channel.first), descriptor(frameSchema())));
    auto defaultChannel = createBackpressureChannel();
    SinkCatalog catalog;
    const auto sink
        = catalog.addSinkDescriptor(Identifier::parse("CAMERA"), frameSchema(), Identifier::parse("V4L2"), Host{"localhost"}, {}, {})
              .value();
    EXPECT_NO_THROW(V4L2Sink(std::move(defaultChannel.first), sink));
}

TEST_F(V4L2SinkTest, RejectsTextOutputAndNullableFrames)
{
    auto textChannel = createBackpressureChannel();
    EXPECT_THROW(V4L2Sink(std::move(textChannel.first), descriptor(frameSchema(), "CSV")), Exception);
    auto nullableChannel = createBackpressureChannel();
    EXPECT_THROW(V4L2Sink(std::move(nullableChannel.first), descriptor(frameSchema(true))), Exception);
}

TEST_F(V4L2SinkTest, RejectsWrongSchemaOrder)
{
    auto schema = frameSchema();
    const Schema<UnqualifiedUnboundField, Ordered> reordered{*schema[1], *schema[0], *schema[2], *schema[3], *schema[4]};
    auto channel = createBackpressureChannel();
    EXPECT_THROW(V4L2Sink(std::move(channel.first), descriptor(reordered)), Exception);
}

TEST_F(V4L2SinkTest, ValidatesConfiguration)
{
    const auto defaults = V4L2Sink::validateAndFormat({});
    EXPECT_EQ(std::get<std::string>(defaults.at("DEVICE")), "/dev/video10");
    EXPECT_THROW(V4L2Sink::validateAndFormat({{"DEVICE", ""}}), Exception);
    EXPECT_THROW(V4L2Sink::validateAndFormat({{"FRAME_RATE", "0"}}), Exception);
    EXPECT_THROW(V4L2Sink::validateAndFormat({{"POLL_TIMEOUT_MS", "0"}}), Exception);
    EXPECT_THROW(V4L2Sink::validateAndFormat({{"POLL_TIMEOUT_MS", "2147483648"}}), Exception);
    EXPECT_THROW(V4L2Sink::validateAndFormat({{"UNKNOWN", "1"}}), Exception);
}

TEST_F(V4L2SinkTest, ReadsMultipleFramesWithChildBufferOffsets)
{
    auto parent = buffers->getBufferBlocking();
    auto child = buffers->getBufferBlocking();
    const std::array<uint8_t, 8> bytes{99, 1, 2, 3, 4, 5, 6, 99};
    std::memcpy(child.getAvailableMemoryArea().data(), bytes.data(), bytes.size());
    const auto childIndex = parent.storeChildBuffer(child);
    const std::array frames{
        Frame{
            .timestamp = 1,
            .width = 1,
            .height = 1,
            .pixelFormat = V4L2_PIX_FMT_RGB24,
            .image = VariableSizedAccess{childIndex, VariableSizedAccess::Offset{1}, VariableSizedAccess::Size{3}}},
        Frame{
            .timestamp = 2,
            .width = 1,
            .height = 1,
            .pixelFormat = V4L2_PIX_FMT_RGB24,
            .image = VariableSizedAccess{childIndex, VariableSizedAccess::Offset{4}, VariableSizedAccess::Size{3}}}};
    std::memcpy(parent.getAvailableMemoryArea().data(), frames.data(), sizeof(frames));
    parent.setNumberOfTuples(frames.size());

    for (size_t index = 0; index < frames.size(); ++index)
    {
        const auto frame = detail::readV4L2Frame(parent, index);
        EXPECT_EQ(frame.timestamp, index + 1);
        const auto image = detail::getV4L2Image(parent, frame.image);
        ASSERT_EQ(image.size(), 3);
        EXPECT_EQ(std::memcmp(image.data(), bytes.data() + 1 + index * 3, 3), 0);
    }
}

TEST_F(V4L2SinkTest, RejectsInvalidImageReferences)
{
    auto parent = buffers->getBufferBlocking();
    EXPECT_THROW(detail::getV4L2Image(parent, VariableSizedAccess{ChildBufferIndex{0}, VariableSizedAccess::Size{1}}), Exception);
    auto child = buffers->getBufferBlocking();
    const auto childSize = child.getBufferSize();
    const auto childIndex = parent.storeChildBuffer(child);
    EXPECT_THROW(detail::getV4L2Image(parent, VariableSizedAccess{childIndex, VariableSizedAccess::Size{0}}), Exception);
    EXPECT_THROW(
        detail::getV4L2Image(
            parent, VariableSizedAccess{childIndex, VariableSizedAccess::Offset{childSize - 1}, VariableSizedAccess::Size{2}}),
        Exception);
    EXPECT_THROW(
        detail::getV4L2Image(
            parent, VariableSizedAccess{childIndex, VariableSizedAccess::Offset{childSize + 1}, VariableSizedAccess::Size{1}}),
        Exception);
    EXPECT_THROW(
        detail::getV4L2Image(
            parent,
            VariableSizedAccess{
                childIndex, VariableSizedAccess::Offset{1}, VariableSizedAccess::Size{std::numeric_limits<uint64_t>::max()}}),
        Exception);
}

TEST_F(V4L2SinkTest, RejectsInvalidFrameMetadataAndTupleCounts)
{
    auto parent = buffers->getBufferBlocking();
    parent.setNumberOfTuples(parent.getBufferSize() / sizeof(Frame) + 1);
    EXPECT_THROW(detail::readV4L2Frame(parent, 0), Exception);

    parent.setNumberOfTuples(1);
    Frame frame{.timestamp = 0, .width = 0, .height = 240, .pixelFormat = V4L2_PIX_FMT_YUYV, .image = {}};
    std::memcpy(parent.getAvailableMemoryArea().data(), &frame, sizeof(frame));
    EXPECT_THROW(detail::readV4L2Frame(parent, 0), Exception);
    frame.width = uint64_t{1} << 32;
    std::memcpy(parent.getAvailableMemoryArea().data(), &frame, sizeof(frame));
    EXPECT_THROW(detail::readV4L2Frame(parent, 0), Exception);
    EXPECT_THROW(detail::readV4L2Frame(parent, 1), Exception);
}

}
