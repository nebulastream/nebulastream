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

#include <V4L2Source.hpp>

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <ostream>
#include <ranges>
#include <stop_token>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

#include <fcntl.h>
#include <poll.h>
#include <unistd.h>
#include <linux/videodev2.h>
#include <sys/ioctl.h>
#include <sys/mman.h>

#include <DataTypes/DataType.hpp>
#include <Identifiers/Identifier.hpp>
#include <Interface/VariableSizedAccess.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Ranges.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{
struct VideoTuple
{
    uint64_t timestamp;
    uint64_t width;
    uint64_t height;
    uint64_t pixelFormat;
    VariableSizedAccess image;
};

struct VideoField
{
    std::string_view name;
    DataType::Type type;
    std::string_view typeName;
};

constexpr std::array videoFields{
    VideoField{.name = "TIMESTAMP", .type = DataType::Type::UINT64, .typeName = "UINT64"},
    VideoField{.name = "WIDTH", .type = DataType::Type::UINT64, .typeName = "UINT64"},
    VideoField{.name = "HEIGHT", .type = DataType::Type::UINT64, .typeName = "UINT64"},
    VideoField{.name = "PIXEL_FORMAT", .type = DataType::Type::UINT64, .typeName = "UINT64"},
    VideoField{.name = "IMAGE", .type = DataType::Type::VARSIZED, .typeName = "VARSIZED"},
};

uint32_t parsePixelFormat(const std::string_view format)
{
    PRECONDITION(format.size() == 4, "V4L2 pixel format must contain four characters");
    return v4l2_fourcc(format[0], format[1], format[2], format[3]);
}

std::string pixelFormatToString(const uint32_t format)
{
    return std::string{
        static_cast<char>(format & 0xFF),
        static_cast<char>((format >> 8) & 0xFF),
        static_cast<char>((format >> 16) & 0xFF),
        static_cast<char>((format >> 24) & 0xFF)};
}

int callIoctl(const int device, const unsigned long request, void* argument)
{
    int result = 0;
    do
    {
        result = ::ioctl(device, request, argument);
    } while (result == -1 && errno == EINTR);
    return result;
}

void checkedIoctl(const int device, const unsigned long request, void* argument, const std::string_view operation)
{
    if (callIoctl(device, request, argument) == -1)
    {
        throw CannotOpenSource("V4L2 source failed while {}: {}", operation, std::strerror(errno));
    }
}

void validateSchema(const SourceDescriptor& sourceDescriptor)
{
    if (sourceDescriptor.getInputFormatType() != "NATIVE")
    {
        throw CannotOpenSource("V4L2 source requires the NATIVE input formatter");
    }

    const auto schema = sourceDescriptor.getLogicalSource().getSchema();
    if (schema->size() != videoFields.size())
    {
        throw CannotOpenSource("V4L2 source expects {} non-nullable fields, but got {}", videoFields.size(), schema->size());
    }

    for (const auto [index, expected] : videoFields | views::enumerate)
    {
        const auto actual = (*schema)[index];
        INVARIANT(actual.has_value(), "V4L2 source schema field {} is missing", index);
        const auto& actualName = static_cast<const Identifier&>(actual->getFullyQualifiedName());
        if (actualName != Identifier::parse(std::string{expected.name}) || actual->getDataType().type != expected.type
            || actual->getDataType().nullable)
        {
            throw CannotOpenSource(
                "V4L2 source field {} must be non-nullable {} in tuple position {}, but got {}",
                expected.name,
                expected.typeName,
                index,
                actual->getFullyQualifiedName());
        }
    }
}

uint64_t captureTimestampInNanoseconds()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}
}

V4L2Source::V4L2Source(const SourceDescriptor& sourceDescriptor)
    : devicePath(sourceDescriptor.getFromConfig(ConfigParametersV4L2::DEVICE))
    , requestedWidth(sourceDescriptor.getFromConfig(ConfigParametersV4L2::WIDTH))
    , requestedHeight(sourceDescriptor.getFromConfig(ConfigParametersV4L2::HEIGHT))
    , requestedFrameRate(sourceDescriptor.getFromConfig(ConfigParametersV4L2::FRAME_RATE))
    , requestedPixelFormat(parsePixelFormat(sourceDescriptor.getFromConfig(ConfigParametersV4L2::PIXEL_FORMAT)))
    , requestedBufferCount(sourceDescriptor.getFromConfig(ConfigParametersV4L2::BUFFER_COUNT))
    , pollTimeoutMs(sourceDescriptor.getFromConfig(ConfigParametersV4L2::POLL_TIMEOUT_MS))
{
    validateSchema(sourceDescriptor);
}

V4L2Source::~V4L2Source()
{
    close();
}

void V4L2Source::open(std::shared_ptr<AbstractBufferProvider> provider)
{
    PRECONDITION(provider, "V4L2 source requires a buffer provider");
    PRECONDITION(device == -1, "V4L2 source is already open");
    bufferProvider = std::move(provider);

    try
    {
        device = ::open(devicePath.c_str(), O_RDWR | O_NONBLOCK | O_CLOEXEC);
        if (device == -1)
        {
            throw CannotOpenSource("Could not open V4L2 device '{}': {}", devicePath, std::strerror(errno));
        }

        v4l2_capability capabilities{};
        checkedIoctl(device, VIDIOC_QUERYCAP, &capabilities, "querying device capabilities");
        const auto deviceCapabilities
            = (capabilities.capabilities & V4L2_CAP_DEVICE_CAPS) ? capabilities.device_caps : capabilities.capabilities;
        if (!(deviceCapabilities & V4L2_CAP_VIDEO_CAPTURE) || !(deviceCapabilities & V4L2_CAP_STREAMING))
        {
            throw CannotOpenSource("V4L2 device '{}' does not support streaming video capture", devicePath);
        }

        v4l2_format format{};
        format.type = V4L2_BUF_TYPE_VIDEO_CAPTURE;
        format.fmt.pix.width = requestedWidth;
        format.fmt.pix.height = requestedHeight;
        format.fmt.pix.pixelformat = requestedPixelFormat;
        format.fmt.pix.field = V4L2_FIELD_ANY;
        checkedIoctl(device, VIDIOC_S_FMT, &format, "negotiating frame format");
        negotiatedWidth = format.fmt.pix.width;
        negotiatedHeight = format.fmt.pix.height;
        negotiatedPixelFormat = format.fmt.pix.pixelformat;

        v4l2_streamparm parameters{};
        parameters.type = V4L2_BUF_TYPE_VIDEO_CAPTURE;
        parameters.parm.capture.timeperframe.numerator = 1;
        parameters.parm.capture.timeperframe.denominator = requestedFrameRate;
        if (requestedFrameRate > 0 && callIoctl(device, VIDIOC_S_PARM, &parameters) == -1)
        {
            NES_WARNING("V4L2 device '{}' did not accept frame rate {}: {}", devicePath, requestedFrameRate, std::strerror(errno));
        }

        v4l2_requestbuffers request{};
        request.count = requestedBufferCount;
        request.type = V4L2_BUF_TYPE_VIDEO_CAPTURE;
        request.memory = V4L2_MEMORY_MMAP;
        checkedIoctl(device, VIDIOC_REQBUFS, &request, "requesting memory-mapped capture buffers");
        if (request.count < 2)
        {
            throw CannotOpenSource("V4L2 device '{}' provided only {} capture buffers", devicePath, request.count);
        }

        mappedBuffers.reserve(request.count);
        for (uint32_t index = 0; index < request.count; ++index)
        {
            v4l2_buffer buffer{};
            buffer.type = V4L2_BUF_TYPE_VIDEO_CAPTURE;
            buffer.memory = V4L2_MEMORY_MMAP;
            buffer.index = index;
            checkedIoctl(device, VIDIOC_QUERYBUF, &buffer, "querying capture buffer");
            void* const mapping = ::mmap(nullptr, buffer.length, PROT_READ | PROT_WRITE, MAP_SHARED, device, buffer.m.offset);
            if (mapping == MAP_FAILED)
            {
                throw CannotOpenSource("V4L2 source failed while mapping capture buffer: {}", std::strerror(errno));
            }
            mappedBuffers.push_back(MappedBuffer{.data = mapping, .size = buffer.length});
            checkedIoctl(device, VIDIOC_QBUF, &buffer, "queueing capture buffer");
        }

        auto type = v4l2_buf_type{V4L2_BUF_TYPE_VIDEO_CAPTURE};
        checkedIoctl(device, VIDIOC_STREAMON, &type, "starting video capture");
        streaming = true;
        NES_INFO(
            "Opened V4L2 device '{}' at {}x{}, pixel format '{}', requested {}x{} '{}' at {} FPS",
            devicePath,
            negotiatedWidth,
            negotiatedHeight,
            pixelFormatToString(negotiatedPixelFormat),
            requestedWidth,
            requestedHeight,
            pixelFormatToString(requestedPixelFormat),
            requestedFrameRate);
    }
    catch (...)
    {
        close();
        throw;
    }
}

Source::FillTupleBufferResult V4L2Source::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken)
{
    PRECONDITION(device != -1 && streaming, "V4L2 source was not opened");
    PRECONDITION(bufferProvider, "V4L2 source has no buffer provider");
    PRECONDITION(tupleBuffer.getBufferSize() >= sizeof(VideoTuple), "Video tuple does not fit into tuple buffer");

    constexpr auto stopCheckIntervalMs = 100U;
    while (!stopToken.stop_requested())
    {
        pollfd descriptor{.fd = device, .events = POLLIN, .revents = 0};
        const auto waitMs = static_cast<int>(std::min(pollTimeoutMs, stopCheckIntervalMs));
        const auto pollResult = ::poll(&descriptor, 1, waitMs);
        if (pollResult == -1 && errno == EINTR)
        {
            continue;
        }
        if (pollResult == -1)
        {
            throw CannotOpenSource("V4L2 source failed while waiting for a frame: {}", std::strerror(errno));
        }
        if (pollResult == 0)
        {
            continue;
        }
        if (descriptor.revents & (POLLERR | POLLHUP | POLLNVAL))
        {
            throw CannotOpenSource("V4L2 device '{}' reported poll error flags {}", devicePath, descriptor.revents);
        }

        v4l2_buffer buffer{};
        buffer.type = V4L2_BUF_TYPE_VIDEO_CAPTURE;
        buffer.memory = V4L2_MEMORY_MMAP;
        if (callIoctl(device, VIDIOC_DQBUF, &buffer) == -1)
        {
            if (errno == EAGAIN)
            {
                continue;
            }
            throw CannotOpenSource("V4L2 source failed while dequeuing a frame: {}", std::strerror(errno));
        }

        try
        {
            if (buffer.index >= mappedBuffers.size() || buffer.bytesused > mappedBuffers[buffer.index].size)
            {
                throw CannotOpenSource("V4L2 device '{}' returned an invalid capture buffer", devicePath);
            }
            auto childBuffer = bufferProvider->getUnpooledBuffer(buffer.bytesused);
            if (!childBuffer.has_value())
            {
                throw CannotOpenSource("Could not allocate {} bytes for a V4L2 frame", buffer.bytesused);
            }
            std::memcpy(childBuffer->getAvailableMemoryArea().data(), mappedBuffers[buffer.index].data, buffer.bytesused);

            const VideoTuple tuple{
                .timestamp = captureTimestampInNanoseconds(),
                .width = negotiatedWidth,
                .height = negotiatedHeight,
                .pixelFormat = negotiatedPixelFormat,
                .image = VariableSizedAccess{tupleBuffer.storeChildBuffer(*childBuffer), VariableSizedAccess::Size{buffer.bytesused}}};
            std::memcpy(tupleBuffer.getAvailableMemoryArea().data(), &tuple, sizeof(tuple));
        }
        catch (...)
        {
            checkedIoctl(device, VIDIOC_QBUF, &buffer, "requeueing capture buffer after an error");
            throw;
        }
        checkedIoctl(device, VIDIOC_QBUF, &buffer, "requeueing capture buffer");
        return FillTupleBufferResult::withBytes(1);
    }
    return FillTupleBufferResult::eos();
}

void V4L2Source::close()
{
    if (device != -1 && streaming)
    {
        auto type = v4l2_buf_type{V4L2_BUF_TYPE_VIDEO_CAPTURE};
        if (callIoctl(device, VIDIOC_STREAMOFF, &type) == -1)
        {
            NES_WARNING("Could not stop V4L2 device '{}': {}", devicePath, std::strerror(errno));
        }
        streaming = false;
    }
    for (const auto& buffer : mappedBuffers)
    {
        if (::munmap(buffer.data, buffer.size) == -1)
        {
            NES_WARNING("Could not unmap a V4L2 capture buffer: {}", std::strerror(errno));
        }
    }
    mappedBuffers.clear();
    if (device != -1)
    {
        if (::close(device) == -1)
        {
            NES_WARNING("Could not close V4L2 device '{}': {}", devicePath, std::strerror(errno));
        }
        device = -1;
    }
    bufferProvider.reset();
}

DescriptorConfig::Config V4L2Source::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersV4L2>(std::move(config), NAME);
}

std::ostream& V4L2Source::toString(std::ostream& stream) const
{
    return stream << "V4L2Source(device=" << devicePath << ", requested=" << requestedWidth << 'x' << requestedHeight
                  << ", pixelFormat=" << pixelFormatToString(requestedPixelFormat) << ", frameRate=" << requestedFrameRate << ')';
}

}
