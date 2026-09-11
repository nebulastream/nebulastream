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

#include <V4L2Sink.hpp>

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <exception>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <variant>
#include <fcntl.h>
#include <poll.h>
#include <unistd.h>
#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Strings.hpp>
#include <fmt/format.h>
#include <linux/videodev2.h>
#include <sys/ioctl.h>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <V4L2Frame.hpp>

namespace NES
{
namespace
{
/// Diagnostics must not interrupt frame delivery or device cleanup.
template <typename... Args>
void logStderr(fmt::format_string<Args...> format, Args&&... args) noexcept
{
    try
    {
        fmt::print(stderr, format, std::forward<Args>(args)...);
        std::fflush(stderr);
    }
    catch (...)
    {
    }
}

std::string pixelFormatName(const uint32_t format)
{
    return {
        static_cast<char>(format & 0xFF),
        static_cast<char>((format >> 8) & 0xFF),
        static_cast<char>((format >> 16) & 0xFF),
        static_cast<char>((format >> 24) & 0xFF)};
}

void logFrameStatistics(
    const std::string& devicePath, const uint64_t frameNumber, const detail::V4L2FrameTuple& frame, const std::span<const uint8_t> image)
{
    const auto [minimum, maximum] = std::minmax_element(image.begin(), image.end());
    logStderr(
        "[V4L2Sink] device={} received frame={} timestamp={} size={}x{} format={} bytes={} byte_min={} byte_max={}\n",
        devicePath,
        frameNumber,
        frame.timestamp,
        frame.width,
        frame.height,
        pixelFormatName(static_cast<uint32_t>(frame.pixelFormat)),
        image.size(),
        *minimum,
        *maximum);

    /// Inspect tightly packed YUYV, as emitted by the face-alignment query.
    /// Do not include row padding in the brightness statistics.
    if (frame.pixelFormat == V4L2_PIX_FMT_YUYV && image.size() / 2 / frame.height == frame.width && image.size() % (2 * frame.height) == 0)
    {
        uint8_t minimumY = 255;
        uint8_t maximumY = 0;
        uint64_t sumY = 0;
        bool neutralChroma = true;
        for (size_t offset = 0; offset < image.size(); offset += 2)
        {
            minimumY = std::min(minimumY, image[offset]);
            maximumY = std::max(maximumY, image[offset]);
            sumY += image[offset];
            neutralChroma = neutralChroma && image[offset + 1] == 128;
        }
        logStderr(
            "[V4L2Sink] device={} frame={} Y_min={} Y_max={} Y_mean={:.2f} neutral_chroma={} uniform_black={}\n",
            devicePath,
            frameNumber,
            minimumY,
            maximumY,
            static_cast<double>(sumY) / (image.size() / 2),
            neutralChroma,
            minimumY == 16 && maximumY == 16 && neutralChroma);
    }
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
        const auto error = errno;
        logStderr("[V4L2Sink] fd={} failed while {}: {}\n", device, operation, std::strerror(error));
        throw CannotOpenSink("V4L2 sink failed while {}: {}", operation, std::strerror(error));
    }
}

void validateSchema(const SinkDescriptor& descriptor)
{
    if (toUpperCase(descriptor.getFormatType()) != "NATIVE")
    {
        throw CannotOpenSink("V4L2 sink requires the NATIVE output format");
    }
    const auto schemaVariant = descriptor.getSchema();
    const auto* schema = std::get_if<std::shared_ptr<const Schema<UnqualifiedUnboundField, Ordered>>>(&schemaVariant);
    constexpr std::array<std::string_view, 5> names{"TIMESTAMP", "WIDTH", "HEIGHT", "PIXEL_FORMAT", "IMAGE"};
    if (!schema || !*schema || (*schema)->size() != names.size())
    {
        throw CannotOpenSink("V4L2 sink expects the ordered schema TIMESTAMP, WIDTH, HEIGHT, PIXEL_FORMAT, IMAGE");
    }
    for (size_t index = 0; index < names.size(); ++index)
    {
        const auto field = (**schema)[index];
        const auto expectedType = index == names.size() - 1 ? DataType::Type::VARSIZED : DataType::Type::UINT64;
        if (!field || static_cast<const Identifier&>(field->getFullyQualifiedName()) != Identifier::parse(std::string{names[index]})
            || field->getDataType().type != expectedType || field->getDataType().nullable)
        {
            throw CannotOpenSink(
                "V4L2 sink field {} must be non-nullable {} in tuple position {}",
                names[index],
                index == names.size() - 1 ? "VARSIZED" : "UINT64",
                index);
        }
    }
}
}

V4L2Sink::V4L2Sink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor)
    : Sink(std::move(backpressureController))
    , devicePath(sinkDescriptor.getFromConfig(ConfigParametersV4L2Sink::DEVICE))
    , frameRate(sinkDescriptor.getFromConfig(ConfigParametersV4L2Sink::FRAME_RATE))
    , pollTimeoutMs(sinkDescriptor.getFromConfig(ConfigParametersV4L2Sink::POLL_TIMEOUT_MS))
{
    validateSchema(sinkDescriptor);
}

V4L2Sink::~V4L2Sink()
{
    close();
}

void V4L2Sink::start(PipelineExecutionContext&)
{
    const std::scoped_lock lock(mutex);
    PRECONDITION(device == -1, "V4L2 sink is already open");
    framesReceived = 0;
    framesWritten = 0;
    framesDropped = 0;
    lastWrittenTimestamp.reset();
    lastFrameLog = {};
    lastDropLog = {};
    logStderr("[V4L2Sink] device={} opening frame_rate={} poll_timeout_ms={}\n", devicePath, frameRate, pollTimeoutMs);
    device = ::open(devicePath.c_str(), O_RDWR | O_NONBLOCK | O_CLOEXEC);
    if (device == -1)
    {
        const auto error = errno;
        logStderr("[V4L2Sink] device={} open failed: {}\n", devicePath, std::strerror(error));
        throw CannotOpenSink(
            "Could not open V4L2 output device '{}': {}. Create a v4l2loopback device and grant the worker write access.",
            devicePath,
            std::strerror(error));
    }
    try
    {
        v4l2_capability capabilities{};
        checkedIoctl(device, VIDIOC_QUERYCAP, &capabilities, "querying output capabilities");
        const auto flags = (capabilities.capabilities & V4L2_CAP_DEVICE_CAPS) ? capabilities.device_caps : capabilities.capabilities;
        if (!(flags & V4L2_CAP_VIDEO_OUTPUT) || !(flags & V4L2_CAP_READWRITE))
        {
            throw CannotOpenSink(
                "V4L2 device '{}' must support video output and write() I/O; use an available v4l2loopback output device", devicePath);
        }
        logStderr("[V4L2Sink] device={} opened fd={} capabilities=0x{:08x}; waiting for first frame\n", devicePath, device, flags);
    }
    catch (...)
    {
        close();
        throw;
    }
}

void V4L2Sink::configureFormat(const uint32_t width, const uint32_t height, const uint32_t pixelFormat)
{
    /// Enumerate to distinguish fixed-size raw frames from variable-size encoded images.
    v4l2_fmtdesc description{};
    description.type = V4L2_BUF_TYPE_VIDEO_OUTPUT;
    while (true)
    {
        if (callIoctl(device, VIDIOC_ENUM_FMT, &description) == -1)
        {
            if (errno == EINVAL)
            {
                throw CannotOpenSink(
                    "V4L2 device '{}' does not support PIXEL_FORMAT {}. Convert frames to a supported format, such as YUYV.",
                    devicePath,
                    pixelFormat);
            }
            throw CannotOpenSink("Could not enumerate V4L2 output formats: {}", std::strerror(errno));
        }
        if (description.pixelformat == pixelFormat)
        {
            break;
        }
        ++description.index;
    }

    v4l2_format format{};
    format.type = V4L2_BUF_TYPE_VIDEO_OUTPUT;
    format.fmt.pix.width = width;
    format.fmt.pix.height = height;
    format.fmt.pix.pixelformat = pixelFormat;
    format.fmt.pix.field = V4L2_FIELD_NONE;
    checkedIoctl(device, VIDIOC_S_FMT, &format, "setting output format");
    const auto setFormatSize = format.fmt.pix.sizeimage;
    /// Some v4l2loopback versions return their page-aligned allocation size from
    /// S_FMT (e.g. 28672 for a 25088-byte frame). G_FMT returns the actual image
    /// layout, including row padding, which is the size required for write().
    format = {};
    format.type = V4L2_BUF_TYPE_VIDEO_OUTPUT;
    checkedIoctl(device, VIDIOC_G_FMT, &format, "reading negotiated output format");
    logStderr(
        "[V4L2Sink] device={} requested={}x{} {} negotiated={}x{} {} bytes_per_line={} image_bytes={} S_FMT_bytes={}\n",
        devicePath,
        width,
        height,
        pixelFormatName(pixelFormat),
        format.fmt.pix.width,
        format.fmt.pix.height,
        pixelFormatName(format.fmt.pix.pixelformat),
        format.fmt.pix.bytesperline,
        format.fmt.pix.sizeimage,
        setFormatSize);
    if (format.fmt.pix.width != width || format.fmt.pix.height != height || format.fmt.pix.pixelformat != pixelFormat)
    {
        throw CannotOpenSink(
            "V4L2 device '{}' changed the requested {}x{} format {}; the sink does not resize or convert frames",
            devicePath,
            width,
            height,
            pixelFormat);
    }
    if (format.fmt.pix.sizeimage == 0)
    {
        throw CannotOpenSink("V4L2 device '{}' returned an invalid output frame size", devicePath);
    }

    v4l2_streamparm parameters{};
    parameters.type = V4L2_BUF_TYPE_VIDEO_OUTPUT;
    parameters.parm.output.timeperframe.numerator = 1;
    parameters.parm.output.timeperframe.denominator = frameRate;
    if (callIoctl(device, VIDIOC_S_PARM, &parameters) == -1)
    {
        const auto error = errno;
        logStderr("[V4L2Sink] device={} could not set frame_rate={}: {}\n", devicePath, frameRate, std::strerror(error));
        NES_WARNING("V4L2 output device '{}' did not accept frame rate {}: {}", devicePath, frameRate, std::strerror(error));
    }

    negotiatedWidth = width;
    negotiatedHeight = height;
    negotiatedPixelFormat = pixelFormat;
    frameSize = format.fmt.pix.sizeimage;
    compressed = description.flags & V4L2_FMT_FLAG_COMPRESSED;
    NES_INFO("Opened V4L2 output '{}' at {}x{}, pixel format {}, requested {} FPS", devicePath, width, height, pixelFormat, frameRate);
}

void V4L2Sink::writeFrame(const std::span<const uint8_t> image)
{
    if ((!compressed && image.size() != frameSize) || (compressed && image.size() > frameSize))
    {
        throw CannotOpenSink(
            "V4L2 sink IMAGE has {} bytes; device '{}' expects {} {} bytes. Check pixel format, dimensions, and row stride.",
            image.size(),
            devicePath,
            compressed ? "at most" : "exactly",
            frameSize);
    }

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(pollTimeoutMs);
    while (true)
    {
        const auto written = ::write(device, image.data(), image.size());
        if (written >= 0)
        {
            /// A short write must not be continued as a second frame on loopback devices.
            if (static_cast<size_t>(written) != image.size())
            {
                throw CannotOpenSink("V4L2 device '{}' wrote only {} of {} frame bytes", devicePath, written, image.size());
            }
            return;
        }
        const auto error = errno;
        if (error != EINTR && error != EAGAIN)
        {
            throw CannotOpenSink("Could not write a frame to V4L2 device '{}': {}", devicePath, std::strerror(error));
        }
        const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now());
        if (remaining.count() <= 0)
        {
            throw CannotOpenSink("Timed out writing a frame to V4L2 device '{}'", devicePath);
        }
        if (error == EINTR)
        {
            continue;
        }
        pollfd descriptor{.fd = device, .events = POLLOUT, .revents = 0};
        const auto result = ::poll(&descriptor, 1, static_cast<int>(remaining.count()));
        if (result == -1 && errno != EINTR)
        {
            throw CannotOpenSink("Could not wait for V4L2 output device '{}': {}", devicePath, std::strerror(errno));
        }
        if (result > 0 && (descriptor.revents & (POLLERR | POLLHUP | POLLNVAL)))
        {
            throw CannotOpenSink("V4L2 output device '{}' reported poll error flags {}", devicePath, descriptor.revents);
        }
    }
}

void V4L2Sink::execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext&)
{
    PRECONDITION(inputTupleBuffer, "V4L2 sink received an invalid buffer");
    const std::scoped_lock lock(mutex);
    PRECONDITION(device != -1, "V4L2 sink was not started");
    try
    {
        for (size_t index = 0; index < inputTupleBuffer.getNumberOfTuples(); ++index)
        {
            ++framesReceived;
            const auto frame = detail::readV4L2Frame(inputTupleBuffer, index);
            const auto now = std::chrono::steady_clock::now();
            if (lastWrittenTimestamp && frame.timestamp < *lastWrittenTimestamp)
            {
                ++framesDropped;
                if (framesDropped <= 3 || now - lastDropLog >= std::chrono::seconds(1))
                {
                    lastDropLog = now;
                    logStderr(
                        "[V4L2Sink] device={} dropped out-of-order frame={} timestamp={} last_written_timestamp={} total_dropped={}\n",
                        devicePath,
                        framesReceived,
                        frame.timestamp,
                        *lastWrittenTimestamp,
                        framesDropped);
                }
                continue;
            }
            const auto image = detail::getV4L2Image(inputTupleBuffer, frame.image);
            const bool logFrame = framesReceived <= 3 || now - lastFrameLog >= std::chrono::seconds(1);
            if (logFrame)
            {
                lastFrameLog = now;
                logFrameStatistics(devicePath, framesReceived, frame, image);
            }
            if (frameSize == 0)
            {
                configureFormat(
                    static_cast<uint32_t>(frame.width), static_cast<uint32_t>(frame.height), static_cast<uint32_t>(frame.pixelFormat));
            }
            if (frame.width != negotiatedWidth || frame.height != negotiatedHeight || frame.pixelFormat != negotiatedPixelFormat)
            {
                throw CannotOpenSink("V4L2 sink WIDTH, HEIGHT, and PIXEL_FORMAT must remain constant for the lifetime of the stream");
            }
            writeFrame(image);
            lastWrittenTimestamp = frame.timestamp;
            ++framesWritten;
            if (logFrame)
            {
                logStderr(
                    "[V4L2Sink] device={} wrote frame={} timestamp={} bytes={} total_written={} total_dropped={}\n",
                    devicePath,
                    framesReceived,
                    frame.timestamp,
                    image.size(),
                    framesWritten,
                    framesDropped);
            }
        }
    }
    catch (const std::exception& exception)
    {
        logStderr(
            "[V4L2Sink] device={} failed received={} written={} dropped={}: {}\n",
            devicePath,
            framesReceived,
            framesWritten,
            framesDropped,
            exception.what());
        throw;
    }
}

void V4L2Sink::stop(PipelineExecutionContext&)
{
    const std::scoped_lock lock(mutex);
    close();
}

void V4L2Sink::close()
{
    if (device != -1)
    {
        logStderr(
            "[V4L2Sink] device={} closing received={} written={} dropped={}\n", devicePath, framesReceived, framesWritten, framesDropped);
        if (::close(device) == -1)
        {
            NES_WARNING("Could not close V4L2 output device '{}': {}", devicePath, std::strerror(errno));
        }
        device = -1;
    }
    frameSize = 0;
}

DescriptorConfig::Config V4L2Sink::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersV4L2Sink>(std::move(config), NAME);
}

std::ostream& V4L2Sink::toString(std::ostream& stream) const
{
    return stream << "V4L2Sink(device=" << devicePath << ", frameRate=" << frameRate << ')';
}

}
