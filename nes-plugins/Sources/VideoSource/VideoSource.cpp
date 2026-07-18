/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
*/

#include <VideoSource.hpp>

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <utility>

#include <DataTypes/DataType.hpp>
#include <ErrorHandling.hpp>
#include <Identifiers/Identifier.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Ranges.hpp>

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
    VideoField{"TIMESTAMP", DataType::Type::UINT64, "UINT64"},
    VideoField{"WIDTH", DataType::Type::UINT64, "UINT64"},
    VideoField{"HEIGHT", DataType::Type::UINT64, "UINT64"},
    VideoField{"PIXEL_FORMAT", DataType::Type::UINT64, "UINT64"},
    VideoField{"IMAGE", DataType::Type::VARSIZED, "VARSIZED"},
};

void validateSchema(const SourceDescriptor& sourceDescriptor)
{
    const auto schema = sourceDescriptor.getLogicalSource().getSchema();
    if (schema->size() != videoFields.size())
    {
        throw CannotOpenSource("Video source expects {} non-nullable fields, but got {}", videoFields.size(), schema->size());
    }

    for (const auto [index, expected] : videoFields | views::enumerate)
    {
        const auto actual = (*schema)[index];
        INVARIANT(actual.has_value(), "Video source schema field {} is missing", index);
        const auto& actualName = static_cast<const Identifier&>(actual->getFullyQualifiedName());
        if (actualName != Identifier::parse(std::string{expected.name})
            || actual->getDataType().type != expected.type
            || actual->getDataType().nullable)
        {
            throw CannotOpenSource(
                "Video source field {} must be non-nullable {} in tuple position {}, but got {}",
                expected.name,
                expected.typeName,
                index,
                actual->getFullyQualifiedName());
        }
    }
}

class StreamBuffer
{
public:
    explicit StreamBuffer(ArvStream* stream) : stream(stream), buffer(arv_stream_timeout_pop_buffer(stream, 1'000'000)) { }
    StreamBuffer(const StreamBuffer&) = delete;
    StreamBuffer& operator=(const StreamBuffer&) = delete;

    ~StreamBuffer()
    {
        if (buffer)
        {
            arv_stream_push_buffer(stream, buffer);
        }
    }

    [[nodiscard]] ArvBufferStatus status() const { return buffer ? arv_buffer_get_status(buffer) : ARV_BUFFER_STATUS_TIMEOUT; }
    [[nodiscard]] uint64_t timestamp() const { return arv_buffer_get_timestamp(buffer); }
    [[nodiscard]] uint64_t width() const { return arv_buffer_get_image_width(buffer); }
    [[nodiscard]] uint64_t height() const { return arv_buffer_get_image_height(buffer); }
    [[nodiscard]] uint64_t pixelFormat() const { return arv_buffer_get_image_pixel_format(buffer); }

    [[nodiscard]] std::span<const std::byte> data() const
    {
        size_t size = 0;
        const auto* bytes = static_cast<const std::byte*>(arv_buffer_get_data(buffer, &size));
        return {bytes, size};
    }

private:
    ArvStream* stream;
    ArvBuffer* buffer;
};

void checkError(std::string_view operation, GError* error)
{
    if (error)
    {
        const std::string message = error->message;
        g_error_free(error);
        throw CannotOpenSource("Video source failed while {}: {}", operation, message);
    }
}
}

VideoSource::VideoSource(const SourceDescriptor& sourceDescriptor)
    : sourceSelector(sourceDescriptor.getFromConfig(ConfigParametersVideo::SOURCE_SELECTOR))
{
    validateSchema(sourceDescriptor);
}

VideoSource::~VideoSource()
{
    close();
}

void VideoSource::open(std::shared_ptr<AbstractBufferProvider> provider)
{
    bufferProvider = std::move(provider);
    GError* error = nullptr;
    camera.reset(arv_camera_new(nullptr, &error));
    checkError("finding camera", error);
    if (!camera)
    {
        throw CannotOpenSource("Could not find a camera");
    }

    arv_camera_set_integer(camera.get(), "FLK_TI_StreamDataSourceSelector", sourceSelector, &error);
    checkError("selecting camera stream", error);
    if (arv_camera_is_gv_device(camera.get()))
    {
        arv_camera_gv_set_packet_size(camera.get(), 1444, &error);
        checkError("setting GigE packet size", error);
    }
    arv_camera_set_acquisition_mode(camera.get(), ARV_ACQUISITION_MODE_CONTINUOUS, &error);
    checkError("setting continuous acquisition", error);

    stream.reset(arv_camera_create_stream(camera.get(), nullptr, nullptr, &error));
    checkError("creating camera stream", error);
    if (!stream)
    {
        throw CannotOpenSource("Could not create camera stream");
    }

    const auto payloadSize = arv_camera_get_payload(camera.get(), &error);
    checkError("reading camera payload size", error);
    for (size_t index = 0; index < 5; ++index)
    {
        arv_stream_push_buffer(stream.get(), arv_buffer_new(payloadSize, nullptr));
    }

    arv_camera_start_acquisition(camera.get(), &error);
    checkError("starting acquisition", error);
}

Source::FillTupleBufferResult VideoSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken)
{
    PRECONDITION(bufferProvider, "Video source was not opened");
    PRECONDITION(stream, "Video source has no camera stream");
    PRECONDITION(tupleBuffer.getBufferSize() >= sizeof(VideoTuple), "Video tuple does not fit into tuple buffer");

    while (!stopToken.stop_requested())
    {
        StreamBuffer image{stream.get()};
        if (image.status() == ARV_BUFFER_STATUS_TIMEOUT)
        {
            continue;
        }
        if (image.status() != ARV_BUFFER_STATUS_SUCCESS)
        {
            if (ARV_IS_GV_STREAM(stream.get()))
            {
                guint64 resentPackets = 0;
                guint64 missingPackets = 0;
                arv_gv_stream_get_statistics(ARV_GV_STREAM(stream.get()), &resentPackets, &missingPackets);
                NES_WARNING(
                    "Video source discarded Aravis buffer with status {} (resent packets: {}, missing packets: {})",
                    static_cast<int>(image.status()),
                    resentPackets,
                    missingPackets);
            }
            else
            {
                NES_WARNING("Video source discarded Aravis buffer with status {}", static_cast<int>(image.status()));
            }
            continue;
        }

        const auto bytes = image.data();
        auto childBuffer = bufferProvider->getUnpooledBuffer(bytes.size()).value();
        std::memcpy(childBuffer.getAvailableMemoryArea().data(), bytes.data(), bytes.size());

        const VideoTuple tuple{
            .timestamp = image.timestamp(),
            .width = image.width(),
            .height = image.height(),
            .pixelFormat = image.pixelFormat(),
            .image = VariableSizedAccess{tupleBuffer.storeChildBuffer(childBuffer), VariableSizedAccess::Size{bytes.size()}}};
        std::memcpy(tupleBuffer.getAvailableMemoryArea().data(), &tuple, sizeof(tuple));
        return FillTupleBufferResult::withBytes(1);
    }
    return FillTupleBufferResult::eos();
}

void VideoSource::close()
{
    if (camera)
    {
        arv_camera_stop_acquisition(camera.get(), nullptr);
    }
    stream.reset();
    camera.reset();
    bufferProvider.reset();
}

DescriptorConfig::Config VideoSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersVideo>(std::move(config), NAME);
}

std::ostream& VideoSource::toString(std::ostream& stream) const
{
    return stream << "VideoSource(selector=" << sourceSelector << ')';
}

SourceValidationRegistryReturnType RegisterVideoSourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    return VideoSource::validateAndFormat(std::move(sourceConfig.config));
}

SourceRegistryReturnType SourceGeneratedRegistrar::RegisterVideoSource(SourceRegistryArguments sourceRegistryArguments)
{
    return std::make_unique<VideoSource>(sourceRegistryArguments.sourceDescriptor);
}
}
