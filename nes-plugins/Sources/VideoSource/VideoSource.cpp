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

#include "VideoSource.hpp"
#include <bit>
#include <cerrno> /// For socket error
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <memory>
#include <ostream>
#include <span>
#include <stop_token>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <arv.h>
#include <netdb.h>
#include <unistd.h> /// For read

#include <API/AttributeField.hpp>
#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <asm-generic/socket.h>
#include <bits/types/struct_timeval.h>
#include <sys/socket.h> /// For socket functions
#include <sys/types.h>
#include <ErrorHandling.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>
#include <glib-object.h>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
namespace NES::Sources
{

VideoSource::VideoSource(const SourceDescriptor& descriptor)
    : sourceSelector(descriptor.getFromConfig(ConfigParametersVideo::SOURCE_SELECTOR))
{
    auto timestamp = descriptor.schema->getFieldByIndex(0);
    auto width = descriptor.schema->getFieldByIndex(1);
    auto height = descriptor.schema->getFieldByIndex(2);
    auto image = descriptor.schema->getFieldByIndex(3);
    PRECONDITION(
        width->getName().ends_with("width") && height->getName().ends_with("height") && timestamp->getName().ends_with("timestamp")
            && image->getName().ends_with("image"),
        "Expected schema to have width, height and timestamp as well as image data");
    NES_TRACE("VideoSource::VideoSource: Init VideoSource.");
}


std::ostream& VideoSource::toString(std::ostream& str) const
{
    str << "\nVideoSource(";
    str << ")\n";
    return str;
}

namespace
{
void checkError(std::string_view context, GError* error)
{
    if (error)
    {
        throw CannotOpenSource("VideoSource when {}: {}", context, error->message);
    }
}

template <typename T>
g_unique_ptr<T> adopt(T* t)
{
    return g_unique_ptr<T>(t, g_clear<T>{});
}

}


VideoSource::~VideoSource() = default;

void onConnectionLost(ArvDevice*, void* userData)
{
    NES_WARNING("VideoSource onConnectionLost");
    auto* context = static_cast<Context*>(userData);
    context->requireReconnect = true;
}

void VideoSource::open()
{
    GError* error = nullptr;
    auto camera = adopt(arv_camera_new(NULL, &error));


    checkError("finding camera", error);
    if (!ARV_IS_CAMERA(camera.get()))
    {
        throw CannotOpenSource("Could not find camera");
    }

    arv_camera_set_integer(camera.get(), "FLK_TI_StreamDataSourceSelector", sourceSelector, &error);
    checkError("selecting camera stream", error);

    arv_camera_set_acquisition_mode(camera.get(), ARV_ACQUISITION_MODE_CONTINUOUS, &error);

    const char* pixelFormat = arv_camera_get_string(camera.get(), "PixelFormat", &error);
    checkError("checking pixel format", error);
    NES_INFO("VideoSource: Pixel format is {}", pixelFormat);

    auto stream = adopt(arv_camera_create_stream(camera.get(), NULL, NULL, &error));
    checkError("creating stream", error);
    if (!ARV_IS_STREAM(stream.get()))
    {
        throw CannotOpenSource("Could not open stream");
    }

    auto payload = arv_camera_get_payload(camera.get(), &error);
    checkError("creating payload", error);
    /* Insert some buffers in the stream buffer pool */
    for (size_t i = 0; i < 5; i++)
    {
        arv_stream_push_buffer(stream.get(), arv_buffer_new(payload, NULL));
    }

    arv_camera_start_acquisition(camera.get(), &error);

    checkError("starting acquisition", error);
    this->context = std::make_unique<Context>(std::move(camera), std::move(stream), false);

    auto* device = arv_camera_get_device(context->camera.get());
    g_signal_connect(device, "control-lost", G_CALLBACK(onConnectionLost), this->context.get());
}

struct StreamBuffer
{
    StreamBuffer(ArvStream* stream) : stream(stream)
    {
        buffer
            = arv_stream_timeout_pop_buffer(stream, std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::seconds(1)).count());
    }

    StreamBuffer(const StreamBuffer& other) = delete;
    StreamBuffer(StreamBuffer&& other) noexcept : stream(other.stream), buffer(other.buffer)
    {
        other.stream = nullptr;
        other.buffer = nullptr;
    }
    StreamBuffer& operator=(const StreamBuffer& other) = delete;
    StreamBuffer& operator=(StreamBuffer&& other) noexcept
    {
        if (this == &other)
            return *this;
        stream = other.stream;
        buffer = other.buffer;

        other.buffer = nullptr;
        other.stream = nullptr;
        return *this;
    }

    ~StreamBuffer()
    {
        if (buffer)
        {
            arv_stream_push_buffer(stream, buffer);
        }
    }

    ArvBufferStatus status() const
    {
        if (!buffer)
        {
            return ARV_BUFFER_STATUS_TIMEOUT;
        }

        return arv_buffer_get_status(buffer);
    }

    size_t height() const
    {
        PRECONDITION(buffer, "Invalid buffer");
        return arv_buffer_get_image_height(buffer);
    }

    size_t width() const
    {
        PRECONDITION(buffer, "Invalid buffer");
        return arv_buffer_get_image_width(buffer);
    }

    size_t timestamp() const
    {
        PRECONDITION(buffer, "Invalid buffer");
        return arv_buffer_get_timestamp(buffer);
    }

    std::span<const std::byte> data() const
    {
        PRECONDITION(buffer, "Invalid buffer");
        size_t bufferSize = 0;
        const auto* data = static_cast<const std::byte*>(arv_buffer_get_data(buffer, &bufferSize));
        return {data, bufferSize};
    }

    ArvStream* stream;
    ArvBuffer* buffer;
};

size_t
VideoSource::fillTupleBuffer(NES::Memory::TupleBuffer& tupleBuffer, Memory::AbstractBufferProvider& bufferProvider, const std::stop_token&)
{
    try
    {
        while (true)
        {
            StreamBuffer buffer(context->stream.get());

            if (buffer.status() != ARV_BUFFER_STATUS_SUCCESS)
            {
                if (context->requireReconnect)
                {
                    NES_WARNING("Reconnecting to stream");
                    {
                        auto _ = std::move(buffer);
                    }
                    open();
                }
                continue;
            }

            auto timestamp = buffer.timestamp();
            auto width = buffer.width();
            auto height = buffer.height();
            tupleBuffer.getBuffer<size_t>()[0] = timestamp;
            tupleBuffer.getBuffer<size_t>()[1] = width;
            tupleBuffer.getBuffer<size_t>()[2] = height;
            auto data = buffer.data();
            auto imageBuffer = bufferProvider.getUnpooledBuffer(data.size()).value();
            imageBuffer.getBuffer<uint32_t>()[0] = static_cast<uint32_t>(data.size());
            std::ranges::copy(data, imageBuffer.getBuffer<std::byte>() + sizeof(uint32_t));
            auto index = tupleBuffer.storeChildBuffer(imageBuffer);
            tupleBuffer.getBuffer<uint32_t>()[7] = index;
            tupleBuffer.setNumberOfTuples(1);
            return 1;
        }
    }
    catch (const std::exception& e)
    {
        NES_ERROR("VideoSource::receiveData: Failed to fill the TupleBuffer. Error: {}.", e.what());
        throw e;
    }
}

NES::Configurations::DescriptorConfig::Config VideoSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return Configurations::DescriptorConfig::validateAndFormat<ConfigParametersVideo>(std::move(config), NAME);
}

void VideoSource::close()
{
    GError* error = nullptr;
    arv_camera_stop_acquisition(context->camera.get(), &error);
    checkError("stopping acquisition", error);
    context.reset();
}


SourceValidationRegistryReturnType
SourceValidationGeneratedRegistrar::RegisterVideoSourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    return VideoSource::validateAndFormat(std::move(sourceConfig.config));
}

SourceRegistryReturnType SourceGeneratedRegistrar::RegisterVideoSource(SourceRegistryArguments sourceRegistryArguments)
{
    return std::make_unique<VideoSource>(sourceRegistryArguments.sourceDescriptor);
}

}
