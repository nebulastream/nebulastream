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

#include "VideoSourceService.hpp"

#include <Identifiers/Identifiers.hpp>
#include <ErrorHandling.hpp>

#include <arv.h>
#include <fmt/ranges.h>

namespace NES
{
VideoSourceService::Handle VideoSourceService::addConsumer(AsyncSource::AsyncSourceEmit&& consumer)
{
    auto state = stateMutex->wlock();
    auto handle = state->nextHandle++;
    state->consumers.emplace(handle, std::move(consumer));
    return handle;
}

void VideoSourceService::removeConsumer(Handle handle)
{
    auto state = stateMutex->wlock();
    state->consumers.erase(handle);
}

namespace
{
template <typename T>
struct g_clear
{
    void operator()(T* ptr) const { g_clear_object(&ptr); }
};

template <typename T>
using g_unique_ptr = std::unique_ptr<T, g_clear<T>>;

struct Context
{
    g_unique_ptr<ArvCamera> camera;
    g_unique_ptr<ArvStream> stream;
    std::atomic_bool requireReconnect;
};

template <typename T>
g_unique_ptr<T> adopt(T* t)
{
    return g_unique_ptr<T>(t, g_clear<T>{});
}

void onConnectionLost(ArvDevice*, void* userData)
{
    NES_WARNING("VideoSource onConnectionLost");
    auto* context = static_cast<Context*>(userData);
    context->requireReconnect = true;
}

void checkError(std::string_view context, GError* error)
{
    if (error)
    {
        throw CannotOpenSource("VideoSource when {}: {}", context, error->message);
    }
}
}

std::unique_ptr<Context> open(const SourceDescriptor& config)
{
    GError* error = nullptr;
    auto camera = adopt(arv_camera_new(NULL, &error));

    checkError("finding camera", error);
    if (!ARV_IS_CAMERA(camera.get()))
    {
        throw CannotOpenSource("Could not find camera");
    }

    NES_DEBUG("Picking Stream {}", config.getFromConfig(ConfigParametersVideo::SOURCE_SELECTOR));
    arv_camera_set_integer(
        camera.get(), "FLK_TI_StreamDataSourceSelector", config.getFromConfig(ConfigParametersVideo::SOURCE_SELECTOR), &error);

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
    auto context = std::make_unique<Context>(std::move(camera), std::move(stream), false);

    auto* device = arv_camera_get_device(context->camera.get());
    g_signal_connect(device, "control-lost", G_CALLBACK(onConnectionLost), context.get());
    return context;
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

    size_t pixelFormat() const
    {
        PRECONDITION(buffer, "Invalid buffer");
        return arv_buffer_get_image_pixel_format(buffer);
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

void sourceThread(
    const std::stop_token& token,
    SourceDescriptor descriptor,
    std::shared_ptr<AbstractBufferProvider> bufferProvider,
    std::shared_ptr<folly::Synchronized<VideoSourceService::State>> stateMutex)
{
    using Tuple = struct
    {
        size_t timestamp;
        size_t width;
        size_t height;
        uint64_t pixelFormat;
        uint32_t index;
    } __attribute__((packed));

    try
    {
        auto context = open(descriptor);
        while (!token.stop_requested())
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
                    context = open(descriptor);
                }
                NES_WARNING("Arv: {}", magic_enum::enum_name(buffer.status()));
                continue;
            }

            auto timestamp = buffer.timestamp();
            auto width = buffer.width();
            auto height = buffer.height();
            auto pixelFormat = buffer.pixelFormat();

            auto data = buffer.data();
            auto imageBuffer = bufferProvider->getUnpooledBuffer(data.size() + sizeof(uint32_t)).value();
            std::ranges::copy(data, imageBuffer.getAvailableMemoryArea<std::byte>().data() + sizeof(uint32_t));
            imageBuffer.getAvailableMemoryArea<std::uint32_t>()[0] = static_cast<uint32_t>(data.size());

            auto state = stateMutex->rlock();
            for (auto& emit : state->consumers | std::views::values)
            {
                auto imageBufferPerSource = imageBuffer;
                auto tupleBuffer = bufferProvider->getBufferBlocking();
                auto& tuple = *tupleBuffer.getAvailableMemoryArea<Tuple>().data();
                tuple.height = height;
                tuple.width = width;
                tuple.index = std::bit_cast<uint32_t>(tupleBuffer.storeChildBuffer(imageBufferPerSource));
                tuple.timestamp = timestamp;
                tuple.pixelFormat = pixelFormat;
                tupleBuffer.setNumberOfTuples(1);
                emit(SourceReturnType::Data{tupleBuffer});
            }
        }
    }
    catch (const std::exception& e)
    {
        NES_ERROR("VideoSource::receiveData: Failed to fill the TupleBuffer. Error: {}.", e.what());
        wrapExternalException();
    }
}

static folly::Synchronized<std::vector<std::pair<SourceDescriptor, std::weak_ptr<VideoSourceService>>>> servicesMutex{};

std::shared_ptr<VideoSourceService>
VideoSourceService::instance(SourceDescriptor descriptor, std::shared_ptr<AbstractBufferProvider> bufferProvider)
{
    auto services = servicesMutex.wlock();
    NES_INFO("{}", fmt::join(std::views::keys(*services), ", "));
    auto existingService = std::ranges::find(*services, descriptor, [](const auto& p) { return p.first; });
    if (existingService != std::ranges::end(*services))
    {
        if (auto runningService = existingService->second.lock())
        {
            NES_INFO("Reusing Video Source: {}", descriptor);
            return runningService;
        }
    }

    NES_INFO("Creating new Video Source: {}", descriptor);
    auto service = std::make_shared<VideoSourceService>(Private{}, descriptor, std::move(bufferProvider));
    if (existingService != std::ranges::end(*services))
    {
        existingService->second = service;
    }
    else
    {
        services->emplace_back(descriptor, service);
    }

    return service;
}

VideoSourceService::VideoSourceService(Private, SourceDescriptor config, std::shared_ptr<AbstractBufferProvider> bufferProvider)
    : stateMutex(std::make_shared<folly::Synchronized<State>>()), thread(sourceThread, config, bufferProvider, stateMutex)
{
}
}
