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

#include "MQTTPlaybackVideoSourceService.hpp"
#include <cstddef>
#include <memory>
#include <string_view>

#include <arv.h>

#include <Identifiers/Identifiers.hpp>
#include <Util/Overloaded.hpp>
#include <fmt/chrono.h>
#include <fmt/ranges.h>
#include <fmt/std.h>
#include <opencv4/opencv2/core/mat.hpp>
#include <opencv4/opencv2/imgcodecs.hpp>
#include <opencv4/opencv2/imgproc.hpp>
#include <openssl/evp.h>
#include <scn/scan.h>
#include <ErrorHandling.hpp>
#include "Sources/AsyncSource.hpp"

namespace NES
{
MQTTPlaybackVideoSourceService::Handle MQTTPlaybackVideoSourceService::addConsumer(AsyncSource::AsyncSourceEmit&& consumer)
{
    auto state = stateMutex->wlock();
    auto handle = state->nextHandle++;
    state->consumers.emplace(handle, std::move(consumer));
    return handle;
}

void MQTTPlaybackVideoSourceService::removeConsumer(Handle handle)
{
    auto state = stateMutex->wlock();
    state->consumers.erase(handle);
}

static folly::Synchronized<std::vector<std::pair<SourceDescriptor, std::weak_ptr<MQTTPlaybackVideoSourceService>>>> servicesMutex{};

std::shared_ptr<MQTTPlaybackVideoSourceService>
MQTTPlaybackVideoSourceService::instance(SourceDescriptor descriptor, std::shared_ptr<AbstractBufferProvider> bufferProvider)
{
    auto services = servicesMutex.wlock();
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
    auto service = std::make_shared<MQTTPlaybackVideoSourceService>(Private{}, descriptor, std::move(bufferProvider));
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

class ConnectionListener final : public mqtt::iaction_listener, std::enable_shared_from_this<ConnectionListener>
{
    void on_failure(const mqtt::token&) override
    {
        owner.onFailure("Could not connect to MQTT server");
        keepMeAlive = nullptr;
    }

    void on_success(const mqtt::token&) override
    {
        std::visit(
            Overloaded{
                [&](const Connection& connection) { owner.client.subscribe(connection.topic, connection.QOS, nullptr, *this); },
                [&](const Subscription&) { keepMeAlive = nullptr; },
            },
            state);
    }

    struct Connection
    {
        std::string topic;
        int QOS;
    };

    struct Subscription
    {
    };

    struct Private
    {
    };

public:
    ConnectionListener(Private, std::variant<Connection, Subscription> state, MQTTPlaybackVideoSourceService& owner)
        : state(std::move(state)), owner(owner)
    {
    }

    static ConnectionListener& create(MQTTPlaybackVideoSourceService& owner, std::string topic, int qos)
    {
        auto connectionListener = std::make_shared<ConnectionListener>(Private{}, Connection{std::move(topic), qos}, owner);
        connectionListener->keepMeAlive = connectionListener;
        return *connectionListener;
    }

private:
    std::variant<Connection, Subscription> state;
    MQTTPlaybackVideoSourceService& owner;
    std::shared_ptr<ConnectionListener> keepMeAlive = nullptr;
};

void MQTTPlaybackVideoSourceService::MQTTCallback::connected(const std::string&)
{
    NES_INFO("Connection established");
}

void MQTTPlaybackVideoSourceService::MQTTCallback::connection_lost(const std::string& reason)
{
    owner.onFailure(fmt::format("Connection lost: {}", reason));
}

void MQTTPlaybackVideoSourceService::MQTTCallback::message_arrived(mqtt::const_message_ptr message)
{
    if (message)
    {
        owner.onData(std::move(message->get_payload()));
    }
}

void MQTTPlaybackVideoSourceService::onFailure(std::string error)
{
    auto state = stateMutex->rlock();
    for (auto& emit : state->consumers | std::views::values)
    {
        emit(SourceReturnType::Error{CannotOpenSource(std::move(error))});
    }
}

std::optional<std::string>
deserializeImage(std::string_view input, size_t width, size_t height, size_t pixelFormat, std::span<std::byte> output)
{
    thread_local std::vector<std::byte> encodedImageBuffer;

    auto trimmed = Util::trimWhiteSpaces(input);
    if (trimmed.size() % 4 != 0)
    {
        return "Invalid format. Received malformed base64";
    }
    encodedImageBuffer.resize((trimmed.size() / 4) * 3);
    const auto lengthOfDecoded = EVP_DecodeBlock(
        reinterpret_cast<unsigned char*>(encodedImageBuffer.data()),
        reinterpret_cast<const unsigned char*>(trimmed.data()),
        trimmed.length());

    if (lengthOfDecoded < 0)
    {
        return "Invalid format. Received malformed base64";
    }

    try
    {
        const cv::Mat inputMat(1, encodedImageBuffer.size(), CV_8UC1, encodedImageBuffer.data());

        if (pixelFormat == ARV_PIXEL_FORMAT_YUV_422_YUYV_PACKED)
        {
            // Decode JPEG to BGR (temporary Mat)
            cv::Mat bgr = cv::imdecode(inputMat, cv::IMREAD_COLOR);
            if (bgr.empty())
            {
                return fmt::format("OpenCV Error:{}.\nInput: {}", "Invalid JPG", input);
            }
            // Convert BGR to YUYV into your buffer
            cv::Mat outputMat(height, width, CV_8UC2, output.data());
            cv::cvtColor(bgr, outputMat, cv::COLOR_BGR2YUV_YUYV);
        }
        else if (pixelFormat == ARV_PIXEL_FORMAT_MONO_16)
        {
            cv::Mat outputMat(height, width, CV_16UC1, output.data());
            cv::imdecode(inputMat, cv::IMREAD_UNCHANGED, &outputMat);
            if (outputMat.empty())
            {
                return fmt::format("OpenCV Error:{}.\nInput: {}", "Invalid PNG", input);
            }
        }
        else
        {
            return fmt::format("Invalid format pixel format: {}", pixelFormat);
        }
    }
    catch (cv::Exception& e)
    {
        return fmt::format("OpenCV Error:{}.\nInput: {}", e.err, input);
    }

    return std::nullopt;
}

void dispatcherThread(std::stop_token token, MQTTPlaybackVideoSourceService* owner)
{
    while (true)
    {
        if (token.stop_requested())
        {
            return;
        }

        std::string message;
        if (!owner->dataQueue.tryReadUntil(std::chrono::system_clock::now() + std::chrono::milliseconds(10), message))
        {
            continue;
        }

        using Tuple = struct
        {
            size_t timestamp;
            size_t width;
            size_t height;
            uint64_t pixelFormat;
            VariableSizedAccess::Index::Underlying index;
        } __attribute__((packed));

        auto result = scn::scan<size_t, size_t, size_t, size_t, std::string_view>(message, "{},{},{},{},{}\n");
        if (!result)
        {
            owner->onFailure("Invalid format. Expected timestamp,size,width,height,pixelFormat,base64EncodedImage");
            return;
        }

        auto [timestamp, width, height, pixelFormat, base64Encoded] = result->values();

        auto rawImageBuffer = owner->bufferProvider->getUnpooledBuffer(4 + (width * height * 2)).value();
        rawImageBuffer.getAvailableMemoryArea<uint32_t>()[0] = width * height * 2;
        if (auto error = deserializeImage(
                base64Encoded,
                width,
                height,
                pixelFormat,
                {rawImageBuffer.getAvailableMemoryArea<std::byte>().data() + 4, width * height * 2}))
        {
            owner->onFailure(fmt::format("Error while deserializing image: {}", error.value()));
        }

        auto state = owner->stateMutex->rlock();
        for (auto& emit : state->consumers | std::views::values)
        {
            auto imageBufferPerSource = rawImageBuffer;
            auto tupleBuffer = owner->bufferProvider->getBufferBlocking();
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

void MQTTPlaybackVideoSourceService::onData(std::string data)
{
    if (!dataQueue.writeIfNotFull(std::move(data)))
    {
        NES_WARNING("Dropping Frames");
    }
}

MQTTPlaybackVideoSourceService::MQTTPlaybackVideoSourceService(
    Private, SourceDescriptor config, std::shared_ptr<AbstractBufferProvider> bufferProvider)
    : stateMutex(std::make_shared<folly::Synchronized<State>>())
    , client(
          config.getFromConfig(ConfigParametersMQTTVideoPlayback::SERVER_URI),
          config.getFromConfig(ConfigParametersMQTTVideoPlayback::CLIENT_ID))
    , callback(*this)
    , bufferProvider(std::move(bufferProvider))
    , dispatcher(std::jthread{dispatcherThread, this})
{
    client.set_callback(callback);
    mqtt::connect_options connOpts;
    connOpts.set_keep_alive_interval(20);
    connOpts.set_clean_session(true);
    client.connect(
        connOpts,
        nullptr,
        ConnectionListener::create(
            *this,
            config.getFromConfig(ConfigParametersMQTTVideoPlayback::TOPIC),
            config.getFromConfig(ConfigParametersMQTTVideoPlayback::QOS)));
}
}
