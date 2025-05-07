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

#pragma once
#include <Sources/SourceHandle.hpp>
#include <folly/MPMCQueue.h>
#include <folly/Synchronized.h>
#include <mqtt/async_client.h>
#include "SourceRegistry.hpp"
#include "VideoSourceValidation.hpp"

namespace NES
{
class MQTTPlaybackVideoSourceService
{
public:
    using Handle = size_t;

private:
    struct State
    {
        Handle nextHandle;
        std::unordered_map<Handle, AsyncSource::AsyncSourceEmit> consumers;
    };

    struct MQTTCallback final : mqtt::callback
    {
        void connected(const std::string& /*cause*/) override;
        void connection_lost(const std::string& /*cause*/) override;
        void message_arrived(mqtt::const_message_ptr message) override;
        MQTTPlaybackVideoSourceService& owner;

        explicit MQTTCallback(MQTTPlaybackVideoSourceService& mqtt_playback_video_source_service)
            : owner(mqtt_playback_video_source_service)
        {
        }
    };

    std::shared_ptr<folly::Synchronized<State>> stateMutex;
    mqtt::async_client client;
    MQTTCallback callback;
    std::shared_ptr<AbstractBufferProvider> bufferProvider;

    folly::MPMCQueue<std::string> dataQueue{10};
    std::jthread dispatcher;


    friend void dispatcherThread(std::stop_token token, MQTTPlaybackVideoSourceService* owner);

    struct Private
    {
    };

    friend class ConnectionListener;

    void onFailure(std::string error);
    void onData(std::string data);

public:
    MQTTPlaybackVideoSourceService(Private, SourceDescriptor config, std::shared_ptr<AbstractBufferProvider> bufferProvider);
    Handle addConsumer(AsyncSource::AsyncSourceEmit&& consumer);
    void removeConsumer(Handle handle);

    static std::shared_ptr<MQTTPlaybackVideoSourceService>
    instance(SourceDescriptor descriptor, std::shared_ptr<AbstractBufferProvider> bufferProvider);
};
}
