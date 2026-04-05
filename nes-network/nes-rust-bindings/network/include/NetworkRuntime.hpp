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

#include <cstddef>
#include <memory>
#include <string>
#include <network/lib.h>
#include <rust/cxx.h>
#include <NetworkOptions.hpp>
#include <WorkerLocalSingleton.hpp>

namespace NES
{

/// Worker-scoped singleton holding both the Rust sender and receiver network services.
/// Created by SingleNodeWorker during initialization, automatically
/// propagated to child threads via WorkerLocalSingleton/Thread hooks.
/// NetworkSink and NetworkSource access it via NetworkRuntime::instance().
class NetworkRuntime : public WorkerLocalSingleton<NetworkRuntime>
{
    rust::Box<SenderNetworkService> sender;
    rust::Box<ReceiverNetworkService> receiver;

public:
    /// Creates both sender and receiver Rust network services.
    NetworkRuntime(const std::string& connectionAddr, const std::string& workerId, const NetworkOptions& options);
    ~NetworkRuntime();

    /// Register a sender channel to a downstream worker.
    /// @param connectionAddr target worker address (host:port)
    /// @param channelId UUID identifying this channel
    /// @param senderQueueSize per-channel override, 0 = use worker default
    /// @param maxPendingAcks per-channel override, 0 = use worker default
    [[nodiscard]] rust::Box<SenderDataChannel>
    registerSenderChannel(const std::string& connectionAddr, const std::string& channelId, size_t senderQueueSize, size_t maxPendingAcks);

    /// Register a receiver channel for incoming data.
    /// @param channelId UUID identifying this channel
    /// @param receiverQueueSize per-channel override, 0 = use worker default
    [[nodiscard]] rust::Box<ReceiverDataChannel> registerReceiverChannel(const std::string& channelId, size_t receiverQueueSize);
};

}
