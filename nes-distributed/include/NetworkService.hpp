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

#include <future>
#include <thread>
#include <unordered_map>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <absl/functional/any_invocable.h>
#include <folly/MPMCQueue.h>
#include <folly/Synchronized.h>

namespace NES::Memory
{
class AbstractBufferProvider;
}
namespace NES::Networking
{
class NetworkService
{
public:
    explicit NetworkService(std::shared_ptr<Memory::AbstractBufferProvider> buffer_provider);

    using ConnectionHandle = uint16_t;
    using ChannelHandle = std::string;
    using EmitFn = std::function<bool(NES::Memory::TupleBuffer)>;
    using SendQueue = folly::MPMCQueue<std::pair<ChannelHandle, Memory::TupleBuffer>>;
    using NewConnectionRequest = std::pair<ConnectionHandle, std::unique_ptr<std::promise<std::shared_ptr<SendQueue>>>>;
    using NewConnectionRequestQueue = folly::MPMCQueue<NewConnectionRequest>;


    static NetworkService& getInstance();
    static void terminate();
    static void initialize(std::shared_ptr<Memory::AbstractBufferProvider>);

    std::future<std::shared_ptr<SendQueue>> registerSenderChannel(ConnectionHandle h);
    [[nodiscard]] ChannelHandle registerChannel(ChannelHandle h, EmitFn emit)
    {
        registeredReceiveChannel.wlock()->emplace(std::move(h), std::move(emit));
        return h;
    }
    void unregisterChannel(const ChannelHandle& handle) { registeredReceiveChannel.wlock()->erase(handle); }

private:
    std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider;
    folly::Synchronized<std::unordered_map<ChannelHandle, EmitFn>> registeredReceiveChannel{};
    NewConnectionRequestQueue establishConnectionQueue{10};

    std::jthread senderThread;
    std::jthread receiveThread;
};

}
