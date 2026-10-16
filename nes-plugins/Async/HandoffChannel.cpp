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

#include <HandoffChannel.hpp>

#include <algorithm>
#include <cstddef>
#include <memory>
#include <mutex>
#include <optional>
#include <stop_token>
#include <string>
#include <unordered_map>
#include <Runtime/TupleBuffer.hpp>

namespace NES
{

namespace
{
struct Registry
{
    std::mutex mutex;
    std::unordered_map<std::string, std::shared_ptr<HandoffChannel>> channels;
};

Registry& registry()
{
    static Registry instance;
    return instance;
}
}

/// A channel that holds nothing would reject every buffer and hold the producer back for good.
HandoffChannel::HandoffChannel(const size_t capacity) : capacity(std::max<size_t>(1, capacity))
{
}

std::shared_ptr<HandoffChannel> HandoffChannel::get(const std::string& id, const size_t capacity)
{
    auto& [mutex, channels] = registry();
    const std::scoped_lock lock(mutex);
    auto& channel = channels[id];
    if (channel == nullptr)
    {
        channel = std::make_shared<HandoffChannel>(capacity);
    }
    return channel;
}

void HandoffChannel::release(const std::string& id)
{
    std::shared_ptr<HandoffChannel> channel;
    {
        auto& [mutex, channels] = registry();
        const std::scoped_lock lock(mutex);
        if (const auto entry = channels.find(id); entry != channels.end())
        {
            channel = std::move(entry->second);
            channels.erase(entry);
        }
    }
    if (channel)
    {
        const std::scoped_lock lock(channel->mutex);
        channel->consumerGone = true;
        channel->buffers.clear();
    }
}

HandoffChannel::PushResult HandoffChannel::tryPush(const TupleBuffer& buffer)
{
    {
        const std::scoped_lock lock(mutex);
        if (consumerGone)
        {
            return PushResult::CONSUMER_GONE;
        }
        if (buffers.size() >= capacity)
        {
            return PushResult::FULL;
        }
        buffers.push_back(buffer);
    }
    changed.notify_one();
    return PushResult::PUSHED;
}

std::optional<TupleBuffer> HandoffChannel::pop(const std::stop_token& stopToken)
{
    std::unique_lock lock(mutex);
    if (!changed.wait(lock, stopToken, [this] { return !buffers.empty() || closed; }) || buffers.empty())
    {
        return std::nullopt;
    }
    auto buffer = std::move(buffers.front());
    buffers.pop_front();
    return buffer;
}

void HandoffChannel::close()
{
    {
        const std::scoped_lock lock(mutex);
        closed = true;
    }
    changed.notify_all();
}

}
