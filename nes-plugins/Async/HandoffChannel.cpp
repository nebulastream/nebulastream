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
/// ponytail: fixed capacity; make it a descriptor option if the backlog needs tuning
constexpr size_t CHANNEL_CAPACITY = 64;

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

HandoffChannel::HandoffChannel(const size_t capacity) : capacity(capacity)
{
}

std::shared_ptr<HandoffChannel> HandoffChannel::get(const std::string& id)
{
    auto& [mutex, channels] = registry();
    const std::scoped_lock lock(mutex);
    auto& channel = channels[id];
    if (channel == nullptr)
    {
        channel = std::make_shared<HandoffChannel>(CHANNEL_CAPACITY);
    }
    return channel;
}

void HandoffChannel::release(const std::string& id)
{
    auto& [mutex, channels] = registry();
    const std::scoped_lock lock(mutex);
    channels.erase(id);
}

bool HandoffChannel::tryPush(const TupleBuffer& buffer)
{
    {
        const std::scoped_lock lock(mutex);
        if (buffers.size() >= capacity)
        {
            return false;
        }
        buffers.push_back(buffer);
    }
    changed.notify_one();
    return true;
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
