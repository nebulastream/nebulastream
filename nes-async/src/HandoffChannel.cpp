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

#include <Async/HandoffChannel.hpp>

#include <cstddef>
#include <memory>
#include <mutex>
#include <optional>
#include <stop_token>
#include <string>
#include <unordered_map>
#include <utility>

#include <Runtime/TupleBuffer.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

HandoffChannel::HandoffChannel(const size_t capacity) : capacityLimit(capacity)
{
    PRECONDITION(capacity > 0, "A handoff channel needs room for at least one buffer");
}

bool HandoffChannel::tryPush(TupleBuffer buffer)
{
    {
        const std::lock_guard lock(mutex);
        if (closed || consumerClosed || queue.size() >= capacityLimit)
        {
            return false;
        }
        queue.push_back(std::move(buffer));
    }
    notEmpty.notify_one();
    return true;
}

std::optional<TupleBuffer> HandoffChannel::popBlocking(const std::stop_token& stopToken)
{
    std::unique_lock lock(mutex);
    notEmpty.wait(lock, stopToken, [this] { return not queue.empty() || closed; });

    /// Empty at this point means either "closed and drained" or "stop requested". Both end
    /// the consumer's loop; only the caller can tell them apart, via the stop token.
    if (queue.empty())
    {
        return std::nullopt;
    }

    auto buffer = std::move(queue.front());
    queue.pop_front();
    return buffer;
}

void HandoffChannel::close()
{
    {
        const std::lock_guard lock(mutex);
        closed = true;
    }
    /// Wake every waiter: a consumer blocked on an empty, now-closed channel must return.
    notEmpty.notify_all();
}

void HandoffChannel::closeConsumer()
{
    /// Moved out under the lock and released after it, so returning buffers to the pool does
    /// not happen while a producer waits for the mutex.
    std::deque<TupleBuffer> abandoned;
    {
        const std::lock_guard lock(mutex);
        consumerClosed = true;
        abandoned.swap(queue);
    }
    notEmpty.notify_all();
}

bool HandoffChannel::isClosed() const
{
    const std::lock_guard lock(mutex);
    return closed;
}

bool HandoffChannel::isConsumerClosed() const
{
    const std::lock_guard lock(mutex);
    return consumerClosed;
}

size_t HandoffChannel::size() const
{
    const std::lock_guard lock(mutex);
    return queue.size();
}

size_t HandoffChannel::capacity() const
{
    const std::lock_guard lock(mutex);
    return capacityLimit;
}

namespace
{

struct Registry
{
    std::mutex mutex;
    /// Weak, so a channel disappears from the directory once both halves released it.
    std::unordered_map<std::string, std::weak_ptr<HandoffChannel>> channels;
};

Registry& registry()
{
    static Registry instance;
    return instance;
}

/// Removes entries whose channel has already been destroyed. Called under the lock.
void eraseExpired(Registry& reg)
{
    std::erase_if(reg.channels, [](const auto& entry) { return entry.second.expired(); });
}

}

std::shared_ptr<HandoffChannel> HandoffChannelRegistry::getOrCreate(const std::string& channelId, const size_t capacity)
{
    auto& reg = registry();
    const std::lock_guard lock(reg.mutex);

    if (const auto it = reg.channels.find(channelId); it != reg.channels.end())
    {
        if (auto existing = it->second.lock())
        {
            return existing;
        }
    }

    auto channel = std::make_shared<HandoffChannel>(capacity);
    reg.channels.insert_or_assign(channelId, channel);
    return channel;
}

std::shared_ptr<HandoffChannel> HandoffChannelRegistry::find(const std::string& channelId)
{
    auto& reg = registry();
    const std::lock_guard lock(reg.mutex);

    if (const auto it = reg.channels.find(channelId); it != reg.channels.end())
    {
        return it->second.lock();
    }
    return nullptr;
}

void HandoffChannelRegistry::drop(const std::string& channelId)
{
    auto& reg = registry();
    const std::lock_guard lock(reg.mutex);
    reg.channels.erase(channelId);
}

size_t HandoffChannelRegistry::registeredChannels()
{
    auto& reg = registry();
    const std::lock_guard lock(reg.mutex);
    eraseExpired(reg);
    return reg.channels.size();
}

}
