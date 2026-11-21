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
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stop_token>
#include <string>
#include <unordered_map>
#include <utility>
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

/// Both locks are taken registry first, so that a source cannot attach to a channel that a sink is removing.
HandoffChannel::Lease::Lease(std::string id, const size_t capacity, const Role role) : id(std::move(id)), role(role)
{
    auto& [mutex, channels] = registry();
    const std::scoped_lock lock(mutex);
    auto& registered = channels[this->id];
    if (registered == nullptr)
    {
        registered = std::make_shared<HandoffChannel>(capacity);
    }
    channel = registered;
    if (role == Role::SOURCE)
    {
        const std::scoped_lock channelLock(channel->mutex);
        channel->sourceAttached = true;
    }
}

HandoffChannel::Lease::~Lease()
{
    release();
}

void HandoffChannel::Lease::release()
{
    if (!channel)
    {
        return;
    }
    std::deque<TupleBuffer> dropped;
    {
        auto& [mutex, channels] = registry();
        const std::scoped_lock lock(mutex);
        const std::scoped_lock channelLock(channel->mutex);
        const bool removes = role == Role::SOURCE || (!channel->sourceAttached && !channel->closed);
        if (role == Role::SOURCE)
        {
            channel->consumerGone = true;
            channel->listener = {};
            dropped = std::exchange(channel->buffers, {});
        }
        /// A half that comes after this one may have registered a channel of its own under the same id.
        if (const auto entry = channels.find(id); removes && entry != channels.end() && entry->second == channel)
        {
            channels.erase(entry);
        }
    }
    channel.reset();
}

HandoffChannel::PushResult HandoffChannel::tryPush(const TupleBuffer& buffer)
{
    {
        const std::scoped_lock lock(mutex);
        if (consumerGone)
        {
            return PushResult::CONSUMER_GONE;
        }
        /// Before the source attached, a buffer stays with the sink, whose pending retry also holds back a graceful stop.
        if (!sourceAttached || buffers.size() >= capacity)
        {
            return PushResult::FULL;
        }
        buffers.push_back(buffer);
        if (listener)
        {
            listener();
        }
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

std::optional<TupleBuffer> HandoffChannel::tryPop()
{
    const std::scoped_lock lock(mutex);
    if (buffers.empty())
    {
        return std::nullopt;
    }
    auto buffer = std::move(buffers.front());
    buffers.pop_front();
    return buffer;
}

bool HandoffChannel::exhausted()
{
    const std::scoped_lock lock(mutex);
    return closed && buffers.empty();
}

void HandoffChannel::close()
{
    {
        const std::scoped_lock lock(mutex);
        closed = true;
        if (listener)
        {
            listener();
        }
    }
    changed.notify_all();
}

void HandoffChannel::setListener(std::function<void()> onChange)
{
    const std::scoped_lock lock(mutex);
    listener = std::move(onChange);
}

}
