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

#include <PythonProcessChannel.hpp>

#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>

#include <unistd.h>
#include <sys/eventfd.h>

#include <Runtime/TupleBuffer.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
struct Registry
{
    std::mutex mutex;
    std::unordered_map<std::string, std::shared_ptr<PythonProcessChannel>> channels;
};

Registry& registry()
{
    static Registry instance;
    return instance;
}
}

/// Under the lock of the registry, so that a source cannot attach to a channel that a sink is removing.
PythonProcessChannel::Lease::Lease(std::string id, const Role role) : id(std::move(id)), role(role)
{
    auto& [mutex, channels] = registry();
    const std::scoped_lock lock(mutex);
    auto& registered = channels[this->id];
    if (registered == nullptr)
    {
        registered = std::make_shared<PythonProcessChannel>();
    }
    channel = registered;
    if (role == Role::SOURCE)
    {
        channel->sourceAttached = true;
    }
}

PythonProcessChannel::Lease::~Lease()
{
    release();
}

void PythonProcessChannel::Lease::release()
{
    if (!channel)
    {
        return;
    }
    {
        auto& [mutex, channels] = registry();
        const std::scoped_lock lock(mutex);
        const bool removes = role == Role::SOURCE || (!channel->sourceAttached && !channel->closed);
        /// A half that comes after this one may have registered a channel of its own under the same id.
        if (const auto entry = channels.find(id); removes && entry != channels.end() && entry->second == channel)
        {
            channels.erase(entry);
        }
    }
    /// Outside the lock, as dropping a buffer may run its recycler. A push that comes after drops its buffer itself, see tryPush.
    if (role == Role::SOURCE)
    {
        channel->consumerGone.store(true);
        std::atomic_thread_fence(std::memory_order_seq_cst);
        channel->drain();
    }
    channel.reset();
}

PythonProcessChannel::PythonProcessChannel() : eventFd(eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK))
{
    if (eventFd < 0)
    {
        throw CannotLoadUdf("Failed to create the eventfd of a UDF process channel: {}", std::strerror(errno)); /// NOLINT(concurrency-mt-unsafe)
    }
}

PythonProcessChannel::~PythonProcessChannel()
{
    ::close(eventFd);
}

void PythonProcessChannel::notify() const
{
    const uint64_t one = 1;
    /// Can only fail if the counter overflows, and then it is readable anyway.
    [[maybe_unused]] const auto written = ::write(eventFd, &one, sizeof(one));
}

PythonProcessChannel::PushResult PythonProcessChannel::tryPush(const TupleBuffer& buffer)
{
    if (consumerGone.load(std::memory_order_relaxed))
    {
        return PushResult::CONSUMER_GONE;
    }
    /// Before the source opened, a buffer stays with the sink, whose pending retry also holds back a graceful stop.
    auto* const open = openQueue.load(std::memory_order_acquire);
    if (open == nullptr || !open->write(buffer))
    {
        return PushResult::FULL;
    }
    /// Orders the push before the checks, against the source setting its flags and checking the queue in the opposite order.
    std::atomic_thread_fence(std::memory_order_seq_cst);
    if (consumerGone.load(std::memory_order_relaxed))
    {
        drain();
        return PushResult::CONSUMER_GONE;
    }
    if (sourceWaiting.load(std::memory_order_relaxed) && sourceWaiting.exchange(false))
    {
        notify();
    }
    return PushResult::PUSHED;
}

PythonProcessChannel::Popped PythonProcessChannel::tryPop()
{
    auto* const open = openQueue.load(std::memory_order_acquire);
    if (open == nullptr)
    {
        return {.buffer = std::nullopt, .ended = closed.load()};
    }
    TupleBuffer buffer;
    if (open->read(buffer))
    {
        return {.buffer = std::move(buffer), .ended = false};
    }
    /// The queue looks empty: a push from now on wakes the source. Closed is read before the queue is checked again, so that the
    /// buffers pushed before the close are taken before the end.
    sourceWaiting.store(true);
    std::atomic_thread_fence(std::memory_order_seq_cst);
    const bool wasClosed = closed.load();
    if (open->read(buffer))
    {
        sourceWaiting.store(false, std::memory_order_relaxed);
        return {.buffer = std::move(buffer), .ended = false};
    }
    return {.buffer = std::nullopt, .ended = wasClosed};
}

void PythonProcessChannel::setCapacity(const size_t buffers)
{
    PRECONDITION(queue == nullptr, "The capacity of a UDF process channel is set once");
    queue = std::make_unique<folly::MPMCQueue<TupleBuffer>>(buffers);
    openQueue.store(queue.get(), std::memory_order_release);
}

void PythonProcessChannel::drain()
{
    if (auto* const open = openQueue.load(std::memory_order_acquire))
    {
        TupleBuffer buffer;
        while (open->read(buffer))
        {
            buffer = TupleBuffer{};
        }
    }
}

void PythonProcessChannel::close()
{
    closed.store(true);
    notify();
}

}
