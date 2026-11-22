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
#include <Util/Reflection.hpp>

#include <condition_variable>
#include <cstddef>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stop_token>
#include <utility>
#include <Runtime/TupleBuffer.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

struct HandoffChannel::State
{
    explicit State(size_t capacity) : capacity(capacity) { }

    void checkAborted() const
    {
        if (aborted)
        {
            throw CannotReadFromSource("Handoff producer was destroyed without closing the channel");
        }
    }

    std::mutex mutex;
    std::condition_variable_any changed;
    std::deque<TupleBuffer> buffers;
    size_t capacity;
    bool closed = false;
    bool aborted = false;
    bool consumerGone = false;
    std::function<void()> listener;
};

std::pair<HandoffChannel::Sender, HandoffChannel::Receiver> HandoffChannel::create(size_t capacity)
{
    if (capacity == 0)
    {
        throw InvalidQuerySyntax("Handoff channel capacity must be positive");
    }
    auto state = std::make_shared<State>(capacity);
    return {Sender{state}, Receiver{state}};
}

HandoffChannel::Sender::Sender(std::shared_ptr<State> state) : state(std::move(state))
{
}

HandoffChannel::Sender::Sender(Sender&&) noexcept = default;

HandoffChannel::Sender& HandoffChannel::Sender::operator=(Sender&& other) noexcept
{
    if (this != &other)
    {
        cancel();
        state = std::move(other.state);
    }
    return *this;
}

HandoffChannel::Sender::~Sender()
{
    cancel();
}

void HandoffChannel::Sender::cancel()
{
    if (!state)
    {
        return;
    }
    std::deque<TupleBuffer> dropped;
    {
        const std::scoped_lock lock(state->mutex);
        if (!state->closed && !state->aborted)
        {
            state->aborted = true;
            dropped.swap(state->buffers);
            if (state->listener)
            {
                state->listener();
            }
        }
    }
    state->changed.notify_all();
}

HandoffChannel::PushResult HandoffChannel::Sender::tryPush(const TupleBuffer& buffer)
{
    if (!state)
    {
        throw CannotWriteToSink("Handoff sender was moved from");
    }
    {
        const std::scoped_lock lock(state->mutex);
        if (state->closed || state->aborted)
        {
            throw CannotWriteToSink("Handoff sender is closed");
        }
        if (state->consumerGone)
        {
            return PushResult::CONSUMER_GONE;
        }
        if (state->buffers.size() >= state->capacity)
        {
            return PushResult::FULL;
        }
        state->buffers.push_back(buffer);
        if (state->listener)
        {
            state->listener();
        }
    }
    state->changed.notify_one();
    return PushResult::PUSHED;
}

void HandoffChannel::Sender::close()
{
    if (!state)
    {
        return;
    }
    {
        const std::scoped_lock lock(state->mutex);
        if (state->closed)
        {
            return;
        }
        state->closed = true;
        if (state->listener)
        {
            state->listener();
        }
    }
    state->changed.notify_all();
}

HandoffChannel::Receiver::Receiver(std::shared_ptr<State> state) : state(std::move(state))
{
}

HandoffChannel::Receiver::Receiver(Receiver&&) noexcept = default;

HandoffChannel::Receiver& HandoffChannel::Receiver::operator=(Receiver&& other) noexcept
{
    if (this != &other)
    {
        close();
        state = std::move(other.state);
    }
    return *this;
}

HandoffChannel::Receiver::~Receiver()
{
    close();
}

void HandoffChannel::Receiver::close()
{
    if (!state)
    {
        return;
    }
    std::deque<TupleBuffer> dropped;
    {
        const std::scoped_lock lock(state->mutex);
        state->consumerGone = true;
        state->listener = {};
        dropped.swap(state->buffers);
    }
    state->changed.notify_all();
}

std::optional<TupleBuffer> HandoffChannel::Receiver::pop(const std::stop_token& stopToken)
{
    if (!state)
    {
        throw CannotReadFromSource("Handoff receiver was moved from");
    }
    std::unique_lock lock(state->mutex);
    state->changed.wait(
        lock, stopToken, [this] { return state->aborted || state->closed || state->consumerGone || !state->buffers.empty(); });
    if (stopToken.stop_requested())
    {
        return std::nullopt;
    }
    state->checkAborted();
    if (state->buffers.empty())
    {
        return std::nullopt;
    }
    auto buffer = std::move(state->buffers.front());
    state->buffers.pop_front();
    return buffer;
}

std::optional<TupleBuffer> HandoffChannel::Receiver::tryPop()
{
    if (!state)
    {
        throw CannotReadFromSource("Handoff receiver was moved from");
    }
    const std::scoped_lock lock(state->mutex);
    state->checkAborted();
    if (state->buffers.empty())
    {
        return std::nullopt;
    }
    auto buffer = std::move(state->buffers.front());
    state->buffers.pop_front();
    return buffer;
}

bool HandoffChannel::Receiver::exhausted()
{
    if (!state)
    {
        throw CannotReadFromSource("Handoff receiver was moved from");
    }
    const std::scoped_lock lock(state->mutex);
    state->checkAborted();
    return (state->closed || state->consumerGone) && state->buffers.empty();
}

void HandoffChannel::Receiver::setListener(std::function<void()> onChange)
{
    if (!state)
    {
        return;
    }
    const std::scoped_lock lock(state->mutex);
    if (!state->consumerGone)
    {
        state->listener = std::move(onChange);
    }
}

HandoffChannel::HandoffChannel(size_t capacity) : capacity(capacity)
{
    auto endpoints = create(capacity);
    sender.emplace(std::move(endpoints.first));
    receiver.emplace(std::move(endpoints.second));
}

HandoffChannel::Sender HandoffChannel::takeSender()
{
    if (!sender)
    {
        throw InvalidQuerySyntax("Handoff sender already claimed");
    }
    auto result = std::move(*sender);
    sender.reset();
    return result;
}

HandoffChannel::Receiver HandoffChannel::takeReceiver()
{
    if (!receiver)
    {
        throw InvalidQuerySyntax("Handoff receiver already claimed");
    }
    auto result = std::move(*receiver);
    receiver.reset();
    return result;
}

void HandoffChannel::validate() const
{
    if (sender || receiver)
    {
        throw InvalidQuerySyntax("Handoff channel requires both a sender and a receiver");
    }
}

Reflected Reflector<HandoffChannel>::operator()(const HandoffChannel& channel, const ReflectionContext& context) const
{
    return context.reflect(channel.getCapacity());
}

HandoffChannel Unreflector<HandoffChannel>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    return HandoffChannel{context.unreflect<size_t>(reflected)};
}

}
