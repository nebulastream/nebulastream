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
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <stop_token>
#include <utility>
#include <Runtime/TupleBuffer.hpp>
#include <Util/ReflectionFwd.hpp>

namespace NES
{

/// Bounded, zero-copy queue with exactly one owning endpoint on each side.
class HandoffChannel
{
    struct State;

public:
    enum class PushResult : uint8_t
    {
        PUSHED,
        FULL,
        CONSUMER_GONE,
    };

    static constexpr size_t DEFAULT_CAPACITY = 64;

    class Sender
    {
    public:
        Sender(const Sender&) = delete;
        Sender& operator=(const Sender&) = delete;
        Sender(Sender&&) noexcept;
        Sender& operator=(Sender&&) noexcept;
        /// Destruction without close aborts the channel, rather than signalling EOS.
        ~Sender();

        /// Throws if this endpoint was closed or moved from.
        [[nodiscard]] PushResult tryPush(const TupleBuffer& buffer);
        /// Idempotent normal EOS; already queued buffers remain readable.
        void close();

    private:
        friend class HandoffChannel;
        explicit Sender(std::shared_ptr<State> state);
        void cancel();
        std::shared_ptr<State> state;
    };

    class Receiver
    {
    public:
        Receiver(const Receiver&) = delete;
        Receiver& operator=(const Receiver&) = delete;
        Receiver(Receiver&&) noexcept;
        Receiver& operator=(Receiver&&) noexcept;
        ~Receiver();

        /// Nullopt on normal drained EOS, receiver close, or stop request. Aborts throw.
        std::optional<TupleBuffer> pop(const std::stop_token& stopToken);
        /// Nullopt when empty; aborts throw even if buffers were queued.
        std::optional<TupleBuffer> tryPop();
        [[nodiscard]] bool exhausted();
        /// Drops queued buffers and rejects future pushes. Idempotent.
        void close();
        /// Invoked under the channel lock on push, EOS, and abort. Must not block, throw,
        /// or call into this channel. Replacing/clearing waits for any current invocation.
        /// Receiver close clears the listener before returning.
        void setListener(std::function<void()> onChange);

    private:
        friend class HandoffChannel;
        explicit Receiver(std::shared_ptr<State> state);
        std::shared_ptr<State> state;
    };

    /// Capacity must be positive. The receiver exists immediately, so sends need not wait for deployment.
    static std::pair<Sender, Receiver> create(size_t capacity);

    explicit HandoffChannel(size_t capacity);
    HandoffChannel(const HandoffChannel&) = delete;
    HandoffChannel& operator=(const HandoffChannel&) = delete;
    HandoffChannel(HandoffChannel&&) noexcept = default;

    /// Endpoint claims are made once, during runtime construction before execution starts.
    Sender takeSender();
    Receiver takeReceiver();
    /// Rejects configurations whose endpoints have not both been claimed.
    void validate() const;

    [[nodiscard]] size_t getCapacity() const { return capacity; }

private:
    size_t capacity;
    std::optional<Sender> sender;
    std::optional<Receiver> receiver;
};

/// Serialize configuration only, never live queue state or endpoint claim state.
template <>
struct Reflector<HandoffChannel>
{
    Reflected operator()(const HandoffChannel& channel, const ReflectionContext& context) const;
};

template <>
struct Unreflector<HandoffChannel>
{
    HandoffChannel operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

}
