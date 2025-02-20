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
#include <type_traits>

#include <Runtime/BufferManagerPromise.hpp>

namespace NES::Memory
{

template <typename Future>
concept ProgressableFutureConcept = requires(Future future) {
    { future.pollOnce() } -> std::same_as<void>;
    { future.waitOnce() } -> std::same_as<void>;
    { future.isDone() } -> std::same_as<bool>;
} && std::is_move_assignable_v<Future>;


class GetInMemorySegmentFuture
{
public:
    using promise_type = GetInMemorySegmentPromise<GetInMemorySegmentFuture>;

private:
    std::shared_ptr<promise_type> promise;

public:
    explicit GetInMemorySegmentFuture(const std::coroutine_handle<promise_type> handle);
    GetInMemorySegmentFuture(const GetInMemorySegmentFuture& other) = default;
    GetInMemorySegmentFuture(GetInMemorySegmentFuture&& other) noexcept = default;
    GetInMemorySegmentFuture& operator=(const GetInMemorySegmentFuture& other) = default;
    GetInMemorySegmentFuture& operator=(GetInMemorySegmentFuture&& other) noexcept = default;

    ~GetInMemorySegmentFuture() = default;

    void pollOnce() noexcept;
    void waitOnce() noexcept;

    [[nodiscard]] bool isDone() const noexcept;
    //TODO add timeout to task and give back nullopt when timeout exceededc
    std::variant<std::vector<detail::DataSegment<detail::InMemoryLocation>>, uint32_t> waitUntilDone() noexcept;

    bool await_ready() const noexcept;

    std::coroutine_handle<> await_suspend(std::coroutine_handle<> suspending) noexcept;

    std::variant<std::vector<detail::DataSegment<detail::InMemoryLocation>>, uint32_t> await_resume() const noexcept;
};

static_assert(ProgressableFutureConcept<GetInMemorySegmentFuture>);

class ReadSegmentFuture
{
public:
    using promise_type = ReadSegmentPromise<ReadSegmentFuture>;

private:
    std::shared_ptr<promise_type> promise;

public:
    explicit ReadSegmentFuture(const std::coroutine_handle<promise_type> handle);
    ReadSegmentFuture(const ReadSegmentFuture& other) = default;
    ReadSegmentFuture(ReadSegmentFuture&& other) noexcept = default;
    ReadSegmentFuture& operator=(const ReadSegmentFuture& other) = default;
    ReadSegmentFuture& operator=(ReadSegmentFuture&& other) noexcept = default;
    ~ReadSegmentFuture() = default;

    void pollOnce() noexcept;
    void waitOnce() noexcept;

    [[nodiscard]] bool isDone() const noexcept;

    //TODO add timeout to task and give back nullopt when timeout exceededc
    std::variant<ssize_t, uint32_t> waitUntilDone() noexcept;

    bool await_ready() const noexcept;

    std::coroutine_handle<> await_suspend(std::coroutine_handle<> suspending) noexcept;

    std::variant<ssize_t, uint32_t> await_resume() const noexcept;
};
static_assert(ProgressableFutureConcept<ReadSegmentFuture>);

class PunchHoleFuture
{
public:
    using promise_type = PunchHolePromise<PunchHoleFuture>;

private:
    std::shared_ptr<promise_type> promise;

public:
    class OwnershipTransferredAwaiter
    {
        std::shared_ptr<promise_type> promise;

    public:
        explicit OwnershipTransferredAwaiter(const std::shared_ptr<promise_type>& promise);
        OwnershipTransferredAwaiter(const OwnershipTransferredAwaiter& other) = default;
        OwnershipTransferredAwaiter(OwnershipTransferredAwaiter&& other) noexcept = default;
        OwnershipTransferredAwaiter& operator=(const OwnershipTransferredAwaiter& other) = default;
        OwnershipTransferredAwaiter& operator=(OwnershipTransferredAwaiter&& other) noexcept = default;
        bool await_ready() const { return promise->getYielded().value_or(false); }

        bool await_suspend(std::coroutine_handle<> toSuspend);

        bool await_resume() const noexcept;
    };
    explicit PunchHoleFuture() = default;
    explicit PunchHoleFuture(const std::coroutine_handle<promise_type> handle);
    explicit PunchHoleFuture(const std::shared_ptr<promise_type>& handle);
    PunchHoleFuture(const PunchHoleFuture& other) noexcept = default;
    PunchHoleFuture(PunchHoleFuture&& other) noexcept = default;
    PunchHoleFuture& operator=(const PunchHoleFuture& other) = default;
    PunchHoleFuture& operator=(PunchHoleFuture&& other) noexcept = default;

    ~PunchHoleFuture() noexcept = default;

    void pollOnce() noexcept;
    void waitOnce() noexcept;

    bool waitUntilYield() noexcept;

    [[nodiscard]] bool isDone() const noexcept;
    [[nodiscard]] std::optional<std::variant<UringSuccessOrError, CoroutineError>> getResult() const noexcept;

    //TODO add timeout to task and give back nullopt when timeout exceeded
    [[nodiscard]] std::optional<std::variant<UringSuccessOrError, CoroutineError>> waitUntilDone() noexcept;

    [[nodiscard]] detail::DataSegment<detail::OnDiskLocation> getTarget() const noexcept;

    OwnershipTransferredAwaiter awaitYield() const noexcept;
    bool await_ready() const noexcept;

    std::coroutine_handle<> await_suspend(std::coroutine_handle<> suspending) const noexcept;

    std::variant<UringSuccessOrError, CoroutineError> await_resume() const noexcept;
};
static_assert(ProgressableFutureConcept<PunchHoleFuture>);


class RepinBufferFuture
{
public:
    using promise_type = RepinBufferPromise<RepinBufferFuture>;

private:
    std::shared_ptr<promise_type> promise;

public:
    explicit RepinBufferFuture() = default;
    explicit RepinBufferFuture(const std::coroutine_handle<promise_type> handle);
    explicit RepinBufferFuture(const std::shared_ptr<promise_type>& handle);
    RepinBufferFuture(const RepinBufferFuture& other) noexcept = default;
    RepinBufferFuture(RepinBufferFuture&& other) noexcept = default;
    RepinBufferFuture& operator=(const RepinBufferFuture& other) = default;
    RepinBufferFuture& operator=(RepinBufferFuture&& other) noexcept = default;

    ~RepinBufferFuture() noexcept = default;

    void pollOnce() const noexcept;
    void waitOnce() const noexcept;

    [[nodiscard]] bool isDone() const noexcept;

    std::variant<PinnedBuffer, uint32_t> waitUntilDone() const noexcept;

    bool await_ready() const noexcept;

    std::coroutine_handle<> await_suspend(std::coroutine_handle<> suspending) const noexcept;

    std::variant<PinnedBuffer, uint32_t> await_resume() const noexcept;


    static RepinBufferFuture fromPinnedBuffer(PinnedBuffer&) noexcept;
};
}
