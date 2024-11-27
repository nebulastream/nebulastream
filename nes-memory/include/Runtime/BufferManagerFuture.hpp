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

#include <Runtime/BufferManagerCoroCore.hpp>
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
    using safeAwaiter = std::true_type;

    class WeakFuture
    {
        std::weak_ptr<promise_type> promise;

    public:
        using SafeAwaiter = GetInMemorySegmentFuture;

        explicit WeakFuture(std::weak_ptr<promise_type> promisePtr);
        WeakFuture(const WeakFuture& other) = default;
        WeakFuture(WeakFuture&& other) noexcept = default;
        WeakFuture& operator=(const WeakFuture& other) = default;
        WeakFuture& operator=(WeakFuture&& other) noexcept = default;

        std::optional<GetInMemorySegmentFuture> upgrade() const noexcept
        {
            if (const auto upgraded = promise.lock())
            {
                return GetInMemorySegmentFuture{upgraded};
            }
            return std::nullopt;
        }
    };

private:
    std::shared_ptr<promise_type> promise;

public:
    explicit GetInMemorySegmentFuture(const std::shared_ptr<promise_type>& promisePtr);
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

    template <detail::SharedPromise Promise>
    std::coroutine_handle<> await_suspend(std::coroutine_handle<Promise> suspending) noexcept
    {
        promise->updateIsDone();
        const auto previous = promise->setWaitingCoroutine(suspending.promise().getSharedPromise());
        INVARIANT(!previous.lock(), "Multiple coroutines trying to await the same promise");
        if (!isDone())
        {
            return std::noop_coroutine();
        }
        if (auto lockedPromise = promise->setWaitingCoroutine(std::weak_ptr<detail::VirtualPromise>{}).lock())
        {
            return lockedPromise->getHandle();
        }
        return std::noop_coroutine();
    }

    std::variant<std::vector<detail::DataSegment<detail::InMemoryLocation>>, uint32_t> await_resume() const noexcept;

    WeakFuture toWeak() const noexcept { return WeakFuture{std::weak_ptr{promise}}; }
};

static_assert(ProgressableFutureConcept<GetInMemorySegmentFuture>);
static_assert(detail::SafeAwaiter<GetInMemorySegmentFuture>);

class ReadSegmentFuture
{
public:
    using promise_type = ReadSegmentPromise<ReadSegmentFuture>;
    using safeAwaiter = std::true_type;

    class WeakFuture
    {
        std::weak_ptr<promise_type> promise;

    public:
        using SafeAwaiter = ReadSegmentFuture;

        explicit WeakFuture(std::weak_ptr<promise_type> promisePtr);
        WeakFuture(const WeakFuture& other) = default;
        WeakFuture(WeakFuture&& other) noexcept = default;
        WeakFuture& operator=(const WeakFuture& other) = default;
        WeakFuture& operator=(WeakFuture&& other) noexcept = default;

        std::optional<ReadSegmentFuture> upgrade() const noexcept
        {
            if (const auto upgraded = promise.lock())
            {
                return ReadSegmentFuture{upgraded};
            }
            return std::nullopt;
        }
    };

private:
    std::shared_ptr<promise_type> promise;

public:
    explicit ReadSegmentFuture(const std::shared_ptr<promise_type>& handle);
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

    template <detail::SharedPromise Promise>
    std::coroutine_handle<> await_suspend(std::coroutine_handle<Promise> suspending) noexcept
    {
        /* First set the waiting coroutine, then check for isDone
         * Example on why we can't first check isDone and then set waiting coroutine
         * if (!isDone()){
         *      //If this thread gets suspended right here, the coroutine could get suspended and never resumed.
         *      //While this thread is suspended here, another thread might run promise#return_value, set the result and isDone (it doesn't matter for this thread anymore)
         *      //and get and resume the coroutine in the promise, which is still not set to our suspending coroutine.
         *      //Then, our thread resumes, sets the waiting coroutine and suspends the coroutine, but no one will ever call resume anymore.
         *      promise->setWaitingCoroutine(suspending);
         *      return true;
         * }
         * return false;
         *
        */

        promise->updateIsDone();
        //First ensure that suspending is stored in the promise
        const auto previous = promise->setWaitingCoroutine(suspending.promise().getSharedPromise());
        INVARIANT(!previous.lock(), "Multiple coroutines trying to await the same promise");
        //If hasn't been set, suspend.
        //If the result gets set after we call isDone, we still give up control over the coroutine and whoever set the result resumes it
        if (!isDone())
        {
            return std::noop_coroutine();
        }

        /* Try to get our coroutine back.
         * There are two edge cases that could happen but that will still work out:
         * 1. We checked isDone, then got suspended, and another thread in return_value set isDone and got our coroutine.
         *      In that case we get an empty coroutine handle,
         *      causing us to suspend here and the thread setting the return value is continuing with our coroutine.
         * 2. Another thread registered itself. We usually don't want multiple threads awaiting on the same thing,
         *      because in most places we don't have a way to deal with multiple awaiting coroutines, but in this case it's fine.
         *      Either another thread triggered an invariance or its at the same point and also just continues with our coroutine.
         * In both cases we get a coroutine handle that is different to ours, but because we know that ours gets executed by another thread,
         * we just resume with the one we got.
        */

        if (auto lockedPromise = promise->setWaitingCoroutine(std::weak_ptr<detail::VirtualPromise>{}).lock())
        {
            return lockedPromise->getHandle();
        }
        return std::noop_coroutine();
    }

    std::variant<ssize_t, uint32_t> await_resume() const noexcept;

    WeakFuture toWeak() const noexcept { return WeakFuture{std::weak_ptr{promise}}; }

    // static constexpr bool safeAwaiter = true;
};

static_assert(detail::SafeAwaiter<ReadSegmentFuture>);
static_assert(ProgressableFutureConcept<ReadSegmentFuture>);

class PunchHoleFuture
{
public:
    using promise_type = PunchHolePromise<PunchHoleFuture>;
    using safeAwaiter = std::true_type;

private:
    std::shared_ptr<promise_type> promise;

public:
    class WeakFuture
    {
        std::weak_ptr<promise_type> promise;

    public:
        using SafeAwaiter = PunchHoleFuture;

        explicit WeakFuture(std::weak_ptr<promise_type> promisePtr);
        WeakFuture(const WeakFuture& other) = default;
        WeakFuture(WeakFuture&& other) noexcept = default;
        WeakFuture& operator=(const WeakFuture& other) = default;
        WeakFuture& operator=(WeakFuture&& other) noexcept = default;

        std::optional<PunchHoleFuture> upgrade() const noexcept
        {
            if (const auto upgraded = promise.lock())
            {
                return PunchHoleFuture{upgraded};
            }
            return std::nullopt;
        }
    };
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

        template <detail::SharedPromise Promise>
        bool await_suspend(std::coroutine_handle<Promise> toSuspend)
        {
            if (!promise->getYielded())
            {
                const auto previous = promise->setNextCoroutineOnYield(toSuspend.promise().getSharedPromise());
                INVARIANT(!previous.lock(), "Multiple couroutines trying to await the same promise");
                return true;
            }
            return false;
        }

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
    [[nodiscard]] std::optional<std::variant<UringSuccessOrError, detail::CoroutineError>> getResult() const noexcept;

    //TODO add timeout to task and give back nullopt when timeout exceeded
    [[nodiscard]] std::optional<std::variant<UringSuccessOrError, detail::CoroutineError>> waitUntilDone() noexcept;

    [[nodiscard]] detail::DataSegment<detail::OnDiskLocation> getTarget() const noexcept;

    OwnershipTransferredAwaiter awaitYield() const noexcept;
    bool await_ready() const noexcept;

    template <detail::SharedPromise Promise>
    std::coroutine_handle<> await_suspend(std::coroutine_handle<Promise> suspending) const noexcept
    {
        promise->updateIsDone();
        const auto previous = promise->setWaitingCoroutine(suspending.promise().getSharedPromise());
        INVARIANT(!previous.lock(), "Multiple coroutines trying to await the same promise");
        if (!isDone())
        {
            return std::noop_coroutine();
        }
        if (auto lockedPromise = promise->setWaitingCoroutine(std::weak_ptr<detail::VirtualPromise>{}).lock())
        {
            return lockedPromise->getHandle();
        }
        return std::noop_coroutine();
    }

    std::variant<UringSuccessOrError, detail::CoroutineError> await_resume() const noexcept;

    WeakFuture toWeak() const noexcept { return WeakFuture{std::weak_ptr{promise}}; }

    // static constexpr bool safeAwaiter = true;
};

static_assert(detail::SafeAwaiter<PunchHoleFuture>);
static_assert(ProgressableFutureConcept<PunchHoleFuture>);


class RepinBufferFuture
{
public:
    using promise_type = RepinBufferPromise<RepinBufferFuture>;
    using safeAwaiter = std::true_type;

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

    template <detail::SharedPromise Promise>
    std::coroutine_handle<> await_suspend(std::coroutine_handle<Promise> suspending) const noexcept
    {
        promise->updateIsDone();
        const auto previous = promise->setWaitingCoroutine(suspending.promise().getSharedPromise());
        INVARIANT(!previous.lock(), "Multiple coroutines trying to await the same promise");
        if (!isDone())
        {
            return std::noop_coroutine();
        }
        if (auto lockedPromise = promise->setWaitingCoroutine(std::weak_ptr<detail::VirtualPromise>{}).lock())
        {
            return lockedPromise->getHandle();
        }
        return std::noop_coroutine();
    }

    std::variant<PinnedBuffer, uint32_t> await_resume() const noexcept;


    static RepinBufferFuture fromPinnedBuffer(PinnedBuffer) noexcept;

    // static constexpr bool safeAwaiter = true;
};

static_assert(detail::SafeAwaiter<RepinBufferFuture>);
static_assert(ProgressableFutureConcept<RepinBufferFuture>);
}
