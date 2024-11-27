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
#include <any>
#include <atomic>
#include <coroutine>
#include <memory>
#include <optional>
#include <variant>
#include <vector>

#include <Runtime/BufferManagerCoroCore.hpp>
#include <Runtime/DataSegment.hpp>
#include <boost/date_time/date_generators.hpp>

#include <ErrorHandling.hpp>

#include <Runtime/PinnedBuffer.hpp>

namespace NES::Memory
{

namespace detail
{
class VirtualAwaiterForProgress;
}
class BufferManager;


template <typename Future>
class GetInMemorySegmentPromise : public detail::VirtualPromise
{
    std::optional<std::variant<std::vector<detail::DataSegment<detail::InMemoryLocation>>, detail::CoroutineError>> result{};
    //both final suspend and returnValueSet are strictly speaking not needed, but they are used in invariants ensuring the correct usage of isDone
    std::atomic_flag returnValueSet{false};
    std::atomic_flag finalSuspendStarted{false};
    std::atomic_flag isDone{false};
    std::atomic<std::weak_ptr<detail::VirtualAwaiterForProgress>> lastBlockAwaiter{std::weak_ptr<detail::VirtualAwaiterForProgress>{}};

    //The comment used to be just:
    //"Raw pointer because the ResumeAfterBlockAwaiter is allocated on the "stack" of the coroutine anyway but we also need a not-present value"
    //Until I ran into dangling pointers
    std::atomic<std::weak_ptr<detail::VirtualAwaiterForProgress>> currentBlockAwaiter{std::weak_ptr<detail::VirtualAwaiterForProgress>{}};

    std::variant<std::shared_ptr<GetInMemorySegmentPromise>, std::weak_ptr<GetInMemorySegmentPromise>> sharedHandle;
    std::atomic<std::weak_ptr<VirtualPromise>> nextCoroutine{std::weak_ptr<VirtualPromise>{}};
    int awaiterCounter = 0;

public:
    //Constructor call is inserted by the compiler when calling the coroutine function,
    //passing instance and any arguments to the coroutine to this function
    GetInMemorySegmentPromise()
        : sharedHandle(
              std::shared_ptr<GetInMemorySegmentPromise>{
                  this, [](auto promise) { std::coroutine_handle<GetInMemorySegmentPromise>::from_promise(*promise).destroy(); }})
    {
    }
    Future get_return_object()
    {
        return std::visit(
            [&](auto&& ptr)
            {
                using T = std::decay_t<decltype(ptr)>;
                if constexpr (std::is_same_v<T, std::shared_ptr<std::remove_pointer_t<decltype(this)>>>)
                {
                    auto temp = ptr;
                    sharedHandle = std::weak_ptr{temp};
                    return Future(temp);
                }
                else if constexpr (std::is_same_v<T, std::weak_ptr<std::remove_pointer_t<decltype(this)>>>)
                {
                    return Future(ptr.lock());
                }
            },
            sharedHandle);
    }
    void return_value(std::variant<std::vector<detail::DataSegment<detail::InMemoryLocation>>, detail::CoroutineError> segment) noexcept
    {
        //First set result, then set returnValueSet.
        //If copy is still in progress, then getResult will still return std::nullopt.
        result = segment;
        const auto wasSetBefore = returnValueSet.test_and_set();
        INVARIANT(!wasSetBefore, "Promise return_value method was invoked twice");
    }

    void unhandled_exception() noexcept { };
    std::suspend_never initial_suspend() noexcept { return {}; };
    //Needs to always suspend, because otherwise the coroutine self-destructs after co_return.
    //When then the destructor of the future is called because it goes out-of-scope, it causes a segmentation fault
    detail::ErasedAwaiter final_suspend() noexcept
    {
        //retrieve the next coroutine first, because this object can be destroyed any moment when finalSuspendStarted is set
        auto awaiterPtr = std::unique_ptr<detail::VirtualAwaiter>(
            new detail::ContinueOuterCoroutineAwaiter<GetInMemorySegmentPromise>(nextCoroutine.exchange(std::weak_ptr<VirtualPromise>{})));
        const auto finalSuspendedBefore = finalSuspendStarted.test_and_set();
        INVARIANT(!finalSuspendedBefore, "Coroutines final suspend called more than once");
        return detail::ErasedAwaiter{std::move(awaiterPtr)};
    }

    std::weak_ptr<detail::VirtualAwaiterForProgress> exchangeAwaiter(detail::VirtualAwaiterForProgress* newAwaiter)
    {
        //For now, we only deal with weak ptrs
        INVARIANT(newAwaiter == nullptr, "Setting non-nullptr awaiter over exchange Awaiter is not supported for GetInMemoryPromise");
        return currentBlockAwaiter.exchange(std::weak_ptr<detail::VirtualAwaiterForProgress>{});
    }
    std::weak_ptr<detail::VirtualAwaiterForProgress> exchangeAwaiter(std::weak_ptr<detail::VirtualAwaiterForProgress> newAwaiter)
    {
        auto oldAwaiter = currentBlockAwaiter.exchange(newAwaiter);
        if (newAwaiter.lock().get() != nullptr)
        {
            lastBlockAwaiter = newAwaiter;
        }
        return oldAwaiter;
    }

    std::optional<std::variant<std::vector<detail::DataSegment<detail::InMemoryLocation>>, detail::CoroutineError>>
    getResult() const noexcept
    {
        /*Originally, we only checked returnValueSet, but this can lead to race conditions:
         * while thread A is always checking if a result has set and if not runs the external progress function (like in waitUntilDone),
         * another thread B could be completing the coroutine.
         * Because returnValueSet in return_value is set in the beginning, we might still be running that method, but A could have the result
         * and now drops the future, destroying the coroutine while it is still running.
         *
         * TODO we might not actually need returnValueSet
        */

        if (isDone.test())
        {
            INVARIANT(result.has_value(), "Promise was marked as done, but no result was set");
            return *result;
        }
        return std::nullopt;
    }

    std::shared_ptr<GetInMemorySegmentPromise> getSharedPromise()
    {
        return std::visit(
            [](auto&& ptr)
            {
                using T = std::decay_t<decltype(ptr)>;
                if constexpr (std::is_same_v<T, std::shared_ptr<GetInMemorySegmentPromise>>)
                {
                    return ptr;
                }
                else if constexpr (std::is_same_v<T, std::weak_ptr<GetInMemorySegmentPromise>>)
                {
                    return ptr.lock();
                }
            },
            sharedHandle);
    }
    std::weak_ptr<VirtualPromise> setWaitingCoroutine(const std::weak_ptr<VirtualPromise> next) noexcept override
    {
        return nextCoroutine.exchange(next);
    }

    void increaseAwaitCounter() { awaiterCounter++; }

    void updateIsDone() noexcept
    {
        if (returnValueSet.test() && finalSuspendStarted.test())
        {
            const auto wasCompletedBefore = isDone.test_and_set();
            // INVARIANT(!wasCompletedBefore, "Trying to mark coroutine as done, but was already set done");
        }
    }
    std::coroutine_handle<> getHandle() noexcept override { return std::coroutine_handle<decltype(*this)>::from_promise(*this); }
};

template <typename Future>
class ReadSegmentPromise : public detail::VirtualPromise
{
    //friend class ReadSegmentFuture;
    detail::DataSegment<detail::OnDiskLocation> source;
    detail::DataSegment<detail::InMemoryLocation> target;
    std::atomic<std::weak_ptr<detail::VirtualAwaiterForProgress>> lastBlockAwaiter{std::weak_ptr<detail::VirtualAwaiterForProgress>{}};
    std::atomic<std::weak_ptr<detail::VirtualAwaiterForProgress>> currentBlockAwaiter{std::weak_ptr<detail::VirtualAwaiterForProgress>{}};
    std::variant<ssize_t, detail::CoroutineError> result;

    std::atomic_flag finalSuspendStarted{false};
    std::atomic_flag hasResult{false};
    std::atomic_flag isDone{false};

    std::variant<std::shared_ptr<ReadSegmentPromise>, std::weak_ptr<ReadSegmentPromise>> sharedHandle;

    std::atomic<std::weak_ptr<detail::VirtualPromise>> nextCoroutine{std::weak_ptr<detail::VirtualPromise>{}};

public:
    //Constructor call is inserted by the compiler when calling the coroutine function,
    //passing instance and any arguments to the coroutine to this function
    ReadSegmentPromise(
        BufferManager&, detail::DataSegment<detail::OnDiskLocation> source, detail::DataSegment<detail::InMemoryLocation> target)
        : source(source)
        , target(target)
        , sharedHandle(
              std::shared_ptr<ReadSegmentPromise>{
                  this, [](auto promise) { std::coroutine_handle<ReadSegmentPromise>::from_promise(*promise).destroy(); }})
    {
    }
    Future get_return_object()
    {
        return std::visit(
            [&](auto&& ptr)
            {
                using T = std::decay_t<decltype(ptr)>;
                if constexpr (std::is_same_v<T, std::shared_ptr<std::remove_pointer_t<decltype(this)>>>)
                {
                    auto temp = ptr;
                    sharedHandle = std::weak_ptr{temp};
                    return Future(temp);
                }
                else if constexpr (std::is_same_v<T, std::weak_ptr<std::remove_pointer_t<decltype(this)>>>)
                {
                    return Future(ptr.lock());
                }
            },
            sharedHandle);
    }
    void return_value(std::variant<ssize_t, detail::CoroutineError> result) noexcept
    {
        this->result = result;
        hasResult.test_and_set();
        auto resumption = nextCoroutine.exchange({});
    }

    void unhandled_exception() noexcept { };
    std::suspend_never initial_suspend() noexcept { return {}; };
    //Needs to always suspend, because otherwise the coroutine self-destructs after co_return.
    //When then the destructor of the future is called because it goes out-of-scope, it causes a segmentation fault
    detail::ErasedAwaiter final_suspend() noexcept
    {
        const auto finalSuspendedBefore = finalSuspendStarted.test_and_set();
        INVARIANT(!finalSuspendedBefore, "Coroutines final suspend called more than once");

        auto awaiterPtr = std::unique_ptr<detail::VirtualAwaiter>(
            new detail::ContinueOuterCoroutineAwaiter<ReadSegmentPromise>(nextCoroutine.exchange(std::weak_ptr<VirtualPromise>{})));
        return detail::ErasedAwaiter{std::move(awaiterPtr)};
    }


    std::optional<std::variant<ssize_t, detail::CoroutineError>> getResult()
    {
        if (isDone.test())
        {
            return result;
        }
        return std::nullopt;
    }
    std::weak_ptr<detail::VirtualAwaiterForProgress> exchangeAwaiter(detail::VirtualAwaiterForProgress* newAwaiter)
    {
        //For now, we only deal with weak ptrs
        INVARIANT(newAwaiter == nullptr, "Setting non-nullptr awaiter over exchange Awaiter is not supported for GetInMemoryPromise");
        return currentBlockAwaiter.exchange(std::weak_ptr<detail::VirtualAwaiterForProgress>{});
    }
    std::weak_ptr<detail::VirtualAwaiterForProgress> exchangeAwaiter(std::weak_ptr<detail::VirtualAwaiterForProgress> newAwaiter)
    {
        auto oldAwaiter = currentBlockAwaiter.exchange(newAwaiter);
        if (newAwaiter.lock().get() != nullptr)
        {
            lastBlockAwaiter = newAwaiter;
        }
        return oldAwaiter;
    }

    std::shared_ptr<ReadSegmentPromise> getSharedPromise()
    {
        return std::visit(
            [](auto&& ptr)
            {
                using T = std::decay_t<decltype(ptr)>;
                if constexpr (std::is_same_v<T, std::shared_ptr<std::remove_pointer_t<decltype(this)>>>)
                {
                    return ptr;
                }
                else if constexpr (std::is_same_v<T, std::weak_ptr<std::remove_pointer_t<decltype(this)>>>)
                {
                    return ptr.lock();
                }
            },
            sharedHandle);
    }
    std::weak_ptr<detail::VirtualPromise> setWaitingCoroutine(const std::weak_ptr<detail::VirtualPromise> next) noexcept override
    {
        return nextCoroutine.exchange(next);
    }

    void updateIsDone()
    {
        if (hasResult.test() && finalSuspendStarted.test())
        {
            const auto wasCompletedBefore = isDone.test_and_set();
            // INVARIANT(!wasCompletedBefore, "Trying to mark coroutine as done, but was already set done");
        }
    }

    std::coroutine_handle<> getHandle() noexcept override { return std::coroutine_handle<decltype(*this)>::from_promise(*this); }
};


template <typename Future>
class RepinBufferPromise : public detail::VirtualPromise
{
    //friend class RepinBufferFuture;

    std::variant<PinnedBuffer, detail::CoroutineError> result{};

    std::atomic_flag finalSuspendStarted{false};
    std::atomic_flag returnValueSet{false};
    std::atomic_flag isDone{false};
    std::atomic<std::weak_ptr<detail::VirtualAwaiterForProgress>> lastBlockAwaiter{std::weak_ptr<detail::VirtualAwaiterForProgress>{}};
    std::atomic<std::weak_ptr<detail::VirtualAwaiterForProgress>> currentBlockAwaiter{std::weak_ptr<detail::VirtualAwaiterForProgress>{}};

    std::variant<std::shared_ptr<RepinBufferPromise>, std::weak_ptr<RepinBufferPromise>> sharedHandle;

    std::atomic<std::weak_ptr<detail::VirtualPromise>> nextCoroutine{std::weak_ptr<detail::VirtualPromise>{}};

public:
    RepinBufferPromise()
        : sharedHandle(
              std::shared_ptr<RepinBufferPromise>{
                  this, [](auto promise) { std::coroutine_handle<RepinBufferPromise>::from_promise(*promise).destroy(); }})
    {
    }

    Future get_return_object()
    {
        return std::visit(
            [&](auto&& ptr)
            {
                using T = std::decay_t<decltype(ptr)>;
                if constexpr (std::is_same_v<T, std::shared_ptr<std::remove_pointer_t<decltype(this)>>>)
                {
                    auto temp = ptr;
                    sharedHandle = std::weak_ptr{temp};
                    return Future(temp);
                }
                else if constexpr (std::is_same_v<T, std::weak_ptr<std::remove_pointer_t<decltype(this)>>>)
                {
                    return Future(ptr.lock());
                }
            },
            sharedHandle);
    }
    void return_value(const std::variant<PinnedBuffer, detail::CoroutineError>& result)
    {
        this->result = result;
        returnValueSet.test_and_set();
    }

    void unhandled_exception() noexcept { };
    std::suspend_never initial_suspend() noexcept { return {}; }
    detail::ErasedAwaiter final_suspend() noexcept
    {
        const auto finalSuspendedBefore = finalSuspendStarted.test_and_set();
        INVARIANT(!finalSuspendedBefore, "Coroutines final suspend called more than once");
        // auto awaiterPtr = std::unique_ptr<detail::VirtualAwaiter>(dynamic_cast<detail::VirtualAwaiter*>(
        //     new detail::ContinueOuterCoroutineAwaiter<RepinBufferPromise>(nextCoroutine.exchange(std::weak_ptr<VirtualPromise>{}))));
        auto awaiterPtr = std::unique_ptr<detail::VirtualAwaiter>(
            new detail::ContinueOuterCoroutineAwaiter<RepinBufferPromise>(nextCoroutine.exchange(std::weak_ptr<VirtualPromise>{})));
        return detail::ErasedAwaiter{std::move(awaiterPtr)};
    }

    std::optional<std::variant<PinnedBuffer, detail::CoroutineError>> getResult() const noexcept
    {
        if (isDone.test())
        {
            INVARIANT(returnValueSet.test(), "Promise was marked as done, but no result was set");
            return result;
        }
        return std::nullopt;
    }
    std::weak_ptr<detail::VirtualAwaiterForProgress> exchangeAwaiter(detail::VirtualAwaiterForProgress* newAwaiter)
    {
        //For now, we only deal with weak ptrs
        INVARIANT(newAwaiter == nullptr, "Setting non-nullptr awaiter over exchange Awaiter is not supported for GetInMemoryPromise");
        return currentBlockAwaiter.exchange(std::weak_ptr<detail::VirtualAwaiterForProgress>{});
    }
    std::weak_ptr<detail::VirtualAwaiterForProgress> exchangeAwaiter(std::weak_ptr<detail::VirtualAwaiterForProgress> newAwaiter)
    {
        auto oldAwaiter = currentBlockAwaiter.exchange(newAwaiter);
        if (newAwaiter.lock().get() != nullptr)
        {
            lastBlockAwaiter = newAwaiter;
        }
        return oldAwaiter;
    }
    std::shared_ptr<RepinBufferPromise> getSharedPromise()
    {
        return std::visit(
            [](auto&& ptr)
            {
                using T = std::decay_t<decltype(ptr)>;
                if constexpr (std::is_same_v<T, std::shared_ptr<std::remove_pointer_t<decltype(this)>>>)
                {
                    return ptr;
                }
                else if constexpr (std::is_same_v<T, std::weak_ptr<std::remove_pointer_t<decltype(this)>>>)
                {
                    return ptr.lock();
                }
            },
            sharedHandle);
    }

    std::weak_ptr<detail::VirtualPromise> setWaitingCoroutine(const std::weak_ptr<detail::VirtualPromise> next) noexcept override
    {
        return nextCoroutine.exchange(next);
    }

    void updateIsDone()
    {
        if (returnValueSet.test() && finalSuspendStarted.test())
        {
            const auto wasCompletedBefore = isDone.test_and_set();
            // INVARIANT(!wasCompletedBefore, "Trying to mark coroutine as done, but was already set done");
        }
    }

    std::coroutine_handle<> getHandle() noexcept override { return std::coroutine_handle<decltype(*this)>::from_promise(*this); }
};


class UringSuccessOrError
{
    //0 indicates success,
    //negative values indicate iouring errors, that must be negated to get an error message through strerror
    //positive values indicate our errors
    uint32_t returnCode;

public:
    constexpr UringSuccessOrError(const UringSuccessOrError& other) = default;
    constexpr UringSuccessOrError(UringSuccessOrError&& other) noexcept = default;
    constexpr UringSuccessOrError& operator=(const UringSuccessOrError& other) = default;
    constexpr UringSuccessOrError& operator=(UringSuccessOrError&& other) noexcept = default;
    constexpr explicit UringSuccessOrError(const uint32_t returnCode) : returnCode(returnCode) { }

    [[nodiscard]] constexpr bool isSuccess() const { return returnCode == 0; }

    [[nodiscard]] constexpr std::optional<uint32_t> getUringError() const
    {
        if (returnCode > 0)
        {
            return returnCode;
        }
        return std::nullopt;
    }

    // static constexpr UringSuccessOrError FAILED_TO_TRANSFER_OWNERSHIP() { return UringSuccessOrError{ErrorCode::FailedToTransferCleanupOwnership}; }
    //
    // static constexpr UringSuccessOrError FAILED_TO_SUBMIT() { return UringSuccessOrError{ErrorCode::CannotSubmitBufferIO}; }
    //
    // static constexpr UringSuccessOrError CONTINUED_WITHOUT_RESULT() { return UringSuccessOrError{ErrorCode::CoroutineContinuedWithoutResult}; }
};

template <typename Future>
class PunchHolePromise : detail::VirtualPromise
{
    //friend class PunchHoleFuture;
    detail::DataSegment<detail::OnDiskLocation> target;
    std::atomic<std::weak_ptr<detail::VirtualAwaiterForProgress>> lastBlockAwaiter{std::weak_ptr<detail::VirtualAwaiterForProgress>{}};
    std::atomic<std::weak_ptr<detail::VirtualAwaiterForProgress>> currentBlockAwaiter{std::weak_ptr<detail::VirtualAwaiterForProgress>{}};

    std::atomic_flag finalSuspendStarted{false};
    std::atomic_flag isDone{false};
    std::atomic<std::optional<std::variant<UringSuccessOrError, detail::CoroutineError>>> result{};
    //TODO is three value logic really necessary?
    std::optional<bool> hasSubmitted;
    std::variant<std::shared_ptr<PunchHolePromise>, std::weak_ptr<PunchHolePromise>> sharedHandle;
    std::atomic<std::weak_ptr<detail::VirtualPromise>> nextCoroutineOnReturn{std::weak_ptr<detail::VirtualPromise>{}};
    std::atomic<std::weak_ptr<detail::VirtualPromise>> nextCoroutineOnYield{std::weak_ptr<detail::VirtualPromise>{}};

public:
    //Constructor call is inserted by the compiler when calling the coroutine function,
    //passing instance and any arguments to the coroutine to this function
    PunchHolePromise(BufferManager&, detail::DataSegment<detail::OnDiskLocation> target)
        : target(target)
        , sharedHandle(
              std::shared_ptr<PunchHolePromise>{
                  this, [](auto promise) { std::coroutine_handle<PunchHolePromise>::from_promise(*promise).destroy(); }})
    {
    }
    Future get_return_object()
    {
        return std::visit(
            [&](auto&& ptr)
            {
                using T = std::decay_t<decltype(ptr)>;
                if constexpr (std::is_same_v<T, std::shared_ptr<std::remove_pointer_t<decltype(this)>>>)
                {
                    auto temp = ptr;
                    sharedHandle = std::weak_ptr{temp};
                    return Future(temp);
                }
                else if constexpr (std::is_same_v<T, std::weak_ptr<std::remove_pointer_t<decltype(this)>>>)
                {
                    return Future(ptr.lock());
                }
            },
            sharedHandle);
    }
    void return_value(std::variant<UringSuccessOrError, detail::CoroutineError> result) noexcept
    {
        this->result = result;
        auto resumption = nextCoroutineOnReturn.exchange({});
    }
    void unhandled_exception() noexcept { };

    //Suspend never because it is just information for the caller.
    //Either, it succeeds to submit and then suspends when waiting for the confirmation, or it needs to reach co_return so that the coroutine
    //does not get leaked through shared handles in the coroutine itself.
    detail::ErasedAwaiter yield_value(bool hasSubmitted) noexcept
    {
        this->hasSubmitted = hasSubmitted;
        auto resumption = nextCoroutineOnYield.exchange(std::weak_ptr<VirtualPromise>{});
        auto awaiterPtr = std::unique_ptr<detail::VirtualAwaiter>(
            new detail::ContinueOuterCoroutineAwaiter<PunchHolePromise, false>(nextCoroutineOnYield.exchange(std::weak_ptr<VirtualPromise>{})));
        return detail::ErasedAwaiter{std::move(awaiterPtr)};
    }

    std::suspend_never initial_suspend() noexcept { return {}; };
    //Needs to always suspend, because otherwise the coroutine self-destructs after co_return.
    //When then the destructor of the future is called because it goes out-of-scope, it causes a segmentation fault
    detail::ErasedAwaiter final_suspend() noexcept
    {
        const auto finalSuspendedBefore = finalSuspendStarted.test_and_set();
        INVARIANT(!finalSuspendedBefore, "Coroutines final suspend called more than once");
        auto awaiterPtr = std::make_unique<detail::ContinueOuterCoroutineAwaiter<PunchHolePromise>>(
            nextCoroutineOnReturn.exchange(std::weak_ptr<VirtualPromise>{}));
        return detail::ErasedAwaiter{std::move(awaiterPtr)};
    }

    std::optional<std::variant<UringSuccessOrError, detail::CoroutineError>> getResult() const noexcept
    {
        if (isDone.test())
        {
            return result;
        }
        return std::nullopt;
    }

    std::optional<bool> getYielded() const noexcept { return hasSubmitted; }

    std::weak_ptr<detail::VirtualAwaiterForProgress> exchangeAwaiter(detail::VirtualAwaiterForProgress* newAwaiter)
    {
        //For now, we only deal with weak ptrs
        INVARIANT(newAwaiter == nullptr, "Setting non-nullptr awaiter over exchange Awaiter is not supported for GetInMemoryPromise");
        return currentBlockAwaiter.exchange(std::weak_ptr<detail::VirtualAwaiterForProgress>{});
    }
    std::weak_ptr<detail::VirtualAwaiterForProgress> exchangeAwaiter(std::weak_ptr<detail::VirtualAwaiterForProgress> newAwaiter)
    {
        auto oldAwaiter = currentBlockAwaiter.exchange(newAwaiter);
        if (newAwaiter.lock().get() != nullptr)
        {
            lastBlockAwaiter = newAwaiter;
        }
        return oldAwaiter;
    }

    std::shared_ptr<PunchHolePromise> getSharedPromise()
    {
        return std::visit(
            [](auto&& ptr)
            {
                using T = std::decay_t<decltype(ptr)>;
                if constexpr (std::is_same_v<T, std::shared_ptr<std::remove_pointer_t<decltype(this)>>>)
                {
                    return ptr;
                }
                else if constexpr (std::is_same_v<T, std::weak_ptr<std::remove_pointer_t<decltype(this)>>>)
                {
                    return ptr.lock();
                }
            },
            sharedHandle);
    }

    std::weak_ptr<detail::VirtualPromise> setWaitingCoroutine(const std::weak_ptr<detail::VirtualPromise> next) noexcept override
    {
        return nextCoroutineOnReturn.exchange(next);
    }

    std::weak_ptr<detail::VirtualPromise> setNextCoroutineOnYield(const std::weak_ptr<detail::VirtualPromise> next) noexcept
    {
        return nextCoroutineOnYield.exchange(next);
    }

    detail::DataSegment<detail::OnDiskLocation> getTarget() const noexcept { return target; }
    //static_assert(SharedPromise<PunchHolePromise>);

    void updateIsDone()
    {
        if (finalSuspendStarted.test() && result.load().has_value())
        {
            const auto wasCompletedBefore = isDone.test_and_set();
            // INVARIANT(!wasCompletedBefore, "Trying to mark coroutine as done, but was already set done");
        }
    }

    std::coroutine_handle<> getHandle() noexcept override { return std::coroutine_handle<decltype(*this)>::from_promise(*this); }
};

template <typename Future>
class GetPinnedBufferPromise : public detail::VirtualPromise
{
    std::variant<PinnedBuffer, detail::CoroutineError> result{};
    std::atomic_flag finalSuspendStarted{false};
    std::atomic_flag hasResult{false};
    std::atomic_flag isDone{false};
    std::atomic<std::weak_ptr<detail::VirtualAwaiterForProgress>> lastBlockAwaiter{std::weak_ptr<detail::VirtualAwaiterForProgress>{}};
    std::atomic<std::weak_ptr<detail::VirtualAwaiterForProgress>> currentBlockAwaiter{std::weak_ptr<detail::VirtualAwaiterForProgress>{}};

    std::shared_ptr<GetPinnedBufferPromise> sharedHandle;

    std::atomic<std::weak_ptr<detail::VirtualPromise>> nextCoroutine{std::weak_ptr<detail::VirtualPromise>{}};

public:
    GetPinnedBufferPromise()
        : sharedHandle(this, [](auto promise) { std::coroutine_handle<GetPinnedBufferPromise>::from_promise(*promise).destroy(); })
    {
    }

    Future get_return_object() { return Future(std::coroutine_handle<GetPinnedBufferPromise>::from_promise(*this)); }
    void return_value(const std::variant<PinnedBuffer, detail::CoroutineError>& result)
    {
        this->result = result;
        hasResult.test_and_set();
    }

    void unhandled_exception() noexcept { };
    std::suspend_never initial_suspend() noexcept { return {}; }
    detail::ErasedAwaiter final_suspend() noexcept
    {
        const auto finalSuspendedBefore = finalSuspendStarted.test_and_set();
        INVARIANT(!finalSuspendedBefore, "Coroutines final suspend called more than once");
        auto awaiterPtr = std::make_unique<detail::ContinueOuterCoroutineAwaiter<GetPinnedBufferPromise>>(
            nextCoroutine.exchange(std::weak_ptr<VirtualPromise>{}));
        return detail::ErasedAwaiter{std::move(awaiterPtr)};
    }

    std::optional<std::variant<PinnedBuffer, detail::CoroutineError>> getResult() const noexcept
    {
        if (isDone.test())
        {
            return result;
        }
        return std::nullopt;
    }
    std::weak_ptr<detail::VirtualAwaiterForProgress> exchangeAwaiter(detail::VirtualAwaiterForProgress* newAwaiter)
    {
        //For now, we only deal with weak ptrs
        INVARIANT(newAwaiter == nullptr, "Setting non-nullptr awaiter over exchange Awaiter is not supported for GetInMemoryPromise");
        return currentBlockAwaiter.exchange(std::weak_ptr<detail::VirtualAwaiterForProgress>{});
    }
    std::weak_ptr<detail::VirtualAwaiterForProgress> exchangeAwaiter(std::weak_ptr<detail::VirtualAwaiterForProgress> newAwaiter)
    {
        auto oldAwaiter = currentBlockAwaiter.exchange(newAwaiter);
        if (newAwaiter.lock().get() != nullptr)
        {
            lastBlockAwaiter = newAwaiter;
        }
        return oldAwaiter;
    }
    std::shared_ptr<GetPinnedBufferPromise> getSharedPromise()
    {
        return std::visit(
            [](auto&& ptr)
            {
                using T = std::decay_t<decltype(ptr)>;
                if constexpr (std::is_same_v<T, std::shared_ptr<std::remove_pointer_t<decltype(this)>>>)
                {
                    return ptr;
                }
                else if constexpr (std::is_same_v<T, std::weak_ptr<std::remove_pointer_t<decltype(this)>>>)
                {
                    return ptr.lock();
                }
            },
            sharedHandle);
    }

    std::weak_ptr<detail::VirtualPromise> setWaitingCoroutine(const std::weak_ptr<detail::VirtualPromise> next) noexcept override
    {
        return nextCoroutine.exchange(next);
    }

    void updateIsDone()
    {
        if (hasResult.test() && finalSuspendStarted.test())
        {
            const auto wasCompletedBefore = isDone.test_and_set();
            // INVARIANT(!wasCompletedBefore, "Trying to mark coroutine as done, but was already set done");
        }
    }

    std::coroutine_handle<> getHandle() noexcept override { return std::coroutine_handle<decltype(*this)>::from_promise(*this); }
};
}
