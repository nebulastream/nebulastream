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
#include <atomic>
#include <coroutine>
#include <memory>
#include <optional>
#include <variant>
#include <vector>

#include <Runtime/DataSegment.hpp>

#include <ErrorHandling.hpp>

#include <Runtime/PinnedBuffer.hpp>

namespace NES::Memory
{

namespace detail
{
    class VirtualAwaiterForProgress;
}
class BufferManager;

using CoroutineError = uint32_t;
class VirtualPromise
{
public:
    //Delete destructor, needs to be handled over the destroy function of the coroutine handle
    virtual ~VirtualPromise() = default;
    virtual std::coroutine_handle<> setWaitingCoroutine(std::coroutine_handle<>) noexcept = 0;
};

template <typename Promise>
concept SharedPromise = requires(Promise promise) {
    { promise.getSharedPromise() } -> std::same_as<std::shared_ptr<Promise>>;
};

template <typename Future>
class GetInMemorySegmentPromise : public VirtualPromise
{
    //friend class GetInMemorySegmentFuture;
    std::optional<std::variant<std::vector<detail::DataSegment<detail::InMemoryLocation>>, CoroutineError>> result{};
    std::atomic_flag isDone{false};
    //Raw pointer because the ResumeAfterBlockAwaiter is allocated on the "stack" of the coroutine anyway but we also need a not-present value
    std::atomic<detail::VirtualAwaiterForProgress*> currentBlockAwaiter{};

    std::shared_ptr<GetInMemorySegmentPromise> sharedHandle{};
    std::atomic<std::coroutine_handle<>> nextCoroutine{};

public:
    //Constructor call is inserted by the compiler when calling the coroutine function,
    //passing instance and any arguments to the coroutine to this function
    GetInMemorySegmentPromise()
        : currentBlockAwaiter(nullptr)
        , sharedHandle(this, [](auto promise) { std::coroutine_handle<GetInMemorySegmentPromise>::from_promise(*promise).destroy(); })
    {
    }
    Future get_return_object() { return Future(std::coroutine_handle<GetInMemorySegmentPromise>::from_promise(*this)); }
    void return_value(std::variant<std::vector<detail::DataSegment<detail::InMemoryLocation>>, CoroutineError> segment) noexcept
    {
        //First set result, then set isDone.
        //If copy is still in progress, then getResult will still return std::nullopt.
        result = segment;
        const auto wasSetBefore = isDone.test_and_set();
        INVARIANT(!wasSetBefore, "Promise return_value method was invoked twice");
        if (const auto resumption = nextCoroutine.exchange({}); resumption != nullptr)
        {
            resumption.resume();
        }
    }
    void setBlockingFunction(detail::VirtualAwaiterForProgress* newBlocking) noexcept { currentBlockAwaiter.store(newBlocking); }

    void unhandled_exception() noexcept { };
    std::suspend_never initial_suspend() noexcept { return {}; };
    //Needs to always suspend, because otherwise the coroutine self-destructs after co_return.
    //When then the destructor of the future is called because it goes out-of-scope, it causes a segmentation fault
    std::suspend_always final_suspend() noexcept
    {
        sharedHandle.reset();
        return {};
    };

    detail::VirtualAwaiterForProgress* exchangeAwaiter(detail::VirtualAwaiterForProgress* newAwaiter) { return currentBlockAwaiter.exchange(newAwaiter); }

    std::optional<std::variant<std::vector<detail::DataSegment<detail::InMemoryLocation>>, CoroutineError>> getResult() const noexcept
    {
        if (isDone.test())
        {
            INVARIANT(result.has_value(), "Promise was marked as done, but no result was set");
            return *result;
        }
        return std::nullopt;
    }

    std::shared_ptr<GetInMemorySegmentPromise> getSharedPromise() { return sharedHandle; }
    std::coroutine_handle<> setWaitingCoroutine(const std::coroutine_handle<> next) noexcept override
    {
        return nextCoroutine.exchange(next);
    }
};

template <typename Future>
class ReadSegmentPromise : public VirtualPromise
{
    //friend class ReadSegmentFuture;
    detail::DataSegment<detail::OnDiskLocation> source;
    detail::DataSegment<detail::InMemoryLocation> target;
    //Raw pointer because the ResumeAfterBlockAwaiter is allocated on the "stack" of the coroutine anyway but we also need a not-present value
    std::atomic<detail::VirtualAwaiterForProgress*> currentBlockAwaiter;
    std::variant<ssize_t, CoroutineError> result;
    std::atomic_flag isDone{false};

    std::shared_ptr<ReadSegmentPromise> sharedHandle;

    std::atomic<std::coroutine_handle<>> nextCoroutine{};

public:
    //Constructor call is inserted by the compiler when calling the coroutine function,
    //passing instance and any arguments to the coroutine to this function
    ReadSegmentPromise(BufferManager&, detail::DataSegment<detail::OnDiskLocation> source, detail::DataSegment<detail::InMemoryLocation> target)
        : source(source)
        , target(target)
        , currentBlockAwaiter(nullptr)
        , sharedHandle(this, [](auto promise) { std::coroutine_handle<ReadSegmentPromise>::from_promise(*promise).destroy(); })
    {
    }
    Future get_return_object() { return Future(std::coroutine_handle<ReadSegmentPromise>::from_promise(*this)); }
    void return_value(std::variant<ssize_t, CoroutineError> result) noexcept
    {
        this->result = result;
        isDone.test_and_set();
        auto resumption = nextCoroutine.exchange({});
        if (resumption != nullptr)
        {
            resumption.resume();
        }
    }
    void setBlockingFunction(detail::VirtualAwaiterForProgress* newBlocking) noexcept { currentBlockAwaiter = newBlocking; }

    void unhandled_exception() noexcept { };
    std::suspend_never initial_suspend() noexcept { return {}; };
    //Needs to always suspend, because otherwise the coroutine self-destructs after co_return.
    //When then the destructor of the future is called because it goes out-of-scope, it causes a segmentation fault
    std::suspend_always final_suspend() noexcept
    {
        sharedHandle.reset();
        return {};
    };


    std::optional<std::variant<ssize_t, CoroutineError>> getResult()
    {
        if (isDone.test())
        {
            return result;
        }
        return std::nullopt;
    }
    detail::VirtualAwaiterForProgress* exchangeAwaiter(detail::VirtualAwaiterForProgress* newAwaiter) { return currentBlockAwaiter.exchange(newAwaiter); }
    std::shared_ptr<ReadSegmentPromise> getSharedPromise() { return sharedHandle; }
    std::coroutine_handle<> setWaitingCoroutine(const std::coroutine_handle<> next) noexcept override
    {
        return nextCoroutine.exchange(next);
    }
};


template <typename Future>
class RepinBufferPromise : public VirtualPromise
{
    //friend class RepinBufferFuture;

    std::variant<PinnedBuffer, CoroutineError> result{};
    std::atomic_flag isDone{false};
    std::atomic<detail::VirtualAwaiterForProgress*> currentBlockAwaiter = nullptr;

    std::shared_ptr<RepinBufferPromise> sharedHandle;

    std::atomic<std::coroutine_handle<>> nextCoroutine{};

public:
    RepinBufferPromise()
        : sharedHandle(this, [](auto promise) { std::coroutine_handle<RepinBufferPromise>::from_promise(*promise).destroy(); })
    {
    }

    Future get_return_object() { return Future(std::coroutine_handle<RepinBufferPromise>::from_promise(*this)); }
    void return_value(const std::variant<PinnedBuffer, CoroutineError>& result)
    {
        this->result = result;
        isDone.test_and_set();
        auto resumption = nextCoroutine.exchange({});
        if (resumption != nullptr)
        {
            resumption.resume();
        }
    }

    void unhandled_exception() noexcept { };
    void setBlockingFunction(detail::VirtualAwaiterForProgress* newBlocking) noexcept { currentBlockAwaiter = newBlocking; }
    std::suspend_never initial_suspend() noexcept { return {}; }
    std::suspend_always final_suspend() noexcept
    {
        sharedHandle.reset();
        return {};
    };

    std::optional<std::variant<PinnedBuffer, CoroutineError>> getResult() const noexcept
    {
        if (isDone.test())
        {
            return result;
        }
        return std::nullopt;
    }
    detail::VirtualAwaiterForProgress* exchangeAwaiter(detail::VirtualAwaiterForProgress* newAwaiter) { return currentBlockAwaiter.exchange(newAwaiter); }
    std::shared_ptr<RepinBufferPromise> getSharedPromise() { return sharedHandle; }

    std::coroutine_handle<> setWaitingCoroutine(const std::coroutine_handle<> next) noexcept override
    {
        return nextCoroutine.exchange(next);
    }
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
class PunchHolePromise : VirtualPromise
{
    //friend class PunchHoleFuture;
    detail::DataSegment<detail::OnDiskLocation> target;
    //Raw pointer because the ResumeAfterBlockAwaiter is allocated on the "stack" of the coroutine anyway but we also need a not-present value
    std::atomic<detail::VirtualAwaiterForProgress*> currentBlockAwaiter;


    std::atomic<std::optional<std::variant<UringSuccessOrError, CoroutineError>>> result{};
    //TODO is three value logic really necessary?
    std::optional<bool> hasSubmitted;
    std::shared_ptr<PunchHolePromise> sharedHandle;
    std::atomic<std::coroutine_handle<>> nextCoroutineOnReturn{};
    std::atomic<std::coroutine_handle<>> nextCoroutineOnYield{};

public:
    //Constructor call is inserted by the compiler when calling the coroutine function,
    //passing instance and any arguments to the coroutine to this function
    PunchHolePromise(BufferManager&, detail::DataSegment<detail::OnDiskLocation> target)
        : target(target)
        , currentBlockAwaiter(nullptr)
        , sharedHandle(this, [](auto promise) { std::coroutine_handle<PunchHolePromise>::from_promise(*promise).destroy(); })
    {
    }
    Future get_return_object() { return Future(std::coroutine_handle<PunchHolePromise>::from_promise(*this)); }
    void return_value(std::variant<UringSuccessOrError, CoroutineError> result) noexcept
    {
        this->result = result;
        auto resumption = nextCoroutineOnReturn.exchange({});
        if (resumption != nullptr)
        {
            resumption.resume();
        }
    }
    void unhandled_exception() noexcept { };

    //Suspend never because it is just information for the caller.
    //Either, it succeeds to submit and then suspends when waiting for the confirmation, or it needs to reach co_return so that the coroutine
    //does not get leaked through shared handles in the coroutine itself.
    std::suspend_never yield_value(bool hasSubmitted) noexcept
    {
        this->hasSubmitted = hasSubmitted;
        auto resumption = nextCoroutineOnYield.exchange({});
        if (resumption != nullptr)
        {
            resumption.resume();
        }
        return {};
    }
    void setBlockingFunction(detail::VirtualAwaiterForProgress* newBlocking) noexcept { currentBlockAwaiter = newBlocking; }

    std::suspend_never initial_suspend() noexcept { return {}; };
    //Needs to always suspend, because otherwise the coroutine self-destructs after co_return.
    //When then the destructor of the future is called because it goes out-of-scope, it causes a segmentation fault
    std::suspend_always final_suspend() noexcept
    {
        sharedHandle.reset();
        return {};
    };

    std::optional<std::variant<UringSuccessOrError, CoroutineError>> getResult() const noexcept { return result; }

    std::optional<bool> getYielded() const noexcept { return hasSubmitted; }

    detail::VirtualAwaiterForProgress* exchangeAwaiter(detail::VirtualAwaiterForProgress* newAwaiter) { return currentBlockAwaiter.exchange(newAwaiter); }

    std::shared_ptr<PunchHolePromise> getSharedPromise() { return sharedHandle; }

    std::coroutine_handle<> setWaitingCoroutine(const std::coroutine_handle<> next) noexcept override
    {
        return nextCoroutineOnReturn.exchange(next);
    }

    std::coroutine_handle<> setNextCoroutineOnYield(const std::coroutine_handle<> next) noexcept
    {
        return nextCoroutineOnYield.exchange(next);
    }

    detail::DataSegment<detail::OnDiskLocation> getTarget() const noexcept { return target; }
    //static_assert(SharedPromise<PunchHolePromise>);
};

}
