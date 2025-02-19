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
#include <concepts>
#include <coroutine>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <utility>
#include <unistd.h>
#include <Runtime/BufferPrimitives.hpp>
#include <Runtime/PinnedBuffer.hpp>

#include <Runtime/DataSegment.hpp>
#include <Util/Logger/Logger.hpp>
#include <folly/MPMCQueue.h>

#include <shared_mutex>
#include <Runtime/BufferFiles.hpp>
#include <Util/Ranges.hpp>
#include <Util/Variant.hpp>
#include <ErrorHandling.hpp>

namespace NES::Memory
{

class BufferManager;
namespace detail
{
using CoroutineError = uint32_t;

class BufferControlBlock;
class SegmentWriteRequest
{
    BufferControlBlock* const controlBlock;
    const DataSegment<InMemoryLocation> segment;
    const ChildOrMainDataKey key;
    const File file;
    const uint64_t fileOffset;
    uint64_t writtenSize;

public:
    [[nodiscard]] SegmentWriteRequest(
        BufferControlBlock* const control_block,
        const DataSegment<InMemoryLocation>& segment,
        ChildOrMainDataKey key,
        const File file,
        const uint64_t fileOffset)
        : controlBlock(control_block), segment(segment), key(key), file(file), fileOffset(fileOffset), writtenSize(0)
    {
    }
    [[nodiscard]] BufferControlBlock* getControlBlock() const { return controlBlock; }
    [[nodiscard]] DataSegment<InMemoryLocation> getSegment() const { return segment; }
    [[nodiscard]] File getFileId() const { return file; }
    [[nodiscard]] uint64_t getFileOffset() const { return fileOffset; }
    [[nodiscard]] uint64_t getWrittenSize() const { return writtenSize; }
    [[nodiscard]] ChildOrMainDataKey getKey() const { return key; }
};

template <typename T>
concept SuspendReturnType = std::same_as<T, bool> || std::same_as<T, std::coroutine_handle<>>;

//Using a concept allows us to "forward declare" behaviour of types that at the end depend on the future type again
template <typename UnderlyingAwaiter, typename Promise, typename ReturnType, typename WrappedAwaiter>
concept Awaiter = requires(UnderlyingAwaiter underlyingAwaiter, std::coroutine_handle<Promise> handle, WrappedAwaiter wrappedAwaiter) {
    { underlyingAwaiter.await_ready() } -> std::same_as<bool>;
    { underlyingAwaiter.await_suspend(handle) } -> SuspendReturnType;
    { underlyingAwaiter.await_resume() } -> std::same_as<ReturnType>;
    { underlyingAwaiter.isDone() } -> std::same_as<bool>;
    { handle.promise().exchangeAwaiter(&wrappedAwaiter) };
};

//Wrap around different Awaiters with a virtual class
class VirtualAwaiterForProgress
{
public:
    virtual std::function<void()> getPollFn() = 0;
    virtual std::function<void()> getWaitFn() = 0;
    virtual bool isDone() = 0;
    virtual ~VirtualAwaiterForProgress() = default;
};

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
    std::optional<std::variant<std::vector<DataSegment<InMemoryLocation>>, CoroutineError>> result{};
    std::atomic_flag isDone{false};
    //Raw pointer because the ResumeAfterBlockAwaiter is allocated on the "stack" of the coroutine anyway but we also need a not-present value
    std::atomic<VirtualAwaiterForProgress*> currentBlockAwaiter{};

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
    void return_value(std::variant<std::vector<DataSegment<InMemoryLocation>>, CoroutineError> segment) noexcept
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
    void setBlockingFunction(VirtualAwaiterForProgress* newBlocking) noexcept { currentBlockAwaiter = newBlocking; }

    void unhandled_exception() noexcept { };
    std::suspend_never initial_suspend() noexcept { return {}; };
    //Needs to always suspend, because otherwise the coroutine self-destructs after co_return.
    //When then the destructor of the future is called because it goes out-of-scope, it causes a segmentation fault
    std::suspend_always final_suspend() noexcept { return {}; };

    VirtualAwaiterForProgress* exchangeAwaiter(VirtualAwaiterForProgress* newAwaiter) { return currentBlockAwaiter.exchange(newAwaiter); }

    std::optional<std::variant<std::vector<DataSegment<InMemoryLocation>>, CoroutineError>> getResult() const noexcept
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
    DataSegment<OnDiskLocation> source;
    DataSegment<InMemoryLocation> target;
    //Raw pointer because the ResumeAfterBlockAwaiter is allocated on the "stack" of the coroutine anyway but we also need a not-present value
    std::atomic<VirtualAwaiterForProgress*> currentBlockAwaiter;
    std::variant<ssize_t, CoroutineError> result;
    std::atomic_flag isDone{false};

    std::shared_ptr<ReadSegmentPromise> sharedHandle;

    std::atomic<std::coroutine_handle<>> nextCoroutine{};

public:
    //Constructor call is inserted by the compiler when calling the coroutine function,
    //passing instance and any arguments to the coroutine to this function
    ReadSegmentPromise(BufferManager&, DataSegment<OnDiskLocation> source, DataSegment<InMemoryLocation> target)
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
    void setBlockingFunction(VirtualAwaiterForProgress* newBlocking) noexcept { currentBlockAwaiter = newBlocking; }

    void unhandled_exception() noexcept { };
    std::suspend_never initial_suspend() noexcept { return {}; };
    //Needs to always suspend, because otherwise the coroutine self-destructs after co_return.
    //When then the destructor of the future is called because it goes out-of-scope, it causes a segmentation fault
    std::suspend_always final_suspend() noexcept { return {}; };


    std::optional<std::variant<ssize_t, CoroutineError>> getResult()
    {
        if (isDone.test())
        {
            return result;
        }
        return std::nullopt;
    }
    VirtualAwaiterForProgress* exchangeAwaiter(VirtualAwaiterForProgress* newAwaiter) { return currentBlockAwaiter.exchange(newAwaiter); }
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
    std::atomic<VirtualAwaiterForProgress*> currentBlockAwaiter = nullptr;

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
    void setBlockingFunction(VirtualAwaiterForProgress* newBlocking) noexcept { currentBlockAwaiter = newBlocking; }
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
    VirtualAwaiterForProgress* exchangeAwaiter(VirtualAwaiterForProgress* newAwaiter) { return currentBlockAwaiter.exchange(newAwaiter); }
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
    DataSegment<OnDiskLocation> target;
    //Raw pointer because the ResumeAfterBlockAwaiter is allocated on the "stack" of the coroutine anyway but we also need a not-present value
    std::atomic<VirtualAwaiterForProgress*> currentBlockAwaiter;


    std::atomic<std::optional<std::variant<UringSuccessOrError, CoroutineError>>> result{};
    //TODO is three value logic really necessary?
    std::optional<bool> hasSubmitted;
    std::shared_ptr<PunchHolePromise> sharedHandle;
    std::atomic<std::coroutine_handle<>> nextCoroutineOnReturn{};
    std::atomic<std::coroutine_handle<>> nextCoroutineOnYield{};

public:
    //Constructor call is inserted by the compiler when calling the coroutine function,
    //passing instance and any arguments to the coroutine to this function
    PunchHolePromise(BufferManager&, DataSegment<OnDiskLocation> target)
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
    void setBlockingFunction(VirtualAwaiterForProgress* newBlocking) noexcept { currentBlockAwaiter = newBlocking; }

    std::suspend_never initial_suspend() noexcept { return {}; };
    //Needs to always suspend, because otherwise the coroutine self-destructs after co_return.
    //When then the destructor of the future is called because it goes out-of-scope, it causes a segmentation fault
    std::suspend_always final_suspend() noexcept { return {}; };

    std::optional<std::variant<UringSuccessOrError, CoroutineError>> getResult() const noexcept { return result; }

    std::optional<bool> getYielded() const noexcept { return hasSubmitted; }

    VirtualAwaiterForProgress* exchangeAwaiter(VirtualAwaiterForProgress* newAwaiter) { return currentBlockAwaiter.exchange(newAwaiter); }

    std::shared_ptr<PunchHolePromise> getSharedPromise() { return sharedHandle; }

    std::coroutine_handle<> setWaitingCoroutine(const std::coroutine_handle<> next) noexcept override
    {
        return nextCoroutineOnReturn.exchange(next);
    }

    std::coroutine_handle<> setNextCoroutineOnYield(const std::coroutine_handle<> next) noexcept
    {
        return nextCoroutineOnYield.exchange(next);
    }

    DataSegment<OnDiskLocation> getTarget() const noexcept { return target; }
    //static_assert(SharedPromise<PunchHolePromise>);
};

///Utility hack to get the handle of a coroutine from inside the coroutine itself.
///Only works for promise types that expose a shared pointer to themselves, otherwise correct freeing cannot be guaranteed.
template <SharedPromise Promise>
class GetCoroutineHandle
{
    std::shared_ptr<Promise> handle;

public:
    explicit GetCoroutineHandle() noexcept = default;

    bool await_ready() const noexcept { return handle != nullptr; }

    bool await_suspend(std::coroutine_handle<Promise> handle)
    {
        this->handle = handle.promise().getSharedPromise();
        //We never need to actually suspend
        return false;
    }

    std::shared_ptr<Promise> await_resume() const noexcept { return handle; }
};

//This class is not 100% necessary, because the virtual awaiter already gets swapped out atomically in the future when the wait or poll function is called
//Still, it provides a clear definition of a function having run once, which in the future would only be a side effect of the current implementation
template <typename Promise>
class ResumeAfterBlockAwaiter
{
    static const inline std::function<void()> noOpPoll = [] { };
    std::function<void()> blockingFn;
    //We want to be able to return references on pollingFn,
    //if we'd store it in an optional we'd rely on the reference on *optional staying valid
    const bool hasPolling;
    std::function<void()> pollingFn;
    std::function<bool()> underlyingPoll;
    std::atomic<std::coroutine_handle<>> handle{std::coroutine_handle{}};
    std::atomic_flag hasResumed = false;
    std::atomic_flag hasPassed = false;

public:
    explicit ResumeAfterBlockAwaiter(const std::function<void()>& blocking) : hasPolling(false)
    {
        blockingFn = [&, blocking]()
        {
            //These objects are strictly oneshot
            const bool check = hasResumed.test_and_set();
            INVARIANT(!check, "Coroutine was resumed concurrently");
            //Is blocking getting copied into this lambda correctly?
            blocking();
            const bool passedCheck = hasPassed.test_and_set();
            INVARIANT(!passedCheck, "ResumeAfterBlockAwaiter was passed concurrently");
            if (const auto toResume = handle.exchange(std::coroutine_handle{}); toResume.address() != nullptr)
            {
                toResume.resume();
            }
        };
    }
    explicit ResumeAfterBlockAwaiter(const std::function<void()>& blocking, const std::function<bool()>& poll)
        : hasPolling(true), underlyingPoll(poll)
    {
        blockingFn = [&, blocking]()
        {
            //These objects are strictly oneshot
            const bool check = hasResumed.test_and_set();
            INVARIANT(!check, "Coroutine was resumed concurrently");
            //Is blocking getting copied into this lambda correctly?
            blocking();
            const bool passedCheck = hasPassed.test_and_set();
            INVARIANT(!passedCheck, "ResumeAfterBlockAwaiter was passed concurrently");
            if (const auto toResume = handle.exchange(std::coroutine_handle{}); toResume.address() != nullptr)
            {
                toResume.resume();
            }
        };

        pollingFn = [&, poll]()
        {
            const bool check = hasResumed.test_and_set();
            INVARIANT(!check, "Coroutine was polled while concurrently making progress");
            if (!hasPassed.test())
            {
                if (poll())
                {
                    hasPassed.test_and_set();
                }
            }
            if (hasPassed.test())
            {
                if (const auto toResume = handle.exchange(std::coroutine_handle{}); toResume.address() != nullptr)
                {
                    toResume.resume();
                }
            }
            else
            {
                hasResumed.clear();
            }
        };
    }


    bool await_ready() const noexcept { return hasPassed.test(); }
    std::coroutine_handle<> await_suspend(std::coroutine_handle<Promise> suspendingHandle) noexcept
    {
        auto prev = handle.exchange(suspendingHandle);
        INVARIANT(prev == std::coroutine_handle<>{}, "Multiple coroutines where awaiting the same ResumeAfterBlockAwaiter");
        if (!hasPassed.test())
        {
            return {};
        }
        return handle.exchange(std::coroutine_handle{});
    }

    void await_resume() const noexcept { }

    bool isDone() const noexcept { return hasResumed.test(); }

    [[nodiscard]] const std::function<void()>& getBlocking() const noexcept { return blockingFn; }
    [[nodiscard]] const std::function<void()>& getPolling() const noexcept
    {
        if (hasPolling)
        {
            return pollingFn;
        }
        return noOpPoll;
    }
};


template <typename Promise>
class AvailableSegmentAwaiter
{
    DataSegment<InMemoryLocation> result{};
    std::coroutine_handle<Promise> handle;

    //This mutex is for synchronizing access on the result and handle.
    //The coroutine might try to set the handle while another thread is already filling in the result.
    //Without further synchronization, it could lead to the coroutine never waking up because the awaiter was already dequeued.
    //Alternatively, we could put both in an atomic tuple which should be 24 bytes big, but the logic would be more complicated.
    mutable std::recursive_mutex mutex;

public:
    explicit AvailableSegmentAwaiter() noexcept { }
    //Copy and move are deleted because the whole point of this class is synchronizing access through the mutex
    AvailableSegmentAwaiter(const AvailableSegmentAwaiter& other) = delete;

    AvailableSegmentAwaiter(AvailableSegmentAwaiter&& other) = delete;
    AvailableSegmentAwaiter& operator=(const AvailableSegmentAwaiter& other) = delete;

    AvailableSegmentAwaiter& operator=(AvailableSegmentAwaiter&& other) = delete;
    bool await_ready() const noexcept { return result.getLocation().getPtr() != nullptr; }
    bool await_suspend(std::coroutine_handle<Promise> handle) noexcept
    {
        std::scoped_lock lock{mutex};
        if (result.getLocation().getPtr() != nullptr)
        {
            return false;
        }
        this->handle = handle;

        //We could also do the IO uring write here, but I think the synchronization/batching is easier to do in the coroutine
        return true;
    }

    void setResultAndContinue(DataSegment<InMemoryLocation> segment)
    {
        const std::scoped_lock lock{mutex};
        result = segment;
        if (handle)
        {
            handle.resume();
        }
    }

    std::optional<DataSegment<InMemoryLocation>> await_resume() noexcept
    {
        //This lock should not be necessary because this method should only be called through setResultAndContinues handle.resume(),
        //but there is no way to guarantee that this class is always used like that.
        const std::scoped_lock lock{mutex};
        if (result.getLocation().getPtr() == nullptr)
        {
            return std::nullopt;
        }
        return result;
    }

    bool isDone() const noexcept
    {
        //I'm not sure if this lock is really necessary
        const std::scoped_lock lock{mutex};
        return result.getLocation().getPtr() != nullptr;
    }

    ///Creates a new awaiter and tries to put it into the request queue.
    ///@return The successfully created and inserted awaiter or nullopt if it wasn't inserted
    static std::optional<std::shared_ptr<AvailableSegmentAwaiter>>
    create(folly::MPMCQueue<std::shared_ptr<AvailableSegmentAwaiter>>& requestQueue)
    {
        auto awaiter = std::make_shared<AvailableSegmentAwaiter>();
        if (requestQueue.write(awaiter))
        {
            return awaiter;
        }
        return std::nullopt;
    }
};

template <typename Promise>
class ReadSegmentAwaiter
{
    std::optional<ssize_t> returnValue = std::nullopt;

    std::coroutine_handle<Promise> handle;

    mutable std::recursive_mutex mutex;

public:
    explicit ReadSegmentAwaiter() { }

    //Can't move or copy instances because of the mutex
    ReadSegmentAwaiter(const ReadSegmentAwaiter& other) = delete;

    ReadSegmentAwaiter(ReadSegmentAwaiter&& other) = delete;
    ReadSegmentAwaiter& operator=(const ReadSegmentAwaiter& other) = delete;

    ReadSegmentAwaiter& operator=(ReadSegmentAwaiter&& other) = delete;

    bool await_ready() const noexcept { return returnValue != std::nullopt; }
    bool await_suspend(std::coroutine_handle<Promise> handle)
    {
        std::scoped_lock lock{mutex};
        if (returnValue)
        {
            return false;
        }
        this->handle = handle;
        return true;
    }

    std::optional<ssize_t> await_resume() const noexcept
    {
        std::scoped_lock lock{mutex};
        return returnValue;
    }

    void setResultAndContinue(ssize_t returnValue)
    {
        std::scoped_lock lock{mutex};
        this->returnValue = returnValue;
        if (handle)
        {
            handle.resume();
        }
    }

    bool isDone() const noexcept { return returnValue.has_value(); }

    static std::optional<std::shared_ptr<ReadSegmentAwaiter>> create(
        folly::MPMCQueue<std::shared_ptr<ReadSegmentAwaiter>>& requestQueue,
        DataSegment<OnDiskLocation> source,
        DataSegment<InMemoryLocation> target)
    {
        auto awaiter = std::make_shared<ReadSegmentAwaiter>(source, target);
        if (requestQueue.write(awaiter))
        {
            return awaiter;
        }
        return std::nullopt;
    }
};

template <typename Promise>
class SubmitSegmentReadAwaiter
{
    DataSegment<OnDiskLocation> source;
    DataSegment<InMemoryLocation> target;

    std::shared_ptr<ReadSegmentAwaiter<Promise>> nextAwaiter = nullptr;
    std::coroutine_handle<Promise> handle;

    mutable std::recursive_mutex mutex;

public:
    explicit SubmitSegmentReadAwaiter(DataSegment<OnDiskLocation> source, DataSegment<InMemoryLocation> target)
        : source(source), target(target)
    {
    }

    //Can't move or copy instances because of the mutex
    SubmitSegmentReadAwaiter(const SubmitSegmentReadAwaiter& other) = delete;

    SubmitSegmentReadAwaiter(SubmitSegmentReadAwaiter&& other) = delete;
    SubmitSegmentReadAwaiter& operator=(const SubmitSegmentReadAwaiter& other) = delete;

    SubmitSegmentReadAwaiter& operator=(SubmitSegmentReadAwaiter&& other) = delete;

    bool await_ready() const noexcept { return nextAwaiter != nullptr; }
    bool await_suspend(std::coroutine_handle<Promise> handle)
    {
        std::scoped_lock lock{mutex};
        if (nextAwaiter)
        {
            return false;
        }
        this->handle = handle;
        return true;
    }

    std::shared_ptr<ReadSegmentAwaiter<Promise>> await_resume() const noexcept { return nextAwaiter; }

    void setResult(std::shared_ptr<ReadSegmentAwaiter<Promise>>&& nextAwaiter) noexcept
    {
        std::scoped_lock lock{mutex};
        this->nextAwaiter = nextAwaiter;
    }

    void continueCoroutine() noexcept
    {
        std::scoped_lock lock{mutex};
        if (handle)
        {
            handle.resume();
        }
    }

    void setResultAndContinue()
    {
        std::scoped_lock lock{mutex};
        if (handle)
        {
            handle.resume();
        }
    }

    bool isDone() const noexcept { return nextAwaiter != nullptr; }

    static std::optional<std::shared_ptr<SubmitSegmentReadAwaiter>> create(
        folly::MPMCQueue<std::shared_ptr<SubmitSegmentReadAwaiter>>& requestQueue,
        DataSegment<OnDiskLocation> source,
        DataSegment<InMemoryLocation> target)
    {
        auto awaiter = std::make_shared<SubmitSegmentReadAwaiter>(source, target);
        if (requestQueue.write(awaiter))
        {
            return awaiter;
        }
        return std::nullopt;
    }
    [[nodiscard]] DataSegment<OnDiskLocation> getSource() const { return source; }
    [[nodiscard]] DataSegment<InMemoryLocation> getTarget() const { return target; }
};

template <typename Promise>
class PunchHoleSegmentAwaiter
{
    std::optional<ssize_t> returnValue = std::nullopt;
    std::coroutine_handle<Promise> handle;
    mutable std::mutex mutex;

public:
    explicit PunchHoleSegmentAwaiter() { }

    //Can't move or copy instances because of the mutex
    PunchHoleSegmentAwaiter(const PunchHoleSegmentAwaiter& other) = delete;

    PunchHoleSegmentAwaiter(PunchHoleSegmentAwaiter&& other) = delete;
    PunchHoleSegmentAwaiter& operator=(const PunchHoleSegmentAwaiter& other) = delete;

    PunchHoleSegmentAwaiter& operator=(PunchHoleSegmentAwaiter&& other) = delete;

    bool await_ready() const noexcept { return returnValue != std::nullopt; }
    bool await_suspend(std::coroutine_handle<Promise> handle)
    {
        std::scoped_lock lock{mutex};
        if (returnValue)
        {
            return false;
        }
        this->handle = handle;
        return true;
    }

    std::optional<ssize_t> await_resume() const noexcept
    {
        std::scoped_lock lock{mutex};
        return returnValue;
    }

    void setResultAndContinue(ssize_t returnValue)
    {
        std::scoped_lock lock{mutex};
        this->returnValue = returnValue;
        if (handle)
        {
            handle.resume();
        }
    }

    bool isDone() const noexcept { return returnValue.has_value(); }
};

template <typename Promise>
class SubmitPunchHoleSegmentAwaiter
{
    DataSegment<OnDiskLocation> target;
    std::shared_ptr<PunchHoleSegmentAwaiter<Promise>> nextAwaiter = nullptr;
    std::coroutine_handle<Promise> handle;
    mutable std::mutex mutex;

    explicit SubmitPunchHoleSegmentAwaiter(const DataSegment<OnDiskLocation> target) : target(target) { }

public:
    //Can't move or copy instances because of the mutex
    SubmitPunchHoleSegmentAwaiter(const SubmitPunchHoleSegmentAwaiter& other) = delete;

    SubmitPunchHoleSegmentAwaiter(SubmitPunchHoleSegmentAwaiter&& other) = delete;
    SubmitPunchHoleSegmentAwaiter& operator=(const SubmitPunchHoleSegmentAwaiter& other) = delete;

    SubmitPunchHoleSegmentAwaiter& operator=(SubmitPunchHoleSegmentAwaiter&& other) = delete;

    bool await_ready() const noexcept { return nextAwaiter != nullptr; }
    bool await_suspend(std::coroutine_handle<Promise> handle)
    {
        std::scoped_lock lock{mutex};
        if (nextAwaiter)
        {
            return false;
        }
        this->handle = handle;
        return true;
    }

    std::shared_ptr<PunchHoleSegmentAwaiter<Promise>> await_resume() const noexcept { return nextAwaiter; }

    void setResult(std::shared_ptr<PunchHoleSegmentAwaiter<Promise>>&& nextAwaiter) noexcept
    {
        std::scoped_lock lock{mutex};
        this->nextAwaiter = nextAwaiter;
    }

    void continueCoroutine() noexcept
    {
        std::scoped_lock lock{mutex};
        if (handle)
        {
            handle.resume();
        }
    }

    void setResultAndContinue()
    {
        std::scoped_lock lock{mutex};
        if (handle)
        {
            handle.resume();
        }
    }

    bool isDone() const noexcept { return nextAwaiter != nullptr; }

    static std::optional<std::shared_ptr<SubmitPunchHoleSegmentAwaiter>>
    create(folly::MPMCQueue<std::shared_ptr<SubmitPunchHoleSegmentAwaiter>>& requestQueue, DataSegment<OnDiskLocation> target)
    {
        auto awaiterRaw = new SubmitPunchHoleSegmentAwaiter(target);
        auto awaiter = std::shared_ptr<SubmitPunchHoleSegmentAwaiter>{awaiterRaw};
        if (requestQueue.write(awaiter))
        {
            return awaiter;
        }
        return std::nullopt;
    }

    [[nodiscard]] DataSegment<OnDiskLocation> getTarget() const noexcept { return target; }
};


template <typename Promise, typename ReturnType, Awaiter<Promise, ReturnType, VirtualAwaiterForProgress> Awaiter>
class AwaitExternalProgress : public VirtualAwaiterForProgress
{
    std::function<void()> poll;
    std::function<void()> wait;
    /*
     * Everything is in the frame of the coroutine anyway.
     * Doing something stupid on purpose like
     * AwaitExternalProgress awaitExternalProg;
     * {
     *      Awaiter underlying = {};
     *      awaitExternalProg = {underlying}
     * }
     * is still probably not a good idea, but even that should work because the coroutine frame is allocated at once
     * and not partially deallocated or reused (although that might change in the future).
     * We can't move in the underlying awaiters because of the mutexes.
     * An alternative would be unique pointers, which would be one more level of indirection.
     */
    Awaiter& underlyingAwaiter;

public:
    explicit AwaitExternalProgress(const std::function<void()>& poll, const std::function<void()>& wait, Awaiter& awaiter)
        : poll(poll), wait(wait), underlyingAwaiter(awaiter) { };
    explicit AwaitExternalProgress(ResumeAfterBlockAwaiter<Promise>& resumeAfterBlock)
        : poll(resumeAfterBlock.getPolling()), wait(resumeAfterBlock.getBlocking()), underlyingAwaiter(resumeAfterBlock)
    {
    }

    bool await_ready() const noexcept { return underlyingAwaiter.await_ready(); }

    template <typename Suspension = decltype(std::declval<Awaiter>().await_suspend(std::declval<std::coroutine_handle<Promise>>()))>
    Suspension await_suspend(std::coroutine_handle<Promise> handle) noexcept
    {
        //The underlying awaiters are responsible for ensuring thread safety and to guarantee resumption
        poll();
        if (!underlyingAwaiter.await_ready())
        {
            handle.promise().exchangeAwaiter(this);
            return underlyingAwaiter.await_suspend(handle);
        }
        if constexpr (std::is_assignable_v<std::coroutine_handle<>, Suspension>)
        {
            return handle;
        }
        else
        {
            return false;
        }
    }

    ReturnType await_resume() noexcept { return underlyingAwaiter.await_resume(); }

    std::function<void()> getPollFn() override { return poll; }

    std::function<void()> getWaitFn() override { return wait; }

    bool isDone() override { return underlyingAwaiter.isDone(); }
};


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
    explicit GetInMemorySegmentFuture(const std::coroutine_handle<promise_type> handle) : promise(handle.promise().getSharedPromise()) { }
    GetInMemorySegmentFuture(const GetInMemorySegmentFuture& other) = default;
    GetInMemorySegmentFuture(GetInMemorySegmentFuture&& other) noexcept = default;
    GetInMemorySegmentFuture& operator=(const GetInMemorySegmentFuture& other) = default;
    GetInMemorySegmentFuture& operator=(GetInMemorySegmentFuture&& other) noexcept = default;

    ~GetInMemorySegmentFuture() = default;

    void pollOnce() noexcept
    {
        if (auto* pollAwaiter = promise->exchangeAwaiter(nullptr))
        {
            pollAwaiter->getPollFn()();
            //If polling didn't make any progress, schedule it again
            if (!pollAwaiter->isDone())
            {
                promise->exchangeAwaiter(pollAwaiter);
            }
        }
    }
    void waitOnce() noexcept
    {
        //atomic exchange ensures that only one thread can run the method and resume the coroutine
        if (auto* currentBlockAwaiter = promise->exchangeAwaiter(nullptr))
        {
            currentBlockAwaiter->getWaitFn()();
            if (!currentBlockAwaiter->isDone())
            {
                promise->exchangeAwaiter(currentBlockAwaiter);
            }
        }
    }

    [[nodiscard]] bool isDone() const noexcept { return promise->getResult().has_value(); }
    //TODO add timeout to task and give back nullopt when timeout exceededc
    std::variant<std::vector<DataSegment<InMemoryLocation>>, uint32_t> waitUntilDone() noexcept
    {
        auto result = promise->getResult();
        while (!result)
        {
            waitOnce();
            result = promise->getResult();
        }
        return *result;
    };

    bool await_ready() const noexcept { return isDone(); }

    std::coroutine_handle<> await_suspend(std::coroutine_handle<> suspending) noexcept
    {
        const auto previous = promise->setWaitingCoroutine(suspending);
        INVARIANT(previous.address() == nullptr, "Multiple coroutines trying to await the same promise");
        if (!isDone())
        {
            return {};
        }
        return promise->setWaitingCoroutine({});
    }

    std::variant<std::vector<DataSegment<InMemoryLocation>>, uint32_t> await_resume() const noexcept
    {
        INVARIANT(promise->getResult(), "Resumed GetInMemorySegmentFuture but result was not set.");
        return *promise->getResult();
    }
};
static_assert(ProgressableFutureConcept<GetInMemorySegmentFuture>);

class ReadSegmentFuture
{
public:
    using promise_type = ReadSegmentPromise<ReadSegmentFuture>;

private:
    std::shared_ptr<promise_type> promise;

public:
    explicit ReadSegmentFuture(const std::coroutine_handle<promise_type> handle) : promise(handle.promise().getSharedPromise()) { }
    ReadSegmentFuture(const ReadSegmentFuture& other) = default;
    ReadSegmentFuture(ReadSegmentFuture&& other) noexcept = default;
    ReadSegmentFuture& operator=(const ReadSegmentFuture& other) = default;
    ReadSegmentFuture& operator=(ReadSegmentFuture&& other) noexcept = default;
    ~ReadSegmentFuture() = default;

    void pollOnce() noexcept
    {
        if (auto* pollAwaiter = promise->exchangeAwaiter(nullptr))
        {
            pollAwaiter->getPollFn()();
            //If polling didn't make any progress, schedule it again
            if (!pollAwaiter->isDone())
            {
                promise->exchangeAwaiter(pollAwaiter);
            }
        }
    }
    void waitOnce() noexcept
    {
        //atomic exchange ensures that only one thread can run the method and resume the coroutine
        if (auto* currentBlockAwaiter = promise->exchangeAwaiter(nullptr))
        {
            currentBlockAwaiter->getWaitFn()();
            if (!currentBlockAwaiter->isDone())
            {
                promise->exchangeAwaiter(currentBlockAwaiter);
            }
        }
    }

    [[nodiscard]] bool isDone() const noexcept { return promise->getResult().has_value(); }

    //TODO add timeout to task and give back nullopt when timeout exceededc
    std::variant<ssize_t, uint32_t> waitUntilDone() noexcept
    {
        while (!isDone())
        {
            waitOnce();
        }
        return *promise->getResult();
    };

    bool await_ready() const noexcept { return isDone(); }

    std::coroutine_handle<> await_suspend(std::coroutine_handle<> suspending) noexcept
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

        //First ensure that suspending is stored in the promise
        const auto previous = promise->setWaitingCoroutine(suspending);
        INVARIANT(previous.address() == nullptr, "Multiple coroutines trying to await the same promise");
        //If hasn't been set, suspend.
        //If the result gets set after we call isDone, we still give up control over the coroutine and whoever set the result resumes it
        if (!isDone())
        {
            return {};
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

        return promise->setWaitingCoroutine({});
    }

    std::variant<ssize_t, uint32_t> await_resume() const noexcept
    {
        INVARIANT(promise->getResult(), "Resumed coroutine but no result was set in promise");
        return *promise->getResult();
    }
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
        explicit OwnershipTransferredAwaiter(const std::shared_ptr<promise_type>& promise) : promise(promise) { }
        OwnershipTransferredAwaiter(const OwnershipTransferredAwaiter& other) = default;
        OwnershipTransferredAwaiter(OwnershipTransferredAwaiter&& other) noexcept = default;
        OwnershipTransferredAwaiter& operator=(const OwnershipTransferredAwaiter& other) = default;
        OwnershipTransferredAwaiter& operator=(OwnershipTransferredAwaiter&& other) noexcept = default;
        bool await_ready() const { return promise->getYielded().value_or(false); }

        bool await_suspend(std::coroutine_handle<> toSuspend)
        {
            if (!promise->getYielded())
            {
                const auto previous = promise->setNextCoroutineOnYield(toSuspend);
                INVARIANT(previous.address() == nullptr, "Multiple couroutines trying to await the same promise");
                return true;
            }
            return false;
        }

        bool await_resume() const noexcept
        {
            INVARIANT(
                promise->getYielded(), "Coroutine waiting on OwnershipTransferredAwaiter resumed, but nothing was yielded in promise.");
            return *promise->getYielded();
        }
    };
    explicit PunchHoleFuture() = default;
    explicit PunchHoleFuture(const std::coroutine_handle<promise_type> handle) : promise(handle.promise().getSharedPromise()) { }
    explicit PunchHoleFuture(const std::shared_ptr<promise_type>& handle) : promise(handle) { }
    PunchHoleFuture(const PunchHoleFuture& other) noexcept = default;
    PunchHoleFuture(PunchHoleFuture&& other) noexcept = default;
    PunchHoleFuture& operator=(const PunchHoleFuture& other) = default;
    PunchHoleFuture& operator=(PunchHoleFuture&& other) noexcept = default;

    ~PunchHoleFuture() noexcept = default;

    void pollOnce() noexcept
    {
        if (auto* pollAwaiter = promise->exchangeAwaiter(nullptr))
        {
            pollAwaiter->getPollFn()();
            //If polling didn't make any progress, schedule it again
            if (!pollAwaiter->isDone())
            {
                promise->exchangeAwaiter(pollAwaiter);
            }
        }
    }
    void waitOnce() noexcept
    {
        //atomic exchange ensures that only one thread can run the method and resume the coroutine
        if (auto* currentBlockAwaiter = promise->exchangeAwaiter(nullptr))
        {
            currentBlockAwaiter->getWaitFn()();
            if (!currentBlockAwaiter->isDone())
            {
                promise->exchangeAwaiter(currentBlockAwaiter);
            }
        }
    }

    bool waitUntilYield() noexcept
    {
        while (!promise->getYielded().has_value())
        {
            waitOnce();
        }

        return *promise->getYielded();
    }

    [[nodiscard]] bool isDone() const noexcept { return promise->getYielded().has_value(); }
    [[nodiscard]] std::optional<std::variant<UringSuccessOrError, CoroutineError>> getResult() const noexcept
    {
        return promise->getResult();
    }

    //TODO add timeout to task and give back nullopt when timeout exceeded
    [[nodiscard]] std::optional<std::variant<UringSuccessOrError, CoroutineError>> waitUntilDone() noexcept
    {
        while (!isDone())
        {
            waitOnce();
        }
        return promise->getResult();
    };

    [[nodiscard]] DataSegment<OnDiskLocation> getTarget() const noexcept { return promise->getTarget(); }

    OwnershipTransferredAwaiter awaitYield() const noexcept { return OwnershipTransferredAwaiter{promise}; }
    bool await_ready() const noexcept { return isDone(); }

    std::coroutine_handle<> await_suspend(std::coroutine_handle<> suspending) noexcept
    {
        const auto previous = promise->setWaitingCoroutine(suspending);
        INVARIANT(previous.address() == nullptr, "Multiple coroutines trying to await the same promise");
        if (!isDone())
        {
            return {};
        }
        return promise->setWaitingCoroutine({});
    }

    std::variant<UringSuccessOrError, CoroutineError> await_resume() const noexcept
    {
        INVARIANT(promise->getResult(), "Resumed coroutine but no result was set in promise");
        return *promise->getResult();
    }
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
    explicit RepinBufferFuture(const std::coroutine_handle<promise_type> handle) : promise(handle.promise().getSharedPromise()) { }
    explicit RepinBufferFuture(const std::shared_ptr<promise_type>& handle) : promise(handle) { }
    RepinBufferFuture(const RepinBufferFuture& other) noexcept = default;
    RepinBufferFuture(RepinBufferFuture&& other) noexcept = default;
    RepinBufferFuture& operator=(const RepinBufferFuture& other) = default;
    RepinBufferFuture& operator=(RepinBufferFuture&& other) noexcept = default;

    ~RepinBufferFuture() noexcept = default;

    void pollOnce() noexcept
    {
        if (auto* pollAwaiter = promise->exchangeAwaiter(nullptr))
        {
            pollAwaiter->getPollFn()();
            //If polling didn't make any progress, schedule it again
            if (!pollAwaiter->isDone())
            {
                promise->exchangeAwaiter(pollAwaiter);
            }
        }
    }
    void waitOnce() noexcept
    {
        //atomic exchange ensures that only one thread can run the method and resume the coroutine
        if (auto* currentBlockAwaiter = promise->exchangeAwaiter(nullptr))
        {
            currentBlockAwaiter->getWaitFn()();
            if (!currentBlockAwaiter->isDone())
            {
                promise->exchangeAwaiter(currentBlockAwaiter);
            }
        }
    }

    [[nodiscard]] bool isDone() const noexcept { return promise->getResult().has_value(); }

    std::variant<PinnedBuffer, uint32_t> waitUntilDone() noexcept
    {
        while (!isDone())
        {
            waitOnce();
        }
        return *promise->getResult();
    };

    bool await_ready() const noexcept { return isDone(); }

    std::coroutine_handle<> await_suspend(std::coroutine_handle<> suspending) noexcept
    {
        const auto previous = promise->setWaitingCoroutine(suspending);
        INVARIANT(previous.address() == nullptr, "Multiple coroutines trying to await the same promise");
        if (!isDone())
        {
            return {};
        }
        return promise->setWaitingCoroutine({});
    }

    std::variant<PinnedBuffer, uint32_t> await_resume() const noexcept
    {
        INVARIANT(promise->getResult(), "Resumed coroutine but no result was set in promise");
        return *promise->getResult();
    }
};


}
}
