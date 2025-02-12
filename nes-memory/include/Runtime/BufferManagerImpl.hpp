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

#include <Runtime/DataSegment.hpp>
#include <Util/Logger/Logger.hpp>
#include <folly/MPMCQueue.h>

#include <Util/Ranges.hpp>
#include <folly/SharedMutex.h>
#include <google/protobuf/stubs/port.h>
#include <Runtime/BufferFiles.hpp>
#include <ErrorHandling.hpp>

namespace NES::Memory
{

class BufferManager;
namespace detail
{

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


//Using a concept allows us to "forward declare" behaviour of types that at the end depend on the future type again
template <typename UnderlyingAwaiter, typename Promise, typename ReturnType, typename WrappedAwaiter>
concept Awaiter = requires(UnderlyingAwaiter underlyingAwaiter, std::coroutine_handle<Promise> handle, WrappedAwaiter wrappedAwaiter) {
    { underlyingAwaiter.await_ready() } -> std::same_as<bool>;
    { underlyingAwaiter.await_suspend(handle) } -> std::same_as<bool>;
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

template <typename Future>
class GetInMemorySegmentPromise
{
    friend class GetInMemorySegmentFuture;
    DataSegment<InMemoryLocation> result;
    //Raw pointer because the ResumeAfterBlockAwaiter is allocated on the "stack" of the coroutine anyway but we also need a not-present value
    std::atomic<VirtualAwaiterForProgress*> currentBlockAwaiter;

    BufferManager* bufferManager;

public:
    //Constructor call is inserted by the compiler when calling the coroutine function,
    //passing instance and any arguments to the coroutine to this function
    GetInMemorySegmentPromise(BufferManager& bufferManager) : currentBlockAwaiter(nullptr), bufferManager(&bufferManager) { }
    Future get_return_object() { return Future(std::coroutine_handle<GetInMemorySegmentPromise>::from_promise(*this)); }
    void unhandled_exception() noexcept { };
    void return_value(DataSegment<InMemoryLocation> segment) noexcept { result = segment; }
    void setBlockingFunction(VirtualAwaiterForProgress* newBlocking) noexcept { currentBlockAwaiter = newBlocking; }

    std::suspend_never initial_suspend() noexcept { return {}; };
    //Needs to always suspend, because otherwise the coroutine self-destructs after co_return.
    //When then the destructor of the future is called because it goes out-of-scope, it causes a segmentation fault
    std::suspend_always final_suspend() noexcept { return {}; };

    VirtualAwaiterForProgress* exchangeAwaiter(VirtualAwaiterForProgress* newAwaiter) { return currentBlockAwaiter.exchange(newAwaiter); }
};

template <typename Future>
class ReadSegmentPromise
{
    friend class ReadSegmentFuture;
    DataSegment<OnDiskLocation> source;
    DataSegment<InMemoryLocation> target;
    //Raw pointer because the ResumeAfterBlockAwaiter is allocated on the "stack" of the coroutine anyway but we also need a not-present value
    std::atomic<VirtualAwaiterForProgress*> currentBlockAwaiter;

    BufferManager* bufferManager;

    std::optional<ssize_t> result;

public:
    //Constructor call is inserted by the compiler when calling the coroutine function,
    //passing instance and any arguments to the coroutine to this function
    ReadSegmentPromise(BufferManager& bufferManager, DataSegment<OnDiskLocation> source, DataSegment<InMemoryLocation> target)
        : source(source), target(target), currentBlockAwaiter(nullptr), bufferManager(&bufferManager)
    {
    }
    Future get_return_object() { return Future(std::coroutine_handle<ReadSegmentPromise>::from_promise(*this)); }
    void unhandled_exception() noexcept { };
    void return_value(std::optional<ssize_t> result) noexcept { this->result = result; }
    void setBlockingFunction(VirtualAwaiterForProgress* newBlocking) noexcept { currentBlockAwaiter = newBlocking; }

    std::suspend_never initial_suspend() noexcept { return {}; };
    //Needs to always suspend, because otherwise the coroutine self-destructs after co_return.
    //When then the destructor of the future is called because it goes out-of-scope, it causes a segmentation fault
    std::suspend_always final_suspend() noexcept { return {}; };

    VirtualAwaiterForProgress* exchangeAwaiter(VirtualAwaiterForProgress* newAwaiter) { return currentBlockAwaiter.exchange(newAwaiter); }
};

template <typename Promise>
concept SharedPromise = requires(Promise promise) {
    { promise.getSharedPromise() } -> std::same_as<std::shared_ptr<Promise>>;
};

class PunchHoleResult
{
    //0 indicates success,
    //negative values indicate iouring errors, that must be negated to get an error message through strerror
    //positive values indicate our errors
    ssize_t returnCode;

public:
    constexpr PunchHoleResult(const PunchHoleResult& other) = default;
    constexpr PunchHoleResult(PunchHoleResult&& other) noexcept = default;
    constexpr PunchHoleResult& operator=(const PunchHoleResult& other) = default;
    constexpr PunchHoleResult& operator=(PunchHoleResult&& other) noexcept = default;
    constexpr explicit PunchHoleResult(ssize_t returnCode) : returnCode(returnCode) { }

    [[nodiscard]] constexpr bool isSuccess() const { return returnCode == 0; }

    [[nodiscard]] constexpr std::optional<uint32_t> getUringError() const
    {
        if (returnCode <= 0)
        {
            return -returnCode;
        }
        return std::nullopt;
    }

    [[nodiscard]] constexpr std::optional<uint32_t> getInternalError() const
    {
        if (returnCode >= 0)
        {
            return returnCode;
        }
        return std::nullopt;
    }


    static constexpr PunchHoleResult FAILED_TO_TRANSFER_OWNERSHIP() { return PunchHoleResult{ErrorCode::FailedToTransferCleanupOwnership}; }

    static constexpr PunchHoleResult FAILED_TO_SUBMIT() { return PunchHoleResult{ErrorCode::CannotSubmitBufferIO}; }

    static constexpr PunchHoleResult CONTINUED_WITHOUT_RESULT() { return PunchHoleResult{ErrorCode::CoroutineContinuedWithoutResult}; }
};

template <typename Future>
class PunchHolePromise
{
    friend class PunchHoleFuture;
    DataSegment<OnDiskLocation> target;
    //Raw pointer because the ResumeAfterBlockAwaiter is allocated on the "stack" of the coroutine anyway but we also need a not-present value
    std::atomic<VirtualAwaiterForProgress*> currentBlockAwaiter;

    BufferManager* bufferManager;

    std::optional<PunchHoleResult> result;

    std::optional<bool> hasSubmitted;
    std::shared_ptr<PunchHolePromise> sharedHandle;

public:
    //Constructor call is inserted by the compiler when calling the coroutine function,
    //passing instance and any arguments to the coroutine to this function
    PunchHolePromise(BufferManager& bufferManager, DataSegment<OnDiskLocation> target)
        : target(target)
        , currentBlockAwaiter(nullptr)
        , bufferManager(&bufferManager)
        , sharedHandle(this, [](auto promise) { std::coroutine_handle<PunchHolePromise>::from_promise(*promise).destroy(); })
    {
    }
    Future get_return_object() { return Future(std::coroutine_handle<PunchHolePromise>::from_promise(*this)); }
    void unhandled_exception() noexcept { };
    void return_value(std::optional<PunchHoleResult> result) noexcept { this->result = result; }

    //Suspend never because it is just information for the caller.
    //Either, it succeeds to submit and then suspends when waiting for the confirmation, or it needs to reach co_return so that the coroutine
    //does not get leaked through shared handles in the coroutine itself.
    std::suspend_never yield_value(bool hasSubmitted) noexcept
    {
        this->hasSubmitted = hasSubmitted;
        return {};
    }
    void setBlockingFunction(VirtualAwaiterForProgress* newBlocking) noexcept { currentBlockAwaiter = newBlocking; }

    std::suspend_never initial_suspend() noexcept { return {}; };
    //Needs to always suspend, because otherwise the coroutine self-destructs after co_return.
    //When then the destructor of the future is called because it goes out-of-scope, it causes a segmentation fault
    std::suspend_always final_suspend() noexcept { return {}; };

    VirtualAwaiterForProgress* exchangeAwaiter(VirtualAwaiterForProgress* newAwaiter) { return currentBlockAwaiter.exchange(newAwaiter); }

    std::shared_ptr<PunchHolePromise> getSharedPromise() { return sharedHandle; }
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
    std::coroutine_handle<Promise> handle;
    std::atomic_flag hasResumed = false;
    std::atomic_flag hasPassed = false;

public:
    explicit ResumeAfterBlockAwaiter(const std::function<void()>& blocking) : hasPolling(false)
    {
        blockingFn = [&, blocking]()
        {
            //These objects are strictly oneshot
            bool check = hasResumed.test_and_set();
            INVARIANT(check, "Coroutine was resumed concurrently");
            //Is blocking getting copied into this lambda correctly?
            blocking();

            handle.resume();
        };
    }
    explicit ResumeAfterBlockAwaiter(const std::function<void()>& blocking, const std::function<bool()>& poll)
        : hasPolling(true), underlyingPoll(poll)
    {
        blockingFn = [&, blocking]()
        {
            //These objects are strictly oneshot
            bool check = hasResumed.test_and_set();
            INVARIANT(!check, "Coroutine was resumed blocking concurrently");
            //Is blocking getting copied into this lambda correctly?
            blocking();

            if (handle)
            {
                handle.resume();
            }
        };

        pollingFn = [&, poll]()
        {
            bool check = hasResumed.test_and_set();
            INVARIANT(!check, "Coroutine was polled while concurrently making blocking progress");
            if (!hasPassed.test())
            {
                if (poll())
                {
                    hasPassed.test_and_set();
                }
            }
            if (hasPassed.test())
            {
                if (handle)
                {
                    handle.resume();
                }
            }
            else
            {
                hasResumed.clear();
            }
        };
    }


    bool await_ready() const noexcept { return hasResumed.test(); }
    bool await_suspend(std::coroutine_handle<Promise> suspendingHandle) noexcept
    {
        bool suspend = true;
        if (hasPolling)
        {
            suspend = !underlyingPoll();
        }
        if (!suspend)
        {
            return false;
        }
        handle = suspendingHandle;
        return true;
    }

    void await_resume() const noexcept { }

    bool isDone() noexcept { return hasResumed.test(); }

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
    DataSegment<InMemoryLocation> result;
    std::coroutine_handle<Promise> handle;

    //This mutex is for synchronizing access on the result and handle.
    //The coroutine might try to set the handle while another thread is already filling in the result.
    //Without further synchronization, it could lead to the coroutine never waking up because the awaiter was already dequeued.
    //Alternatively, we could put both in an atomic tuple which should be 24 bytes big, but the logic would be more complicated.
    std::recursive_mutex mutex;

public:
    explicit AvailableSegmentAwaiter() noexcept { }
    //Copy and move are deleted because the whole point of this class is synchronizing access through the mutex
    AvailableSegmentAwaiter(const AvailableSegmentAwaiter& other) = delete;

    AvailableSegmentAwaiter(AvailableSegmentAwaiter&& other) = delete;
    AvailableSegmentAwaiter& operator=(const AvailableSegmentAwaiter& other) = delete;

    AvailableSegmentAwaiter& operator=(AvailableSegmentAwaiter&& other) = delete;
    bool await_ready() noexcept { return false; }
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
        std::scoped_lock const lock{mutex};
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
        std::scoped_lock const lock{mutex};
        if (result.getLocation().getPtr() == nullptr)
        {
            return std::nullopt;
        }
        return result;
    }

    bool isDone() noexcept
    {
        std::scoped_lock const lock{mutex};
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

    std::recursive_mutex mutex;

public:
    explicit ReadSegmentAwaiter() { }

    //Can't move or copy instances because of the mutex
    ReadSegmentAwaiter(const ReadSegmentAwaiter& other) = delete;

    ReadSegmentAwaiter(ReadSegmentAwaiter&& other) = delete;
    ReadSegmentAwaiter& operator=(const ReadSegmentAwaiter& other) = delete;

    ReadSegmentAwaiter& operator=(ReadSegmentAwaiter&& other) = delete;

    bool await_ready() noexcept { return false; }
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

    std::optional<ssize_t> await_resume() noexcept
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

    std::recursive_mutex mutex;

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

    bool await_ready() const noexcept { return false; }
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
    std::mutex mutex;

public:
    explicit PunchHoleSegmentAwaiter() { }

    //Can't move or copy instances because of the mutex
    PunchHoleSegmentAwaiter(const PunchHoleSegmentAwaiter& other) = delete;

    PunchHoleSegmentAwaiter(PunchHoleSegmentAwaiter&& other) = delete;
    PunchHoleSegmentAwaiter& operator=(const PunchHoleSegmentAwaiter& other) = delete;

    PunchHoleSegmentAwaiter& operator=(PunchHoleSegmentAwaiter&& other) = delete;

    bool await_ready() noexcept { return false; }
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

    std::optional<ssize_t> await_resume() noexcept
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
    std::mutex mutex;

    explicit SubmitPunchHoleSegmentAwaiter(const DataSegment<OnDiskLocation> target) : target(target) { }

public:
    //Can't move or copy instances because of the mutex
    SubmitPunchHoleSegmentAwaiter(const SubmitPunchHoleSegmentAwaiter& other) = delete;

    SubmitPunchHoleSegmentAwaiter(SubmitPunchHoleSegmentAwaiter&& other) = delete;
    SubmitPunchHoleSegmentAwaiter& operator=(const SubmitPunchHoleSegmentAwaiter& other) = delete;

    SubmitPunchHoleSegmentAwaiter& operator=(SubmitPunchHoleSegmentAwaiter&& other) = delete;

    bool await_ready() const noexcept { return false; }
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
     * Everything is in the frame of the coroutine anyway, unless someone does something stupid on purpose like
     * AwaitExternalProgress awaitExternalProg;
     * {
     *      Awaiter underlying = {};
     *      awaitExternalProg = {underlying}
     * }
     * storing a reference should be fine.
     * We can't move in the underlying awaiters because of the mutexes.
     * An alternative would be unique pointers, which would be one more level of indirection.
     */
    Awaiter& underlyingAwaiter;

public:
    explicit AwaitExternalProgress(std::function<void()> poll, std::function<void()> wait, Awaiter& awaiter)
        : poll(poll), wait(wait), underlyingAwaiter(awaiter) { };
    explicit AwaitExternalProgress(ResumeAfterBlockAwaiter<Promise>& resumeAfterBlock)
        : poll(resumeAfterBlock.getPolling()), wait(resumeAfterBlock.getBlocking()), underlyingAwaiter(resumeAfterBlock)
    {
    }

    bool await_ready() noexcept { return underlyingAwaiter.await_ready(); }

    bool await_suspend(std::coroutine_handle<Promise> handle) noexcept
    {
        poll();
        if (!underlyingAwaiter.await_ready())
        {
            handle.promise().exchangeAwaiter(this);
            return underlyingAwaiter.await_suspend(handle);
        }
        return false;
    }

    ReturnType await_resume() noexcept { return underlyingAwaiter.await_resume(); }

    std::function<void()> getPollFn() override { return poll; }

    std::function<void()> getWaitFn() override { return wait; }

    bool isDone() override { return underlyingAwaiter.isDone(); }
};


class GetInMemorySegmentFuture
{
public:
    using promise_type = GetInMemorySegmentPromise<GetInMemorySegmentFuture>;

private:
    std::coroutine_handle<promise_type> handle;

public:
    explicit GetInMemorySegmentFuture(const std::coroutine_handle<promise_type> handle) : handle(handle) { }
    GetInMemorySegmentFuture(const GetInMemorySegmentFuture& other) = delete;
    GetInMemorySegmentFuture(GetInMemorySegmentFuture&& other) noexcept : handle(std::exchange(other.handle, nullptr)) { }
    GetInMemorySegmentFuture& operator=(const GetInMemorySegmentFuture& other) = delete;
    GetInMemorySegmentFuture& operator=(GetInMemorySegmentFuture&& other) noexcept
    {
        if (this == &other)
            return *this;
        if (handle)
        {
            handle.destroy();
        }
        handle = std::exchange(other.handle, nullptr);
        return *this;
    }

    ~GetInMemorySegmentFuture()
    {
        if (handle)
        {
            handle.destroy();
        }
    }

    void pollOnce() noexcept
    {
        if (auto* pollAwaiter = handle.promise().exchangeAwaiter(nullptr))
        {
            pollAwaiter->getPollFn()();
            //If polling didn't make any progress, schedule it again
            if (!pollAwaiter->isDone())
            {
                handle.promise().exchangeAwaiter(pollAwaiter);
            }
        }
    }
    void waitOnce() noexcept
    {
        //atomic exchange ensures that only one thread can run the method and resume the coroutine
        if (auto* currentBlockAwaiter = handle.promise().exchangeAwaiter(nullptr))
        {
            currentBlockAwaiter->getWaitFn()();
            if (!currentBlockAwaiter->isDone())
            {
                handle.promise().exchangeAwaiter(currentBlockAwaiter);
            }
        }
    }

    [[nodiscard]] bool isDone() const noexcept { return handle.promise().result.getLocation().getPtr() != nullptr; }

    //TODO add timeout to task and give back nullopt when timeout exceededc
    std::optional<DataSegment<InMemoryLocation>> waitUntilDone() noexcept
    {
        while (!isDone())
        {
            waitOnce();
        }
        return handle.promise().result;
    };
};

class ReadSegmentFuture
{
public:
    using promise_type = ReadSegmentPromise<ReadSegmentFuture>;

private:
    std::coroutine_handle<promise_type> handle;

public:
    explicit ReadSegmentFuture(const std::coroutine_handle<promise_type> handle) : handle(handle) { }
    ReadSegmentFuture(const ReadSegmentFuture& other) = delete;
    ReadSegmentFuture(ReadSegmentFuture&& other) noexcept : handle(std::exchange(other.handle, nullptr)) { }
    ReadSegmentFuture& operator=(const ReadSegmentFuture& other) = delete;
    ReadSegmentFuture& operator=(ReadSegmentFuture&& other) noexcept
    {
        if (this == &other)
            return *this;
        if (handle)
        {
            handle.destroy();
        }
        handle = std::exchange(other.handle, nullptr);
        return *this;
    }

    ~ReadSegmentFuture()
    {
        if (handle)
        {
            handle.destroy();
        }
    }

    void pollOnce() noexcept
    {
        if (auto* pollAwaiter = handle.promise().exchangeAwaiter(nullptr))
        {
            pollAwaiter->getPollFn()();
            //If polling didn't make any progress, schedule it again
            if (!pollAwaiter->isDone())
            {
                handle.promise().exchangeAwaiter(pollAwaiter);
            }
        }
    }
    void waitOnce() noexcept
    {
        //atomic exchange ensures that only one thread can run the method and resume the coroutine
        if (auto* currentBlockAwaiter = handle.promise().exchangeAwaiter(nullptr))
        {
            currentBlockAwaiter->getWaitFn()();
            if (!currentBlockAwaiter->isDone())
            {
                handle.promise().exchangeAwaiter(currentBlockAwaiter);
            }
        }
    }

    [[nodiscard]] bool isDone() const noexcept { return handle.promise().result.has_value(); }

    //TODO add timeout to task and give back nullopt when timeout exceededc
    std::optional<ssize_t> waitUntilDone() noexcept
    {
        while (!isDone())
        {
            waitOnce();
        }
        return handle.promise().result;
    };
};

class PunchHoleFuture
{
public:
    using promise_type = PunchHolePromise<PunchHoleFuture>;

private:
    std::shared_ptr<promise_type> promise;

public:
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
        while (!promise->hasSubmitted.has_value())
        {
            waitOnce();
        }

        return *promise->hasSubmitted;
    }

    [[nodiscard]] bool isDone() const noexcept { return promise->result.has_value(); }
    [[nodiscard]] std::optional<PunchHoleResult> getResult() const noexcept { return promise->result; }

    //TODO add timeout to task and give back nullopt when timeout exceededc
    [[nodiscard]] std::optional<PunchHoleResult> waitUntilDone() noexcept
    {
        while (!isDone())
        {
            waitOnce();
        }
        return promise->result;
    };

    [[nodiscard]] DataSegment<OnDiskLocation> getTarget() const noexcept { return promise->target; }
};


}
}

// template <>
// struct fmt::formatter<NES::Memory::detail::PunchHoleResult> : std::formatter<std::string_view>
// {
//     auto format(NES::Memory::detail::PunchHoleResult result, format_context& ctx) const -> format_context::iterator
//     {
//         string_view output = "Unknown PunchHoleResult";
//         switch (result)
//         {
//             case NES::Memory::detail::PunchHoleResult::FAILED_TO_TRANSFER_OWNERSHIP(): output = "Failed to transfer ownership of coroutine for hole punching to buffer recycler."; break;
//             case NES::Memory::detail::PunchHoleResult::FAILED_TO_SUBMIT(): output = "Failed to submit request for punching hole into bounded queue."; break;
//             case NES::Memory::detail::PunchHoleResult::CONTINUED_WITHOUT_RESULT(): output = "Coroutine was continued without providing awaiter a result"; break;
//
//         }
//     }
// };