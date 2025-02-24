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
#include <Runtime/BufferManagerPromise.hpp>
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
            return std::noop_coroutine();
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




}
}
