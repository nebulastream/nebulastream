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
#include <Runtime/BufferManagerCoroCore.hpp>
#include <Runtime/BufferPrimitives.hpp>
#include <Runtime/PinnedBuffer.hpp>

#include <Runtime/DataSegment.hpp>
#include <Util/Logger/Logger.hpp>
#include <folly/MPMCQueue.h>

#include <any>
#include <shared_mutex>
#include <Runtime/BufferFiles.hpp>
#include <Util/Ranges.hpp>
#include <Util/Variant.hpp>
#include <ErrorHandling.hpp>

namespace NES::Memory
{
}
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


//Wrap around different Awaiters with a virtual class
class VirtualAwaiterForProgress : public std::enable_shared_from_this<VirtualAwaiterForProgress>
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
template <SharedPromise Promise>
class ResumeAfterBlockAwaiter
{
    static const inline std::function<void()> noOpPoll = [] { };
    std::function<void()> blockingFn;
    //We want to be able to return references on pollingFn,
    //if we'd store it in an optional we'd rely on the reference on *optional staying valid
    const bool hasPolling;
    std::function<void()> pollingFn;
    std::function<bool()> underlyingPoll;
    std::atomic<std::coroutine_handle<Promise>> handle{nullptr};
    std::atomic_flag hasResumed = false;
    std::atomic_flag hasPassed = false;

public:
    ResumeAfterBlockAwaiter(const ResumeAfterBlockAwaiter& other) = delete;
    ResumeAfterBlockAwaiter(ResumeAfterBlockAwaiter&& other) noexcept = delete;
    ResumeAfterBlockAwaiter& operator=(const ResumeAfterBlockAwaiter& other) = delete;
    ResumeAfterBlockAwaiter& operator=(ResumeAfterBlockAwaiter&& other) noexcept = delete;

    explicit ResumeAfterBlockAwaiter(const std::function<void()> blocking) : hasPolling(false)
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
            if (const auto toResume = handle.exchange(std::coroutine_handle<Promise>{}); toResume.address() != nullptr)
            {
                auto sharedPromisePtr = handle.promise().getSharedPromise();
                handle.resume();
                sharedPromisePtr->updateIsDone();
            }
        };
    }
    explicit ResumeAfterBlockAwaiter(const std::function<void()> blocking, const std::function<bool()> poll)
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
            if (const auto toResume = handle.exchange(std::coroutine_handle<Promise>{}); toResume.address() != nullptr)
            {
                auto sharedPromisePtr = toResume.promise().getSharedPromise();
                toResume.resume();
                sharedPromisePtr->updateIsDone();
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
                if (const auto toResume = handle.exchange(std::coroutine_handle<Promise>{}); toResume.address() != nullptr)
                {
                    auto sharedPromisePtr = toResume.promise().getSharedPromise();
                    toResume.resume();
                    sharedPromisePtr->updateIsDone();
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
        return handle.exchange(std::coroutine_handle<Promise>{});
    }

    void await_resume() const noexcept { }

    bool isDone() const noexcept { return hasResumed.test(); }

    [[nodiscard]] const std::function<void()> getBlocking() const noexcept { return blockingFn; }
    [[nodiscard]] const std::function<void()> getPolling() const noexcept
    {
        if (hasPolling)
        {
            return pollingFn;
        }
        return noOpPoll;
    }
};


template <SharedPromise Promise>
class AvailableSegmentAwaiter
{
    std::optional<DataSegment<InMemoryLocation>> result = std::nullopt;
    std::coroutine_handle<Promise> handle{};

    //This mutex is for synchronizing access on the result and handle.
    //The coroutine might try to set the handle while another thread is already filling in the result.
    //Without further synchronization, it could lead to the coroutine never waking up because the awaiter was already dequeued.
    //Alternatively, we could put both in an atomic tuple which should be 24 bytes big, but the logic would be more complicated.
    mutable std::recursive_mutex mutex{};

public:
    explicit AvailableSegmentAwaiter() noexcept { }
    //Copy and move are deleted because the whole point of this class is synchronizing access through the mutex
    AvailableSegmentAwaiter(const AvailableSegmentAwaiter& other) = delete;

    AvailableSegmentAwaiter(AvailableSegmentAwaiter&& other) = delete;
    AvailableSegmentAwaiter& operator=(const AvailableSegmentAwaiter& other) = delete;

    AvailableSegmentAwaiter& operator=(AvailableSegmentAwaiter&& other) = delete;
    bool await_ready() const noexcept
    {
        std::unique_lock lock{mutex};
        return result.has_value();
    }
    bool await_suspend(std::coroutine_handle<Promise> handle) noexcept
    {
        std::unique_lock lock{mutex};
        if (result.has_value())
        {
            return false;
        }
        this->handle = handle;

        //We could also do the IO uring write here, but I think the synchronization/batching is easier to do in the coroutine
        return true;
    }

    void setResultAndContinue(DataSegment<InMemoryLocation> segment)
    {
        std::unique_lock lock{mutex};
        // mutex.lock();
        if (result.has_value())
        {
            INVARIANT(false, "Continuing awaiter twice");
            // mutex.unlock();
            return;
        }
        result = segment;
        if (handle)
        {
            auto sharedPromisePtr = handle.promise().getSharedPromise();
            // mutex.unlock();
            //Ensure that promise does not go out of memory should handle.resume() complete it
            sharedPromisePtr->getHandle().resume();
            sharedPromisePtr->updateIsDone();
            return;
        }
        // mutex.unlock();
    }

    std::optional<DataSegment<InMemoryLocation>> await_resume() noexcept
    {
        //This lock should not be necessary because this method should only be called through setResultAndContinues handle.resume(),
        //but there is no way to guarantee that this class is always used like that.
        std::unique_lock lock{mutex};
        return result;
    }

    bool isDone() const noexcept
    {
        //I'm not sure if this lock is really necessary
        //Update: Yes, it is, accessing stuff inside the result is not atomic
        std::unique_lock lock{mutex};
        return result.has_value();
    }

    ///Creates a new awaiter and tries to put it into the request queue.
    ///@return The successfully created and inserted awaiter or nullopt if it wasn't inserted
    static std::optional<std::shared_ptr<AvailableSegmentAwaiter>>
    create(folly::MPMCQueue<std::weak_ptr<AvailableSegmentAwaiter>>& requestQueue)
    {
        auto awaiter = std::make_shared<AvailableSegmentAwaiter>();
        auto weakAwaiter = std::weak_ptr{awaiter};
        if (requestQueue.write(weakAwaiter))
        {
            return awaiter;
        }
        return std::nullopt;
    }
};

template <SharedPromise Promise>
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
            auto sharedPromisePtr = handle.promise().getSharedPromise();
            handle.resume();
            sharedPromisePtr->updateIsDone();
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

template <SharedPromise Promise>
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

    void setResultAndContinue()
    {
        std::scoped_lock lock{mutex};
        if (handle)
        {
            auto sharedPromisePtr = handle.promise().getSharedPromise();
            handle.resume();
            sharedPromisePtr->updateIsDone();
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

template <SharedPromise Promise>
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
            auto sharedPromisePtr = handle.promise().getSharedPromise();
            handle.resume();
            sharedPromisePtr->updateIsDone();
        }
    }

    bool isDone() const noexcept
    {
        std::scoped_lock lock{mutex};
        return returnValue.has_value();
    }
};

template <SharedPromise Promise>
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


    void setResultAndContinue()
    {
        std::scoped_lock lock{mutex};
        if (handle)
        {
            auto sharedPromisePtr = handle.promise().getSharedPromise();
            handle.resume();
            sharedPromisePtr->updateIsDone();
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


/*
 * The following templates are boilerplate to manage the lifetime of the underlying awaiters that an awaitExternalProgress stores.
 * There are three cases we need to cover:
 * 1) The underlying awaiter is a "normal" awaiter that is heap allocated with make_shared, and we store a weak ptr to it.
 *      The awaiter itself is not copyable, only its pointer is.
 *      This is the case for all awaiters inside this file.
 * 2) The underlying awaiter is wrapper around a shared pointer and we can just store a copy of it.
 *      This is the case for Futures, when they are awaited from another coroutine nested outside of them
 * 3) The underlying awaiter is a wrapper around weak ptr, and we can try to upgrade it to 2).
 *      We need this to store the continuing coroutine inside a promise (so the inverse side of 2), without creating a cyclic reference.
 */

template <typename Accessor, typename Awaiter>
concept AccessibleAwaiter = requires(Accessor accessor) {
    //Retrieves the underlying awaiter when it still exists, when it has expired, return something for which == nullptr is true
    { *(accessor.get()) } -> std::same_as<Awaiter>;
    { Accessor::isEmpty(accessor.get()) } -> std::same_as<bool>;
} && std::is_copy_assignable_v<Accessor>;


template <typename Promise, Awaiter<Promise> Awaiter>
class WeakAwaiterAccessor
{
    std::weak_ptr<Awaiter> ptr;

public:
    WeakAwaiterAccessor(const std::weak_ptr<Awaiter>& ptr) : ptr(ptr) { }
    WeakAwaiterAccessor(const WeakAwaiterAccessor& other) = default;
    WeakAwaiterAccessor(WeakAwaiterAccessor&& other) noexcept = default;
    WeakAwaiterAccessor& operator=(const WeakAwaiterAccessor& other) = default;
    WeakAwaiterAccessor& operator=(WeakAwaiterAccessor&& other) noexcept = default;
    ~WeakAwaiterAccessor() = default;

    std::shared_ptr<Awaiter> get() const { return ptr.lock(); }
    static bool isEmpty(std::shared_ptr<Awaiter> ptr) { return ptr == nullptr; }
};


///Just a wrapper around an awaiter instance to access the data in the same way as the weak awaiter
template <SafeAwaiter Awaiter>
class AutoSharedAccessor
{
    Awaiter awaiter;

public:
    AutoSharedAccessor(const Awaiter& awaiter) : awaiter(awaiter) { }
    AutoSharedAccessor(const AutoSharedAccessor& other) = default;
    AutoSharedAccessor(AutoSharedAccessor&& other) noexcept = default;
    AutoSharedAccessor& operator=(const AutoSharedAccessor& other) = default;
    AutoSharedAccessor& operator=(AutoSharedAccessor&& other) noexcept = default;
    ~AutoSharedAccessor() = default;

    std::optional<Awaiter> get() const { return awaiter; }

    static bool isEmpty(std::optional<Awaiter> opt) { return !opt.has_value(); }
};

template <typename Weak>
concept AutoWeakAwaiter = requires(Weak weakAccessor) {
    { weakAccessor.upgrade() } -> std::same_as<std::optional<typename Weak::SafeAwaiter>>;
} && SafeAwaiter<typename Weak::SafeAwaiter>;

template <SafeAwaiter Safe, AutoWeakAwaiter AutoWeakAwaiter>
class AutoWeakAccessor
{
    AutoWeakAwaiter weakRef;

public:
    AutoWeakAccessor(const AutoWeakAccessor& other) = default;
    AutoWeakAccessor(AutoWeakAccessor&& other) noexcept = default;
    AutoWeakAccessor& operator=(const AutoWeakAccessor& other) = default;
    AutoWeakAccessor& operator=(AutoWeakAccessor&& other) noexcept = default;

    std::optional<Safe> get() { return weakRef.upgrade(); }

    static bool isEmpty(std::optional<Safe> opt) { return !opt.has_value(); }
};
template <typename Promise, typename Awaiter>
struct AwaiterAccessorTraits
{
    using Accessor = WeakAwaiterAccessor<Promise, Awaiter>;
};


template <typename Promise, SafeAwaiter Awaiter>
struct AwaiterAccessorTraits<Promise, Awaiter>
{
    using Accessor = AutoSharedAccessor<Awaiter>;
};

template <typename Promise, AutoWeakAwaiter Awaiter>
struct AwaiterAccessorTraits<Promise, Awaiter>
{
    using Accessor = AutoWeakAccessor<typename Awaiter::SafeAwaiter, Awaiter>;
};



template <
    typename Promise,
    typename ReturnType,
    UnderlyingAwaiter<Promise, ReturnType, VirtualAwaiterForProgress> Awaiter,
    bool heapAllocated = true>
class AwaitExternalProgress : public VirtualAwaiterForProgress
{
public:
    using AwaiterAccessor = typename AwaiterAccessorTraits<Promise, Awaiter>::Accessor;

private:
    const bool alwaysSuspend;

    //Only set alwaysSuspendUnderlying to true if you are sure that this object is the only owner of the underlying awaiter,
    //and does not get completed for example by another thread.
    //Suitable for example for the ResumeAfterBlockAwaiter.
    const bool alwaysSuspendUnderlying;
    const std::function<void()> poll;
    const std::function<void()> wait;

    AwaiterAccessor underlyingAwaiterAccessor;

    struct Private
    {
        explicit Private() = default;
    };
    // public:
    // explicit AwaitExternalProgress(
    //     std::weak_ptr<ResumeAfterBlockAwaiter<Promise>> resumeAfterBlock,
    //     const bool alwaysSuspend = false,
    //     const bool alwaysSuspendUnderlying = false)
    //     : alwaysSuspend(alwaysSuspend)
    //     , alwaysSuspendUnderlying(alwaysSuspendUnderlying)
    //     , poll(resumeAfterBlock.getPolling())
    //     , wait(resumeAfterBlock.getBlocking())
    //     , weakUnderlyingAwaiter(resumeAfterBlock)
    // {
    // }
    //
public:
    explicit AwaitExternalProgress(
        Private,
        const std::function<void()>& poll,
        const std::function<void()>& wait,
        AwaiterAccessor awaiter,
        const bool alwaysSuspend = false,
        const bool alwaysSuspendUnderlying = false)
        : alwaysSuspend(alwaysSuspend)
        , alwaysSuspendUnderlying(alwaysSuspendUnderlying)
        , poll(poll)
        , wait(wait)
        , underlyingAwaiterAccessor(awaiter) { };

    static std::shared_ptr<AwaitExternalProgress> create(
        const std::function<void()>& poll,
        const std::function<void()>& wait,
        AwaiterAccessor awaiter,
        const bool alwaysSuspend = false,
        const bool alwaysSuspendUnderlying = false)
    {
        return std::make_shared<AwaitExternalProgress>(Private(), poll, wait, awaiter, alwaysSuspend, alwaysSuspendUnderlying);
    }


    bool await_ready() const noexcept
    {
        auto underlyingAwaiter = underlyingAwaiterAccessor.get();
        INVARIANT(underlyingAwaiter, "Attempted to check await ready on underlying awaiter, but it already expired");
        return underlyingAwaiter->await_ready();
    }

    template <typename Suspension = decltype(std::declval<Awaiter>().await_suspend(std::declval<std::coroutine_handle<Promise>>()))>
    Suspension await_suspend(std::coroutine_handle<Promise> handle) noexcept
    {
        if constexpr (requires(Promise p) {
                          { p.increaseAwaitCounter() };
                      })
        {
            handle.promise().increaseAwaitCounter();
        }
        //The underlying awaiters are responsible for ensuring thread safety and to guarantee resumption
        if (!alwaysSuspend && !alwaysSuspendUnderlying)
        {
            INVARIANT(
                !AwaiterAccessor::isEmpty(underlyingAwaiterAccessor.get()), "Tried to poll, but underlying awaiter was already expired");
            poll();
        }

        auto underlyingAwaiter = underlyingAwaiterAccessor.get();
        INVARIANT(!AwaiterAccessor::isEmpty(underlyingAwaiter), "Await suspend called, but underlying awaiter already expired");
        //Suspension depends on underlying awaiter
        //We need to call suspend first, because the moment we register this awaiter in the promise, the poll and wait functions could be called,
        //and if the underlying awaiter didn't have a chance to get the handle, it might never be resumed
        Suspension underlyingReturned;
        if (alwaysSuspendUnderlying)
        {
            underlyingReturned = underlyingAwaiter->await_suspend(handle);
        }
        if (alwaysSuspend || !underlyingAwaiter->await_ready())
        {
            if (heapAllocated)
            {
                //First create weak reference because the moment we suspend the underlying, another thread might pick up resuming the coroutine
                //and destroy this object, creating a race condition between the destroying the shared ptr and weak from this
                auto weakSelf = weak_from_this();
                if (!alwaysSuspendUnderlying)
                {
                    underlyingReturned = underlyingAwaiter->await_suspend(handle);
                }
                handle.promise().exchangeAwaiter(weakSelf);
            }
            else
            {
                if (!alwaysSuspendUnderlying)
                {
                    underlyingReturned = underlyingAwaiter->await_suspend(handle);
                }
                handle.promise().exchangeAwaiter(this);
            }

            if (alwaysSuspend)
            {
                //Always suspend
                if constexpr (std::is_assignable_v<std::coroutine_handle<>, Suspension>)
                {
                    return std::noop_coroutine();
                }
                else
                {
                    return true;
                }
            }
            return underlyingReturned;
        }

        //No suspension
        if constexpr (std::is_assignable_v<std::coroutine_handle<>, Suspension>)
        {
            return handle;
        }
        else
        {
            return false;
        }
    }

    ReturnType await_resume() noexcept
    {
        auto underlyingAwaiter = underlyingAwaiterAccessor.get();
        INVARIANT(underlyingAwaiter, "Attempted to resume underlying awaiter, but it already expired");
        return underlyingAwaiter->await_resume();
    }

    std::function<void()> getPollFn() override
    {
        auto awaiter = underlyingAwaiterAccessor.get();
        if (!AwaiterAccessor::isEmpty(awaiter))
        {
            return poll;
        }
        return [] { };
    }

    std::function<void()> getWaitFn() override
    {
        auto awaiter = underlyingAwaiterAccessor.get();
        if (!AwaiterAccessor::isEmpty(awaiter))
        {
            return wait;
        }
        return [] { };
    }

    bool isDone() override
    {
        if (auto underlyingAwaiter = underlyingAwaiterAccessor.get())
        {
            return underlyingAwaiter->isDone();
        }
        return true;
    }
};

//Just a convenience method for the ResumeAfterBlockAwaiters, because they also carry their wait and poll function with them
template <typename Promise>
static std::shared_ptr<AwaitExternalProgress<Promise, void, ResumeAfterBlockAwaiter<Promise>, true>> createAEPFromResumeAfterBlock(
    std::shared_ptr<ResumeAfterBlockAwaiter<Promise>> resumeAfterBlock,
    const bool alwaysSuspend = false,
    const bool alwaysSuspendUnderlying = false)
{
    return AwaitExternalProgress<Promise, void, ResumeAfterBlockAwaiter<Promise>, true>::create(
        resumeAfterBlock->getPolling(),
        resumeAfterBlock->getBlocking(),
        std::weak_ptr{resumeAfterBlock},
        alwaysSuspend,
        alwaysSuspendUnderlying);
}


}
}
