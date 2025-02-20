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

#include <include/Runtime/BufferManagerFuture.hpp>

#include <include/Runtime/BufferManagerImpl.hpp>


namespace NES::Memory
{
GetInMemorySegmentFuture::GetInMemorySegmentFuture(const std::coroutine_handle<promise_type> handle)
    : promise(handle.promise().getSharedPromise())
{
}
void GetInMemorySegmentFuture::pollOnce() noexcept
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
void GetInMemorySegmentFuture::waitOnce() noexcept
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
bool GetInMemorySegmentFuture::isDone() const noexcept
{
    return promise->getResult().has_value();
}
std::variant<std::vector<detail::DataSegment<detail::InMemoryLocation>>, uint32_t> GetInMemorySegmentFuture::waitUntilDone() noexcept
{
    auto result = promise->getResult();
    while (!result)
    {
        waitOnce();
        result = promise->getResult();
    }
    return *result;
}
bool GetInMemorySegmentFuture::await_ready() const noexcept
{
    return isDone();
}
std::coroutine_handle<> GetInMemorySegmentFuture::await_suspend(std::coroutine_handle<> suspending) noexcept
{
    const auto previous = promise->setWaitingCoroutine(suspending);
    INVARIANT(previous.address() == nullptr, "Multiple coroutines trying to await the same promise");
    if (!isDone())
    {
        return {};
    }
    return promise->setWaitingCoroutine({});
}
std::variant<std::vector<detail::DataSegment<detail::InMemoryLocation>>, uint32_t> GetInMemorySegmentFuture::await_resume() const noexcept
{
    INVARIANT(promise->getResult(), "Resumed GetInMemorySegmentFuture but result was not set.");
    return *promise->getResult();
}
ReadSegmentFuture::ReadSegmentFuture(const std::coroutine_handle<promise_type> handle) : promise(handle.promise().getSharedPromise())
{
}
void ReadSegmentFuture::pollOnce() noexcept
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
void ReadSegmentFuture::waitOnce() noexcept
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
bool ReadSegmentFuture::isDone() const noexcept
{
    return promise->getResult().has_value();
}
std::variant<ssize_t, uint32_t> ReadSegmentFuture::waitUntilDone() noexcept
{
    while (!isDone())
    {
        waitOnce();
    }
    return *promise->getResult();
}
bool ReadSegmentFuture::await_ready() const noexcept
{
    return isDone();
}
std::coroutine_handle<> ReadSegmentFuture::await_suspend(std::coroutine_handle<> suspending) noexcept
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
std::variant<ssize_t, uint32_t> ReadSegmentFuture::await_resume() const noexcept
{
    INVARIANT(promise->getResult(), "Resumed coroutine but no result was set in promise");
    return *promise->getResult();
}
PunchHoleFuture::OwnershipTransferredAwaiter::OwnershipTransferredAwaiter(const std::shared_ptr<promise_type>& promise) : promise(promise)
{
}
bool PunchHoleFuture::OwnershipTransferredAwaiter::await_suspend(std::coroutine_handle<> toSuspend)
{
    if (!promise->getYielded())
    {
        const auto previous = promise->setNextCoroutineOnYield(toSuspend);
        INVARIANT(previous.address() == nullptr, "Multiple couroutines trying to await the same promise");
        return true;
    }
    return false;
}
bool PunchHoleFuture::OwnershipTransferredAwaiter::await_resume() const noexcept
{
    INVARIANT(promise->getYielded(), "Coroutine waiting on OwnershipTransferredAwaiter resumed, but nothing was yielded in promise.");
    return *promise->getYielded();
}
PunchHoleFuture::PunchHoleFuture(const std::coroutine_handle<promise_type> handle) : promise(handle.promise().getSharedPromise())
{
}
PunchHoleFuture::PunchHoleFuture(const std::shared_ptr<promise_type>& handle) : promise(handle)
{
}
void PunchHoleFuture::pollOnce() noexcept
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
void PunchHoleFuture::waitOnce() noexcept
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
bool PunchHoleFuture::waitUntilYield() noexcept
{
    while (!promise->getYielded().has_value())
    {
        waitOnce();
    }

    return *promise->getYielded();
}
bool PunchHoleFuture::isDone() const noexcept
{
    return promise->getYielded().has_value();
}
std::optional<std::variant<UringSuccessOrError, CoroutineError>> PunchHoleFuture::getResult() const noexcept
{
    return promise->getResult();
}
std::optional<std::variant<UringSuccessOrError, CoroutineError>> PunchHoleFuture::waitUntilDone() noexcept
{
    while (!isDone())
    {
        waitOnce();
    }
    return promise->getResult();
}
detail::DataSegment<detail::OnDiskLocation> PunchHoleFuture::getTarget() const noexcept
{
    return promise->getTarget();
}
PunchHoleFuture::OwnershipTransferredAwaiter PunchHoleFuture::awaitYield() const noexcept
{
    return OwnershipTransferredAwaiter{promise};
}
bool PunchHoleFuture::await_ready() const noexcept
{
    return isDone();
}
std::coroutine_handle<> NES::Memory::PunchHoleFuture::await_suspend(std::coroutine_handle<> suspending) const noexcept
{
    const auto previous = promise->setWaitingCoroutine(suspending);
    INVARIANT(previous.address() == nullptr, "Multiple coroutines trying to await the same promise");
    if (!isDone())
    {
        return {};
    }
    return promise->setWaitingCoroutine({});
}
std::variant<UringSuccessOrError, CoroutineError> PunchHoleFuture::await_resume() const noexcept
{
    INVARIANT(promise->getResult(), "Resumed coroutine but no result was set in promise");
    return *promise->getResult();
}
RepinBufferFuture::RepinBufferFuture(const std::coroutine_handle<promise_type> handle) : promise(handle.promise().getSharedPromise())
{
}
RepinBufferFuture::RepinBufferFuture(const std::shared_ptr<promise_type>& handle) : promise(handle)
{
}
void RepinBufferFuture::pollOnce() const noexcept
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
void RepinBufferFuture::waitOnce() const noexcept
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
bool RepinBufferFuture::isDone() const noexcept
{
    return promise->getResult().has_value();
}
std::variant<PinnedBuffer, uint32_t> RepinBufferFuture::waitUntilDone() const noexcept
{
    while (!isDone())
    {
        waitOnce();
    }
    return *promise->getResult();
}
bool RepinBufferFuture::await_ready() const noexcept
{
    return isDone();
}
std::coroutine_handle<> RepinBufferFuture::await_suspend(std::coroutine_handle<> suspending) const noexcept
{
    const auto previous = promise->setWaitingCoroutine(suspending);
    INVARIANT(previous.address() == nullptr, "Multiple coroutines trying to await the same promise");
    if (!isDone())
    {
        return {};
    }
    return promise->setWaitingCoroutine({});
}
std::variant<PinnedBuffer, uint32_t> RepinBufferFuture::await_resume() const noexcept
{
    INVARIANT(promise->getResult(), "Resumed coroutine but no result was set in promise");
    return *promise->getResult();
}

RepinBufferFuture RepinBufferFuture::fromPinnedBuffer(PinnedBuffer& buffer) noexcept
{
    co_return buffer;
}
}