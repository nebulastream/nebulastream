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

#include <chrono>
#include <thread>

#include <antlr4-runtime/Vocabulary.h>
#include <include/Runtime/BufferManagerFuture.hpp>

#include <include/Runtime/BufferManagerImpl.hpp>


namespace NES::Memory
{
GetInMemorySegmentFuture::GetInMemorySegmentFuture(const std::shared_ptr<promise_type>& promisePtr) : promise(promisePtr)
{
}
void GetInMemorySegmentFuture::pollOnce() noexcept
{
    if (auto pollAwaiter = promise->exchangeAwaiter(nullptr).lock())
    {
        pollAwaiter->getPollFn()();
        promise->updateIsDone();
        //If polling didn't make any progress, schedule it again
        if (!pollAwaiter->isDone())
        {
            auto weakAwaiter = std::weak_ptr{pollAwaiter};
            promise->exchangeAwaiter(weakAwaiter);
        }
    }
}
void GetInMemorySegmentFuture::waitOnce() noexcept
{
    std::weak_ptr<detail::VirtualAwaiterForProgress> currentWeakBlockAwaiter = promise->exchangeAwaiter(nullptr);
    do
    {
        //atomic exchange ensures that only one thread can run the method and resume the coroutine
        if (auto currentBlockAwaiter = currentWeakBlockAwaiter.lock())
        {
            auto waitFn = currentBlockAwaiter->getWaitFn();
            INVARIANT(waitFn, "Wait function was invalid");
            waitFn();
            promise->updateIsDone();
            if (currentBlockAwaiter->isDone())
            {
                // auto weakAwaiter = currentBlockAwaiter->weak_from_this();
                currentWeakBlockAwaiter = promise->exchangeAwaiter(nullptr);
            }
        } else
        {
            promise->updateIsDone();
            currentWeakBlockAwaiter = promise->exchangeAwaiter(nullptr);
        }
    } while (currentWeakBlockAwaiter.lock().get() != nullptr);
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
        if (!result)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
    return *result;
}
bool GetInMemorySegmentFuture::await_ready() const noexcept
{
    return isDone();
}
std::variant<std::vector<detail::DataSegment<detail::InMemoryLocation>>, uint32_t> GetInMemorySegmentFuture::await_resume() const noexcept
{
    promise->updateIsDone();
    INVARIANT(promise->getResult(), "Resumed GetInMemorySegmentFuture but result was not set.");
    return *promise->getResult();
}
ReadSegmentFuture::ReadSegmentFuture(const std::shared_ptr<promise_type>& promise) : promise(promise)
{
}
void ReadSegmentFuture::pollOnce() noexcept
{
    if (auto pollAwaiter = promise->exchangeAwaiter(nullptr).lock())
    {
        pollAwaiter->getPollFn()();
        promise->updateIsDone();
        //If polling didn't make any progress, schedule it again
        if (!pollAwaiter->isDone())
        {
            auto weakAwaiter = std::weak_ptr{pollAwaiter};
            promise->exchangeAwaiter(weakAwaiter);
        }
    }
}
void ReadSegmentFuture::waitOnce() noexcept
{
    std::weak_ptr<detail::VirtualAwaiterForProgress> currentWeakBlockAwaiter = promise->exchangeAwaiter(nullptr);
    do
    {
        //atomic exchange ensures that only one thread can run the method and resume the coroutine
        if (auto currentBlockAwaiter = currentWeakBlockAwaiter.lock())
        {
            auto waitFn = currentBlockAwaiter->getWaitFn();
            INVARIANT(waitFn, "Wait function was invalid");
            waitFn();
            promise->updateIsDone();
            if (currentBlockAwaiter->isDone())
            {
                // auto weakAwaiter = currentBlockAwaiter->weak_from_this();
                currentWeakBlockAwaiter = promise->exchangeAwaiter(nullptr);
            }
        } else
        {
            promise->updateIsDone();
            currentWeakBlockAwaiter = promise->exchangeAwaiter(nullptr);
        }
    } while (currentWeakBlockAwaiter.lock().get() != nullptr);
}

bool ReadSegmentFuture::isDone() const noexcept
{
    return promise->getResult().has_value();
}
std::variant<ssize_t, uint32_t> ReadSegmentFuture::waitUntilDone() noexcept
{
    auto result = promise->getResult();
    while (!result)
    {
        waitOnce();
        result = promise->getResult();
        if (!result)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
    return *result;
}
bool ReadSegmentFuture::await_ready() const noexcept
{
    return isDone();
}

std::variant<ssize_t, uint32_t> ReadSegmentFuture::await_resume() const noexcept
{
    INVARIANT(promise->getResult(), "Resumed coroutine but no result was set in promise");
    return *promise->getResult();
}
PunchHoleFuture::OwnershipTransferredAwaiter::OwnershipTransferredAwaiter(const std::shared_ptr<promise_type>& promise) : promise(promise)
{
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
    if (auto pollAwaiter = promise->exchangeAwaiter(nullptr).lock())
    {
        pollAwaiter->getPollFn()();
        promise->updateIsDone();
        //If polling didn't make any progress, schedule it again
        if (!pollAwaiter->isDone())
        {
            auto weakAwaiter = std::weak_ptr{pollAwaiter};
            promise->exchangeAwaiter(weakAwaiter);
        }
    }
}
void PunchHoleFuture::waitOnce() noexcept
{
    std::weak_ptr<detail::VirtualAwaiterForProgress> currentWeakBlockAwaiter = promise->exchangeAwaiter(nullptr);
    do
    {
        //atomic exchange ensures that only one thread can run the method and resume the coroutine
        if (auto currentBlockAwaiter = currentWeakBlockAwaiter.lock())
        {
            auto waitFn = currentBlockAwaiter->getWaitFn();
            INVARIANT(waitFn, "Wait function was invalid");
            waitFn();
            promise->updateIsDone();
            if (currentBlockAwaiter->isDone())
            {
                // auto weakAwaiter = currentBlockAwaiter->weak_from_this();
                currentWeakBlockAwaiter = promise->exchangeAwaiter(nullptr);
            }
        } else
        {
            promise->updateIsDone();
            currentWeakBlockAwaiter = promise->exchangeAwaiter(nullptr);
        }
    } while (currentWeakBlockAwaiter.lock().get() != nullptr);
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
std::optional<std::variant<UringSuccessOrError, detail::CoroutineError>> PunchHoleFuture::getResult() const noexcept
{
    return promise->getResult();
}
std::optional<std::variant<UringSuccessOrError, detail::CoroutineError>> PunchHoleFuture::waitUntilDone() noexcept
{
    auto result = promise->getResult();
    while (!result)
    {
        waitOnce();
        result = promise->getResult();
        if (!result)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
    return *result;
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
std::variant<UringSuccessOrError, detail::CoroutineError> PunchHoleFuture::await_resume() const noexcept
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
    if (auto pollAwaiter = promise->exchangeAwaiter(nullptr).lock())
    {
        pollAwaiter->getPollFn()();
        promise->updateIsDone();
        //If polling didn't make any progress, schedule it again
        if (!pollAwaiter->isDone())
        {
            auto weakAwaiter = std::weak_ptr{pollAwaiter};
            promise->exchangeAwaiter(weakAwaiter);
        }
    }
}
void RepinBufferFuture::waitOnce() const noexcept
{
    std::weak_ptr<detail::VirtualAwaiterForProgress> currentWeakBlockAwaiter = promise->exchangeAwaiter(nullptr);
    do
    {
        //atomic exchange ensures that only one thread can run the method and resume the coroutine
        if (auto currentBlockAwaiter = currentWeakBlockAwaiter.lock())
        {
            auto waitFn = currentBlockAwaiter->getWaitFn();
            INVARIANT(waitFn, "Wait function was invalid");
            waitFn();
            promise->updateIsDone();
            if (currentBlockAwaiter->isDone())
            {
                // auto weakAwaiter = currentBlockAwaiter->weak_from_this();
                currentWeakBlockAwaiter = promise->exchangeAwaiter(nullptr);
            }
        } else
        {
            promise->updateIsDone();
            currentWeakBlockAwaiter = promise->exchangeAwaiter(nullptr);
        }
    } while (currentWeakBlockAwaiter.lock().get() != nullptr);
}
bool RepinBufferFuture::isDone() const noexcept
{
    return promise->getResult().has_value();
}
std::variant<PinnedBuffer, uint32_t> RepinBufferFuture::waitUntilDone() const noexcept
{
    auto result = promise->getResult();
    while (!result)
    {
        waitOnce();
        // promise->updateIsDone();
        result = promise->getResult();
        if (!result)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
    return *result;
}
bool RepinBufferFuture::await_ready() const noexcept
{
    return isDone();
}

std::variant<PinnedBuffer, uint32_t> RepinBufferFuture::await_resume() const noexcept
{
    INVARIANT(promise->getResult(), "Resumed coroutine but no result was set in promise");
    return *promise->getResult();
}

RepinBufferFuture RepinBufferFuture::fromPinnedBuffer(PinnedBuffer buffer) noexcept
{
    co_return buffer;
}
}