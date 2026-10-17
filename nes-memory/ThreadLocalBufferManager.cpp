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

#include <Runtime/ThreadLocalBufferManager.hpp>

#include <chrono>
#include <cstddef>
#include <memory>
#include <optional>
#include <utility>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ErrorHandling.hpp>
#include <TupleBufferImpl.hpp>

namespace NES
{

namespace
{
thread_local ThreadLocalBufferManager* boundProvider = nullptr;
}

ThreadLocalBufferManager::ThreadLocalBufferManager(std::shared_ptr<BufferManager> manager)
    : manager(std::move(manager)), enclosing(boundProvider)
{
    PRECONDITION(this->manager != nullptr and this->manager->usesThreadLocalCaches(), "The BufferManager has to use thread local caches");
    const auto numberOfCaches = this->manager->numberOfCaches.fetch_add(1) + 1;
    INVARIANT(
        numberOfCaches <= BufferManager::MAX_MAGAZINE_CACHES,
        "More than {} thread local buffer providers for one BufferManager",
        BufferManager::MAX_MAGAZINE_CACHES);
    boundProvider = this;
}

ThreadLocalBufferManager::~ThreadLocalBufferManager()
{
    INVARIANT(boundProvider == this, "A ThreadLocalBufferManager has to be destroyed by the thread it is bound to, in reverse order");
    boundProvider = enclosing;
    manager->releaseCache(cache);
    manager->numberOfCaches.fetch_sub(1);
}

ThreadLocalBufferManager* ThreadLocalBufferManager::current() noexcept
{
    return boundProvider;
}

BufferManagerType ThreadLocalBufferManager::getBufferManagerType() const
{
    return BufferManagerType::LOCAL;
}

size_t ThreadLocalBufferManager::getBufferSize() const
{
    return manager->getBufferSize();
}

size_t ThreadLocalBufferManager::getNumOfPooledBuffers() const
{
    return manager->getNumOfPooledBuffers();
}

size_t ThreadLocalBufferManager::getNumOfUnpooledBuffers() const
{
    return manager->getNumOfUnpooledBuffers();
}

TupleBuffer ThreadLocalBufferManager::getBufferBlocking()
{
    if (auto buffer = getBufferWithTimeout(GET_BUFFER_TIMEOUT))
    {
        return std::move(*buffer);
    }
    throw BufferAllocationFailure("Global buffer pool could not allocate buffer before timeout({})", GET_BUFFER_TIMEOUT);
}

std::optional<TupleBuffer> ThreadLocalBufferManager::getBufferNoBlocking()
{
    auto* const segment = manager->takeFromCache(cache, false, {});
    if (segment == nullptr)
    {
        return std::nullopt;
    }
    return manager->makeBuffer(segment);
}

std::optional<TupleBuffer> ThreadLocalBufferManager::getBufferWithTimeout(const std::chrono::milliseconds timeout)
{
    auto* const segment = manager->takeFromCache(cache, true, std::chrono::steady_clock::now() + timeout);
    if (segment == nullptr)
    {
        return std::nullopt;
    }
    return manager->makeBuffer(segment);
}

std::optional<TupleBuffer> ThreadLocalBufferManager::getUnpooledBuffer(const size_t bufferSize)
{
    return manager->getUnpooledBuffer(bufferSize);
}

}
