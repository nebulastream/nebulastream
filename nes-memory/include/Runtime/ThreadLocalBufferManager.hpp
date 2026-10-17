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

#include <chrono>
#include <cstddef>
#include <memory>
#include <optional>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES
{

/// A buffer provider that is owned by one thread and caches pooled buffers of a BufferManager that was created with thread local caches.
/// It takes and returns buffers in batches of 64, so a thread only touches the shared state of the BufferManager once per batch.
///
/// The provider binds to the thread that constructs it, and must be destroyed by that thread. Every pooled buffer that this thread
/// returns to the BufferManager, whichever provider handed it out, goes into the cache of the provider, so a thread that takes
/// and returns buffers at about the same rate rarely leaves its cache.
/// Buffers in the cache are invisible to the other threads, which makes this unsuitable for small pools.
class ThreadLocalBufferManager final : public AbstractBufferProvider
{
public:
    explicit ThreadLocalBufferManager(std::shared_ptr<BufferManager> manager);
    ~ThreadLocalBufferManager() override;

    ThreadLocalBufferManager(const ThreadLocalBufferManager&) = delete;
    ThreadLocalBufferManager& operator=(const ThreadLocalBufferManager&) = delete;
    ThreadLocalBufferManager(ThreadLocalBufferManager&&) = delete;
    ThreadLocalBufferManager& operator=(ThreadLocalBufferManager&&) = delete;

    [[nodiscard]] BufferManagerType getBufferManagerType() const override;
    [[nodiscard]] size_t getBufferSize() const override;
    [[nodiscard]] size_t getNumOfPooledBuffers() const override;
    [[nodiscard]] size_t getNumOfUnpooledBuffers() const override;

    TupleBuffer getBufferBlocking() override;
    std::optional<TupleBuffer> getBufferNoBlocking() override;
    std::optional<TupleBuffer> getBufferWithTimeout(std::chrono::milliseconds timeout) override;
    /// Unpooled buffers are not cached.
    std::optional<TupleBuffer> getUnpooledBuffer(size_t bufferSize) override;

    /// The provider that is bound to the calling thread, or nullptr.
    [[nodiscard]] static ThreadLocalBufferManager* current() noexcept;

private:
    friend class BufferManager;

    std::shared_ptr<BufferManager> manager;
    BufferManager::MagazineCache cache;
    /// The provider that was bound to this thread before, if providers are nested.
    ThreadLocalBufferManager* enclosing;
};

}
