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

#include <cstdint>
#include <future>
#include <memory>
#include <optional>
#include <queue>
#include <unordered_map>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <folly/ThreadLocal.h>

namespace NES::Runtime
{

class BufferStorage;
using BufferStoragePtr = std::shared_ptr<Runtime::BufferStorage>;

/**
 * @brief A WorkerContext represents the current state of a worker thread
 * Note that it is not thread-safe per se but it is meant to be used in
 * a thread-safe manner by the ThreadPool.
 */
class WorkerContext
{
private:
    using WorkerContextBufferProviderPtr = std::shared_ptr<Memory::AbstractBufferProvider>;
    using WorkerContextBufferProvider = WorkerContextBufferProviderPtr::element_type;
    using WorkerContextBufferProviderRawPtr = WorkerContextBufferProviderPtr::element_type*;

    /// the id of this worker context (unique per thread).
    WorkerThreadId workerId;
    /// worker local buffer pool stored in tls
    static folly::ThreadLocalPtr<WorkerContextBufferProvider> localBufferPoolTLS;
    /// worker local buffer pool stored :: use this for fast access
    WorkerContextBufferProviderPtr localBufferPool;

public:
    explicit WorkerContext(WorkerThreadId workerId, Memory::BufferManagerPtr& bufferManager, uint64_t numberOfBuffersPerWorker);

    ~WorkerContext();

    /**
     * @brief Allocates a new tuple buffer.
     * @return TupleBuffer
     */
    Memory::TupleBuffer allocateTupleBuffer();

    /**
     * @brief Returns the thread-local buffer provider singleton.
     * This can be accessed at any point in time also without the pointer to the context.
     * Calling this method from a non worker thread results in undefined behaviour.
     * @return WorkerContextBufferProviderRawPtr
     */
    static WorkerContextBufferProviderRawPtr getBufferProviderTLS();

    /**
     * @brief Returns the thread-local buffer provider
     * @return shared_ptr to LocalBufferPool
     */
    WorkerContextBufferProviderPtr getBufferProvider();

    /**
     * @brief get current worker context thread id. This is assigned by calling NesThread::getId()
     * @return current worker context thread id
     */
    WorkerThreadId getId() const;
};
using WorkerContextPtr = std::shared_ptr<WorkerContext>;
using WorkerContextRef = WorkerContext&;
}
