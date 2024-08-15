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

#include <Runtime/BufferManager.hpp>
#include <Runtime/BufferStorage.hpp>
#include <Runtime/WorkerContext.hpp>

namespace NES::Runtime
{

folly::ThreadLocalPtr<AbstractBufferProvider> WorkerContext::localBufferPoolTLS{};

WorkerContext::WorkerContext(WorkerThreadId workerId, BufferManager& bufferManager, uint64_t numberOfBuffersPerWorker, uint32_t queueId)
    : workerId(workerId), queueId(queueId)
{
    ///we changed from a local pool to a fixed sized pool as it allows us to manage the numbers that are hold in the cache via the parameter
    localBufferPool = bufferManager.createLocalBufferPool(numberOfBuffersPerWorker);
    localBufferPoolTLS.reset(
        localBufferPool.get(),
        [](auto*, folly::TLPDestructionMode)
        {
            /// nop
        });
    NES_ASSERT(!!localBufferPool, "Local buffer is not allowed to be null");
    NES_ASSERT(!!localBufferPoolTLS, "Local buffer is not allowed to be null");
}

WorkerContext::~WorkerContext()
{
    localBufferPool->destroy();
    localBufferPoolTLS.reset(nullptr);
}

WorkerThreadId WorkerContext::getId() const
{
    return workerId;
}

uint32_t WorkerContext::getQueueId() const
{
    return queueId;
}

void WorkerContext::setObjectRefCnt(void* object, uint32_t refCnt)
{
    objectRefCounters[reinterpret_cast<uintptr_t>(object)] = refCnt;
}

uint32_t WorkerContext::decreaseObjectRefCnt(void* object)
{
    auto ptr = reinterpret_cast<uintptr_t>(object);
    if (auto it = objectRefCounters.find(ptr); it != objectRefCounters.end())
    {
        auto val = it->second--;
        if (val == 1)
        {
            objectRefCounters.erase(it);
        }
        return val;
    }
    return 0;
}

TupleBuffer WorkerContext::allocateTupleBuffer()
{
    return localBufferPool->getBufferBlocking();
}


AbstractBufferProvider* WorkerContext::getBufferProviderTLS()
{
    return localBufferPoolTLS.get();
}

std::shared_ptr<AbstractBufferProvider> WorkerContext::getBufferProvider()
{
    return localBufferPool;
}

} /// namespace NES::Runtime
