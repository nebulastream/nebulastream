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

#include <Network/NetworkChannel.hpp>
#include <Runtime/BufferStorage.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/LocalBufferPool.hpp>
#include <Runtime/WorkerContext.hpp>

namespace NES::Runtime {

folly::ThreadLocalPtr<LocalBufferPool> WorkerContext::localBufferPoolTLS{};

WorkerContext::WorkerContext(uint32_t workerId,
                             const BufferManagerPtr& bufferManager,
                             uint64_t numberOfBuffersPerWorker,
                             uint32_t queueId)
    : workerId(workerId), queueId(queueId) {
    //we changed from a local pool to a fixed sized pool as it allows us to manage the numbers that are hold in the cache via the paramter
    localBufferPool = bufferManager->createLocalBufferPool(numberOfBuffersPerWorker);
    localBufferPoolTLS.reset(localBufferPool.get(), [](auto*, folly::TLPDestructionMode) {
        // nop
    });
    NES_ASSERT(!!localBufferPool, "Local buffer is not allowed to be null");
    NES_ASSERT(!!localBufferPoolTLS, "Local buffer is not allowed to be null");
}

WorkerContext::~WorkerContext() {
    localBufferPool->destroy();
    localBufferPoolTLS.reset(nullptr);
}

uint32_t WorkerContext::getId() const { return workerId; }

uint32_t WorkerContext::getQueueId() const { return queueId; }

void WorkerContext::setObjectRefCnt(void* object, uint32_t refCnt) {
    objectRefCounters[reinterpret_cast<uintptr_t>(object)] = refCnt;
}

uint32_t WorkerContext::increaseObjectRefCnt(void* object) { return objectRefCounters[reinterpret_cast<uintptr_t>(object)]++; }

uint32_t WorkerContext::decreaseObjectRefCnt(void* object) {
    auto ptr = reinterpret_cast<uintptr_t>(object);
    if (auto it = objectRefCounters.find(ptr); it != objectRefCounters.end()) {
        auto val = it->second--;
        if (val == 1) {
            objectRefCounters.erase(it);
        }
        return val;
    }
    return 0;
}

TupleBuffer WorkerContext::allocateTupleBuffer() { return localBufferPool->getBufferBlocking(); }

void WorkerContext::storeNetworkChannel(NES::OperatorId id, Network::NetworkChannelPtr&& channel) {
    NES_TRACE("WorkerContext: storing channel for operator {}  for context {}", id, workerId);
    dataChannels[id] = std::move(channel);
}

void WorkerContext::storeNetworkChannelFuture(NES::OperatorId id,
    std::pair<std::future<Network::NetworkChannelPtr>, std::promise<bool>>&& channelFuture) {
    NES_TRACE("WorkerContext: storing channel future for operator {}  for context {}", id, workerId);
    dataChannelFutures[id] = std::move(channelFuture);
}

void WorkerContext::createStorage(Network::NesPartition nesPartition) {
    this->storage[nesPartition] = std::make_shared<BufferStorage>();
}

void WorkerContext::insertIntoStorage(Network::NesPartition nesPartition, NES::Runtime::TupleBuffer buffer) {
    auto iteratorPartitionId = this->storage.find(nesPartition);
    if (iteratorPartitionId != this->storage.end()) {
        this->storage[nesPartition]->insertBuffer(buffer);
    } else {
        NES_WARNING("No buffer storage found for partition {}, buffer was dropped", nesPartition);
    }
}

void WorkerContext::insertIntoReconnectBufferStorage(uint64_t sinkId, NES::Runtime::TupleBuffer buffer) {
    reconnectBufferStorage[sinkId].push(std::move(buffer));
}

void WorkerContext::trimStorage(Network::NesPartition nesPartition, uint64_t timestamp) {
    auto iteratorPartitionId = this->storage.find(nesPartition);
    if (iteratorPartitionId != this->storage.end()) {
        this->storage[nesPartition]->trimBuffer(timestamp);
    }
}

std::optional<NES::Runtime::TupleBuffer> WorkerContext::getTopTupleFromStorage(Network::NesPartition nesPartition) {
    auto iteratorPartitionId = this->storage.find(nesPartition);
    if (iteratorPartitionId != this->storage.end()) {
        return this->storage[nesPartition]->getTopElementFromQueue();
    }
    return {};
}

std::optional<NES::Runtime::TupleBuffer> WorkerContext::removeBufferFromReconnectBufferStorage(uint64_t sinkId) {
    auto it = reconnectBufferStorage.find(sinkId);
    if (it != reconnectBufferStorage.end() && !it->second.empty()) {
        auto buffer = it->second.front();
        it->second.pop();
        if (it->second.empty()) {
            reconnectBufferStorage.erase(it);
        }
        return buffer;
    }
    return {};
}

void WorkerContext::removeTopTupleFromStorage(Network::NesPartition nesPartition) {
    auto iteratorPartitionId = this->storage.find(nesPartition);
    if (iteratorPartitionId != this->storage.end()) {
        this->storage[nesPartition]->removeTopElementFromQueue();
    }
}

bool WorkerContext::releaseNetworkChannel(NES::OperatorId id, Runtime::QueryTerminationType terminationType, uint16_t sendingThreadCount) {
    NES_TRACE("WorkerContext: releasing channel for operator {} for context {}", id, workerId);
    if (auto it = dataChannels.find(id); it != dataChannels.end()) {
        if (auto& channel = it->second; channel) {
            channel->close(terminationType, sendingThreadCount);
        }
        dataChannels.erase(it);
        return true;
    }
    return false;
}

void WorkerContext::storeEventOnlyChannel(NES::OperatorId id, Network::EventOnlyNetworkChannelPtr&& channel) {
    NES_TRACE("WorkerContext: storing channel for operator {}  for context {}", id, workerId);
    reverseEventChannels[id] = std::move(channel);
}

bool WorkerContext::releaseEventOnlyChannel(NES::OperatorId id, Runtime::QueryTerminationType terminationType) {
    NES_TRACE("WorkerContext: releasing channel for operator {} for context {}", id, workerId);
    if (auto it = reverseEventChannels.find(id); it != reverseEventChannels.end()) {
        if (auto& channel = it->second; channel) {
            channel->close(terminationType);
        }
        reverseEventChannels.erase(it);
        return true;
    }
    return false;
}

Network::NetworkChannel* WorkerContext::getNetworkChannel(NES::OperatorId ownerId) {
    NES_TRACE("WorkerContext: retrieving channel for operator {} for context {}", ownerId, workerId);
    auto it = dataChannels.find(ownerId);// note we assume it's always available
    return (*it).second.get();
}

std::optional<Network::NetworkChannelPtr> WorkerContext::getAsyncConnectionResult(NES::OperatorId ownerId) {
    NES_TRACE("WorkerContext: retrieving channel for operator {} for context {}", ownerId, workerId);
    auto it = dataChannelFutures.find(ownerId);// note we assume it's always available
    auto& [futureReference, promiseReference] = it->second;
    if (futureReference.wait_for(std::chrono::seconds(0)) == std::future_status::ready) {
        auto channel = it->second.first.get();
        it->second.second.set_value(true);
        dataChannelFutures.erase(it);
        return channel;
    }
    //if the operation has not completed yet, return a nullopt
    return std::nullopt;
}

bool WorkerContext::isAsyncConnectionInProgress(OperatorId ownerId) {
    NES_TRACE("WorkerContext: checking existence of channel future for operator {} for context {}", ownerId, workerId);
    return dataChannelFutures.contains(ownerId);
}

Network::EventOnlyNetworkChannel* WorkerContext::getEventOnlyNetworkChannel(NES::OperatorId ownerId) {
    NES_TRACE("WorkerContext: retrieving event only channel for operator {} for context {}", ownerId, workerId);
    auto it = reverseEventChannels.find(ownerId);// note we assume it's always available
    return (*it).second.get();
}

LocalBufferPool* WorkerContext::getBufferProviderTLS() { return localBufferPoolTLS.get(); }

LocalBufferPoolPtr WorkerContext::getBufferProvider() { return localBufferPool; }

Network::NetworkChannelPtr WorkerContext::waitForAsyncConnection(NES::OperatorId ownerId) {
    auto it = dataChannelFutures.find(ownerId);// note we assume it's always available
    //blocking wait on get
    auto channel = it->second.first.get();
    it->second.second.set_value(true);
    dataChannelFutures.erase(it);
    return channel;
}

void WorkerContext::abortConnectionProcess(NES::OperatorId ownerId) {
    auto it = dataChannelFutures.find(ownerId);// note we assume it's always available
    auto& promise = it->second.second;
    //signal connection process to stop
    promise.set_value(true);
    //wait for the future to be set so we can make sure that channel is closed in case it has already been created
    auto& future = it->second.first;
    auto channel = future.get();
    if (channel) {
        channel->close(QueryTerminationType::Failure);
    }
    dataChannelFutures.erase(it);
}

bool WorkerContext::doesNetworkChannelExist(uint64_t sinkId) {
    return dataChannels.contains(sinkId);
}

void WorkerContext::storeNesPartition(uint64_t ownerId, Network::NesPartition partition) {
    nesPartitions.insert({ownerId, partition});
}

Network::NesPartition WorkerContext::getNesPartition(uint64_t ownerId) {
    return nesPartitions.at(ownerId);
}
}// namespace NES::Runtime