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
#include <absl/types/optional.h>

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

void WorkerContext::storeNetworkChannelFuture(
    NES::OperatorId id,
    std::pair<std::future<Network::NetworkChannelPtr>, std::promise<bool>>&& channelFuture) {
    NES_TRACE("WorkerContext: storing channel future for operator {}  for context {}", id, workerId);
    dataChannelFutures[id] = std::move(channelFuture);
}

void WorkerContext::storeEventChannelFuture(
    OperatorId id,
    std::pair<std::future<Network::EventOnlyNetworkChannelPtr>, std::promise<bool>>&& channelFuture) {
    NES_TRACE("WorkerContext: storing channel future for operator {}  for context {}", id, workerId);
    reverseEventChannelFutures[id] = std::move(channelFuture);
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

void WorkerContext::insertIntoReconnectBufferStorage(OperatorId operatorId, NES::Runtime::TupleBuffer buffer) {
    reconnectBufferStorage[operatorId].push(std::move(buffer));
}

bool WorkerContext::trimStorage(Network::NesPartition nesPartition, uint64_t timestamp) {
    auto iteratorPartitionId = this->storage.find(nesPartition);
    if (iteratorPartitionId != this->storage.end()) {
        this->storage[nesPartition]->trimBuffer(timestamp);
        return true;
    }
    return false;
}

std::optional<NES::Runtime::TupleBuffer> WorkerContext::getTopTupleFromStorage(Network::NesPartition nesPartition) {
    auto iteratorPartitionId = this->storage.find(nesPartition);
    if (iteratorPartitionId != this->storage.end()) {
        return this->storage[nesPartition]->getTopElementFromQueue();
    }
    return {};
}

std::optional<NES::Runtime::TupleBuffer> WorkerContext::removeBufferFromReconnectBufferStorage(OperatorId operatorId) {
    auto iteratorAtOperatorId = reconnectBufferStorage.find(operatorId);
    if (iteratorAtOperatorId != reconnectBufferStorage.end() && !iteratorAtOperatorId->second.empty()) {
        auto buffer = iteratorAtOperatorId->second.front();
        iteratorAtOperatorId->second.pop();
        if (iteratorAtOperatorId->second.empty()) {
            reconnectBufferStorage.erase(iteratorAtOperatorId);
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

bool WorkerContext::releaseNetworkChannel(NES::OperatorId id,
                                          Runtime::QueryTerminationType terminationType,
                                          uint16_t sendingThreadCount,
                                          uint64_t currentMessageSequenceNumber) {
    NES_TRACE("WorkerContext: releasing channel for operator {} for context {}", id, workerId);
    if (auto it = dataChannels.find(id); it != dataChannels.end()) {
        if (auto& channel = it->second; channel) {
            channel->close(terminationType, sendingThreadCount, currentMessageSequenceNumber);
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

std::optional<Network::NetworkChannelPtr> WorkerContext::getAsyncConnectionResult(NES::OperatorId operatorId) {
    NES_TRACE("WorkerContext: retrieving channel for operator {} for context {}", operatorId, workerId);
    auto iteratorOperatorId = dataChannelFutures.find(operatorId);// note we assume it's always available
    if (!iteratorOperatorId->second.has_value()) {
        return std::nullopt;
    }
    auto& [futureReference, promiseReference] = iteratorOperatorId->second.value();
    if (futureReference.wait_for(std::chrono::seconds(0)) == std::future_status::ready) {
        auto channel = futureReference.get();
        promiseReference.set_value(true);
        dataChannelFutures.erase(iteratorOperatorId);
        return channel;
    }
    //if the operation has not completed yet, return a nullopt
    return std::nullopt;
}

std::optional<Network::EventOnlyNetworkChannelPtr>
WorkerContext::getAsyncEventChannelConnectionResult(NES::OperatorId operatorId) {
    NES_TRACE("WorkerContext: retrieving channel for operator {} for context {}", operatorId, workerId);
    auto iteratorOperatorId = reverseEventChannelFutures.find(operatorId);// note we assume it's always available
    auto& [futureReference, promiseReference] = iteratorOperatorId->second;
    if (futureReference.wait_for(std::chrono::seconds(0)) == std::future_status::ready) {
        auto channel = iteratorOperatorId->second.first.get();
        iteratorOperatorId->second.second.set_value(true);
        reverseEventChannelFutures.erase(iteratorOperatorId);
        return channel;
    }
    //if the operation has not completed yet, return a nullopt
    return std::nullopt;
}

bool WorkerContext::isAsyncConnectionInProgress(OperatorId operatorId) {
    NES_TRACE("WorkerContext: checking existence of channel future for operator {} for context {}", operatorId, workerId);
    return dataChannelFutures.contains(operatorId);
}

bool WorkerContext::doNotTryConnectingDataChannel(OperatorId operatorId) {
    if (dataChannelFutures.contains(operatorId)) {
        NES_DEBUG("Cannot set the data channel for operator {}, to buffer without trying to connect, because a connection "
                  "attempt is still ongoing", operatorId);
        return false;
    }
    dataChannelFutures.insert({operatorId, std::nullopt});
    return true;
}

Network::EventOnlyNetworkChannel* WorkerContext::getEventOnlyNetworkChannel(NES::OperatorId operatorId) {
    NES_TRACE("WorkerContext: retrieving event only channel for operator {} for context {}", operatorId, workerId);
    auto iteratorOperatorId = reverseEventChannels.find(operatorId);// note we assume it's always available
    return (*iteratorOperatorId).second.get();
}

LocalBufferPool* WorkerContext::getBufferProviderTLS() { return localBufferPoolTLS.get(); }

LocalBufferPoolPtr WorkerContext::getBufferProvider() { return localBufferPool; }

Network::NetworkChannelPtr WorkerContext::waitForAsyncConnection(NES::OperatorId operatorId) {
    auto iteratorOperatorId = dataChannelFutures.find(operatorId);// note we assume it's always available
    if (!iteratorOperatorId->second.has_value()) {
        dataChannelFutures.erase(iteratorOperatorId);
        return nullptr;
    }
    auto& [futureReference, promiseReference] = iteratorOperatorId->second.value();
    //blocking wait on get
    auto channel = futureReference.get();
    promiseReference.set_value(true);
    dataChannelFutures.erase(iteratorOperatorId);
    return channel;
}

Network::EventOnlyNetworkChannelPtr WorkerContext::waitForAsyncConnectionEventChannel(NES::OperatorId operatorId) {
    auto iteratorOperatorId = reverseEventChannelFutures.find(operatorId);// note we assume it's always available
    //blocking wait on get
    auto channel = iteratorOperatorId->second.first.get();
    iteratorOperatorId->second.second.set_value(true);
    reverseEventChannelFutures.erase(iteratorOperatorId);
    return channel;
}

void WorkerContext::abortConnectionProcess(NES::OperatorId operatorId) {
    auto iteratorOperatorId = dataChannelFutures.find(operatorId);// note we assume it's always available
    if (!iteratorOperatorId->second.has_value()) {
        dataChannelFutures.erase(iteratorOperatorId);
        return;
    }
    auto& [future, promise] = iteratorOperatorId->second.value();
    //signal connection process to stop
    promise.set_value(true);
    //wait for the future to be set so we can make sure that channel is closed in case it has already been created
    auto channel = future.get();
    if (channel) {
        channel->close(QueryTerminationType::Failure);
    }
    dataChannelFutures.erase(iteratorOperatorId);
}

bool WorkerContext::doesNetworkChannelExist(OperatorId operatorId) { return dataChannels.contains(operatorId); }

bool WorkerContext::doesEventChannelExist(OperatorId operatorId) { return reverseEventChannels.contains(operatorId); }
}// namespace NES::Runtime