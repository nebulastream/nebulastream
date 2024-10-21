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

WorkerContext::WorkerContext(WorkerThreadId workerId,
                             const BufferManagerPtr& bufferManager,
                             uint64_t numberOfBuffersPerWorker,
                             uint32_t queueId)
    : workerId(workerId), queueId(queueId) {
    //we changed from a local pool to a fixed sized pool as it allows us to manage the numbers that are hold in the cache via the parameter
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

WorkerThreadId WorkerContext::getId() const { return workerId; }

uint32_t WorkerContext::getQueueId() const { return queueId; }

void WorkerContext::setObjectRefCnt(void* object, uint32_t refCnt) {
    objectRefCounters[reinterpret_cast<uintptr_t>(object)] = refCnt;
}

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

void WorkerContext::storeNetworkChannel(OperatorId id, DecomposedQueryPlanVersion, Network::NetworkChannelPtr&& channel) {
    NES_TRACE("WorkerContext: storing channel for operator {}  for context {}", id, workerId);
    dataChannels[id] = std::move(channel);
}

void WorkerContext::storeNetworkChannelFuture(
    OperatorId id,
    DecomposedQueryPlanVersion,
    std::pair<std::future<Network::NetworkChannelPtr>, std::promise<bool>>&& channelFuture) {
    NES_TRACE("WorkerContext: storing channel future for operator {}  for context {}", id, workerId);
    if (!dataChannelFutures.contains(id)) {
        dataChannelFutures[id] = std::move(channelFuture);
    }
}

bool WorkerContext::containsNetworkChannelFuture(OperatorId id) { return dataChannelFutures.contains(id); }

void WorkerContext::storeEventChannelFuture(
    OperatorId id,
    DecomposedQueryPlanVersion,
    std::pair<std::future<Network::EventOnlyNetworkChannelPtr>, std::promise<bool>>&& channelFuture) {
    NES_TRACE("WorkerContext: storing channel future for operator {}  for context {}", id, workerId);
    if (!reverseEventChannelFutures.contains(id)) {
        reverseEventChannelFutures[id] = std::move(channelFuture);
    }
}

void WorkerContext::createStorage(Network::NesPartition nesPartition) {
    if (!this->storage.contains(nesPartition)) {
        this->storage[nesPartition] = std::make_shared<BufferStorage>();
    }
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
    auto bufferCopy = localBufferPool->getUnpooledBuffer(buffer.getBufferSize()).value();
    std::memcpy(bufferCopy.getBuffer(), buffer.getBuffer(), buffer.getBufferSize());
    bufferCopy.setNumberOfTuples(buffer.getNumberOfTuples());
    bufferCopy.setOriginId(buffer.getOriginId());
    bufferCopy.setWatermark(buffer.getWatermark());
    bufferCopy.setCreationTimestampInMS(buffer.getCreationTimestampInMS());
    bufferCopy.setSequenceNumber(buffer.getSequenceNumber());
    reconnectBufferStorage[operatorId].push(std::move(bufferCopy));
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

std::optional<NES::Runtime::TupleBuffer> WorkerContext::peekBufferFromReconnectBufferStorage(OperatorId operatorId) {
    auto iteratorAtOperatorId = reconnectBufferStorage.find(operatorId);
    if (iteratorAtOperatorId != reconnectBufferStorage.end() && !iteratorAtOperatorId->second.empty()) {
        auto buffer = iteratorAtOperatorId->second.front();
        return buffer;
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

bool WorkerContext::releaseNetworkChannel(OperatorId id,
                                          DecomposedQueryPlanVersion,
                                          Runtime::QueryTerminationType terminationType,
                                          uint16_t sendingThreadCount,
                                          uint64_t currentMessageSequenceNumber,
                                          bool shouldPropagateMarker,
                                          const std::optional<ReconfigurationMarkerPtr>& reconfigurationMarker) {
    NES_TRACE("WorkerContext: releasing channel for operator {} for context {}", id, workerId);
    if (auto it = dataChannels.find(id); it != dataChannels.end()) {
        if (auto& channel = it->second; channel) {
            channel->close(terminationType,
                           sendingThreadCount,
                           currentMessageSequenceNumber,
                           shouldPropagateMarker,
                           reconfigurationMarker);
        }
        dataChannels.erase(it);
        return true;
    }
    return false;
}

void WorkerContext::storeEventOnlyChannel(OperatorId id, Network::EventOnlyNetworkChannelPtr&& channel) {
    NES_TRACE("WorkerContext: storing channel for operator {}  for context {}", id, workerId);
    reverseEventChannels[id] = std::move(channel);
}

bool WorkerContext::releaseEventOnlyChannel(OperatorId id, Runtime::QueryTerminationType terminationType) {
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

Network::NetworkChannel* WorkerContext::getNetworkChannel(OperatorId ownerId, DecomposedQueryPlanVersion) {
    NES_TRACE("WorkerContext: retrieving channel for operator {} for context {}", ownerId, workerId);
    auto it = dataChannels.find(ownerId);// note we assume it's always available
    return (*it).second.get();
}

std::optional<Network::NetworkChannelPtr> WorkerContext::getAsyncConnectionResult(OperatorId operatorId,
                                                                                  DecomposedQueryPlanVersion) {
    NES_TRACE("WorkerContext: retrieving channel for operator {} for context {}", operatorId, workerId);
    auto iteratorOperatorId = dataChannelFutures.find(operatorId);// note we assume it's always available
    auto& [futureReference, promiseReference] = iteratorOperatorId->second;
    if (futureReference.wait_for(std::chrono::seconds(0)) == std::future_status::ready) {
        auto channel = iteratorOperatorId->second.first.get();
        iteratorOperatorId->second.second.set_value(true);
        dataChannelFutures.erase(iteratorOperatorId);
        return channel;
    }
    //if the operation has not completed yet, return a nullopt
    return std::nullopt;
}

std::optional<Network::EventOnlyNetworkChannelPtr> WorkerContext::getAsyncEventChannelConnectionResult(OperatorId operatorId) {
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

Network::EventOnlyNetworkChannel* WorkerContext::getEventOnlyNetworkChannel(OperatorId operatorId) {
    NES_TRACE("WorkerContext: retrieving event only channel for operator {} for context {}", operatorId, workerId);
    auto iteratorOperatorId = reverseEventChannels.find(operatorId);// note we assume it's always available
    return (*iteratorOperatorId).second.get();
}

LocalBufferPool* WorkerContext::getBufferProviderTLS() { return localBufferPoolTLS.get(); }

LocalBufferPoolPtr WorkerContext::getBufferProvider() { return localBufferPool; }

Network::NetworkChannelPtr WorkerContext::waitForAsyncConnection(OperatorId operatorId) {
    auto iteratorOperatorId = dataChannelFutures.find(operatorId);// note we assume it's always available
    //blocking wait on get
    auto channel = iteratorOperatorId->second.first.get();
    iteratorOperatorId->second.second.set_value(true);
    dataChannelFutures.erase(iteratorOperatorId);
    return channel;
}

Network::EventOnlyNetworkChannelPtr WorkerContext::waitForAsyncConnectionEventChannel(OperatorId operatorId) {
    auto iteratorOperatorId = reverseEventChannelFutures.find(operatorId);
    if (iteratorOperatorId == reverseEventChannelFutures.end()) {
        NES_WARNING("Did not find a reverse event channel future; operatorId = {}", operatorId);
        return nullptr;
    }
    //blocking wait on get
    auto channel = iteratorOperatorId->second.first.get();
    iteratorOperatorId->second.second.set_value(true);
    reverseEventChannelFutures.erase(iteratorOperatorId);
    return channel;
}

void WorkerContext::abortConnectionProcess(OperatorId operatorId) {
    auto iteratorOperatorId = dataChannelFutures.find(operatorId);// note we assume it's always available
    auto& promise = iteratorOperatorId->second.second;
    //signal connection process to stop
    promise.set_value(true);
    //wait for the future to be set, so we can make sure that channel is closed in case it has already been created
    auto& future = iteratorOperatorId->second.first;
    auto channel = future.get();
    if (channel) {
        channel->close(QueryTerminationType::Failure);
    }
    dataChannelFutures.erase(iteratorOperatorId);
}

bool WorkerContext::doesNetworkChannelExist(OperatorId operatorId) { return dataChannels.contains(operatorId); }

bool WorkerContext::doesEventChannelExist(OperatorId operatorId) { return reverseEventChannels.contains(operatorId); }
}// namespace NES::Runtime
