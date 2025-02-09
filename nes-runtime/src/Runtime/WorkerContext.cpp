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

void WorkerContext::storeNetworkChannel(NES::OperatorId id, Network::NetworkChannelPtr&& channel, WorkerId receiver) {
    // NES_ERROR("WorkerContext: storing channel for operator {}  for context {}", id, workerId);
    auto it = dataChannels.find(id);// note we assume it's always available

    if (it != dataChannels.end()) {
        if (auto& existingChannel = it->second.first; existingChannel) {
            // NES_ERROR("WorkerContext: storing channel for operator {}  for context {} but channel already exists", id, workerId);
            NES_FATAL_ERROR("Cannot drop channel without closing")
        }
    }
    // NES_ERROR("WorkerContext: no network channel exists, proceed to store {}  for context {}", id, workerId);
    dataChannels[id] = {std::move(channel), receiver.getRawValue()};
    // NES_ERROR("WorkerContext: succesfully stored {}  for context {}", id, workerId);
}

void WorkerContext::storeNetworkChannelFuture(
    NES::OperatorId id,
    std::pair<std::pair<std::future<Network::NetworkChannelPtr>, WorkerId>, std::promise<bool>>&& channelFuture) {
    NES_TRACE("WorkerContext: storing channel future for operator {}  for context {}", id, workerId);
    if (!dataChannelFutures.contains(id)) {
        dataChannelFutures[id] = std::move(channelFuture);
    } else {
        NES_FATAL_ERROR("WorkerContext: storing channel future for operator {}  for context {} but there already is a future", id, workerId);
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
                                          Runtime::QueryTerminationType terminationType,
                                          uint16_t sendingThreadCount,
                                          uint64_t currentMessageSequenceNumber,
                                          bool shouldPropagateMarker,
                                          const std::optional<ReconfigurationMarkerPtr>& reconfigurationMarker) {
    NES_TRACE("WorkerContext: releasing channel for operator {} for context {}", id, workerId);
    if (auto it = dataChannels.find(id); it != dataChannels.end()) {
        if (auto& [channel, receiver] = it->second; channel) {
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

std::pair<Network::NetworkChannel*, WorkerId> WorkerContext::getNetworkChannel(NES::OperatorId ownerId) {
    NES_DEBUG("WorkerContext: retrieving channel for operator {} for context {}", ownerId, workerId);
    auto it = dataChannels.find(ownerId);// note we assume it's always available
    if (it == dataChannels.end()) {
        NES_DEBUG("WorkerContext: no channel for operator {} for context {}, returning nullptr", ownerId, workerId);
        return {nullptr, INVALID_WORKER_NODE_ID};
    }
    NES_DEBUG("WorkerContext: found channel for operator {} for context {}, retrieving data", ownerId, workerId);
//    return {(*it).second.first.get(), WorkerId ((*it).second.second)};
    std::pair p = {(*it).second.first.get(), WorkerId ((*it).second.second)};
    NES_DEBUG("WorkerContext: retrieved data for  channel for operator {} for context {}, returning", ownerId, workerId);
    return p;
}

std::optional<std::pair<Network::NetworkChannelPtr, WorkerId>>
WorkerContext::getAsyncConnectionResult(NES::OperatorId operatorId) {
    NES_TRACE("WorkerContext: retrieving channel for operator {} for context {}", operatorId, workerId);
    auto iteratorOperatorId = dataChannelFutures.find(operatorId);// note we assume it's always available
    if (!dataChannelFutures.contains(operatorId)) {
        return std::nullopt;
    }
    if (!iteratorOperatorId->second.has_value()) {
        return std::nullopt;
    }
    auto& [pairRef, promiseReference] = iteratorOperatorId->second.value();
    auto& [futureReference, receiver] = pairRef;

    if (futureReference.wait_for(std::chrono::seconds(0)) == std::future_status::ready) {
        auto channel = futureReference.get();
        promiseReference.set_value(true);
        dataChannelFutures.erase(iteratorOperatorId);
        return {std::make_pair(std::move(channel), receiver)};
    }
    //if the operation has not completed yet, return a nullopt
    return std::nullopt;
}
void WorkerContext::dropReconnectBufferStorage(OperatorId operatorId) {
    reconnectBufferStorage.erase(operatorId);
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

bool WorkerContext::doNotTryConnectingDataChannel(OperatorId operatorId) {
    if (dataChannelFutures.contains(operatorId)) {
        NES_DEBUG("Cannot set the data channel for operator {}, to buffer without trying to connect, because a connection "
                  "attempt is still ongoing",
                  operatorId);
        return false;
    }
    dataChannelFutures[operatorId] = std::nullopt;
    return true;
}

Network::EventOnlyNetworkChannel* WorkerContext::getEventOnlyNetworkChannel(NES::OperatorId operatorId) {
    NES_TRACE("WorkerContext: retrieving event only channel for operator {} for context {}", operatorId, workerId);
    auto iteratorOperatorId = reverseEventChannels.find(operatorId);// note we assume it's always available
    return (*iteratorOperatorId).second.get();
}

LocalBufferPool* WorkerContext::getBufferProviderTLS() { return localBufferPoolTLS.get(); }

LocalBufferPoolPtr WorkerContext::getBufferProvider() { return localBufferPool; }

std::pair<Network::NetworkChannelPtr, WorkerId> WorkerContext::waitForAsyncConnection(NES::OperatorId operatorId,
                                                                                      uint64_t retries) {
    auto iteratorOperatorId = dataChannelFutures.find(operatorId);// note we assume it's always available
    if (iteratorOperatorId == dataChannelFutures.end()) {
        return {nullptr, INVALID_WORKER_NODE_ID};
    }
    if (!iteratorOperatorId->second.has_value()) {
        dataChannelFutures.erase(iteratorOperatorId);
        return {nullptr, INVALID_WORKER_NODE_ID};
    }
    auto& [pairRef, promiseReference] = iteratorOperatorId->second.value();
    auto& [futureReference, receiver] = pairRef;
    Network::NetworkChannelPtr channel = nullptr;
    for (uint64_t i = 0; i < retries; ++i) {
        if (futureReference.wait_for(std::chrono::seconds(0)) == std::future_status::ready) {
            channel = futureReference.get();
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    //blocking wait on get
    //auto channel = futureReference.get();
    promiseReference.set_value(true);
    dataChannelFutures.erase(iteratorOperatorId);
    return {std::move(channel), receiver};
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

//todo: make sure that this propagates the marker
void WorkerContext::abortConnectionProcess(OperatorId operatorId) {
    auto iteratorOperatorId = dataChannelFutures.find(operatorId);// note we assume it's always available
    if (!iteratorOperatorId->second.has_value()) {
        dataChannelFutures.erase(iteratorOperatorId);
        return;
    }
    auto& [pairRef, promise] = iteratorOperatorId->second.value();
    auto& [future, receiver] = pairRef;
    // auto& [future, promise] = iteratorOperatorId->second.value();
    //signal connection process to stop
    promise.set_value(true);
    //wait for the future to be set so we can make sure that channel is closed in case it has already been created
    auto channel = future.get();
    if (channel) {
        uint16_t numSendingThreads = 0;
        uint16_t currentMessageSequenceNumber = 0;
        channel->close(QueryTerminationType::Failure, numSendingThreads, currentMessageSequenceNumber);
    }
    dataChannelFutures.erase(iteratorOperatorId);
}

bool WorkerContext::doesNetworkChannelExist(OperatorId operatorId) { return dataChannels.contains(operatorId); }

bool WorkerContext::doesEventChannelExist(OperatorId operatorId) { return reverseEventChannels.contains(operatorId); }

void WorkerContext::increaseReconnectCount(OperatorId operatorId, WorkerId newWorker) {
    NES_ASSERT(operatorId != INVALID_OPERATOR_ID, "Invalid operator id supplied when increasing reconnect count");
    if (reconnectCounts.contains(operatorId)) {
        NES_ERROR("WorkerContext: increasing existing reconnect count for operator {} for context {}", operatorId, workerId);
        NES_ASSERT(reconnectCounts.size() == 1, "More than one reconnect count found");
//        reconnectCounts[operatorId] = reconnectCounts[operatorId] + 1;
        auto& [count, id] = reconnectCounts.at(operatorId);
        if (id != newWorker) {
            count += 1;
            id = newWorker;
        }
    } else {
        NES_ERROR("WorkerContext: creating new reconnect count for operator {} for context {}", operatorId, workerId);
        reconnectCounts.insert({operatorId, {1, newWorker}});
    }
}

uint64_t WorkerContext::getReconnectCount(OperatorId operatorId) {
    NES_ASSERT(operatorId != INVALID_OPERATOR_ID, "Invalid operator id supplied when accessing reconnect count");
//    NES_ERROR("WorkerContext: getting reconnect count for operator {} for context {}", operatorId, workerId);
    if (reconnectCounts.contains(operatorId)) {
        return reconnectCounts.at(operatorId).first;
    }
    return 0;
}
}// namespace NES::Runtime
