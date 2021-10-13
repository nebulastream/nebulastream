/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INCLUDE_RUNTIME_WORKER_CONTEXT_HPP_
#define NES_INCLUDE_RUNTIME_WORKER_CONTEXT_HPP_

#include <Network/NesPartition.hpp>
#include <Network/NetworkChannel.hpp>
#include <Runtime/NesThread.hpp>
#include <Runtime/NodeEngineForwaredRefs.hpp>
#include <cstdint>
#include <memory>
#include <unordered_map>

namespace NES::Runtime {

/**
 * @brief A WorkerContext represents the current state of a worker thread
 * Note that it is not thread-safe per se but it is meant to be used in
 * a thread-safe manner by the ThreadPool.
 */
class WorkerContext {
  private:
    /// the id of this worker context (unique per thread).
    uint32_t workerId;
    /// object reference counters
    std::unordered_map<uintptr_t, uint32_t> objectRefCounters;
    /// data channels that send data downstream
    std::unordered_map<Network::OperatorId, Network::NetworkChannelPtr> dataChannels;
    /// event only channeks that send events upstream
    std::unordered_map<Network::OperatorId, Network::EventOnlyNetworkChannelPtr> reverseEventChannels;
    /// worker local buffer pool
    LocalBufferPoolPtr localBufferPool;
    /// numa location of current worker
    uint32_t numaNode = 0;

  public:
    explicit WorkerContext(uint32_t workerId,
                           const BufferManagerPtr& bufferManager,
                           uint64_t numberOfBuffersPerWorker,
                           uint32_t numaNode = 0);

    /**
     * @brief Allocates a new tuple buffer.
     * @return TupleBuffer
     */
    TupleBuffer allocateTupleBuffer();

    /**
     * @brief get current worker context thread id. This is assigned by calling NesThread::getId()
     * @return current worker context thread id
     */
    uint32_t getId() const;

    /**
     * @brief
     * @param object
     * @param refCnt
     */
    void setObjectRefCnt(void* object, uint32_t refCnt);

    /**
     * @brief
     * @param object
     * @return
     */
    uint32_t increaseObjectRefCnt(void* object);

    /**
     * @brief
     * @param object
     * @return
     */
    uint32_t decreaseObjectRefCnt(void* object);


    /**
     * @brief get the numa node of the current worker
     * @return current numa Node
     */
    uint32_t getNumaNode() const;

    /**
     * @brief This stores a network channel for an operator
     * @param id of the operator that we want to store the output channel
     * @param channel the output channel
     */
    void storeNetworkChannel(Network::OperatorId id, Network::NetworkChannelPtr&& channel);

    /**
     * @brief removes a registered network channel
     * @param id of the operator that we want to store the output channel
     */
    bool releaseNetworkChannel(Network::OperatorId id);

    /**
     * @brief This stores a network channel for an operator
     * @param id of the operator that we want to store the output channel
     * @param channel the output channel
     */
    void storeEventOnlyChannel(Network::OperatorId id, Network::EventOnlyNetworkChannelPtr&& channel);

    /**
     * @brief removes a registered network channel
     * @param id of the operator that we want to store the output channel
     */
    bool releaseEventOnlyChannel(Network::OperatorId id);

    /**
     * @brief retrieve a registered output channel
     * @param ownerId id of the operator that we want to store the output channel
     * @return an output channel
     */
    Network::NetworkChannel* getNetworkChannel(Network::OperatorId ownerId);

    /**
     * @brief retrieve a registered output channel
     * @param ownerId id of the operator that we want to store the output channel
     * @return an output channel
     */
    Network::EventOnlyNetworkChannel* getEventOnlyNetworkChannel(Network::OperatorId ownerId);
};
}// namespace NES::Runtime
#endif// NES_INCLUDE_RUNTIME_WORKER_CONTEXT_HPP_
