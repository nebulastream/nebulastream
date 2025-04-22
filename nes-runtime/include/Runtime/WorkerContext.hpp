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

#ifndef NES_RUNTIME_INCLUDE_RUNTIME_WORKERCONTEXT_HPP_
#define NES_RUNTIME_INCLUDE_RUNTIME_WORKERCONTEXT_HPP_

#include <HDFS/HDFSClient.hpp>
#include <Network/NetworkForwardRefs.hpp>
#include <Operators/LogicalOperators/Network/NesPartition.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <cstdint>
#include <folly/ThreadLocal.h>
#include <fstream>
#include <future>
#include <iomanip>
#include <memory>
#include <optional>
#include <queue>
#include <unordered_map>

namespace NES {
class ReconfigurationMarker;
using HashOperatorId = std::pair<OperatorId, DecomposedQueryPlanVersion>;
using ReconfigurationMarkerPtr = std::shared_ptr<ReconfigurationMarker>;
}// namespace NES

namespace NES::Runtime {

struct BufferOrdering : public std::greater<TupleBuffer> {
    bool operator()(const TupleBuffer& lhs, const TupleBuffer& rhs) { return lhs.getWatermark() > rhs.getWatermark(); }
};

class AbstractBufferProvider;
class BufferStorage;
using BufferStoragePtr = std::shared_ptr<Runtime::BufferStorage>;

/**
 * @brief A WorkerContext represents the current state of a worker thread
 * Note that it is not thread-safe per se but it is meant to be used in
 * a thread-safe manner by the ThreadPool.
 */
class WorkerContext {
  private:
    using WorkerContextBufferProviderPtr = LocalBufferPoolPtr;
    using WorkerContextBufferProvider = WorkerContextBufferProviderPtr::element_type;
    using WorkerContextBufferProviderRawPtr = WorkerContextBufferProviderPtr::element_type*;

    /// the id of this worker context (unique per thread).
    WorkerThreadId workerId;
    /// object reference counters
    std::unordered_map<uintptr_t, uint32_t> objectRefCounters;
    /// data channels that send data downstream
    // std::unordered_map<HashOperatorId , Network::NetworkChannelPtr> dataChannels;
    std::unordered_map<OperatorId, Network::NetworkChannelPtr> dataChannels;
    /// data channels that have not established a connection yet
    std::unordered_map<OperatorId, std::pair<std::future<Network::NetworkChannelPtr>, std::promise<bool>>> dataChannelFutures;
    /// event only channels that send events upstream
    std::unordered_map<OperatorId, Network::EventOnlyNetworkChannelPtr> reverseEventChannels;
    /// reverse event channels that have not established a connection yet
    std::unordered_map<OperatorId, std::pair<std::future<Network::EventOnlyNetworkChannelPtr>, std::promise<bool>>>
        reverseEventChannelFutures;
    /// worker local buffer pool stored in tls
    static folly::ThreadLocalPtr<WorkerContextBufferProvider> localBufferPoolTLS;
    /// worker local buffer pool stored :: use this for fast access
    WorkerContextBufferProviderPtr localBufferPool;
    /// numa location of current worker
    uint32_t queueId = 0;
    std::unordered_map<OperatorId, std::queue<NES::Runtime::TupleBuffer>> reconnectBufferStorage;
    /// queue of tuple buffers that were processed by the thread
    std::unordered_map<Network::NesPartition, std::priority_queue<TupleBuffer, std::vector<TupleBuffer>, BufferOrdering>> storage;
    /// Schemas that belong to the tuples in storage
    std::unordered_map<Network::NesPartition, SchemaPtr> schemas;
    /// HDFSClient connection opened in case of fault tolerance is checkpointing
    HDFSClient* hdfsClient = nullptr;
    std::ofstream statisticsFile;
    std::ofstream storageFile;
    uint64_t currentEpoch;

  public:
    explicit WorkerContext(WorkerThreadId workerId,
                           const BufferManagerPtr& bufferManager,
                           uint64_t numberOfBuffersPerWorker,
                           uint32_t queueId = 0);

    ~WorkerContext();

    /**
     * @brief Allocates a new tuple buffer.
     * @return TupleBuffer
     */
    TupleBuffer allocateTupleBuffer();

    /**
     * @brief Returns the thread-local buffer provider singleton.
     * This can be accessed at any point in time also without the pointer to the context.
     * Calling this method from a non worker thread results in undefined behaviour.
     * @return raw pointer to AbstractBufferProvider
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

    /**
     * @brief Sets the ref counter for a generic object using its pointer address as lookup
     * @param object the object that we want to track
     * @param refCnt the initial ref cnt
     */
    void setObjectRefCnt(void* object, uint32_t refCnt);

    /**
     * @brief Reduces by one the ref cnt. It deletes the object as soon as ref cnt reaches 0.
     * @param object the object that we want to ref count
     * @return the prev ref cnt
     */
    uint32_t decreaseObjectRefCnt(void* object);

    /**
     * @brief get the queue id of the the current worker
     * @return current queue id
     */
    uint32_t getQueueId() const;

    /**
     * @brief This stores a network channel for an operator
     * @param id of the operator that we want to store the output channel
     * @param channel the output channel
     */
    void storeNetworkChannel(OperatorId id, DecomposedQueryPlanVersion version, Network::NetworkChannelPtr&& channel);

    bool containsNetworkChannel(OperatorId id);

 void printStatistics(Runtime::TupleBuffer& inputBuffer, std::string str);

    /**
     * @brief This method creates a network storage for a thread
     * @param nesPartitionId partition id
     */
    void createStorage(Network::NesPartition nesPartitionId);

    /**
    * @brief This method saves the schema for the tuples stored in storage of a thread
    * @param nesPartitionId partition id
    */
    void storeSchema(Network::NesPartition nesPartitionId, const SchemaPtr& schema);

    /**
      * @brief This method deletes Checkpoints from HDFS
      * @param nesPartition partition
      * @param timestamp timestamp
      * @return success in the case something was trimmed
      */
    bool hdfsTrimCheckpoint(Network::NesPartition nesPartition, uint64_t timestamp);

    /**
    * @brief This method writes a tuple to the remote storage as a checkpoint
    * @param nesPartitionId partition id
    * @param inputBuffer tuple
    */
    void hdfsCreateCheckpoint(Network::NesPartition nesPartitionId, Runtime::TupleBuffer& inputBuffer);

    /**
       * @brief This method returns the storage as binary to be used as a checkpoint
       * @param nesPartitionId partition id
       * @return storage as binary
       */
    std::vector<char> getBinaryStorage(Network::NesPartition nesPartition);

    /**
    * @brief This method serializes buffers
    * @param buffer buffer to serialize
    * @param schema schema of the tuples in the buffer
    */
    std::vector<char> serializeBuffer(NES::Runtime::TupleBuffer& buffer, const SchemaPtr& schema);

    /**
     * @brief This stores a future for network channel creation and a promise which can be used to abort the creation
     * @param id of the operator that we want to store the output channel
     * @param channelFuture a pair of a future waiting for the output channel to be connected and a promise to be used if the connection
     * process is to be aborted
     */
    void storeNetworkChannelFuture(OperatorId id,
                                   DecomposedQueryPlanVersion version,
                                   std::pair<std::future<Network::NetworkChannelPtr>, std::promise<bool>>&& channelFuture);

    bool containsNetworkChannelFuture(OperatorId id);

    /**
      * @brief This method inserts a tuple buffer into the storage
      * @param nesPartition partition
      * @param TupleBuffer tuple buffer
      */
    void insertIntoStorage(Network::NesPartition nesPartition, NES::Runtime::TupleBuffer buffer);

    /**
      * @brief This method deletes a tuple buffer from the storage
      * @param nesPartition partition
      * @param timestamp timestamp
      */
    void trimStorage(Network::NesPartition nesPartition, uint64_t timestamp);

    /**
     * @brief get the oldest buffered tuple for the specified partition
     * @param nesPartition partition
     * @return an optional containing the tuple or nullopt if the storage is empty
     */
    std::optional<NES::Runtime::TupleBuffer> getTopTupleFromStorage(Network::NesPartition nesPartition);

    /**
     * @brief if the storage is not empty get the oldest buffered tuple for the specified partition without removing it
     * @param nesPartition the target partition of the network channel
     */
    std::optional<NES::Runtime::TupleBuffer> peekBufferFromReconnectBufferStorage(OperatorId operatorId);

    /**
     * @brief if the storage is not empty remove the oldest buffered tuple for the specified partition
     * @param nesPartition partition
     */
    void removeTopTupleFromStorage(Network::NesPartition nesPartition);

    /**
     * @brief removes a registered network channel with a termination type
     * @param id of the operator that we want to store the output channel
     * @param terminationType the termination type
     * @param currentMessageSequenceNumber represents the total number of data buffer messages sent
     * @param shouldPropagateMarker marker should be sent to downstream, when closing channel
     * @param reconfigurationMarker an optional containing the reconfiguration marker if this channel is closed as part of a
     * reconfiguration
     */
    bool releaseNetworkChannel(OperatorId id,
                               DecomposedQueryPlanVersion version,
                               Runtime::QueryTerminationType terminationType,
                               uint16_t sendingThreadCount,
                               uint64_t currentMessageSequenceNumber,
                               bool shouldPropagateMarker,
                               const std::optional<ReconfigurationMarkerPtr>& reconfigurationMarker);

    /**
     * @brief This stores a network channel for an operator
     * @param id of the operator that we want to store the output channel
     * @param channel the output channel
     */
    void storeEventOnlyChannel(OperatorId id, Network::EventOnlyNetworkChannelPtr&& channel);

    /**
     * @brief removes a registered network channel
     * @param id of the operator that we want to store the output channel
     * @param terminationType the termination type
     */
    bool releaseEventOnlyChannel(OperatorId id, Runtime::QueryTerminationType terminationType);

    /**
     * @brief retrieve a registered output channel
     * @param ownerId id of the operator that we want to store the output channel
     * @return an output channel
     */
    Network::NetworkChannel* getNetworkChannel(OperatorId ownerId, DecomposedQueryPlanVersion ownerVersion);

    /**
     * @brief retrieves an asynchronously established output channel.
     * @param operatorId id of the operator which will use the network channel
     * @return an optional containing a network channel ptr:
     * - nullopt if the operation has not yet completed
     * - optional containing nullptr if the conneciton timed out
     * - optional containing valid ptr if connection succeeded
     */
    std::optional<Network::NetworkChannelPtr> getAsyncConnectionResult(OperatorId operatorId, DecomposedQueryPlanVersion version);

    /**
     * @brief blocks until async connection of a network channel has succeeded or timed out
     * @param operatorId id of the operator which will use the network channel
     * @return a pointer to the network channel or nullptr if the connection timed out
     */
    Network::NetworkChannelPtr waitForAsyncConnection(OperatorId operatorId);

    /**
     * @brief check if an async connection that was started by the operator with the specified id is currently in progress
     * @param operatorId id of the operator which will use the network channel
     * @return true if a connection is currently being established
     */
    bool isAsyncConnectionInProgress(OperatorId operatorId);

    /**
     * @brief retrieve a registered output channel
     * @param operatorId id of the operator that we want to store the output channel
     * @return an output channel
     */
    Network::EventOnlyNetworkChannel* getEventOnlyNetworkChannel(OperatorId operatorId);

    /**
     * @brief insert a tuple buffer into the reconnect buffer storage
     * @param operatorId the id of the buffering sink
     * @param buffer the data to be buffered
     */
    void insertIntoReconnectBufferStorage(OperatorId operatorId, NES::Runtime::TupleBuffer buffer);

    /**
     * @brief retrieve and delete a tuple buffer from the tuple buffer storage
     * @param operatorId the id of the buffering sink
     * @return the buffer that was removed from the storage
     */
    std::optional<TupleBuffer> removeBufferFromReconnectBufferStorage(OperatorId operatorId);

    /**
     * @brief stop a connection process which is currently in progress
     * @param operatorId the id of the operator that started the connection process
     */
    void abortConnectionProcess(OperatorId operatorId);

    /**
     * @brief check if a network channel exists for the sink in question
     * @param operatorId
     * @return
     */
    [[maybe_unused]] bool doesNetworkChannelExist(OperatorId operatorId);

    /**
     * @brief store a future for an event channel that is in the process of connecting
     * @param id the id of the operator which the channel belongs to
     * @param channelFuture the future to be stored
     */
    void storeEventChannelFuture(OperatorId id,
                                 DecomposedQueryPlanVersion version,
                                 std::pair<std::future<Network::EventOnlyNetworkChannelPtr>, std::promise<bool>>&& channelFuture);

    /**
     * @brief retrieves an asynchronously established event channel.
     * @param operatorId id of the operator which will use the event channel
     * @return an optional containing a event channel ptr:
     * - nullopt if the operation has not yet completed
     * - optional containing nullptr if the conneciton timed out
     * - optional containing valid ptr if connection succeeded
     */
    std::optional<Network::EventOnlyNetworkChannelPtr> getAsyncEventChannelConnectionResult(OperatorId operatorId);

    /**
     * @brief blocks until async connection of an event channel has succeeded or timed out
     * @param operatorId id of the operator which will use the event channel
     * @return a pointer to the event channel or nullptr if the connection timed out
     */
    Network::EventOnlyNetworkChannelPtr waitForAsyncConnectionEventChannel(OperatorId operatorId);

    /**
     * @brief This method reutrn buffer storage size
     * @return size
     */
    size_t getStorageSize(Network::NesPartition nesPartitionId);

    /**
     * @brief check if a network channel exists for the operator in question
     * @param operatorId the unique identifier of the operator to which the channel belongs
     * @return true if a channel was found
     */
    bool doesEventChannelExist(OperatorId operatorId);
};
using WorkerContextPtr = std::shared_ptr<WorkerContext>;
}// namespace NES::Runtime
#endif// NES_RUNTIME_INCLUDE_RUNTIME_WORKERCONTEXT_HPP_
