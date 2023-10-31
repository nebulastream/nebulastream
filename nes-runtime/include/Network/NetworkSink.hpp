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

#ifndef NES_CORE_INCLUDE_NETWORK_NETWORKSINK_HPP_
#define NES_CORE_INCLUDE_NETWORK_NETWORKSINK_HPP_

#include <Network/NetworkForwardRefs.hpp>
#include <Operators/LogicalOperators/Network/NodeLocation.hpp>
#include <Runtime/EpochMessage.hpp>
#include <Runtime/RuntimeEventListener.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Util/FaultToleranceType.hpp>
#include <string>

namespace NES::Network {

/**
 * @brief This represent a sink operator that acts as a connecting API between query processing and network stack.
 */
class NetworkSink : public SinkMedium, public Runtime::RuntimeEventListener {
    using inherited0 = SinkMedium;
    using inherited1 = Runtime::RuntimeEventListener;

  public:
    /**
    * @brief constructor for the network sink
    * @param schema
    * @param networkManager
    * @param nodeLocation
    * @param nesPartition
    * @param faultToleranceType: fault-tolerance guarantee chosen by a user
    */
    explicit NetworkSink(const SchemaPtr& schema,
                         uint64_t uniqueNetworkSinkDescriptorId,
                         QueryId queryId,
                         QuerySubPlanId querySubPlanId,
                         NodeLocation const& destination,
                         NesPartition nesPartition,
                         Runtime::NodeEnginePtr nodeEngine,
                         size_t numOfProducers,
                         std::chrono::milliseconds waitTime,
                         uint8_t retryTimes,
                         FaultToleranceType faultToleranceType = FaultToleranceType::NONE,
                         uint64_t numberOfOrigins = 0,
                         uint16_t numberOfInputSources = 0);

    /**
    * @brief Writes data to the underlying output channel
    * @param inputBuffer
    * @param workerContext
    * @return true if no error occurred
    */
    bool writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContext& workerContext) override;

  protected:
    /**
     * @brief This method is called once an event is triggered for the current sink
     * @param event
     */
    void onEvent(Runtime::BaseEvent& event) override;
    /**
     * @brief API method called upon receiving an event.
     * @note Only calls onEvent(event)
     * @param event
     * @param workerContext
     */
    void onEvent(Runtime::BaseEvent& event, Runtime::WorkerContextRef workerContext);

  public:
    /**
    * @return the string representation of the network sink
    */
    std::string toString() const override;

    /**
    * @brief reconfiguration machinery for the network sink
    * @param task descriptor of the reconfiguration
    * @param workerContext the thread on which this is called
    */
    void reconfigure(Runtime::ReconfigurationMessage& task, Runtime::WorkerContext& workerContext) override;

    void postReconfigurationCallback(Runtime::ReconfigurationMessage&) override;

    /**
    * @brief setup method to configure the network sink via a reconfiguration
    */
    void setup() override;

    /**
     * @brief
     */
    void preSetup();

    /**
    * @brief Destroys the network sink
    */
    void shutdown() override;

    /**
    * @brief method to return the type of medium
    * @return type of medium
    */
    SinkMediumTypes getSinkMediumType() override;

    /**
     * @brief method to return the network sinks descriptor id
     * @return id
     */
    uint64_t getUniqueNetworkSinkDescriptorId() const;

    /**
     * @brief method to return the node engine pointer
     * @return node engine pointer
     */
    Runtime::NodeEnginePtr getNodeEngine();

    /**
     * @brief method to check if network sink started trimming its buffer storages
     * @return success if it started trimming
     */
    bool hasTrimmed();

    /**
     * @brief schedule a reconfiguration which lets this sink reconnect to the specified source once it has been drained
     * @param newReceiverLocation the location of the node hosting the new source
     * @param newPartition the partition of the new source
     */
    void addPendingReconfiguration(NesPartition newPartition, const NodeLocation& newReceiverLocation);

    /**
     * @brief reconfigure this sink to point to another downstream network source
     * @param newPartition the partition of the new downstram source
     * @param newReceiverLocation the location of the node where the new downstream source is located
     */
    void configureNewReceiverAndPartition(NesPartition newPartition, const NodeLocation& newReceiverLocation);

    /**
     * @brief returns the number of sources which produce data that is consumed by this sink
     * @return the number of input sources
     */
    [[maybe_unused]] uint16_t getNumberOfInputSources() const;

    friend bool operator<(const NetworkSink& lhs, const NetworkSink& rhs) { return lhs.nesPartition < rhs.nesPartition; }

  private:
    /**
     * @brief store a future in the worker context, spawn a new thread that will create a new network channel and on establishing
     * a connection pass the the new channel into the future and pass a reconfiguration message to the sink to signal that the
     * connection has completed
     * @param workerContext the worker context to store the future in
     * @param newNodeLocation the location of the node to which the connection is to be established
     * @param newNesPartition the partition of the source to which the connection is to be established
     */
    void clearOldAndConnectToNewChannelAsync(Runtime::WorkerContext& workerContext,
                                             const NodeLocation& newNodeLocation,
                                             NesPartition newNesPartition);

    /**
     * @brief write all data from the reconnect buffer to the currently active network channel
     * @param workerContext the context where buffers and channel are stored
     */
    void unbuffer(Runtime::WorkerContext& workerContext);

    /**
     * @brief Checks if a network channel has been established. If so, stores it in the worker context and unbuffers tuples into the channel
     * @param workerContext
     * @return true if the new channel is ready, false otherwise
     */
    bool retrieveNewChannelAndUnbuffer(Runtime::WorkerContext& workerContext);

    uint64_t uniqueNetworkSinkDescriptorId;
    Runtime::NodeEnginePtr nodeEngine;
    NetworkManagerPtr networkManager;
    Runtime::QueryManagerPtr queryManager;
    NodeLocation receiverLocation;
    Runtime::BufferManagerPtr bufferManager;
    NesPartition nesPartition;
    size_t numOfProducers;
    const std::chrono::milliseconds waitTime;
    const uint8_t retryTimes;
    std::function<void(Runtime::TupleBuffer&, Runtime::WorkerContext& workerContext)> insertIntoStorageCallback;
    uint16_t numberOfInputSources;
    std::atomic<uint16_t> receivedVersionDrainEvents;
    std::optional<std::pair<NodeLocation, NesPartition>> pendingReconfiguration;
    std::function<void(EpochMessage, Runtime::WorkerContext& workerContext)> deleteFromStorageCallback;
    std::function<bool(EpochMessage, Runtime::WorkerContext& workerContext)> deleteFromStorageCallback;
    std::atomic<bool> trimmingStarted;
};
}// namespace NES::Network
#endif// NES_CORE_INCLUDE_NETWORK_NETWORKSINK_HPP_
