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

#ifndef NES_RUNTIME_INCLUDE_NETWORK_NETWORKSINK_HPP_
#define NES_RUNTIME_INCLUDE_NETWORK_NETWORKSINK_HPP_

#include <Network/NetworkForwardRefs.hpp>
#include <Operators/LogicalOperators/Network/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Network/NodeLocation.hpp>
#include <Reconfiguration/ReconfigurationMarker.hpp>
#include <Runtime/RuntimeEventListener.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
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
    * @param version The initial version of this sink when it starts
    */
    explicit NetworkSink(const SchemaPtr& schema,
                         OperatorId uniqueNetworkSinkDescriptorId,
                         SharedQueryId sharedQueryId,
                         DecomposedQueryId decomposedQueryId,
                         DecomposedQueryPlanVersion decomposedQueryVersion,
                         NodeLocation const& destination,
                         NesPartition nesPartition,
                         Runtime::NodeEnginePtr nodeEngine,
                         size_t numOfProducers,
                         std::chrono::milliseconds waitTime,
                         uint8_t retryTimes,
                         uint64_t numberOfOrigins,
                         DecomposedQueryPlanVersion version);

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
     * @brief try writing a buffered tuple buffer to the network channel without inserting it into the reconnect buffer on failure
     * This function is to be used from the unbuffer function instead of the writeData() function to avoid recursively calling
     * unbuffer again
     * @param inputBuffer the buffer to write.
     * @param workerContext the worker context
     * @return true if the buffer was written successfully, false otherwise
     */
    bool writeBufferedData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContext& workerContext);

    /**
     * @brief method to return the network sinks descriptor id
     * @return id
     */
    OperatorId getUniqueNetworkSinkDescriptorId() const;

    /**
     * @brief method to return the node engine pointer
     * @return node engine pointer
     */
    Runtime::NodeEnginePtr getNodeEngine();

    /**
     * @brief method to return the plan version
     * @return decomposed plan version
     */
    DecomposedQueryPlanVersion getVersion();

    /**
     * @brief reconfigure this sink to point to another downstream network source
     * @param newPartition the partition of the new downstram source
     * @param newReceiverLocation the location of the node where the new downstream source is located
     * @param newVersion The new version number assigned to this sink
     * @param marker the reconfiguration marker to be passed to the old downstream source
     */
    void configureNewSinkDescriptor(const NetworkSinkDescriptor& newNetworkSinkDescriptor, ReconfigurationMarkerPtr marker);

    /**
     * @brief schedule a new receiver location and new receiver partition and versio number to be set for this sink.
     * @param networkSinkDescriptor : a sink descriptor containing the new location and partition which will be set as
     * pending
     * @return false if the changes have already been applied, true otherwise
     */
    bool scheduleNewDescriptor(const NetworkSinkDescriptor& networkSinkDescriptor);

    /**
     * @brief apply pending changes to the receiver location, receiver partition and version number
     * @param marker the reconfiguration marker to be passed to the old downstream source
     * @return true if pending changes were found and applied, false if no pending changes could be found
     */
    bool applyNextSinkDescriptor(ReconfigurationMarkerPtr marker);

    friend bool operator<(const NetworkSink& lhs, const NetworkSink& rhs) { return lhs.nesPartition < rhs.nesPartition; }

  private:
    /**
     * @brief store a future in the worker context, spawn a new thread that will create a new network channel and on establishing
     * a connection pass the the new channel into the future and pass a reconfiguration message to the sink to signal that the
     * connection has completed
     * @param workerContext the worker context to store the future in
     * @param newNodeLocation the location of the node to which the connection is to be established
     * @param newNesPartition the partition of the source to which the connection is to be established
     * @param newVersion The new version number used by the receiver to determine if it can already accept this channel
     * @param reconfigurationMarker in case of reconfiguration a reconfiguration marker to be processed and propagated
     */
    void clearOldAndConnectToNewChannelAsync(Runtime::WorkerContext& workerContext,
                                             const NodeLocation& newNodeLocation,
                                             NesPartition newNesPartition,
                                             DecomposedQueryPlanVersion newVersion,
                                             const std::optional<ReconfigurationMarkerPtr>& reconfigurationMarker = std::nullopt);

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

    OperatorId uniqueNetworkSinkDescriptorId;
    Runtime::NodeEnginePtr nodeEngine;
    NetworkManagerPtr networkManager;
    Runtime::QueryManagerPtr queryManager;
    NodeLocation receiverLocation;
    Runtime::BufferManagerPtr bufferManager;
    NesPartition nesPartition;
    std::atomic<uint64_t> messageSequenceNumber;
    size_t numOfProducers;
    const std::chrono::milliseconds waitTime;
    const uint8_t retryTimes;
    DecomposedQueryPlanVersion version;
};
}// namespace NES::Network
#endif// NES_RUNTIME_INCLUDE_NETWORK_NETWORKSINK_HPP_
