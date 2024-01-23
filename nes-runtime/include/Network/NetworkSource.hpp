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

#ifndef NES_RUNTIME_INCLUDE_NETWORK_NETWORKSOURCE_HPP_
#define NES_RUNTIME_INCLUDE_NETWORK_NETWORKSOURCE_HPP_

#include <Network/NetworkForwardRefs.hpp>
#include <Operators/LogicalOperators/Network/NodeLocation.hpp>
#include <Runtime/Execution/DataEmitter.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/DataSource.hpp>
#include <Util/VirtualEnableSharedFromThis.hpp>

namespace NES::Runtime {
class BaseEvent;
}
namespace NES::Network {

/**
 * @brief this class provide a zmq as data source
 */
class NetworkSource : public DataSource {

  public:
    /**
     * @param SchemaPtr
     * @param bufferManager
     * @param queryManager
     * @param networkManager
     * @param nesPartition
     * @param sinkLocation
     * @param numSourceLocalBuffers
     * @param waitTime
     * @param retryTimes
     * @param successors
     * @param version The initial version number of this source when it starts
     * @param uniqueNetworkSourceIdentifier system wide unique id that persists even if the partition of this source is changed
     * @param physicalSourceName
     */
    NetworkSource(SchemaPtr schema,
                  Runtime::BufferManagerPtr bufferManager,
                  Runtime::QueryManagerPtr queryManager,
                  NetworkManagerPtr networkManager,
                  NesPartition nesPartition,
                  NodeLocation sinkLocation,
                  size_t numSourceLocalBuffers,
                  std::chrono::milliseconds waitTime,
                  uint8_t retryTimes,
                  std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors,
                  DecomposedQueryPlanVersion version,
                  uint64_t uniqueNetworkSourceIdentifier,
                  const std::string& physicalSourceName = "defaultPhysicalSourceName");

    /**
     * @brief this method is just dummy and is replaced by the ZmqServer in the NetworkStack. Do not use!
     * @return TupleBufferPtr containing the received buffer
     */
    std::optional<Runtime::TupleBuffer> receiveData() override;

    /**
     * @brief override the toString method
     * @return returns string describing the network source
     */
    std::string toString() const override;

    /**
      * @brief This method is called once an event is triggered for the current source.
      * @param event
      */
    void onEvent(Runtime::BaseEvent& event) override;

    /**
     * @brief Get source type
     */
    SourceType getType() const override;

    /**
     * @brief This method is overridden here to prevent the NetworkSoure to start a thread.
     * It registers the source on the NetworkManager
     * @return true if registration on the network stack is successful
     */
    bool start() final;

    /**
     * @brief This method is overridden here to prevent the NetworkSource to start a thread.
     * It de-registers the source on the NetworkManager
     * @return true if deregistration on the network stack is successful
     */
    bool stop(Runtime::QueryTerminationType = Runtime::QueryTerminationType::Graceful) final;

    /**
     * @brief This method is overridden here to manage failures of NetworkSource.
     * It de-registers the source on the NetworkManager
     * @return true if deregistration on the network stack is successful
     */
    bool fail() final;

    /**
     * @brief This method is overridden here to prevent the NetworkSoure to start a thread.
     * @param bufferManager
     * @param queryManager
     */
    static void runningRoutine(const Runtime::BufferManagerPtr&, const Runtime::QueryManagerPtr&);

    /**
     * @brief This method is invoked when the source receives a reconfiguration message.
     * @param message the reconfiguration message
     * @param context thread context
     */
    void reconfigure(Runtime::ReconfigurationMessage& message, Runtime::WorkerContext& context) override;

    /**
     * @brief This method is invoked when the source receives a reconfiguration message.
     * @param message the reconfiguration message
     */
    void postReconfigurationCallback(Runtime::ReconfigurationMessage& message) override;

    /**
     * @brief API method called upon receiving an event, send event further upstream via Network Channel.
     * @param event
     * @param workerContext
     */
    void onEvent(Runtime::BaseEvent& event, Runtime::WorkerContextRef workerContext) override;

    /**
     * @brief
     * @param terminationType
     */
    void onEndOfStream(Runtime::QueryTerminationType terminationType) override;

    /**
     * @brief handle incoming drain message: if a new version is present, start it. If a the source is marked as
     * migrated, stop it. Otherwise do nothing until a reconfiguration message is received from the coordinator
     */
    void onDrainMessage();

    /**
     * @brief mark this source as migrated. If not incoming channels are connected to this source, stop the source.
     * Otherwise wait for all remaining channels to disconnect and stop the source afterwards.
     */
    void markAsMigrated();

    /**
     * @brief Reconfigures this source with ReconfigurationType::UpdateVersion causing it to close event channels to the old
     * upstream sink and open channels to the new one
     * @return true if a scheduled new version was found and applied, false otherwise
     */
    bool tryStartingNewVersion() override;

    /**
    * @brief Getter for the initial version.
    * @return The version this source was started with
    */
    DecomposedQueryPlanVersion getVersion() override;

    /**
     * @brief getter for the network sinks unique id
     * @return the unique id
     */
    OperatorId getUniqueId() const;

    /**
     * @brief set a new source descriptor to be applied once tryStartingNewVersion() is called
     * @param networkSourceDescriptor the new descriptor
     * @return true if the partition to be scheduled if different from the current one and the descriptor was scheduled.
     */
    bool scheduleNewDescriptor(const NetworkSourceDescriptor& networkSourceDescriptor);

    bool bind();

    friend bool operator<(const NetworkSource& lhs, const NetworkSource& rhs) { return lhs.nesPartition < rhs.nesPartition; }

  private:
    NetworkManagerPtr networkManager;
    NesPartition nesPartition;
    NodeLocation sinkLocation;
    // for event channel
    const std::chrono::milliseconds waitTime;
    const uint8_t retryTimes;
    DecomposedQueryPlanVersion version;
    const uint64_t uniqueNetworkSourceIdentifier;
    std::optional<NetworkSourceDescriptor> nextSourceDescriptor;
    bool migrated = false;
    std::recursive_mutex versionMutex;
};

}// namespace NES::Network

#endif// NES_RUNTIME_INCLUDE_NETWORK_NETWORKSOURCE_HPP_
