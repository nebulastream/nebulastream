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
#include <Network/NodeLocation.hpp>
#include <Runtime/RuntimeEventListener.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Util/FaultToleranceType.hpp>
#include <string>
#include <future>

namespace NES {
namespace Network {

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
                         uint64_t numberOfOrigins = 0);

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
    uint64_t getUniqueNetworkSinkDescriptorId();

    /**
     * @brief method to return the node engine pointer
     * @return node engine pointer
     */
    Runtime::NodeEnginePtr getNodeEngine();

    void connectToChannelAsync(Runtime::WorkerContext& workerContext);

    friend bool operator<(const NetworkSink& lhs, const NetworkSink& rhs) { return lhs.nesPartition < rhs.nesPartition; }

    void establishConnection();

  private:
    uint64_t uniqueNetworkSinkDescriptorId;
    Runtime::NodeEnginePtr nodeEngine;
    NetworkManagerPtr networkManager;
    Runtime::QueryManagerPtr queryManager;
    const NodeLocation receiverLocation;
    Runtime::BufferManagerPtr bufferManager;
    NesPartition nesPartition;
    size_t numOfProducers;
    const std::chrono::milliseconds waitTime;
    const uint8_t retryTimes;
    std::function<void(Runtime::TupleBuffer&, Runtime::WorkerContext& workerContext)> insertIntoStorageCallback;
    const bool connectAsync;
    std::future<NetworkChannelPtr> networkChannelFuture;
    std::atomic<bool> reconnectBuffering;
};

}// namespace Network
}// namespace NES

#endif// NES_CORE_INCLUDE_NETWORK_NETWORKSINK_HPP_
