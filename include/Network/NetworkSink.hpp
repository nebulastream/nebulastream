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

#ifndef NES_INCLUDE_NETWORK_NETWORK_SINK_HPP_
#define NES_INCLUDE_NETWORK_NETWORK_SINK_HPP_

#include <Network/NetworkForwardRefs.hpp>
#include <Runtime/Events.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <string>

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
    */
    explicit NetworkSink(const SchemaPtr& schema,
                         QuerySubPlanId querySubPlanId,
                         NetworkManagerPtr networkManager,
                         NodeLocation const& destination,
                         NesPartition nesPartition,
                         const Runtime::BufferManagerPtr& bufferManager,
                         Runtime::QueryManagerPtr queryManager,
                         Runtime::BufferStoragePtr bufferStorage,
                         size_t numOfProducers,
                         std::chrono::seconds waitTime = std::chrono::seconds(5),
                         uint8_t retryTimes = 10);

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
    * @brief Destroys the network sink
    */
    void shutdown() override;

    /**
    * @brief method to return the type of medium
    * @return type of medium
    */
    SinkMediumTypes getSinkMediumType() override;

  private:
    NetworkManagerPtr networkManager;
    Runtime::QueryManagerPtr queryManager;
    const NodeLocation receiverLocation;
    Runtime::BufferManagerPtr bufferManager;
    Runtime::BufferStoragePtr bufferStorage;
    NesPartition nesPartition;
    size_t numOfProducers;
    const std::chrono::seconds waitTime;
    const uint8_t retryTimes;
};

}// namespace Network
}// namespace NES

#endif// NES_INCLUDE_NETWORK_NETWORK_SINK_HPP_
