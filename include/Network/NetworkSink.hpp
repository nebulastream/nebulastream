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

#ifndef NES_NETWORKSINK_HPP
#define NES_NETWORKSINK_HPP

#include <Network/NesPartition.hpp>
#include <Network/NetworkManager.hpp>
#include <Network/OutputChannel.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>

#include <boost/thread/tss.hpp>
#include <string>

namespace NES {
class BufferManager;
typedef std::shared_ptr<BufferManager> BufferManagerPtr;

namespace Network {

class NetworkSink : public SinkMedium {
  public:
    /**
     * @brief constructor for the network sink
     * @param schema
     * @param networkManager
     * @param nodeLocation
     * @param nesPartition
     */
    explicit NetworkSink(SchemaPtr schema,
                         QuerySubPlanId parentPlanId,
                         NetworkManagerPtr networkManager,
                         const NodeLocation nodeLocation,
                         NesPartition nesPartition,
                         BufferManagerPtr bufferManager,
                         QueryManagerPtr queryManager,
                         std::chrono::seconds waitTime = std::chrono::seconds(2),
                         uint8_t retryTimes = 5);

    ~NetworkSink();

    bool writeData(TupleBuffer& inputBuffer, WorkerContext& workerContext) override;

    const std::string toString() const override;

    void reconfigure(ReconfigurationTask& task, WorkerContext& workerContext) override;
    void setup() override;
    void shutdown() override;

    std::string toString() override;

    /**
    * @brief method to return the type of medium
    * @return type of medium
    */
    SinkMediumTypes getSinkMediumType();

  private:
    NetworkManagerPtr networkManager;
    QueryManagerPtr queryManager;
    const NodeLocation nodeLocation;
    NesPartition nesPartition;

    const std::chrono::seconds waitTime;
    const uint8_t retryTimes;
};

}// namespace Network
}// namespace NES

#endif// NES_NETWORKSINK_HPP
