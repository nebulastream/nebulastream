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

#ifndef NES_INCLUDE_NETWORK_NETWORKSOURCE_HPP_
#define NES_INCLUDE_NETWORK_NETWORKSOURCE_HPP_

#include <Network/NesPartition.hpp>
#include <Network/NetworkManager.hpp>
#include <Sources/DataSource.hpp>

namespace NES {
namespace Network {

/**
 * @brief this class provide a zmq as data source
 */
class NetworkSource : public DataSource {

  public:
    NetworkSource(SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager,
                  NetworkManagerPtr networkManager, NesPartition nesPartition);

    ~NetworkSource();

    /**
     * @brief this method is just dummy and is replaced by the ZmqServer in the NetworkStack. Do not use!
     * @return TupleBufferPtr containing the received buffer
     */
    std::optional<TupleBuffer> receiveData() override;

    /**
     * @brief override the toString method
     * @return returns string describing the network source
     */
    const std::string toString() const override;

    /**
     * @brief Get source type
     */
    SourceType getType() const override;

    /**
     * @brief This method is overridden here to prevent the NetworkSoure to start a thread.
     * It registers the source on the NetworkManager
     * @return true if registration on the network stack is successful
     */
    bool start();

    /**
     * @brief This method is overridden here to prevent the NetworkSoure to start a thread.
     * It de-registers the source on the NetworkManager
     * @return true if deregistration on the network stack is successful
     */
    bool stop();

    /**
     * @brief This method is overridden here to prevent the NetworkSoure to start a thread.
     * @param bufferManager
     * @param queryManager
     */
    void runningRoutine(BufferManagerPtr, QueryManagerPtr);

  private:
    NetworkManagerPtr networkManager;
    NesPartition nesPartition;
};

}// namespace Network
}// namespace NES

#endif//NES_INCLUDE_NETWORK_NETWORKSOURCE_HPP_
