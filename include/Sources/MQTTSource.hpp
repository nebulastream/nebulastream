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

#ifndef NES_MQTTSOURCE_HPP
#define NES_MQTTSOURCE_HPP
#ifdef ENABLE_MQTT_BUILD
#include <cstdint>
#include <memory>
#include <string>

#include <Sources/DataSource.hpp>

namespace mqtt {
class async_client;
typedef std::shared_ptr<async_client> async_clientPtr;
}// namespace mqtt

namespace NES {
class TupleBuffer;
/**
 * @brief this class provides an MQTT as data source
 */
class MQTTSource : public DataSource {

  public:
    /**
     * @brief constructor for the MQTT data source
     * @param schema of the data
     * @param bufferManager
     * @param queryManager
     * @param serverAddress address and port of the mqtt broker
     * @param clientId identifies the client connecting to the server, each server has aunique clientID
     * @param user name to connect to the mqtt broker
     * @param topic to listen to, to obtain the desired data
     * @param operatorId
     */
    explicit MQTTSource(SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager, NodeEngine::QueryManagerPtr queryManager,
                        const std::string serverAddress, const std::string clientId, const std::string user,
                        const std::string topic, OperatorId operatorId, size_t numSourceLocalBuffers,
                        GatheringMode gatheringMode);

    /**
     * @brief destructor of mqtt sink that disconnects the queue before deconstruction
     * @note if queue cannot be disconnected, an assertion is raised
     */
    ~MQTTSource();

    /**
     * @brief blocking method to receive a buffer from the mqtt source
     * @return TupleBufferPtr containing the received buffer
     */
    std::optional<NodeEngine::TupleBuffer> receiveData() override;

    /**
     * @brief fill buffer with appropriate data type
     * @param buf buffer to be filled
     * @param data the received data as string
     */
    void fillBuffer(NodeEngine::TupleBuffer& buf, std::string data);

    /**
     * @brief override the toString method for the mqtt source
     * @return returns string describing the mqtt source
     */
    const std::string toString() const override;

    /**
     * @brief getter for ServerAddress
     * @return serverAddress
     */
    const std::string& getServerAddress() const;
    /**
     * @brief getter for clientId
     * @return clientId
     */
    const std::string& getClientId() const;
    /**
     * @brief getter for user
     * @return user
     */
    const std::string& getUser() const;
    /**
     * @brief getter for topic
     * @return topic
     */
    const std::string& getTopic() const;

    /**
    * @brief Get source type
    */
    SourceType getType() const override;

  private:
    /**
     * @brief default constructor required for boost serialization
     */
    MQTTSource();

    /**
     * @brief method to connect zmq using the host and port specified before
     * check if already connected, if not connect try to connect, if already connected return
     * @return bool indicating if connection could be established
     */
    bool connect();

    /**
     * @brief method to make sure mqtt is disconnected
     * check if already disconnected, if not disconnected try to disconnect, if already disconnected return
     * @return bool indicating if connection could be established
     */
    bool disconnect();

    /**
     * @brief method for serialization, all listed variable below are added to the
     * serialization/deserialization process
     */
    friend class DataSource;
    bool connected;
    std::string serverAddress;
    std::string clientId;
    std::string user;
    std::string topic;
    mqtt::async_clientPtr client;
};

typedef std::shared_ptr<MQTTSource> MQTTSourcePtr;
}// namespace NES
#endif//NES_MQTTSOURCE_HPP
#endif
