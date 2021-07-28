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
using async_clientPtr = std::shared_ptr<async_client>;
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
    explicit MQTTSource(SchemaPtr schema,
                        Runtime::BufferManagerPtr bufferManager,
                        Runtime::QueryManagerPtr queryManager,
                        std::string const& serverAddress,
                        std::string const& clientId,
                        std::string const& user,
                        std::string const& topic,
                        std::string const& dataType,
                        OperatorId operatorId,
                        size_t numSourceLocalBuffers,
                        GatheringMode gatheringMode,
                        std::vector<Runtime::Execution::SuccessorExecutablePipeline> executableSuccessors);

    /**
     * @brief destructor of mqtt sink that disconnects the queue before deconstruction
     * @note if queue cannot be disconnected, an assertion is raised
     */
    ~MQTTSource() override;

    /**
     * @brief blocking method to receive a buffer from the mqtt source
     * @return TupleBufferPtr containing the received buffer
     */
    std::optional<Runtime::TupleBuffer> receiveData() override;

    /**
     * @brief fill buffer with appropriate data type
     * @param buf buffer to be filled
     * @param data the received data as string
     */
    void fillBuffer(Runtime::TupleBuffer& buf, std::string& data, uint64_t tupCnt);

    /**
     * @brief override the toString method for the mqtt source
     * @return returns string describing the mqtt source
     */
    std::string toString() const override;

    /**
     * @brief getter for ServerAddress
     * @return serverAddress
     */
    std::string const& getServerAddress() const;
    /**
     * @brief getter for clientId
     * @return clientId
     */
    std::string const& getClientId() const;
    /**
     * @brief getter for user
     * @return user
     */
    std::string const& getUser() const;
    /**
     * @brief getter for topic
     * @return topic
     */
    std::string const& getTopic() const;
    /**
     * @brief getter for dataType
     * @return dataType
     */
    std::string getDataType() const;
    /**
     * @brief getter for tupleSize
     * @return tupleSize
     */
    uint64_t getTupleSize() const;
    /**
    * @brief Get source type
    */
    SourceType getType() const override;

  private:
    /**
     * @brief default constructor required for boost serialization
     */
    MQTTSource() = delete;

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
    std::string dataType;
    mqtt::async_clientPtr client;
    uint64_t tupleSize;
};

using MQTTSourcePtr = std::shared_ptr<MQTTSource>;
}// namespace NES
#endif//NES_MQTTSOURCE_HPP
#endif
