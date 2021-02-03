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

#ifdef ENABLE_MQTT_BUILD
#ifndef NES_MQTTSOURCE_HPP
#define NES_MQTTSOURCE_HPP
#include <cstdint>
#include <memory>
#include <string>
#include <zmq.hpp>

#include <Sources/DataSource.hpp>
#include <mqtt/async_client.h>

namespace NES {
class TupleBuffer;
/**
 * @brief this class provide a zmq as data source
 */
class MQTTSource : public DataSource {

  public:
    /**
     * @brief constructor for the zmq source
     * @param schema of the input buffer
     * @param host name of the source queue
     * @param port of the source queue
     */
    explicit MQTTSource(SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager, NodeEngine::QueryManagerPtr queryManager,
                        const std::string serverAddress, const std::string clientId, const std::string user, const std::string password,
                        const std::string topic, OperatorId operatorId);

    /**
     * @brief destructor of mqtt sink that disconnects the queue before deconstruction
     * @note if queue cannot be disconnected, an assertion is raised
     */
    ~MQTTSource();

    /**
     * @brief blocking method to receive a buffer from the mqtt source
     * @return TupleBufferPtr containing thre received buffer
     */
    std::optional<NodeEngine::TupleBuffer> receiveData() override;

    /**
     * @brief override the toString method for the mqtt source
     * @return returns string describing the mqtt source
     */
    const std::string toString() const override;

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
     * @brief method to disconnect zmq
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
    std::string password;
    std::string topic;
    mqtt::async_client client;
};

typedef std::shared_ptr<MQTTSource> MQTTSourcePtr;
}// namespace NES
#endif//NES_MQTTSOURCE_HPP
#endif
