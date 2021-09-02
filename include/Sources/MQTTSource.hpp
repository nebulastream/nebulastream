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
#include <Sources/Parsers/Parser.hpp>

namespace mqtt {
class async_client;
using async_clientPtr = std::shared_ptr<async_client>;
}

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
     * @param inputFormat data format that broker sends
     * @param qos Quality of Service (0 = at most once delivery, 1 = at leaste once delivery, 2 = exactly once delivery)
     * @param cleanSession true = clean up session after client loses connection, false = keep data for client after connection loss (persistent session)
     * @param bufferFlushIntervalMs OPTIONAL - determine for how long to wait until buffer is flushed (before it is full)
     */
    explicit MQTTSource(SchemaPtr schema,
                        Runtime::BufferManagerPtr bufferManager,
                        Runtime::QueryManagerPtr queryManager,
                        std::string const& serverAddress,
                        std::string const& clientId,
                        std::string const& user,
                        std::string const& topic,
                        OperatorId operatorId,
                        size_t numSourceLocalBuffers,
                        GatheringMode gatheringMode,
                        std::vector<Runtime::Execution::SuccessorExecutablePipeline> executableSuccessors,
                        SourceDescriptor::InputFormat inputFormat,
                        uint32_t qos,
                        bool cleanSession,
                        long bufferFlushIntervalMs);

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
     * @brief fill buffer tuple by tuple using the appropriate parser
     * @param tupleBuffer buffer to be filled
     */
    void fillBuffer(Runtime::TupleBuffer& tupleBuffer);

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
     * @brief getter for inputFormat
     * @return inputFormat
     */
    SourceDescriptor::InputFormat getInputFormat() const;
    /**
     * @brief getter for tupleSize
     * @return tupleSize
     */
    uint64_t getTupleSize() const;
    /**
     * @brief getter for tuplesThisPass
     * @return tuplesThisPass
     */
    uint64_t getTuplesThisPass() const;
    /**
     * @brief getter for Quality of Service
     * @return Quality of Service
     */
    uint32_t getQos() const;
    /**
     * @brief getter for cleanSession
     * @return cleanSession
     */
    bool getCleanSession() const;
    /**
     * @brief Get source type
     */
    SourceType getType() const override;
    /**
     * @brief get physicalTypes
     * @return physicalTypes
     */
    std::vector<PhysicalTypePtr> getPhysicalTypes() const;

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
    SourceDescriptor::InputFormat inputFormat;
    mqtt::async_clientPtr client;
    uint64_t tupleSize;
    uint64_t tuplesThisPass;
    uint32_t qos;
    bool cleanSession;
    std::vector<PhysicalTypePtr> physicalTypes;
    std::unique_ptr<Parser> inputParser;
    long bufferFlushIntervalMs;
    //Read timeout in ms for mqtt message consumer
    long readTimeout;
};

using MQTTSourcePtr = std::shared_ptr<MQTTSource>;
}// namespace NES
#endif//NES_MQTTSOURCE_HPP
#endif
