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

#ifndef NES_RUNTIME_INCLUDE_SOURCES_MQTTSOURCE_HPP_
#define NES_RUNTIME_INCLUDE_SOURCES_MQTTSOURCE_HPP_
#ifdef ENABLE_MQTT_BUILD

#include <Operators/LogicalOperators/Sources/MQTTSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <Sources/DataSource.hpp>
#include <Sources/Parsers/Parser.hpp>
#include <cstdint>
#include <memory>
#include <string>

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
     * @param bufferManager pointer to the buffer manager
     * @param queryManager pointer to the query manager
     * @param mqttSourceType a configuration object to set up the mqttSource
     * @param operatorId the operator ID
     * @param originId represents the identifier of the upstream operator that represents the origin of the input stream
     * @param statisticId represents the unique identifier of components that we can track statistics for
     * @param numSourceLocalBuffers the number of buffers allocated to a source
     * @param gatheringMode the gathering mode (INTERVAL_MODE, INGESTION_RATE_MODE, or ADAPTIVE_MODE)
     * @param physicalSourceName the name and unique identifier of a physical source
     * @param executableSuccessors the subsequent operators in the pipeline to which the data is pushed
     */
    explicit MQTTSource(SchemaPtr schema,
                        Runtime::BufferManagerPtr bufferManager,
                        Runtime::QueryManagerPtr queryManager,
                        const MQTTSourceTypePtr& mqttSourceType,
                        OperatorId operatorId,
                        OriginId originId,
                        StatisticId statisticId,
                        size_t numSourceLocalBuffers,
                        GatheringMode gatheringMode,
                        const std::string& physicalSourceName,
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
     * @brief fill buffer tuple by tuple using the appropriate parser
     * @param tupleBuffer buffer to be filled
     */
    bool fillBuffer(Runtime::MemoryLayouts::TestTupleBuffer& tupleBuffer);

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
    MQTTSourceDescriptor::ServiceQualities getQualityOfService() const;
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

    /**
     * @brief getter for source config
     * @return mqttSourceType
     */
    const MQTTSourceTypePtr& getSourceConfigPtr() const;

  private:
    /**
     * @brief default constructor required for boost serialization
     */
    MQTTSource() = delete;

    /**
     * @brief method to connect mqtt using the host and port specified before
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
    MQTTSourceTypePtr sourceConfig;
    bool connected;
    std::string serverAddress;
    std::string clientId;
    std::string user;
    std::string topic;
    mqtt::async_clientPtr client;
    uint64_t tupleSize;
    uint64_t tuplesThisPass;
    MQTTSourceDescriptor::ServiceQualities qualityOfService;
    bool cleanSession;
    std::vector<PhysicalTypePtr> physicalTypes;
    std::unique_ptr<Parser> inputParser;
    long bufferFlushIntervalMs;
    //Read timeout in ms for mqtt message consumer
    long readTimeoutInMs;
};

using MQTTSourcePtr = std::shared_ptr<MQTTSource>;
}// namespace NES
#endif//NES_MQTTSOURCE_HPP
#endif // NES_RUNTIME_INCLUDE_SOURCES_MQTTSOURCE_HPP_
