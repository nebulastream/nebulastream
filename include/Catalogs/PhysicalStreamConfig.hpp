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

#ifndef NES_INCLUDE_CATALOGS_PHYSICAL_STREAM_CONFIG_HPP_
#define NES_INCLUDE_CATALOGS_PHYSICAL_STREAM_CONFIG_HPP_

#include <Catalogs/AbstractPhysicalStreamConfig.hpp>
#include <Configurations/ConfigOptions/SourceConfigurations/SourceConfig.hpp>
#include <Phases/ConvertLogicalToPhysicalSource.hpp>
#include <memory>
#include <string>

namespace NES {

class PhysicalStreamConfig;
using PhysicalStreamConfigPtr = std::shared_ptr<PhysicalStreamConfig>;

/**
 * @brief this struct covers the information about the attached sensor
 * @param sourceType: string of data source, e.g., DefaultSource or CSVSource
 * @param sourceConf: parameter for the data source, e.g., numberOfProducedBuffer or file path
 * @param sourceFrequency: the sampling frequency in which the stream should sample a result
 * @param numberOfTuplesToProducePerBuffer: the number of tuples that are put into a buffer, e.g., for csv the number of lines read
 * @param numberOfBuffersToProduce: the number of buffers to produce
 * @param physicalStreamName: name of the stream created by this source
 * @param logicalStreamName: name of the logical steam where this physical stream contributes to
 */
class PhysicalStreamConfig : public AbstractPhysicalStreamConfig {

  public:
    static PhysicalStreamConfigPtr createEmpty();
    static PhysicalStreamConfigPtr create(const SourceConfigPtr& sourceConfig);

    ~PhysicalStreamConfig() noexcept override = default;

    /**
     * @brief Get the source type
     * @return string representing source type
     */
    std::string getSourceType() override;

    /**
     * @brief get source frequency
     * @return returns the source frequency
     */
    [[nodiscard]] std::chrono::milliseconds getSourceFrequency() const;

    /**
     * @brief Get udsf, needed for: SenseSource
     */
    [[nodiscard]] std::string getUdsf() const;

    /**
     * @brief Set udsf, needed for: SenseSource
     */
    void setUdsf(std::string udsfV);

    /**
     * @brief Get file path, needed for: CSVSource, BinarySource
     */
    [[nodiscard]] std::string getFilePath() const;

    /**
     * @brief Set file path, needed for: CSVSource, BinarySource
     */
    void setFilePath(std::string filePath);

    /**
     * @brief Get url to connect to needed for: MQTTSource, ZMQSource, OPCSource, KafkaSource
     */
    [[nodiscard]] std::string getUrl() const;

    /**
     * @brief Set url to connect to needed for: MQTTSource, ZMQSource, OPCSource, KafkaSource
     */
    void setUrl(std::string url);

    /**
     * @brief Get namespaceIndex for node, needed for: OPCSource
     */
    [[nodiscard]] uint32_t getNamespaceIndex() const;

    /**
     * @brief Set namespaceIndex for node, needed for: OPCSource
     */
    void setNamespaceIndex(uint32_t namespaceIndex);

    /**
     * @brief Get node identifier, needed for: OPCSource
     */
    [[nodiscard]] std::string getNodeIdentifier() const;

    /**
     * @brief Set node identifier, needed for: OPCSource
     */
    void setNodeIdentifier(std::string nodeIdentifier);

    /**
     * @brief Get clientId, needed for: MQTTSource (needs to be unique for each connected MQTTSource), KafkaSource (use this for groupId)
     */
    [[nodiscard]] std::string getClientId() const;

    /**
     * @brief Set clientId, needed for: MQTTSource (needs to be unique for each connected MQTTSource), KafkaSource (use this for groupId)
     */
    void setClientId(std::string clientId);

    /**
     * @brief Get userName, needed for: MQTTSource (can be chosen arbitrary), OPCSource
     */
    [[nodiscard]] std::string getUserName() const;

    /**
     * @brief Set userName, needed for: MQTTSource (can be chosen arbitrary), OPCSource
     */
    void setUserName(std::string userName);

    /**
     * @brief Get password, needed for: OPCSource
     */
    [[nodiscard]] std::string getPassword() const;

    /**
     * @brief Set password, needed for: OPCSource
     */
    void setPassword(std::string password);

    /**
     * @brief Get topic to listen to, needed for: MQTTSource, KafkaSource
     */
    [[nodiscard]] std::string getTopic() const;

    /**
     * @brief Set topic to listen to, needed for: MQTTSource, KafkaSource
     */
    void setTopic(std::string topic);

    /**
     * @brief Get input data format
     */
    [[nodiscard]] std::string getInputFormat() const;

    /**
     * @brief Set input data format
     */
    void setInputFormat(std::string inputFormat);

    /**
     * @brief Get quality of service, needed for: MQTTSource
     */
    [[nodiscard]] uint32_t getQos() const;

    /**
     * @brief Set quality of service, needed for: MQTTSource
     */
    void setQos(uint32_t qos);

    /**
     * @brief Get cleanSession true = clean up session after client loses connection, false = keep data for client after connection loss (persistent session), needed for: MQTTSource
     */
    [[nodiscard]] bool getCleanSession() const;

    /**
     * @brief Set cleanSession true = clean up session after client loses connection, false = keep data for client after connection loss (persistent session), needed for: MQTTSource
     */
    void setCleanSession(bool cleanSession);

    /**
     * @brief Get tupleBuffer flush interval in milliseconds
     */
    [[nodiscard]] uint32_t getFlushIntervalMS() const;

    /**
     * @brief Set tupleBuffer flush interval in milliseconds
     */
    void setFlushIntervalMS(uint32_t flushIntervalMS);

    /**
     * @brief Get storage layout, true = row layout, false = column layout
     */
    [[nodiscard]] bool getRowLayout() const;

    /**
     * @brief Set storage layout, true = row layout, false = column layout
     */
    void setRowLayout(bool rowLayout);

    /**
     * @brief Get connection time out for source, needed for: KafkaSource
     */
    [[nodiscard]] uint32_t getConnectionTimeout() const;

    /**
     * @brief Set connection time out for source, needed for: KafkaSource
     */
    void setConnectionTimeout(uint32_t connectionTimeout);

    /**
     * @brief Get auto commit, boolean value where 1 equals true, and 0 equals false, needed for: KafkaSource
     */
    [[nodiscard]] uint32_t getAutoCommit() const;

    /**
     * @brief Set auto commit, boolean value where 1 equals true, and 0 equals false, needed for: KafkaSource
     */
    void setAutoCommit(uint32_t autoCommit);

    /**
     * @brief get the number of tuples to produce in a buffer
     * @return returns the number of tuples to produce in a buffer
     */
    [[nodiscard]] uint32_t getNumberOfTuplesToProducePerBuffer() const;

    /**
     * @brief get the number of buffers to produce
     * @return returns the number of buffers to produce
     */
    [[nodiscard]] uint32_t getNumberOfBuffersToProduce() const;

    /**
     * @brief get physical stream name
     * @return physical stream name
     */
    std::string getPhysicalStreamName() override;

    /**
     * @brief get logical stream name
     * @return logical stream name
     */
    std::string getLogicalStreamName() override;

    std::string toString() override;

    [[nodiscard]] bool getSkipHeader() const;

    SourceDescriptorPtr build(SchemaPtr) override;

    void setSourceFrequency(uint32_t sourceFrequency);
    void setNumberOfTuplesToProducePerBuffer(uint32_t numberOfTuplesToProducePerBuffer);
    void setNumberOfBuffersToProduce(uint32_t numberOfBuffersToProduce);

  protected:
    explicit PhysicalStreamConfig(const SourceConfigPtr& sourceConfig);

    std::string sourceType;
    std::string udsf;
    std::string filePath;
    std::string url;
    uint32_t namespaceIndex;
    std::string nodeIdentifier;
    std::string clientId;
    std::string userName;
    std::string password;
    std::string topic;
    std::string inputFormat;
    uint32_t qos;
    bool cleanSession;
    uint32_t flushIntervalMS;
    bool rowLayout;
    uint32_t connectionTimeout;
    uint32_t autoCommit;
    std::chrono::milliseconds sourceFrequency;
    uint32_t numberOfTuplesToProducePerBuffer;
    uint32_t numberOfBuffersToProduce;
    std::string physicalStreamName;
    std::string logicalStreamName;
    bool skipHeader;
};

}// namespace NES
#endif// NES_INCLUDE_CATALOGS_PHYSICAL_STREAM_CONFIG_HPP_
