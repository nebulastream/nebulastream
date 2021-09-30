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

#ifndef NES_INCLUDE_CONFIGURATIONS_CONFIG_OPTIONS_SOURCE_CONFIG_HPP_
#define NES_INCLUDE_CONFIGURATIONS_CONFIG_OPTIONS_SOURCE_CONFIG_HPP_

#include <map>
#include <string>

namespace NES {

class SourceConfig;
using SourceConfigPtr = std::shared_ptr<SourceConfig>;

template<class T>
class ConfigOption;
using FloatConfigOption = std::shared_ptr<ConfigOption<float>>;
using IntConfigOption = std::shared_ptr<ConfigOption<uint32_t>>;
using StringConfigOption = std::shared_ptr<ConfigOption<std::string>>;
using BoolConfigOption = std::shared_ptr<ConfigOption<bool>>;

/**
 * @brief Configuration object for source config
 */
class SourceConfig {

  public:

    /**
     * @brief constructor to create a new source option object initialized with default values as set below
     */
    explicit SourceConfig(std::map<std::string, std::string> sourceConfigMap);

    virtual ~SourceConfig() = default;

    /**
     * @brief resets all options to default values
     */
    virtual void resetSourceOptions();

    /**
     * @brief prints the current source configuration (name: current value)
     */
    virtual std::string toString();

    /**
     * @brief gets a ConfigOption object with sourceType
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<std::string>> getSourceType() const;

    /**
     * @brief set the value for sourceType with the appropriate data format
     */
    void setSourceType(std::string sourceTypeValue);

    /**
     * @brief gets a ConfigOption object with sourceFrequency
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<uint32_t>> getSourceFrequency() const;

    /**
     * @brief set the value for sourceFrequency with the appropriate data format
     */
    void setSourceFrequency(uint32_t sourceFrequencyValue);

    /**
     * @brief gets a ConfigOption object with numberOfBuffersToProduce
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<uint32_t>> getNumberOfBuffersToProduce() const;

    /**
     * @brief set the value for numberOfBuffersToProduce with the appropriate data format
     */
    void setNumberOfBuffersToProduce(uint32_t numberOfBuffersToProduce);

    /**
     * @brief gets a ConfigOption object with numberOfTuplesToProducePerBuffer
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<uint32_t>> getNumberOfTuplesToProducePerBuffer() const;

    /**
     * @brief set the value for numberOfTuplesToProducePerBuffer with the appropriate data format
     */
    void setNumberOfTuplesToProducePerBuffer(uint32_t numberOfTuplesToProducePerBuffer);

    /**
     * @brief gets a ConfigOption object with physicalStreamName
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<std::string>> getPhysicalStreamName() const;

    /**
     * @brief set the value for physicalStreamName with the appropriate data format
     */
    void setPhysicalStreamName(std::string physicalStreamName);

    /**
     * @brief gets a ConfigOption object with logicalStreamName
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<std::string>> getLogicalStreamName() const;

    /**
     * @brief set the value for logicalStreamName with the appropriate data format
     */
    void setLogicalStreamName(std::string logicalStreamName);

    /**
     * @brief Get input data format
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<std::string>> getInputFormat() const;

    /**
     * @brief Set input data format
     */
    void setInputFormat(std::string inputFormat);

    /**
     * @brief Get tupleBuffer flush interval in milliseconds
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<float>> getFlushIntervalMS() const;

    /**
     * @brief Set tupleBuffer flush interval in milliseconds
     */
    void setFlushIntervalMS(float flushIntervalMs);

    /**
     * @brief Get storage layout, true = row layout, false = column layout
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<bool>> getRowLayout() const;

    /**
     * @brief Set storage layout, true = row layout, false = column layout
     */
    void setRowLayout(bool rowLayout);

    /**
     * @brief Get file path, needed for: CSVSource, BinarySource
     */
    virtual std::shared_ptr<ConfigOption<std::string>> getFilePath() const;

    /**
     * @brief Set file path, needed for: CSVSource, BinarySource
     */
    virtual void setFilePath(std::string filePath);

    /**
     * @brief gets a ConfigOption object with skipHeader
     */
    virtual std::shared_ptr<ConfigOption<bool>> getSkipHeader() const;

    /**
     * @brief set the value for skipHeader with the appropriate data format
     */
    virtual void setSkipHeader(bool skipHeader);

    /**
     * @brief Get url to connect
     */
    virtual std::shared_ptr<ConfigOption<std::string>> getUrl() const;

    /**
     * @brief Set url to connect to
     */
    virtual void setUrl(std::string url);

    /**
     * @brief Get clientId
     */
    virtual std::shared_ptr<ConfigOption<std::string>> getClientId() const;

    /**
     * @brief Set clientId
     */
    virtual void setClientId(std::string clientId);

    /**
     * @brief Get userName
     */
    virtual std::shared_ptr<ConfigOption<std::string>> getUserName() const;

    /**
     * @brief Set userName
     */
    virtual void setUserName(std::string userName);

    /**
     * @brief Get topic to listen to
     */
    virtual std::shared_ptr<ConfigOption<std::string>> getTopic() const;

    /**
     * @brief Set topic to listen to
     */
    virtual void setTopic(std::string topic);

    /**
     * @brief Get quality of service
     */
    virtual std::shared_ptr<ConfigOption<uint32_t>> getQos() const;

    /**
     * @brief Set quality of service
     */
    virtual void setQos(uint32_t qos);

    /**
     * @brief Get cleanSession true = clean up session after client loses connection, false = keep data for client after connection loss (persistent session)
     */
    virtual std::shared_ptr<ConfigOption<bool>> getCleanSession() const;

    /**
     * @brief Set cleanSession true = clean up session after client loses connection, false = keep data for client after connection loss (persistent session)
     */
    virtual void setCleanSession(bool cleanSession);

    /**
     * @brief Get udsf
     */
    virtual std::shared_ptr<ConfigOption<std::string>> getUdsf() const;

    /**
     * @brief Set udsf
     */
    virtual void setUdsf(std::string udsf);

    /**
     * @brief Get broker string
     */
    virtual std::shared_ptr<ConfigOption<std::string>> getBrokers() const;

    /**
     * @brief Set broker string
     */
    virtual void setBrokers(std::string brokers);

    /**
     * @brief Get auto commit, boolean value where 1 equals true, and 0 equals false, needed for: KafkaSource
     */
    virtual std::shared_ptr<ConfigOption<uint32_t>> getAutoCommit() const;

    /**
     * @brief Set auto commit, boolean value where 1 equals true, and 0 equals false, needed for: KafkaSource
     */
    virtual void setAutoCommit(uint32_t autoCommit);

    /**
     * @brief get groupId
     */
    virtual std::shared_ptr<ConfigOption<std::string>> getGroupId() const;

    /**
      * @brief set groupId
      */
    virtual void setGroupId(std::string groupId);

    /**
     * @brief Get connection time out for source, needed for: KafkaSource
     */
    virtual std::shared_ptr<ConfigOption<uint32_t>> getConnectionTimeout() const;

    /**
     * @brief Set connection time out for source, needed for: KafkaSource
     */
    virtual void setConnectionTimeout(uint32_t connectionTimeout);

    /**
     * get the name space for OPCSource
     * @return namespace index
     */
    virtual std::shared_ptr<ConfigOption<std::uint32_t>> getNamespaceIndex() const;

    /**
     * @brief Set namespaceIndex for node
     */
    virtual void setNamespaceIndex(uint32_t namespaceIndex);

    /**
     * @brief Get node identifier
     */
    virtual std::shared_ptr<ConfigOption<std::string>> getNodeIdentifier() const;

    /**
     * @brief Set node identifier
     */
    virtual void setNodeIdentifier(std::string nodeIdentifier);

    /**
     * @brief Get password
     */
    virtual std::shared_ptr<ConfigOption<std::string>> getPassword() const;

    /**
     * @brief Set password
     */
    virtual void setPassword(std::string password);

  private:

    IntConfigOption numberOfBuffersToProduce;
    IntConfigOption numberOfTuplesToProducePerBuffer;
    StringConfigOption physicalStreamName;
    StringConfigOption logicalStreamName;
    IntConfigOption sourceFrequency;
    BoolConfigOption rowLayout;
    FloatConfigOption flushIntervalMS;
    StringConfigOption inputFormat;
    StringConfigOption sourceType;

};
}// namespace NES
#endif// NES_INCLUDE_CONFIGURATIONS_CONFIG_OPTIONS_SOURCE_CONFIG_HPP_
