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
using IntConfigOption = std::shared_ptr<ConfigOption<uint32_t>>;
using StringConfigOption = std::shared_ptr<ConfigOption<std::string>>;
using BoolConfigOption = std::shared_ptr<ConfigOption<bool>>;

/**
 * @brief Configuration object for source config
 */
class SourceConfig {

  public:
    static SourceConfigPtr create();

    /**
     * @brief overwrite the default configurations with those loaded from a yaml file
     * @param filePath file path to the yaml file
     */
    void overwriteConfigWithYAMLFileInput(const std::string& filePath);

    /**
     * @brief overwrite the default and the yaml file configurations with command line input
     * @param inputParams map with key=command line parameter and value = value
     */
    void overwriteConfigWithCommandLineInput(const std::map<std::string, std::string>& inputParams);

    /**
     * @brief resets all options to default values
     */
    void resetSourceOptions();

    /**
     * @brief gets a ConfigOption object with sourceType
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<std::string>> getSourceType() const;

    /**
     * @brief set the value for sourceType with the appropriate data format
     */
    void setSourceType(std::string sourceTypeValue);

    /**
     * @brief gets a ConfigOption object with sourceConfig
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<std::string>> getSourceConfig() const;

    /**
     * @brief set the value for sourceConfig with the appropriate data format
     */
    void setSourceConfig(std::string sourceConfigValue);

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
     * @brief gets a ConfigOption object with skipHeader
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<bool>> getSkipHeader() const;

    /**
     * @brief set the value for skipHeader with the appropriate data format
     */
    void setSkipHeader(bool skipHeader);

    /**
     * @brief Get udsf, needed for: SenseSource
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<std::string>> getUdsf() const;

    /**
     * @brief Set udsf, needed for: SenseSource
     */
    void setUdsf(std::string udsf);

    /**
     * @brief Get file path, needed for: CSVSource, BinarySource
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<std::string>> getFilePath() const;

    /**
     * @brief Set file path, needed for: CSVSource, BinarySource
     */
    void setFilePath(std::string filePath);

    /**
     * @brief Get url to connect to needed for: MQTTSource, ZMQSource, OPCSource, KafkaSource
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<std::string>> getUrl() const;

    /**
     * @brief Set url to connect to needed for: MQTTSource, ZMQSource, OPCSource, KafkaSource
     */
    void setUrl(std::string url);

    /**
     * @brief Get namespaceIndex for node, needed for: OPCSource
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<uint32_t>> getNamespaceIndex() const;

    /**
     * @brief Set namespaceIndex for node, needed for: OPCSource
     */
    void setNamespaceIndex(uint32_t namespaceIndex);

    /**
     * @brief Get node identifier, needed for: OPCSource
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<std::string>> getNodeIdentifier() const;

    /**
     * @brief Set node identifier, needed for: OPCSource
     */
    void setNodeIdentifier(std::string nodeIdentifier);

    /**
     * @brief Get clientId, needed for: MQTTSource (needs to be unique for each connected MQTTSource), KafkaSource (use this for groupId)
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<std::string>> getClientId() const;

    /**
     * @brief Set clientId, needed for: MQTTSource (needs to be unique for each connected MQTTSource), KafkaSource (use this for groupId)
     */
    void setClientId(std::string clientId);

    /**
     * @brief Get userName, needed for: MQTTSource (can be chosen arbitrary), OPCSource
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<std::string>> getUserName() const;

    /**
     * @brief Set userName, needed for: MQTTSource (can be chosen arbitrary), OPCSource
     */
    void setUserName(std::string userName);

    /**
     * @brief Get password, needed for: OPCSource
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<std::string>> getPassword() const;

    /**
     * @brief Set password, needed for: OPCSource
     */
    void setPassword(std::string password);

    /**
     * @brief Get topic to listen to, needed for: MQTTSource, KafkaSource
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<std::string>> getTopic() const;

    /**
     * @brief Set topic to listen to, needed for: MQTTSource, KafkaSource
     */
    void setTopic(std::string topic);

    /**
     * @brief Get input data format
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<std::string>> getInputFormat() const;

    /**
     * @brief Set input data format
     */
    void setInputFormat(std::string inputFormat);

    /**
     * @brief Get quality of service, needed for: MQTTSource
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<uint32_t>> getQos() const;

    /**
     * @brief Set quality of service, needed for: MQTTSource
     */
    void setQos(uint32_t qos);

    /**
     * @brief Get cleanSession true = clean up session after client loses connection, false = keep data for client after connection loss (persistent session), needed for: MQTTSource
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<bool>> getCleanSession() const;

    /**
     * @brief Set cleanSession true = clean up session after client loses connection, false = keep data for client after connection loss (persistent session), needed for: MQTTSource
     */
    void setCleanSession(bool cleanSession);

    /**
     * @brief Get tupleBuffer flush interval in milliseconds
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<uint32_t>> getFlushIntervalMs() const;

    /**
     * @brief Set tupleBuffer flush interval in milliseconds
     */
    void setFlushIntervalMs(uint32_t flushIntervalMs);

    /**
     * @brief Get storage layout, true = row layout, false = column layout
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<bool>> getRowLayout() const;

    /**
     * @brief Set storage layout, true = row layout, false = column layout
     */
    void setRowLayout(bool rowLayout);

    /**
     * @brief Get connection time out for source, needed for: KafkaSource
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<uint32_t>> getConnectionTimeout() const;

    /**
     * @brief Set connection time out for source, needed for: KafkaSource
     */
    void setConnectionTimeout(uint32_t connectionTimeout);

    /**
     * @brief Get auto commit, boolean value where 1 equals true, and 0 equals false, needed for: KafkaSource
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<uint32_t>> getAutoCommit() const;

    /**
     * @brief Set auto commit, boolean value where 1 equals true, and 0 equals false, needed for: KafkaSource
     */
    void setAutoCommit(uint32_t autoCommit);

  private:
    /**
     * @brief constructor to create a new source option object initialized with default values as set below
     */
    SourceConfig();
    StringConfigOption sourceType;
    StringConfigOption udsf;
    StringConfigOption filePath;
    StringConfigOption url;
    IntConfigOption namespaceIndex;
    StringConfigOption nodeIdentifier;
    StringConfigOption clientId;
    StringConfigOption userName;
    StringConfigOption password;
    StringConfigOption topic;
    StringConfigOption inputFormat;
    IntConfigOption qos;
    BoolConfigOption cleanSession;
    IntConfigOption flushIntervalMS;
    BoolConfigOption rowLayout;
    IntConfigOption connectionTimeout;
    IntConfigOption autoCommit;
    IntConfigOption sourceFrequency;
    IntConfigOption numberOfBuffersToProduce;
    IntConfigOption numberOfTuplesToProducePerBuffer;
    StringConfigOption physicalStreamName;
    StringConfigOption logicalStreamName;
    BoolConfigOption skipHeader;
};
}// namespace NES
#endif// NES_INCLUDE_CONFIGURATIONS_CONFIG_OPTIONS_SOURCE_CONFIG_HPP_
