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
#include <memory>
#include <string>

namespace NES {

namespace Configurations {

/*
 * Constant config strings to read specific values from yaml or command line input
 */
const std::string NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG = "numberOfBuffersToProduce";
const std::string NUMBER_OF_TUPLES_TO_PRODUCE_PER_BUFFER_CONFIG = "numberOfTuplesToProducePerBuffer";
const std::string PHYSICAL_STREAM_NAME_CONFIG = "physicalStreamName";
const std::string SOURCE_FREQUENCY_CONFIG = "sourceFrequency";
const std::string LOGICAL_STREAM_NAME_CONFIG = "logicalStreamName";
const std::string ROW_LAYOUT_CONFIG = "rowLayout";
const std::string INPUT_FORMAT_CONFIG = "inputFormat";
const std::string SOURCE_TYPE_CONFIG = "sourceType";
const std::string SENSE_SOURCE_UDFS_CONFIG = "SenseSourceUdfs";
const std::string UDFS_CONFIG = "udfs";
const std::string SENSE_SOURCE_CONFIG = "SenseSource";
const std::string CSV_SOURCE_CONFIG = "CSVSource";
const std::string FILE_PATH_CONFIG = "filePath";
const std::string CSV_FILE_PATH_CONFIG = "CSVSourceFilePath";
const std::string SKIP_HEADER_CONFIG = "skipHeader";
const std::string CSV_SOURCE_SKIP_HEADER_CONFIG = "CSVSourceSkipHeader";
const std::string BINARY_SOURCE_CONFIG = "BinarySource";
const std::string BINARY_SOURCE_FILE_PATH_CONFIG = "BinarySourceFilePath";
const std::string MQTT_SOURCE_CONFIG = "MQTTSource";
const std::string URL_CONFIG = "url";
const std::string MQTT_SOURCE_URL_CONFIG = "MQTTSourceUrl";
const std::string CLIENT_ID_CONFIG = "clientId";
const std::string MQTT_SOURCE_CLIENT_ID_CONFIG = "MQTTSourceClientId";
const std::string USER_NAME_CONFIG = "userName";
const std::string MQTT_SOURCE_USER_NAME_CONFIG = "MQTTSourceUserName";
const std::string TOPIC_CONFIG = "topic";
const std::string QOS_CONFIG = "qos";
const std::string MQTT_SOURCE_QOS_CONFIG = "MQTTSourceQos";
const std::string CLEAN_SESSION_CONFIG = "cleanSession";
const std::string MQTT_SOURCE_CLEAN_SESSION_CONFIG = "MQTTSourceCleanSession";
const std::string MQTT_SOURCE_TOPIC_CONFIG = "MQTTSourceTopic";
const std::string FLUSH_INTERVAL_MS_CONFIG = "flushIntervalMS";
const std::string MQTT_SOURCE_FLUSH_INTERVAL_MS_CONFIG = "MQTTSourceFlushIntervalMS";
const std::string KAFKA_SOURCE_CONFIG = "KafkaSource";
const std::string BROKERS_CONFIG = "brokers";
const std::string KAFKA_SOURCE_BROKERS_CONFIG = "KafkaSourceBrokers";
const std::string AUTO_COMMIT = "autoCommit";
const std::string KAFKA_SOURCE_AUTO_COMMIT_CONFIG = "KafkaSourceAutoCommit";
const std::string GROUP_ID_CONFIG = "groupId";
const std::string KAFKA_SOURCE_GROUP_ID_CONFIG = "KafkaSourceGroupId";
const std::string KAFKA_SOURCE_TOPIC_CONFIG = "KafkaSourceTopic";
const std::string CONNECTION_TIMEOUT_CONFIG = "connectionTimeout";
const std::string KAFKA_SOURCE_CONNECTION_TIMEOUT_CONFIG = "KafkaSourceConnectionTimeout";
const std::string NAME_SPACE_INDEX_CONFIG = "namespaceIndex";
const std::string OPC_SOURCE_NAME_SPACE_INDEX_CONFIG = "OPCSourceNamespaceIndex";
const std::string OPC_SOURCE_CONFIG = "OPCSource";
const std::string NODE_IDENTIFIER_CONFIG = "nodeIdentifier";
const std::string OPC_SOURCE_NODE_IDENTIFIER_CONFIG = "OPCSourceNodeIdentifier";
const std::string OPC_SOURCE_USERNAME_CONFIG = "OPCSourceUserName";
const std::string PASSWORD_CONFIG = "password";
const std::string OPC_SOURCE_PASSWORD_CONFIG = "OPCSourcePassword";
const std::string MATERIALIZED_VIEW_ID_CONFIG = "materializedViewId";
const std::string MATERIALIZEDVIEW_SOURCE_CONFIG = "MaterializedViewSource";
const std::string DEFAULT_SOURCE_CONFIG = "DefaultSource";
const std::string SOURCE_CONFIG_PATH_CONFIG = "sourceConfigPath";
const std::string NO_SOURCE_CONFIG = "NoSource";

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
class SourceConfig : public std::enable_shared_from_this<SourceConfig> {

  public:
    /**
     * @brief constructor to create a new source option object initialized with values from sourceConfigMap
     * @param sourceConfigMap with input params
     * @param sourceType type of source from where object was initialized
     */
    explicit SourceConfig(std::map<std::string, std::string> sourceConfigMap, std::string _sourceType);

    /**
     * @brief constructor to create a new source option object initialized with default values as set below
     * @param sourceType type of source from where object was initialized
     */
    explicit SourceConfig(std::string _sourceType);

    virtual ~SourceConfig() = default;

    /**
     * @brief resets all options to default values
     */
    virtual void resetSourceOptions() = 0;

    /**
     * @brief resets all options to default values
     * @param sourceType also reset source type to current source type object
     */
    virtual void resetSourceOptions(std::string _sourceType);

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
     * @brief Get storage layout, true = row layout, false = column layout
     */
    [[nodiscard]] std::shared_ptr<ConfigOption<bool>> getRowLayout() const;

    /**
     * @brief Set storage layout, true = row layout, false = column layout
     */
    void setRowLayout(bool rowLayout);

    /**
     * @brief Checks if the current Source is of type SourceConfig
     * @tparam SourceConfig
     * @return bool true if Source is of SourceConfig
     */
    template<class SourceConfig>
    bool instanceOf() {
        if (dynamic_cast<SourceConfig*>(this)) {
            return true;
        };
        return false;
    };

    /**
    * @brief Dynamically casts the Source to a SourceConfigType
    * @tparam SourceConfigType
    * @return returns a shared pointer of the SourceConfigType
    */
    template<class SourceConfig>
    std::shared_ptr<SourceConfig> as() {
        if (instanceOf<SourceConfig>()) {
            return std::dynamic_pointer_cast<SourceConfig>(this->shared_from_this());
        }
        throw std::logic_error("Node:: we performed an invalid cast of operator " + this->toString() + " to type "
                               + typeid(SourceConfig).name());
        return nullptr;
    }

  private:
    IntConfigOption numberOfBuffersToProduce;
    IntConfigOption numberOfTuplesToProducePerBuffer;
    StringConfigOption physicalStreamName;
    StringConfigOption logicalStreamName;
    IntConfigOption sourceFrequency;
    BoolConfigOption rowLayout;
    StringConfigOption inputFormat;
    StringConfigOption sourceType;
};
}// namespace Configurations
}// namespace NES
#endif// NES_INCLUDE_CONFIGURATIONS_CONFIG_OPTIONS_SOURCE_CONFIG_HPP_
