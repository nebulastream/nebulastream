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

#ifndef NES_INCLUDE_CONFIGURATIONS_WORKER_PHYSICAL_STREAM_CONFIG_SOURCE_TYPE_CONFIG_HPP_
#define NES_INCLUDE_CONFIGURATIONS_WORKER_PHYSICAL_STREAM_CONFIG_SOURCE_TYPE_CONFIG_HPP_

#include <map>
#include <memory>
#include <string>
#include <Util/yaml/rapidyaml.hpp>

namespace NES {

namespace Configurations {

/*
 * Constant config strings to read specific values from yaml or command line input
 */
const std::string SOURCE_TYPE_CONFIG = "sourceType";
const std::string NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG = "numberOfBuffersToProduce";
const std::string NUMBER_OF_TUPLES_TO_PRODUCE_PER_BUFFER_CONFIG = "numberOfTuplesToProducePerBuffer";
const std::string SOURCE_FREQUENCY_CONFIG = "sourceFrequency";
const std::string INPUT_FORMAT_CONFIG = "inputFormat";
const std::string UDFS_CONFIG = "udfs";
const std::string SENSE_SOURCE_CONFIG = "SenseSource";
const std::string CSV_SOURCE_CONFIG = "CSVSource";
const std::string FILE_PATH_CONFIG = "filePath";
const std::string SKIP_HEADER_CONFIG = "skipHeader";
const std::string DELIMITER_CONFIG = "delimiter";
const std::string BINARY_SOURCE_CONFIG = "BinarySource";
const std::string MQTT_SOURCE_CONFIG = "MQTTSource";
const std::string URL_CONFIG = "url";
const std::string CLIENT_ID_CONFIG = "clientId";
const std::string USER_NAME_CONFIG = "userName";
const std::string TOPIC_CONFIG = "topic";
const std::string QOS_CONFIG = "qos";
const std::string CLEAN_SESSION_CONFIG = "cleanSession";
const std::string FLUSH_INTERVAL_MS_CONFIG = "flushIntervalMS";
const std::string KAFKA_SOURCE_CONFIG = "KafkaSource";
const std::string BROKERS_CONFIG = "brokers";
const std::string AUTO_COMMIT_CONFIG = "autoCommit";
const std::string GROUP_ID_CONFIG = "groupId";
const std::string CONNECTION_TIMEOUT_CONFIG = "connectionTimeout";
const std::string NAME_SPACE_INDEX_CONFIG = "namespaceIndex";
const std::string OPC_SOURCE_CONFIG = "OPCSource";
const std::string NODE_IDENTIFIER_CONFIG = "nodeIdentifier";
const std::string PASSWORD_CONFIG = "password";
const std::string MATERIALIZED_VIEW_ID_CONFIG = "materializedViewId";
const std::string MATERIALIZEDVIEW_SOURCE_CONFIG = "MaterializedViewSource";
const std::string DEFAULT_SOURCE_CONFIG = "DefaultSource";
const std::string SOURCE_CONFIG_PATH_CONFIG = "sourceConfigPath";
const std::string NO_SOURCE_CONFIG = "NoSource";

class SourceTypeConfig;
using SourceTypeConfigPtr = std::shared_ptr<SourceTypeConfig>;

template<class T>
class ConfigurationOption;
using FloatConfigOption = std::shared_ptr<ConfigurationOption<float>>;
using IntConfigOption = std::shared_ptr<ConfigurationOption<uint32_t>>;
using StringConfigOption = std::shared_ptr<ConfigurationOption<std::string>>;
using BoolConfigOption = std::shared_ptr<ConfigurationOption<bool>>;

/**
 * @brief Configuration object for source config
 */
class SourceTypeConfig : public std::enable_shared_from_this<SourceTypeConfig> {

  public:

    /**
     * @brief constructor to create a new source option object initialized with default values as set below
     * @param sourceType type of source from where object was initialized
     */
    explicit SourceTypeConfig(std::string _sourceType);

    virtual ~SourceTypeConfig() = default;

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
     * Checks equality
     * @param other sourceConfig ot check equality for
     * @return true if equal, false otherwise
     */
    virtual bool equal(SourceTypeConfigPtr const& other);

    /**
     * @brief gets a ConfigurationOption object with sourceType
     */
    [[nodiscard]] std::shared_ptr<ConfigurationOption<std::string>> getSourceType() const;

    /**
     * @brief set the value for sourceType with the appropriate data format
     */
    void setSourceType(std::string sourceTypeValue);

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
    StringConfigOption sourceType;
};
}// namespace Configurations
}// namespace NES
#endif// NES_INCLUDE_CONFIGURATIONS_WORKER_PHYSICAL_STREAM_CONFIG_SOURCE_TYPE_CONFIG_HPP_
