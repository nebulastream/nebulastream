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

#ifndef NES_INCLUDE_CONFIGURATIONS_CONFIGURATIONOPTION_HPP_
#define NES_INCLUDE_CONFIGURATIONS_CONFIGURATIONOPTION_HPP_

#include <Util/Logger.hpp>
#include <any>
#include <sstream>
#include <string>
#include <typeinfo>
#include <memory>
#include <utility>

namespace NES {

namespace Configurations {

/**
 * @brief Template for a ConfigurationOption object
 * @tparam T template parameter, depends on ConfigOptions
 */
template<class T>
class ConfigurationOption {
  public:
    static std::shared_ptr<ConfigurationOption> create(std::string name, T value, std::string description) {
        return std::make_shared<ConfigurationOption>(ConfigurationOption(name, value, description));
    };

    /**
     * @brief converts a ConfigurationOption Object into human readable format
     * @return string representation of the config
     */
    std::string toString() {
        std::stringstream ss;
        ss << "Config Object: \n";
        ss << "Name: " << name << "\n";
        ss << "Description: " << description << "\n";
        ss << "Value: " << value << "\n";
        ss << "Default Value: " << defaultValue << "\n";
        return ss.str();
    }

    /**
     * @brief converts config object to human readable form, only prints name and current value
     * @return Name: current Value of config object
     */
    std::string toStringNameCurrentValue() {
        std::stringstream ss;
        ss << name << ": " << value << "\n";
        return ss.str();
    }

    /**
     * @brief converts the value of this object into a string
     * @return string of the value of this object
     */
    [[nodiscard]] std::string getValueAsString() const { return string(value); };

    /**
     * @brief get the name of the ConfigurationOption Object
     * @return name of the config
     */
    std::string getName() { return name; }

    /**
     * @brief get the value of the ConfigurationOption Object
     * @return the value of the config if not set then default value
     */
    [[nodiscard]] T getValue() const { return value; };

    /**
     * @brief sets the value
     * @param value: the value to be used
     */
    void setValue(T value) { this->value = value; }

    /**
     * @brief get the description of this parameter
     * @return description of the config
     */
    [[nodiscard]] std::string getDescription() const { return description; };

    /**
     * @brief get the default value of this parameter
     * @return: the default value
     */
    [[nodiscard]] T getDefaultValue() const { return defaultValue; };

    /**
     * @brief perform equality check between two config options
     * @param other: other config option
     * @return true if equal else return false
     */
    bool equals(const std::any& other) {
        if (this == other) {
            return true;
        }
        if (other.has_value() && other.type() == typeid(ConfigurationOption)) {
            auto that = (ConfigurationOption<T>) other;
            return this->name == that.name && this->description == that.description && this->value == that.value
                && this->defaultValue == that.defaultValue;
        }
        return false;
    };

  private:
    /**
     * @brief Constructs a ConfigurationOption<T> object
     * @param name the name of the object
     * @param value the value of the object
     * @param description default value of the object
     */
    explicit ConfigurationOption(std::string name, T value, std::string description)
        : name(std::move(name)), description(std::move(description)), value(value), defaultValue(value) {}

    ConfigurationOption(std::string name, T value, T defaultValue, std::string description)
        : name(std::move(name)), description(std::move(description)), value(value), defaultValue(defaultValue) {}

    std::string name;
    std::string description;
    T value;
    T defaultValue;
};

using IntConfigOption = std::shared_ptr<ConfigurationOption<uint32_t>>;
using StringConfigOption = std::shared_ptr<ConfigurationOption<std::string>>;
using BoolConfigOption = std::shared_ptr<ConfigurationOption<bool>>;
using FloatConfigOption = std::shared_ptr<ConfigurationOption<float>>;

//Coordinator Configuration Names
const std::string REST_PORT_CONFIG = "restPort";
const std::string RPC_PORT_CONFIG = "rpcPort";//used to be coordinator port, renamed to uniform naming
const std::string DATA_PORT_CONFIG = "dataPort";
const std::string REST_IP_CONFIG = "restIp";
const std::string COORDINATOR_IP_CONFIG = "coordinatorIp";
const std::string NUMBER_OF_SLOTS_CONFIG = "numberOfSlots";
const std::string LOG_LEVEL_CONFIG = "logLevel";
const std::string NUMBER_OF_BUFFERS_IN_GLOBAL_BUFFER_MANAGER_CONFIG = "numberOfBuffersInGlobalBufferManager";
const std::string NUMBER_OF_BUFFERS_PER_WORKER_CONFIG = "numberOfBuffersPerWorker";
const std::string NUMBER_OF_BUFFERS_IN_SOURCE_LOCAL_BUFFER_POOL_CONFIG = "numberOfBuffersInSourceLocalBufferPool";
const std::string BUFFERS_SIZE_IN_BYTES_CONFIG = "bufferSizeInBytes";
const std::string ENABLE_MONITORING_CONFIG = "enableMonitoring";
const std::string NUM_WORKER_THREADS_CONFIG = "numWorkerThreads";
const std::string OPTIMIZER_CONFIG = "optimizer";

//Optimizer Configurations
const std::string MEMORY_LAYOUT_POLICY_CONFIG = "memoryLayoutPolicy";
const std::string PERFORM_ONLY_SOURCE_OPERATOR_EXPANSION = "performOnlySourceOperatorExpansion";
const std::string ENABLE_QUERY_RECONFIGURATION = "enableQueryReconfiguration";
const std::string QUERY_BATCH_SIZE_CONFIG = "queryBatchSize";
const std::string QUERY_MERGER_RULE_CONFIG = "queryMergerRule";
const std::string PERFORM_ADVANCE_SEMANTIC_VALIDATION = "advanceSemanticValidation";
const std::string PERFORM_DISTRIBUTED_WINDOW_OPTIMIZATION = "performDistributedWindowOptimization";
const std::string DISTRIBUTED_WINDOW_OPTIMIZATION_CHILD_THRESHOLD = "distributedWindowChildThreshold";
const std::string DISTRIBUTED_WINDOW_OPTIMIZATION_COMBINER_THRESHOLD = "distributedWindowCombinerThreshold";


//Worker Configuration Names
const std::string COORDINATOR_PORT_CONFIG = "coordinatorPort";//needs to be same as RPC Port of Coordinator
const std::string LOCAL_WORKER_IP_CONFIG = "localWorkerIp";
const std::string PARENT_ID_CONFIG = "parentId";
const std::string QUERY_COMPILER_COMPILATION_STRATEGY_CONFIG = "compilationStrategy";
const std::string QUERY_COMPILER_PIPELINING_STRATEGY_CONFIG = "pipeliningStrategy";
const std::string QUERY_COMPILER_OUTPUT_BUFFER_OPTIMIZATION_CONFIG = "outputBufferOptimizationLevel";
const std::string QUERY_COMPILER_WINDOWING_STRATEGY_CONFIG = "windowingStrategy";
const std::string SOURCE_PIN_LIST_CONFIG = "sourcePinList";
const std::string WORKER_PIN_LIST_CONFIG = "workerPinList";
const std::string QUEUE_PIN_LIST_CONFIG = "queuePinList";

const std::string NUMA_AWARENESS_CONFIG = "numaAwareness";
const std::string PHYSICAL_SOURCES = "physicalSources";
const std::string PHYSICAL_SOURCE_TYPE_CONFIGURATION = "configuration";
const std::string QUERY_COMPILER_CONFIG = "queryCompiler";

//Different Stream Types supported in NES
const std::string SENSE_SOURCE_CONFIG = "SenseSource";
const std::string CSV_SOURCE_CONFIG = "CSVSource";
const std::string BINARY_SOURCE_CONFIG = "BinarySource";
const std::string MQTT_SOURCE_CONFIG = "MQTTSource";
const std::string KAFKA_SOURCE_CONFIG = "KafkaSource";
const std::string OPC_SOURCE_CONFIG = "OPCSource";
const std::string DEFAULT_SOURCE_CONFIG = "DefaultSource";
const std::string MATERIALIZEDVIEW_SOURCE_CONFIG = "MaterializedViewSource";

const std::string PHYSICAL_SOURCE_NAME_CONFIG = "physicalSourceName";
const std::string LOGICAL_SOURCE_NAME_CONFIG = "logicalSourceName";

//Configuration names for stream types
const std::string SOURCE_TYPE_CONFIG = "type";
const std::string NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG = "numberOfBuffersToProduce";
const std::string NUMBER_OF_TUPLES_TO_PRODUCE_PER_BUFFER_CONFIG = "numberOfTuplesToProducePerBuffer";
const std::string SOURCE_FREQUENCY_CONFIG = "sourceFrequency";
const std::string INPUT_FORMAT_CONFIG = "inputFormat";
const std::string UDFS_CONFIG = "udfs";
const std::string FILE_PATH_CONFIG = "filePath";

const std::string SKIP_HEADER_CONFIG = "skipHeader";
const std::string DELIMITER_CONFIG = "delimiter";

const std::string URL_CONFIG = "url";
const std::string CLIENT_ID_CONFIG = "clientId";
const std::string USER_NAME_CONFIG = "userName";
const std::string TOPIC_CONFIG = "topic";
const std::string QOS_CONFIG = "qos";
const std::string CLEAN_SESSION_CONFIG = "cleanSession";
const std::string FLUSH_INTERVAL_MS_CONFIG = "flushIntervalMS";

const std::string BROKERS_CONFIG = "brokers";
const std::string AUTO_COMMIT_CONFIG = "autoCommit";
const std::string GROUP_ID_CONFIG = "groupId";
const std::string CONNECTION_TIMEOUT_CONFIG = "connectionTimeout";
const std::string NAME_SPACE_INDEX_CONFIG = "namespaceIndex";

const std::string NODE_IDENTIFIER_CONFIG = "nodeIdentifier";
const std::string PASSWORD_CONFIG = "password";

const std::string SOURCE_CONFIG_PATH_CONFIG = "sourceConfigPath";

const std::string MATERIALIZED_VIEW_ID_CONFIG = "materializedViewId";

const std::string LOCATION_COORDINATES = "locationCoordinates";

}// namespace Configurations
}// namespace NES

#endif  // NES_INCLUDE_CONFIGURATIONS_CONFIGURATIONOPTION_HPP_
