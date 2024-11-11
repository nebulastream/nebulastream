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
#ifndef NES_CONFIGURATIONS_INCLUDE_CONFIGURATIONS_CONFIGURATIONSNAMES_HPP_
#define NES_CONFIGURATIONS_INCLUDE_CONFIGURATIONS_CONFIGURATIONSNAMES_HPP_
#include <cstdint>
#include <string>

using namespace std::string_literals;

namespace NES::Configurations {
/**
 * @brief input format enum gives information whether a JSON or CSV was used to transfer data
 */
enum class InputFormat : uint8_t { JSON, CSV, NES_BINARY };

/**
 * NOTE: this is not related to the network stack at all. Do not mix it up.
 * @brief Decide how a message size is obtained:
 * TUPLE_SEPARATOR: TCP messages are send with a char acting as tuple separator between them, tupleSeperator needs to be set
 * USER_SPECIFIED_BUFFER_SIZE: User specifies the buffer size beforehand, socketBufferSize needs to be set
 * BUFFER_SIZE_FROM_SOCKET: Between each message you also obtain a fixed amount of bytes with the size of the next message,
 * bytesUsedForSocketBufferSizeTransfer needs to be set
 */
enum class TCPDecideMessageSize : uint8_t { TUPLE_SEPARATOR, USER_SPECIFIED_BUFFER_SIZE, BUFFER_SIZE_FROM_SOCKET };

//Coordinator Configuration Names
const std::string REST_PORT_CONFIG = "restPort";
const std::string RPC_PORT_CONFIG = "rpcPort";//used to be coordinator port, renamed to uniform naming
const std::string DATA_PORT_CONFIG = "dataPort";
const std::string REST_IP_CONFIG = "restIp";
const std::string COORDINATOR_HOST_CONFIG = "coordinatorHost";
const std::string NUMBER_OF_SLOTS_CONFIG = "numberOfSlots";
const std::string BANDWIDTH_IN_MBPS = "bandwidthInMbps";
const std::string LATENCY_IN_MS = "latencyInMs";
const std::string LOG_LEVEL_CONFIG = "logLevel";
const std::string LOGICAL_SOURCES = "logicalSources";
const std::string NUMBER_OF_BUFFERS_IN_GLOBAL_BUFFER_MANAGER_CONFIG = "numberOfBuffersInGlobalBufferManager";
const std::string NUMBER_OF_BUFFERS_PER_WORKER_CONFIG = "numberOfBuffersPerWorker";
const std::string NUMBER_OF_BUFFERS_IN_SOURCE_LOCAL_BUFFER_POOL_CONFIG = "numberOfBuffersInSourceLocalBufferPool";
const std::string BUFFERS_SIZE_IN_BYTES_CONFIG = "bufferSizeInBytes";
const std::string LAMBDA_SOURCE_CONFIG = "lambdaSource";
const std::string ENABLE_MONITORING_CONFIG = "enableMonitoring";
const std::string MONITORING_WAIT_TIME = "monitoringWaitTime";
const std::string ENABLE_NEW_REQUEST_EXECUTOR_CONFIG = "enableNewRequestExecutor";
const std::string REQUEST_EXECUTOR_THREAD_CONFIG = "numOfRequestExecutorThread";
const std::string STORAGE_HANDLER_TYPE_CONFIG = "storageHandlerType";
const std::string ENABLE_SOURCE_SHARING_CONFIG = "enableSourceSharing";
const std::string ENABLE_USE_COMPILATION_CACHE_CONFIG = "useCompilationCache";

const std::string ENABLE_STATISTIC_OUTPUT_CONFIG = "enableStatisticOutput";
const std::string NUMBER_OF_BUFFERS_PER_EPOCH = "numberOfBuffersPerEpoch";
const std::string NUM_WORKER_THREADS_CONFIG = "numWorkerThreads";
const std::string OPTIMIZER_CONFIG = "optimizer";
const std::string WORKER_CONFIG = "worker";
const std::string WORKER_CONFIG_PATH = "workerConfigPath";
const std::string CONFIG_PATH = "configPath";
const std::string SENDER_HIGH_WATERMARK = "networkSenderHighWatermark";
const std::string REST_SERVER_CORS_ORIGIN = "restServerCorsAllowedOrigin";

//Configurations for the hash table
const std::string STREAM_HASH_JOIN_NUMBER_OF_PARTITIONS_CONFIG = "numberOfPartitions";
const std::string STREAM_HASH_JOIN_PAGE_SIZE_CONFIG = "pageSize";
const std::string STREAM_HASH_JOIN_PREALLOC_PAGE_COUNT_CONFIG = "preAllocPageCnt";
const std::string STREAM_HASH_JOIN_MAX_HASH_TABLE_SIZE_CONFIG = "maxHashTableSize";

//Configuration for joins
const std::string JOIN_STRATEGY = "joinStrategy";

//Optimizer Configurations
const std::string PLACEMENT_AMENDMENT_MODE_CONFIG = "placementAmendmentMode";
const std::string PLACEMENT_AMENDMENT_THREAD_COUNT = "placementAmendmentThreadCount";
const std::string DISTRIBUTED_JOIN_OPTIMIZATION_MODE_CONFIG = "distributedJoinOptimizationMode";
const std::string MEMORY_LAYOUT_POLICY_CONFIG = "memoryLayoutPolicy";
const std::string PERFORM_ONLY_SOURCE_OPERATOR_EXPANSION = "performOnlySourceOperatorExpansion";
const std::string ENABLE_INCREMENTAL_PLACEMENT = "enableIncrementalPlacement";
const std::string QUERY_BATCH_SIZE_CONFIG = "queryBatchSize";
const std::string QUERY_MERGER_RULE_CONFIG = "queryMergerRule";
const std::string ALLOW_EXHAUSTIVE_CONTAINMENT_CHECK = "allowExhaustiveContainmentCheck";
const std::string PERFORM_ADVANCE_SEMANTIC_VALIDATION = "advanceSemanticValidation";
const std::string ENABLE_NEMO_PLACEMENT = "enableNemoPlacement";

//Elegant Configurations
const auto ELEGANT = "elegant"s;                                // elegant configurations are initialize with this constant
const auto ACCELERATE_JAVA_UDFS = "accelerateJavaUDFs"s;        // accelerate java udfs supplied in Map UDF operator
const auto PLANNER_SERVICE_URL = "plannerServiceURL"s;          // URL for ELEGANT planner
const auto ACCELERATION_SERVICE_URL = "accelerationServiceURL"s;// URL for acceleration service
const auto TRANSFER_RATE = "transferRate"s;                     // Fake transfer rate between two workers

//Worker Configuration Names
const std::string WORKER_ID = "workerId";
const std::string COORDINATOR_PORT_CONFIG = "coordinatorPort";//needs to be same as RPC Port of Coordinator
const std::string LOCAL_WORKER_HOST_CONFIG = "localWorkerHost";
const std::string PARENT_ID_CONFIG = "parentId";
const std::string QUERY_COMPILER_TYPE_CONFIG = "queryCompilerType";
const std::string QUERY_COMPILER_DUMP_MODE = "queryCompilerDumpMode";
const std::string QUERY_COMPILER_NAUTILUS_BACKEND_CONFIG = "queryCompilerNautilusBackendConfig";
const std::string QUERY_COMPILER_COMPILATION_STRATEGY_CONFIG = "compilationStrategy";
const std::string QUERY_COMPILER_PIPELINING_STRATEGY_CONFIG = "pipeliningStrategy";
const std::string QUERY_COMPILER_OUTPUT_BUFFER_OPTIMIZATION_CONFIG = "outputBufferOptimizationLevel";
const std::string QUERY_COMPILER_WINDOWING_STRATEGY_CONFIG = "windowingStrategy";
const std::string SOURCE_PIN_LIST_CONFIG = "sourcePinList";
const std::string WORKER_PIN_LIST_CONFIG = "workerPinList";
const std::string QUEUE_PIN_LIST_CONFIG = "queuePinList";
const std::string LOCATION_COORDINATES_CONFIG = "fieldNodeLocationCoordinates";
const std::string CONNECT_SINKS_ASYNC = "connectSinksAsync";
const std::string CONNECT_SOURCE_ASYNC = "connectSourceEventChannelsAsync";

// CUDA config names
const std::string CUDA_SDK_PATH = "cudaSdkPath";

const std::string NUMA_AWARENESS_CONFIG = "numaAwareness";
const std::string PHYSICAL_SOURCES = "physicalSources";
const std::string PHYSICAL_SOURCE_TYPE_CONFIGURATION = "configuration";
const std::string QUERY_COMPILER_CONFIG = "queryCompiler";
const std::string HEALTH_CHECK_WAIT_TIME = "healthCheckWaitTime";

//worker mobility config names
const std::string MOBILITY_CONFIG_CONFIG = "mobility";
const std::string SPATIAL_TYPE_CONFIG = "nodeSpatialType";
const std::string PATH_PREDICTION_UPDATE_INTERVAL_CONFIG = "pathPredictionUpdateInterval";
const std::string LOCATION_BUFFER_SIZE_CONFIG = "locationBufferSize";
const std::string LOCATION_BUFFER_SAVE_RATE_CONFIG = "locationBufferSaveRate";
const std::string PATH_DISTANCE_DELTA_CONFIG = "pathDistanceDelta";
const std::string NODE_INFO_DOWNLOAD_RADIUS_CONFIG = "nodeInfoDownloadRadius";
const std::string NODE_INDEX_UPDATE_THRESHOLD_CONFIG = "nodeIndexUpdateThreshold";
const std::string DEFAULT_COVERAGE_RADIUS_CONFIG = "defaultCoverageRadius";
const std::string PATH_PREDICTION_LENGTH_CONFIG = "pathPredictionLength";
const std::string SPEED_DIFFERENCE_THRESHOLD_FACTOR_CONFIG = "speedDifferenceThresholdFactor";
const std::string SEND_DEVICE_LOCATION_UPDATE_THRESHOLD_CONFIG = "sendDevicePositionUpdateThreshold";
const std::string PUSH_DEVICE_LOCATION_UPDATES_CONFIG = "pushPositionUpdates";
const std::string SEND_LOCATION_UPDATE_INTERVAL_CONFIG = "mobilityHandlerUpdateInterval";
const std::string LOCATION_PROVIDER_CONFIG = "locationProviderConfig";
const std::string LOCATION_PROVIDER_TYPE_CONFIG = "locationProviderType";
const std::string LOCATION_SIMULATED_START_TIME_CONFIG = "locationProviderSimulatedStartTime";

//Different Source Types supported in NES
const std::string SENSE_SOURCE_CONFIG = "SenseSource";
const std::string CSV_SOURCE_CONFIG = "CSVSource";
const std::string BINARY_SOURCE_CONFIG = "BinarySource";
const std::string MQTT_SOURCE_CONFIG = "MQTTSource";
const std::string KAFKA_SOURCE_CONFIG = "KafkaSource";
const std::string OPC_SOURCE_CONFIG = "OPCSource";
const std::string DEFAULT_SOURCE_CONFIG = "DefaultSource";
const std::string TCP_SOURCE_CONFIG = "TCPSource";
const std::string ARROW_SOURCE_CONFIG = "ArrowSource";

const std::string PHYSICAL_SOURCE_NAME_CONFIG = "physicalSourceName";
const std::string LOGICAL_SOURCE_NAME_CONFIG = "logicalSourceName";

//Configuration names for source types
const std::string SOURCE_TYPE_CONFIG = "type";
const std::string NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG = "numberOfBuffersToProduce";
const std::string NUMBER_OF_TUPLES_TO_PRODUCE_PER_BUFFER_CONFIG = "numberOfTuplesToProducePerBuffer";
const std::string SOURCE_GATHERING_INTERVAL_CONFIG = "sourceGatheringInterval";
const std::string INPUT_FORMAT_CONFIG = "inputFormat";
const std::string UDFS_CONFIG = "udfs";
const std::string FILE_PATH_CONFIG = "filePath";

const std::string SKIP_HEADER_CONFIG = "skipHeader";
const std::string DELIMITER_CONFIG = "delimiter";
const std::string SOURCE_GATHERING_MODE_CONFIG = "sourceGatheringMode";

const std::string URL_CONFIG = "url";
const std::string CLIENT_ID_CONFIG = "clientId";
const std::string USER_NAME_CONFIG = "userName";
const std::string TOPIC_CONFIG = "topic";
const std::string OFFSET_MODE_CONFIG = "offsetMode";
const std::string QOS_CONFIG = "qos";
const std::string CLEAN_SESSION_CONFIG = "cleanSession";
const std::string FLUSH_INTERVAL_MS_CONFIG = "flushIntervalMS";

const std::string BROKERS_CONFIG = "brokers";
const std::string AUTO_COMMIT_CONFIG = "autoCommit";
const std::string GROUP_ID_CONFIG = "groupId";
const std::string CONNECTION_TIMEOUT_CONFIG = "connectionTimeout";
const std::string NUMBER_OF_BUFFER_TO_PRODUCE = "numberOfBuffersToProduce";
const std::string BATCH_SIZE = "batchSize";
const std::string NAME_SPACE_INDEX_CONFIG = "namespaceIndex";

const std::string NODE_IDENTIFIER_CONFIG = "nodeIdentifier";
const std::string PASSWORD_CONFIG = "password";

const std::string SOURCE_CONFIG_PATH_CONFIG = "sourceConfigPath";

const std::string TENSORFLOW_SUPPORTED_CONFIG = "tensorflowSupported";

//TCPSourceType configs
const std::string SOCKET_HOST_CONFIG = "socketHost";
const std::string SOCKET_PORT_CONFIG = "socketPort";
const std::string SOCKET_DOMAIN_CONFIG = "socketDomain";
const std::string SOCKET_TYPE_CONFIG = "socketType";
const std::string DECIDE_MESSAGE_SIZE_CONFIG = "decideMessageSize";
const std::string TUPLE_SEPARATOR_CONFIG = "tupleSeparator";
const std::string SOCKET_BUFFER_SIZE_CONFIG = "socketBufferSize";
const std::string BYTES_USED_FOR_SOCKET_BUFFER_SIZE_TRANSFER_CONFIG = "bytesUsedForSocketBufferSizeTransfer";
const std::string PERSISTENT_TCP_SOURCE = "persistentTcpSource";
const std::string ADD_INGESTION_TIME = "addIngestionTime";

//Runtime configuration
const std::string NUMBER_OF_QUEUES = "numberOfQueues";
const std::string NUMBER_OF_THREAD_PER_QUEUE = "numberOfThreadsPerQueue";
const std::string QUERY_MANAGER_MODE = "queryManagerMode";

// Logical source configurations
const std::string LOGICAL_SOURCE_SCHEMA_FIELDS_CONFIG = "fields";
const std::string LOGICAL_SOURCE_SCHEMA_FIELD_NAME_CONFIG = "name";
const std::string LOGICAL_SOURCE_SCHEMA_FIELD_TYPE_CONFIG = "type";
const std::string LOGICAL_SOURCE_SCHEMA_FIELD_TYPE_LENGTH = "length";

// Synopses Configurations
const std::string SYNOPSIS_CONFIG_TYPE = "synopsisType";
const std::string SYNOPSIS_CONFIG_WIDTH = "synopsisWidth";
const std::string SYNOPSIS_CONFIG_HEIGHT = "synopsisHeight";
const std::string SYNOPSIS_CONFIG_WINDOWSIZE = "synopsisWindowSize";

}// namespace NES::Configurations
#endif// NES_CONFIGURATIONS_INCLUDE_CONFIGURATIONS_CONFIGURATIONSNAMES_HPP_
