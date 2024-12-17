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
#pragma once
#include <cstdint>
#include <string>

using namespace std::string_literals;

namespace NES::Configurations
{

enum class InputFormat : uint8_t
{
    CSV
};

///Coordinator Configuration Names
const std::string REST_PORT_CONFIG = "restPort";
const std::string RPC_PORT_CONFIG = "rpcPort"; ///used to be coordinator port, renamed to uniform naming
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
const std::string ENABLE_NEW_REQUEST_EXECUTOR_CONFIG = "enableNewRequestExecutor";
const std::string REQUEST_EXECUTOR_THREAD_CONFIG = "numOfRequestExecutorThread";
const std::string ENABLE_USE_COMPILATION_CACHE_CONFIG = "useCompilationCache";

const std::string ENABLE_STATISTIC_OUTPUT_CONFIG = "enableStatisticOutput";
const std::string NUMBER_OF_WORKER_THREADS_CONFIG = "numberOfWorkerThreads";
const std::string OPTIMIZER_CONFIG = "optimizer";
const std::string WORKER_CONFIG = "worker";
const std::string WORKER_CONFIG_PATH = "workerConfigPath";
const std::string CONFIG_PATH = "configPath";
const std::string SENDER_HIGH_WATERMARK = "networkSenderHighWatermark";
const std::string REST_SERVER_CORS_ORIGIN = "restServerCorsAllowedOrigin";

///Configurations for the hash table
const std::string STREAM_JOIN_NUMBER_OF_PARTITIONS_CONFIG = "numberOfPartitions";
const std::string STREAM_JOIN_PAGE_SIZE_CONFIG = "pageSize";
const std::string STREAM_JOIN_PREALLOC_PAGE_COUNT_CONFIG = "preAllocPageCnt";
const std::string STREAM_JOIN_MAX_HASH_TABLE_SIZE_CONFIG = "maxHashTableSize";

///Configuration for joins
const std::string JOIN_STRATEGY = "joinStrategy";
const std::string SLICE_STORE_TYPE = "sliceStoreType";

//Configuration for the slice cache
const std::string SLICE_CACHE_TYPE = "sliceCacheType";
const std::string SLICE_CACHE_SIZE = "sliceCacheSize";
const std::string LOCK_SLICE_CACHE = "lockSliceCache";

// Configuration for the sort buffer operator
const std::string SORT_BUFFER_BY_FIELD = "sortBufferByField";
const std::string SORT_BUFFER_ORDER = "sortBufferOrder";

//Configuration for delay
const std::string UNORDEREDNESS = "unorderedness";
const std::string MIN_DELAY = "minDelay";
const std::string MAX_DELAY = "maxDelay";

///Optimizer Configurations
const std::string PLACEMENT_AMENDMENT_THREAD_COUNT = "placementAmendmentThreadCount";
const std::string PERFORM_ONLY_SOURCE_OPERATOR_EXPANSION = "performOnlySourceOperatorExpansion";
const std::string ENABLE_INCREMENTAL_PLACEMENT = "enableIncrementalPlacement";
const std::string QUERY_BATCH_SIZE_CONFIG = "queryBatchSize";
const std::string ALLOW_EXHAUSTIVE_CONTAINMENT_CHECK = "allowExhaustiveContainmentCheck";
const std::string PERFORM_ADVANCE_SEMANTIC_VALIDATION = "advanceSemanticValidation";
const std::string ENABLE_NEMO_PLACEMENT = "enableNemoPlacement";

///Worker Configuration Names
const std::string COORDINATOR_PORT_CONFIG = "coordinatorPort"; ///needs to be same as RPC Port of Coordinator
const std::string LOCAL_WORKER_HOST_CONFIG = "localWorkerHost";
const std::string QUERY_COMPILER_DUMP_MODE = "queryCompilerDumpMode";
const std::string QUERY_COMPILER_DUMP_PATH = "queryCompilerDumpPath";
const std::string QUERY_COMPILER_NAUTILUS_BACKEND_CONFIG = "nautilusBackend";
const std::string QUERY_COMPILER_COMPILATION_STRATEGY_CONFIG = "compilationStrategy";
const std::string QUERY_COMPILER_PIPELINING_STRATEGY_CONFIG = "pipeliningStrategy";
const std::string QUERY_COMPILER_OUTPUT_BUFFER_OPTIMIZATION_CONFIG = "outputBufferOptimizationLevel";
const std::string QUERY_COMPILER_WINDOWING_STRATEGY_CONFIG = "windowingStrategy";

const std::string PHYSICAL_SOURCES = "physicalSources";
const std::string PHYSICAL_SOURCE_TYPE_CONFIGURATION = "configuration";
const std::string QUERY_COMPILER_CONFIG = "queryCompiler";
const std::string HEALTH_CHECK_WAIT_TIME = "healthCheckWaitTime";

///worker mobility config names
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

///Different Source Types supported in NES
const std::string SENSE_SOURCE_CONFIG = "SenseSource";
const std::string CSV_SOURCE_CONFIG = "SourceCSV";
const std::string BINARY_SOURCE_CONFIG = "BinarySource";
const std::string MQTT_SOURCE_CONFIG = "MQTTSource";
const std::string KAFKA_SOURCE_CONFIG = "KafkaSource";
const std::string OPC_SOURCE_CONFIG = "OPCSource";
const std::string DEFAULT_SOURCE_CONFIG = "DefaultSource";
const std::string TCP_SOURCE_CONFIG = "TCPSource";
const std::string ARROW_SOURCE_CONFIG = "ArrowSource";

const std::string LOGICAL_SOURCE_NAME_CONFIG = "logicalSourceName";

///Configuration names for source types
const std::string SOURCE_TYPE_CONFIG = "type";
const std::string NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG = "numberOfBuffersToProduce";
const std::string NUMBER_OF_TUPLES_TO_PRODUCE_PER_BUFFER_CONFIG = "numberOfTuplesToProducePerBuffer";
const std::string INPUT_FORMAT_CONFIG = "inputFormat";
const std::string FILE_PATH_CONFIG = "filePath";

const std::string SKIP_HEADER_CONFIG = "skipHeader";
const std::string DELIMITER_CONFIG = "delimiter";

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

///TCPSourceType configs
const std::string SOCKET_HOST_CONFIG = "socketHost";
const std::string SOCKET_PORT_CONFIG = "socketPort";
const std::string SOCKET_DOMAIN_CONFIG = "socketDomain";
const std::string SOCKET_TYPE_CONFIG = "socketType";
const std::string TUPLE_SEPARATOR_CONFIG = "tupleSeparator";
const std::string SOCKET_BUFFER_SIZE_CONFIG = "socketBufferSize";
const std::string BYTES_USED_FOR_SOCKET_BUFFER_SIZE_TRANSFER_CONFIG = "bytesUsedForSocketBufferSizeTransfer";

/// Logical source configurations
const std::string LOGICAL_SOURCE_SCHEMA_FIELDS_CONFIG = "fields";
const std::string LOGICAL_SOURCE_SCHEMA_FIELD_NAME_CONFIG = "name";
const std::string LOGICAL_SOURCE_SCHEMA_FIELD_TYPE_CONFIG = "type";
const std::string LOGICAL_SOURCE_SCHEMA_FIELD_TYPE_LENGTH = "length";

/// Synopses Configurations
const std::string SYNOPSIS_CONFIG_TYPE = "synopsisType";
const std::string SYNOPSIS_CONFIG_WIDTH = "synopsisWidth";
const std::string SYNOPSIS_CONFIG_HEIGHT = "synopsisHeight";
const std::string SYNOPSIS_CONFIG_WINDOWSIZE = "synopsisWindowSize";

}
