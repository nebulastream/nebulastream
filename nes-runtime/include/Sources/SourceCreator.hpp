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

#ifndef NES_RUNTIME_INCLUDE_SOURCES_SOURCECREATOR_HPP_
#define NES_RUNTIME_INCLUDE_SOURCES_SOURCECREATOR_HPP_

#include <Monitoring/MetricCollectors/MetricCollector.hpp>
#include <Operators/LogicalOperators/Network/NesPartition.hpp>
#include <Operators/LogicalOperators/Network/NodeLocation.hpp>
#include <Operators/LogicalOperators/Sources/CsvSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/KafkaSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/MQTTSourceDescriptor.hpp>
#include <Sources/BenchmarkSource.hpp>
#include <Sources/DataSource.hpp>
#include <Sources/GeneratorSource.hpp>
#include <Sources/MemorySource.hpp>
#include <Sources/TCPSource.hpp>
#include <chrono>
#ifdef ENABLE_OPC_BUILD
#include <open62541/client_config_default.h>
#include <open62541/client_highlevel.h>
#include <open62541/client_subscriptions.h>
#include <open62541/plugin/log_stdout.h>
#endif

namespace NES {
/**
 * @brief function to create a test source which produces 10 tuples within one buffer with value one based on a schema
 * @param schema schema of the data source
 * @param bufferManager pointer to the buffer manager
 * @param queryManager pointer to the query manager
 * @param operatorId current operator id
 * @param originId represents the identifier of the upstream operator that represents the origin of the input stream
 * @param statisticId represents the unique identifier of components that we can track statistics for
 * @param numSourceLocalBuffers the number of buffers allocated to a source
 * @param physicalSourceName the name and unique identifier of a physical source
 * @param successors the subsequent operators in the pipeline to which the data is pushed
 * @return a data source pointer
 */
DataSourcePtr
createDefaultDataSourceWithSchemaForOneBuffer(const SchemaPtr& schema,
                                              const Runtime::BufferManagerPtr& bufferManager,
                                              const Runtime::QueryManagerPtr& queryManager,
                                              OperatorId operatorId,
                                              OriginId originId,
                                              StatisticId statisticId,
                                              size_t numSourceLocalBuffers,
                                              const std::string& physicalSourceName,
                                              const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors);

/**
 * @brief function to create a test source which produces   tuples with value one in N buffers of based on a schema
 * @param schema schema of the data source
 * @param bufferManager pointer to the buffer manager
 * @param queryManager pointer to the query manager
 * @param numberOfBuffersToProduce the number of buffers to be produced by the source
 * @param gatheringInterval the interval at which new buffers are produced
 * @param operatorId current operator id
 * @param originId represents the identifier of the upstream operator that represents the origin of the input stream
 * @param statisticId represents the unique identifier of components that we can track statistics for
 * @param numSourceLocalBuffers the number of buffers allocated to a source
 * @param physicalSourceName the name and unique identifier of a physical source
 * @param successors the subsequent operators in the pipeline to which the data is pushed
 * @return a data source pointer
 */
DataSourcePtr
createDefaultDataSourceWithSchemaForVarBuffers(const SchemaPtr& schema,
                                               const Runtime::BufferManagerPtr& bufferManager,
                                               const Runtime::QueryManagerPtr& queryManager,
                                               uint64_t numberOfBuffersToProduce,
                                               uint64_t gatheringInterval,
                                               OperatorId operatorId,
                                               OriginId originId,
                                               StatisticId statisticId,
                                               size_t numSourceLocalBuffers,
                                               const std::string& physicalSourceName,
                                               const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors);

/**
 * @brief function to create a test source which produces 10 tuples with value one without a schema
 * @param bufferManager pointer to the buffer manager
 * @param queryManager pointer to the query manager
 * @param operatorId current operator id
 * @param originId represents the identifier of the upstream operator that represents the origin of the input stream
 * @param statisticId represents the unique identifier of components that we can track statistics for
 * @param numSourceLocalBuffers the number of buffers allocated to a source
 * @param successors the subsequent operators in the pipeline to which the data is pushed
 * @return a data source pointer
 */
DataSourcePtr
createDefaultSourceWithoutSchemaForOneBuffer(const Runtime::BufferManagerPtr& bufferManager,
                                             const Runtime::QueryManagerPtr& queryManager,
                                             OperatorId operatorId,
                                             OriginId originId,
                                             StatisticId statisticId,
                                             size_t numSourceLocalBuffers,
                                             const std::string& physicalSourceName,
                                             const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors);

/**
 * @brief function to create a lambda source
 * @param schema schema of the data source
 * @param bufferManager pointer to the buffer manager
 * @param queryManager pointer to the query manager
 * @param numberOfBuffersToProduce the number of buffers to be produced by the source
 * @param gatheringValue how many tuples to collect per interval
 * @param generationFunction that creates with which the data is created
 * @param operatorId current operator id
 * @param originId represents the identifier of the upstream operator that represents the origin of the input stream
* @param statisticId represents the unique identifier of components that we can track statistics for
 * @param numSourceLocalBuffers the number of buffers allocated to a source
 * @param gatheringMode the gathering mode (INTERVAL_MODE, INGESTION_RATE_MODE, or ADAPTIVE_MODE)
 * @param sourceAffinity the subsequent operators in the pipeline to which the data is pushed
 * @param taskQueueId the ID of the queue to which the task is pushed
 * @param physicalSourceName the name and unique identifier of a physical source
 * @param successors the subsequent operators in the pipeline to which the data is pushed
 * @return a data source pointer
 */
DataSourcePtr
createLambdaSource(const SchemaPtr& schema,
                   const Runtime::BufferManagerPtr& bufferManager,
                   const Runtime::QueryManagerPtr& queryManager,
                   uint64_t numberOfBuffersToProduce,
                   uint64_t gatheringValue,
                   std::function<void(NES::Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce)>&& generationFunction,
                   OperatorId operatorId,
                   OriginId originId,
                   StatisticId statisticId,
                   size_t numSourceLocalBuffers,
                   GatheringMode gatheringMode,
                   uint64_t sourceAffinity,
                   uint64_t taskQueueId,
                   const std::string& physicalSourceName,
                   const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors);

/**
 * @brief function to create an empty zmq source
 * @param schema schema of data source
 * @param bufferManager pointer to the buffer manager
 * @param queryManager pointer to the query manager
 * @param host the name of the host
 * @param port the port through which to connect to the ZMQSource(ZMQSource)
 * @param operatorId current operator id
 * @param originId represents the identifier of the upstream operator that represents the origin of the input stream
* @param statisticId represents the unique identifier of components that we can track statistics for
 * @param numSourceLocalBuffers the number of buffers allocated to a source
 * @param physicalSourceName the name and unique identifier of a physical source
 * @param successors the subsequent operators in the pipeline to which the data is pushed
 * @return a data source pointer
 */
DataSourcePtr createZmqSource(const SchemaPtr& schema,
                              const Runtime::BufferManagerPtr& bufferManager,
                              const Runtime::QueryManagerPtr& queryManager,
                              const std::string& host,
                              uint16_t port,
                              OperatorId operatorId,
                              OriginId originId,
                              StatisticId statisticId,
                              size_t numSourceLocalBuffers,
                              const std::string& physicalSourceName,
                              const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors);

/**
 * @brief function to create a binary file source
 * @param schema schema of data source
 * @param bufferManager pointer to the buffer manager
 * @param queryManager pointer to the query manager
 * @param pathToFile path to the binary file
 * @param operatorId current operator id
 * @param originId represents the identifier of the upstream operator that represents the origin of the input stream
* @param statisticId represents the unique identifier of components that we can track statistics for
 * @param numSourceLocalBuffers the number of buffers allocated to a source
 * @param physicalSourceName the name and unique identifier of a physical source
 * @param successors the subsequent operators in the pipeline to which the data is pushed
 * @return a data source pointer
 */
DataSourcePtr createBinaryFileSource(const SchemaPtr& schema,
                                     const Runtime::BufferManagerPtr& bufferManager,
                                     const Runtime::QueryManagerPtr& queryManager,
                                     const std::string& pathToFile,
                                     OperatorId operatorId,
                                     OriginId originId,
                                     StatisticId statisticId,
                                     size_t numSourceLocalBuffers,
                                     const std::string& physicalSourceName,
                                     const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors);

/**
 * @brief function to create a sense source
 * @param schema schema of data source
 * @param bufferManager pointer to the buffer manager
 * @param queryManager pointer to the query manager
 * @param udfs of the file
 * @param operatorId current operator id
 * @param originId represents the identifier of the upstream operator that represents the origin of the input stream
* @param statisticId represents the unique identifier of components that we can track statistics for
 * @param numSourceLocalBuffers the number of buffers allocated to a source
 * @param physicalSourceName the name and unique identifier of a physical source
 * @param successors the subsequent operators in the pipeline to which the data is pushed
 * @return a data source pointer
 */
DataSourcePtr createSenseSource(const SchemaPtr& schema,
                                const Runtime::BufferManagerPtr& bufferManager,
                                const Runtime::QueryManagerPtr& queryManager,
                                const std::string& udfs,
                                OperatorId operatorId,
                                OriginId originId,
                                StatisticId statisticId,
                                size_t numSourceLocalBuffers,
                                const std::string& physicalSourceName,
                                const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors);

/**
 * @brief function to create a csvfile source
 * @param schema schema of data source
 * @param bufferManager pointer to the buffer manager
 * @param queryManager pointer to the query manager
 * @param csvSourceType points to the current source configuration object
 * @param operatorId current operator id
 * @param originId represents the identifier of the upstream operator that represents the origin of the input stream
* @param statisticId represents the unique identifier of components that we can track statistics for
 * @param numSourceLocalBuffers the number of buffers allocated to a source
 * @param physicalSourceName the name and unique identifier of a physical source
 * @param successors the subsequent operators in the pipeline to which the data is pushed
 * @return a data source pointer
 */
DataSourcePtr createCSVFileSource(const SchemaPtr& schema,
                                  const Runtime::BufferManagerPtr& bufferManager,
                                  const Runtime::QueryManagerPtr& queryManager,
                                  const CSVSourceTypePtr& csvSourceType,
                                  OperatorId operatorId,
                                  OriginId originId,
                                  StatisticId statisticId,
                                  size_t numSourceLocalBuffers,
                                  const std::string& physicalSourceName,
                                  const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors,
                                  bool shouldDelay);

/**
 * @brief function which creates a memory source
 * @param schema the schema of the source
 * @param bufferManager pointer to the buffer manager
 * @param queryManager pointer to the query manager
 * @param memoryArea the memory buffer to scan and create buffers out of
 * @param memoryAreaSize the size of the memory buffer
 * @param numBuffersToProcess
 * @param gatheringValue how many tuples to collect per interval
 * @param operatorId current operator id
 * @param originId represents the identifier of the upstream operator that represents the origin of the input stream
* @param statisticId represents the unique identifier of components that we can track statistics for
 * @param numSourceLocalBuffers the number of buffers allocated to a source
 * @param gatheringMode the gathering mode (INTERVAL_MODE, INGESTION_RATE_MODE, or ADAPTIVE_MODE)
 * @param sourceAffinity the subsequent operators in the pipeline to which the data is pushed
 * @param taskQueueId the ID of the queue to which the task is pushed
 * @param physicalSourceName the name and unique identifier of a physical source
 * @param successors the subsequent operators in the pipeline to which the data is pushed
 * @return a data source pointer
 */
DataSourcePtr createMemorySource(const SchemaPtr& schema,
                                 const Runtime::BufferManagerPtr& bufferManager,
                                 const Runtime::QueryManagerPtr& queryManager,
                                 const std::shared_ptr<uint8_t>& memoryArea,
                                 size_t memoryAreaSize,
                                 uint64_t numBuffersToProcess,
                                 uint64_t gatheringValue,
                                 OperatorId operatorId,
                                 OriginId originId,
                                 StatisticId statisticId,
                                 size_t numSourceLocalBuffers,
                                 GatheringMode gatheringMode,
                                 uint64_t sourceAffinity,
                                 uint64_t taskQueueId,
                                 const std::string& physicalSourceName,
                                 const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors);

/**
 * @brief create a table source
 * @param schema the schema of the source
 * @param pathTableFile
 * @param bufferManager pointer to the buffer manager
 * @param queryManager pointer to the query manager
 * @param operatorId current operator id
 * @param originId represents the identifier of the upstream operator that represents the origin of the input stream
* @param statisticId represents the unique identifier of components that we can track statistics for
 * @param numSourceLocalBuffers the number of buffers allocated to a source
 * @param physicalSourceName the name and unique identifier of a physical source
 * @param successors the subsequent operators in the pipeline to which the data is pushed
 * @return a data source pointer
 */
namespace Experimental {
DataSourcePtr createStaticDataSource(const SchemaPtr& schema,
                                     const std::string pathTableFile,
                                     const bool lateStart,
                                     const Runtime::BufferManagerPtr& bufferManager,
                                     const Runtime::QueryManagerPtr& queryManager,
                                     OperatorId operatorId,
                                     OriginId originId,
                                     StatisticId statisticId,
                                     size_t numSourceLocalBuffers,
                                     const std::string& physicalSourceName,
                                     const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors);
}// namespace Experimental
/**
 * @brief create a benchmark source
 * @param schema the schema of the source
 * @param bufferManager pointer to the buffer manager
 * @param queryManager pointer to the query manager
 * @param memoryArea the memory buffer to scan and create buffers out of
 * @param memoryAreaSize the size of the memory buffer
 * @param numberOfBuffersToProcess
 * @param gatheringValue how many tuples to collect per interval
 * @param operatorId current operator id
 * @param originId represents the identifier of the upstream operator that represents the origin of the input stream
 * @param statisticId represents the unique identifier of components that we can track statistics for
 * @param numSourceLocalBuffers the number of buffers allocated to a source
 * @param gatheringMode the gathering mode (INTERVAL_MODE, INGESTION_RATE_MODE, or ADAPTIVE_MODE)
 * @param sourceMode
 * @param sourceAffinity the subsequent operators in the pipeline to which the data is pushed
 * @param taskQueueId the ID of the queue to which the task is pushed
 * @param physicalSourceName the name and unique identifier of a physical source
 * @param successors the subsequent operators in the pipeline to which the data is pushed
 * @return a data source pointer
 */
DataSourcePtr createBenchmarkSource(const SchemaPtr& schema,
                                    const Runtime::BufferManagerPtr& bufferManager,
                                    const Runtime::QueryManagerPtr& queryManager,
                                    const std::shared_ptr<uint8_t>& memoryArea,
                                    size_t memoryAreaSize,
                                    uint64_t numberOfBuffersToProcess,
                                    uint64_t gatheringValue,
                                    OperatorId operatorId,
                                    OriginId originId,
                                    StatisticId statisticId,
                                    size_t numSourceLocalBuffers,
                                    GatheringMode gatheringMode,
                                    SourceMode sourceMode,
                                    uint64_t sourceAffinity,
                                    uint64_t taskQueueId,
                                    const std::string& physicalSourceName,
                                    const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors);

/**
 * @brief function to create a network source
 * @param schema the schema of the source
 * @param bufferManager a pointer to the buffer manager
 * @param queryManager a pointer to the query manager
 * @param networkManager a pointer to the network manager
 * @param nesPartition the unique identifier of the network source that consumes the input buffers
 * @param sinkLocation location of the sink that writes to the network source
 * @param numSourceLocalBuffers the number of buffers allocated to a source
 * @param waitTime the amount of time the networkSource is supposed to wait for a reply
 * @param retryTimes the number of times the networkSource resends a message, if it does not receive a reply within waitTime
 * @param physicalSourceName the name and unique identifier of a physical source
 * @param version the version number of the source at its time of creation
 * @param successors the subsequent operators in the pipeline to which the data is pushed
 * @param uniqueNetworkSourceId a unique identifier of the network source which does not change over its lifetime
 * @return a data source pointer
 */
DataSourcePtr createNetworkSource(const SchemaPtr& schema,
                                  const Runtime::BufferManagerPtr& bufferManager,
                                  const Runtime::QueryManagerPtr& queryManager,
                                  const Network::NetworkManagerPtr& networkManager,
                                  Network::NesPartition nesPartition,
                                  Network::NodeLocation sinkLocation,
                                  size_t numSourceLocalBuffers,
                                  std::chrono::milliseconds waitTime,
                                  uint8_t retryTimes,
                                  const std::string& physicalSourceName,
                                  DecomposedQueryPlanVersion version,
                                  const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors,
                                  OperatorId uniqueNetworkSourceId);

/**
 * @brief function to create a monitoring source
 * @param metricCollector a pointer to a NES::Monitoring::MetricCollector
 * @param waitTime the amount of time to wait
 * @param bufferManager a pointer to the buffer manager
 * @param queryManager a pointer to the query manager
 * @param operatorId current operator id
 * @param originId represents the identifier of the upstream operator that represents the origin of the input stream
 * @param statisticId represents the unique identifier of components that we can track statistics for
 * @param numSourceLocalBuffers the number of buffers allocated to a source
 * @param physicalSourceName the name and unique identifier of a physical source
 * @param successors the subsequent operators in the pipeline to which the data is pushed
 * @return a data source pointer
 */
DataSourcePtr createMonitoringSource(Monitoring::MetricCollectorPtr metricCollector,
                                     std::chrono::milliseconds waitTime,
                                     Runtime::BufferManagerPtr bufferManager,
                                     Runtime::QueryManagerPtr queryManager,
                                     OperatorId operatorId,
                                     OriginId originId,
                                     StatisticId statisticId,
                                     size_t numSourceLocalBuffers,
                                     const std::string& physicalSourceName,
                                     std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors);

#ifdef ENABLE_KAFKA_BUILD
/**
 * @brief a function which creates a kafka source
 * @param schema schema of the elements
 * @param bufferManager a pointer to the buffer manager
 * @param queryManager a pointer to the query manager
 * @param numberOfBuffersToProduce the number of buffers to be produced by the source
 * @param brokers list of brokers
 * @param topic the kafka topic which organizes tuples of the same entity. Usually the name of a data stream
 * @param groupId the ID of a logical group which consumes messages from one or more kafka topics
 * @param autoCommit bool indicating if offset has to be committed automatically or not
 * @param kafkaConsumerTimeout the timeperiod after which a timeout is issued for the consumer
 * @param offsetMode instructs the broker from which point of a topic a consumer wants consume messages from
 * @param kafkaSourceType
 * @param operatorId operator id
 * @param originId represents the identifier of the upstream operator that represents the origin of the input stream
 * @param statisticId represents the unique identifier of components that we can track statistics for
 * @param numSourceLocalBuffers the number of buffers allocated to a source
 * @param batchSize the maximum amount of data (in bytes) that a Kafka producer can accumulate before sending a batch of messages to the Kafka
 * @param physicalSourceName the name and unique identifier of a physical source
 * @param successors the subsequent operators in the pipeline to which the data is pushed
 * @return a data source pointer
 */
const DataSourcePtr createKafkaSource(SchemaPtr schema,
                                      Runtime::BufferManagerPtr bufferManager,
                                      Runtime::QueryManagerPtr queryManager,
                                      uint64_t numberOfBuffersToProduce,
                                      std::string brokers,
                                      std::string topic,
                                      std::string groupId,
                                      bool autoCommit,
                                      uint64_t kafkaConsumerTimeout,
                                      std::string offsetMode,
                                      const KafkaSourceTypePtr& kafkaSourceType,
                                      OperatorId operatorId,
                                      OriginId originId,
                                      StatisticId statisticId,
                                      size_t numSourceLocalBuffers,
                                      size_t batchSize,
                                      const std::string& physicalSourceName,
                                      const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors);
#endif
#ifdef ENABLE_OPC_BUILD

/**
 * @brief function which creates an OPC source
 * @param schema schema of the elements
 * @param bufferManager a pointer to the buffer manager
 * @param queryManager a pointer to the query manager
 * @param url the url of the OPC server
 * @param nodeId the node id of the desired node
 * @param user name if connecting with a server with authentication
 * @param password for authentication if needed
 * @param operatorId operator id
 * @param numSourceLocalBuffers the number of buffers allocated to a source
 * @param physicalSourceName the name and unique identifier of a physical source
 * @param successors the subsequent operators in the pipeline to which the data is pushed
 * @return a data source pointer
 */
const DataSourcePtr createOPCSource(SchemaPtr schema,
                                    Runtime::BufferManagerPtr bufferManager,
                                    Runtime::QueryManagerPtr queryManager,
                                    std::string url,
                                    UA_NodeId nodeId,
                                    std::string user,
                                    std::string password,
                                    OperatorId operatorId,
                                    size_t numSourceLocalBuffers,
                                    const std::string& physicalSourceName,
                                    std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors);
#endif

#ifdef ENABLE_MQTT_BUILD

/**
 * @brief function which creates a MQTT source
 * @param schema schema of the elements
 * @param bufferManager a pointer to the buffer manager
 * @param queryManager a pointer to the query manager
 * @param mqttSourceType mqttSourceType for MQTT
 * @param operatorId current operator id
 * @param originId represents the identifier of the upstream operator that represents the origin of the input stream
 * @param statisticId represents the unique identifier of components that we can track statistics for
 * @param numSourceLocalBuffers the number of buffers allocated to a source
 * @param physicalSourceName the name and unique identifier of a physical source
 * @param successors the subsequent operators in the pipeline to which the data is pushed
 * @return a data source pointer
 */
DataSourcePtr createMQTTSource(const SchemaPtr& schema,
                               const Runtime::BufferManagerPtr& bufferManager,
                               const Runtime::QueryManagerPtr& queryManager,
                               const MQTTSourceTypePtr& mqttSourceType,
                               OperatorId operatorId,
                               OriginId originId,
                               StatisticId statisticId,
                               size_t numSourceLocalBuffers,
                               const std::string& physicalSourceName,
                               const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors);
#endif

/**
 * function to create a TCP source
 * @param schema of this data source
 * @param bufferManager The BufferManager is responsible for: 1. Pooled Buffers: preallocated fixed-size buffers of memory that
 * must be reference counted 2. Unpooled Buffers: variable sized buffers that are allocated on-the-fly.
 * They are also subject to reference counting.
 * @param queryManager comes with functionality to manage the queries
 * @param tcpSourceType see TCPSourceType.hpp for information on this object
 * @param operatorId represents a locally running query execution plan
 * @param originId represents an origin
 * @param statisticId represents the unique identifier of components that we can track statistics for
 * @param numSourceLocalBuffers number of local source buffers
 * @param physicalSourceName the name and unique identifier of a physical source
 * @param successors executable operators coming after this source
 * @return a data source pointer
 */
DataSourcePtr createTCPSource(const SchemaPtr& schema,
                              const Runtime::BufferManagerPtr& bufferManager,
                              const Runtime::QueryManagerPtr& queryManager,
                              const TCPSourceTypePtr& tcpSourceType,
                              OperatorId operatorId,
                              OriginId originId,
                              StatisticId statisticId,
                              size_t numSourceLocalBuffers,
                              const std::string& physicalSourceName,
                              const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors);

}// namespace NES
#endif// NES_RUNTIME_INCLUDE_SOURCES_SOURCECREATOR_HPP_
