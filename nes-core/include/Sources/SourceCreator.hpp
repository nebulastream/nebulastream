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

#ifndef NES_CORE_INCLUDE_SOURCES_SOURCECREATOR_HPP_
#define NES_CORE_INCLUDE_SOURCES_SOURCECREATOR_HPP_

#include <Monitoring/MetricCollectors/MetricCollector.hpp>
#include <Network/NesPartition.hpp>
#include <Network/NodeLocation.hpp>
#include <Operators/LogicalOperators/Sources/CsvSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/MQTTSourceDescriptor.hpp>
#include <Sources/BenchmarkSource.hpp>
#include <Sources/DataSource.hpp>
#include <Sources/GeneratorSource.hpp>
#include <Sources/MaterializedViewSource.hpp>
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
 * @param schema of the data source
 * @param bufferManager
 * @param queryManager
 * @param operatorId
 * @return a const data source pointer
 */
DataSourcePtr
createDefaultDataSourceWithSchemaForOneBuffer(const SchemaPtr& schema,
                                              const Runtime::BufferManagerPtr& bufferManager,
                                              const Runtime::QueryManagerPtr& queryManager,
                                              OperatorId operatorId,
                                              OriginId originId,
                                              size_t numSourceLocalBuffers,
                                              const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors);

/**
 * @brief function to create a test source which produces   tuples with value one in N buffers of based on a schema
 * @param schema of the data source
 * @param bufferManager
 * @param queryManager
 * @param operatorId
 * @param number of buffers that should be produced
 * @param gatheringInterval when to gather the next buffer
 * @return a const data source pointer
 */
DataSourcePtr
createDefaultDataSourceWithSchemaForVarBuffers(const SchemaPtr& schema,
                                               const Runtime::BufferManagerPtr& bufferManager,
                                               const Runtime::QueryManagerPtr& queryManager,
                                               uint64_t numbersOfBufferToProduce,
                                               uint64_t gatheringInterval,
                                               OperatorId operatorId,
                                               OriginId originId,
                                               size_t numSourceLocalBuffers,
                                               const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors);

/**
 * @brief function to create a test source which produces 10 tuples with value one without a schema
 * @param bufferManager
 * @param queryManager
 * @param operatorId
 * @return a const data source pointer
 */
DataSourcePtr
createDefaultSourceWithoutSchemaForOneBuffer(const Runtime::BufferManagerPtr& bufferManager,
                                             const Runtime::QueryManagerPtr& queryManager,
                                             OperatorId operatorId,
                                             OriginId originId,
                                             size_t numSourceLocalBuffers,
                                             const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors);

/**
 * @brief function to create a lambda source
 * @param schema of the data source
 * @param bufferManager
 * @param queryManager
 * @param number of buffers that should be produced
 * @param frequency when to gather the next buffer
 * @param generationFunction
 * @param operatorId
 * @return a const data source pointer */
DataSourcePtr
createLambdaSource(const SchemaPtr& schema,
                   const Runtime::BufferManagerPtr& bufferManager,
                   const Runtime::QueryManagerPtr& queryManager,
                   uint64_t numbersOfBufferToProduce,
                   uint64_t gatheringValue,
                   std::function<void(NES::Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce)>&& generationFunction,
                   OperatorId operatorId,
                   OriginId originId,
                   size_t numSourceLocalBuffers,
                   GatheringMode::Value gatheringMode,
                   uint64_t sourceAffinity,
                   uint64_t taskQueueId,
                   const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors);

/**
 * @brief function to create an empty zmq source
 * @param schema of data source
 * @param bufferManager
 * @param queryManager
 * @param host
 * @param port
 * @param operatorId
 * @return a const data source pointer
 */
DataSourcePtr createZmqSource(const SchemaPtr& schema,
                              const Runtime::BufferManagerPtr& bufferManager,
                              const Runtime::QueryManagerPtr& queryManager,
                              const std::string& host,
                              uint16_t port,
                              OperatorId operatorId,
                              OriginId originId,
                              size_t numSourceLocalBuffers,
                              const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors);

/**
 * @brief function to create a binary file source
 * @param schema of data source
 * @param bufferManager
 * @param queryManager
 * @param path to the file to reading
 * @param operatorId
 * @return a const data source pointer
 */
DataSourcePtr createBinaryFileSource(const SchemaPtr& schema,
                                     const Runtime::BufferManagerPtr& bufferManager,
                                     const Runtime::QueryManagerPtr& queryManager,
                                     const std::string& pathToFile,
                                     OperatorId operatorId,
                                     OriginId originId,
                                     size_t numSourceLocalBuffers,
                                     const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors);

/**
 * @brief function to create a sense source
 * @param schema of data source
 * @param bufferManager
 * @param queryManager
 * @param udfs of the file
 * @param operatorId
 * @return a const data source pointer
 */
DataSourcePtr createSenseSource(const SchemaPtr& schema,
                                const Runtime::BufferManagerPtr& bufferManager,
                                const Runtime::QueryManagerPtr& queryManager,
                                const std::string& udfs,
                                OperatorId operatorId,
                                OriginId originId,
                                size_t numSourceLocalBuffers,
                                const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors);

/**
 * @brief function to create a csvfile source
 * @param schema of data source
 * @param bufferManager
 * @param queryManager
 * @param csvSourceType
 * @param numberOfTuplesToProducePerBuffer
 * @param numBuffersToProcess
 * @param operatorId
 * @return a const data source pointer
 */
DataSourcePtr createCSVFileSource(const SchemaPtr& schema,
                                  const Runtime::BufferManagerPtr& bufferManager,
                                  const Runtime::QueryManagerPtr& queryManager,
                                  const CSVSourceTypePtr& csvSourceType,
                                  OperatorId operatorId,
                                  OriginId originId,
                                  size_t numSourceLocalBuffers,
                                  const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors);

/**
 * @brief create a memory source
 * @param schema of the source
 * @param bufferManager
 * @param queryManager
 * @param memoryArea the memory buffer to scan and create buffers out of
 * @param memoryAreaSize the size of the memory buffer
 * @param numBuffersToProcess
 * @param frequency
 * @param operatorId
 * @return
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
                                 size_t numSourceLocalBuffers,
                                 GatheringMode::Value gatheringMode,
                                 uint64_t sourceAffinity,
                                 uint64_t taskQueueId,
                                 const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors);

/**
 * @brief create a table source
 * @param schema of the source
 * @param bufferManager
 * @param queryManager
 * @param operatorId
 * @param numBuffersToProcess
 * @param successors
 * @return
 */
namespace Experimental {
DataSourcePtr createStaticDataSource(const SchemaPtr& schema,
                                     const std::string pathTableFile,
                                     const bool lateStart,
                                     const Runtime::BufferManagerPtr& bufferManager,
                                     const Runtime::QueryManagerPtr& queryManager,
                                     OperatorId operatorId,
                                     OriginId originId,
                                     size_t numSourceLocalBuffers,
                                     const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors);
}// namespace Experimental
/**
 * @brief create a benchmark source
 * @param schema of the source
 * @param bufferManager
 * @param queryManager
 * @param memoryArea the memory buffer to scan and create buffers out of
 * @param memoryAreaSize the size of the memory buffer
 * @param numBuffersToProcess
 * @param frequency
 * @param operatorId
 * @return
 */
DataSourcePtr createBenchmarkSource(const SchemaPtr& schema,
                                    const Runtime::BufferManagerPtr& bufferManager,
                                    const Runtime::QueryManagerPtr& queryManager,
                                    const std::shared_ptr<uint8_t>& memoryArea,
                                    size_t memoryAreaSize,
                                    uint64_t numBuffersToProcess,
                                    uint64_t gatheringValue,
                                    OperatorId operatorId,
                                    OriginId originId,
                                    size_t numSourceLocalBuffers,
                                    GatheringMode::Value gatheringMode,
                                    SourceMode::Value sourceMode,
                                    uint64_t sourceAffinity,
                                    uint64_t taskQueueId,
                                    const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors);

/**
 * @brief function to create a network source
 * @param schema
 * @param bufferManager
 * @param queryManager
 * @param networkManager
 * @param nesPartition
 * @return a const data source pointer
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
                                  const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors);

/**
 * @brief function to create a monitoring source
 * @param metricCollector
 * @param waitTime
 * @param bufferManager
 * @param queryManager
 * @param operatorId
 * @param originId
 * @param numSourceLocalBuffers
 * @param successors
 * @return a const data source pointer
 */
DataSourcePtr createMonitoringSource(Monitoring::MetricCollectorPtr metricCollector,
                                     std::chrono::milliseconds waitTime,
                                     Runtime::BufferManagerPtr bufferManager,
                                     Runtime::QueryManagerPtr queryManager,
                                     OperatorId operatorId,
                                     OriginId originId,
                                     size_t numSourceLocalBuffers,
                                     std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors);

namespace Experimental::MaterializedView {

/**
 * @brief function to create a materialized view source
 * @param schema
 * @param bufferManager
 * @param queryManager
 * @param operatorId
 * @param numSourceLocalBuffers
 * @param successors
 * @param materialized view pointer
 * @return a const data source pointer
 */
DataSourcePtr createMaterializedViewSource(const SchemaPtr schema,
                                           const Runtime::BufferManagerPtr bufferManager,
                                           const Runtime::QueryManagerPtr queryManager,
                                           const OperatorId operatorId,
                                           OriginId originId,
                                           const size_t numSourceLocalBuffers,
                                           const std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors,
                                           const NES::Experimental::MaterializedView::MaterializedViewPtr view);
}// namespace Experimental::MaterializedView

#ifdef ENABLE_KAFKA_BUILD
/**
 * @brief Create kafka source
 * @param schema schema of the elements
 * @param brokers list of brokers
 * @param topic kafka topic
 * @param groupId group id
 * @param autoCommit bool indicating if offset has to be committed automatically or not
 * @param kafkaConsumerTimeout  kafka consumer timeout
 * @param operatorId: operator id
 * @return
 */
const DataSourcePtr createKafkaSource(SchemaPtr schema,
                                      Runtime::BufferManagerPtr bufferManager,
                                      Runtime::QueryManagerPtr queryManager,
                                      uint64_t numbersOfBufferToProduce,
                                      std::string brokers,
                                      std::string topic,
                                      std::string groupId,
                                      bool autoCommit,
                                      uint64_t kafkaConsumerTimeout,
                                      std::string offsetMode,
                                      OperatorId operatorId,
                                      OriginId originId,
                                      size_t numSourceLocalBuffers,
                                      size_t batchSize,
                                      const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors);
#endif
#ifdef ENABLE_OPC_BUILD

/**
 * @brief Create OPC source
 * @param schema schema of the elements
 * @param url the url of the OPC server
 * @param nodeId the node id of the desired node
 * @param user name if connecting with a server with authentication
 * @param password for authentication if needed
 * @return a const data source pointer
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
                                    std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors);
#endif

#ifdef ENABLE_MQTT_BUILD

/**
 * @brief Create MQTT source
 * @param schema schema of the elements
 * @param mqttSourceType mqttSourceType for MQTT
 * @param inputFormat format of input that we expect to receive
 * @return a const data source pointer
 */
DataSourcePtr createMQTTSource(const SchemaPtr& schema,
                               const Runtime::BufferManagerPtr& bufferManager,
                               const Runtime::QueryManagerPtr& queryManager,
                               const MQTTSourceTypePtr& mqttSourceType,
                               OperatorId operatorId,
                               OriginId originId,
                               size_t numSourceLocalBuffers,
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
 * @param numSourceLocalBuffers number of local source buffers
 * @param successors executable operators coming after this source
 * @return
 */
DataSourcePtr createTCPSource(const SchemaPtr& schema,
                              const Runtime::BufferManagerPtr& bufferManager,
                              const Runtime::QueryManagerPtr& queryManager,
                              const TCPSourceTypePtr& tcpSourceType,
                              OperatorId operatorId,
                              OriginId originId,
                              size_t numSourceLocalBuffers,
                              const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors);

}// namespace NES
#endif// NES_CORE_INCLUDE_SOURCES_SOURCECREATOR_HPP_
