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

#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Network/NetworkSource.hpp>
#include <Runtime/QueryManager.hpp>
#include <Sources/BinarySource.hpp>
#include <Sources/CSVSource.hpp>
#include <Sources/DataSource.hpp>
#include <Sources/DefaultSource.hpp>
#include <Sources/KafkaSource.hpp>
#include <Sources/LambdaSource.hpp>
#include <Sources/MemorySource.hpp>
#include <Sources/MonitoringSource.hpp>
#include <Sources/SenseSource.hpp>
#include <Sources/SourceCreator.hpp>
#include <Sources/StaticDataSource.hpp>
#include <Sources/TCPSource.hpp>
#include <Sources/ZmqSource.hpp>
#include <chrono>

#ifdef ENABLE_OPC_BUILD
#include <Sources/OPCSource.hpp>
#include <open62541/client_config_default.h>
#include <open62541/client_highlevel.h>
#include <open62541/plugin/log_stdout.h>
#endif
#ifdef ENABLE_MQTT_BUILD
#include <Sources/MQTTSource.hpp>
#endif

namespace NES {

DataSourcePtr
createDefaultDataSourceWithSchemaForOneBuffer(const SchemaPtr& schema,
                                              const Runtime::BufferManagerPtr& bufferManager,
                                              const Runtime::QueryManagerPtr& queryManager,
                                              OperatorId operatorId,
                                              OriginId originId,
                                              StatisticId statisticId,
                                              size_t numSourceLocalBuffers,
                                              const std::string& physicalSourceName,
                                              const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors) {
    return std::make_shared<DefaultSource>(schema,
                                           bufferManager,
                                           queryManager,
                                           /*bufferCnt*/ 1,
                                           /*frequency*/ 1000,
                                           operatorId,
                                           originId,
                                           statisticId,
                                           numSourceLocalBuffers,
                                           successors,
                                           physicalSourceName);
}

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
                                               const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors) {
    return std::make_shared<DefaultSource>(schema,
                                           bufferManager,
                                           queryManager,
                                           numberOfBuffersToProduce,
                                           gatheringInterval,
                                           operatorId,
                                           originId,
                                           statisticId,
                                           numSourceLocalBuffers,
                                           successors,
                                           physicalSourceName);
}

DataSourcePtr
createDefaultSourceWithoutSchemaForOneBuffer(const Runtime::BufferManagerPtr& bufferManager,
                                             const Runtime::QueryManagerPtr& queryManager,
                                             OperatorId operatorId,
                                             OriginId originId,
                                             StatisticId statisticId,
                                             size_t numSourceLocalBuffers,
                                             const std::string& physicalSourceName,
                                             const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors) {
    return std::make_shared<DefaultSource>(Schema::create()->addField("id", DataTypeFactory::createUInt64()),
                                           bufferManager,
                                           queryManager,
                                           /**bufferCnt*/ 1,
                                           /*frequency*/ 1000,
                                           operatorId,
                                           originId,
                                           statisticId,
                                           numSourceLocalBuffers,
                                           successors,
                                           physicalSourceName);
}

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
                   const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors) {
    return std::make_shared<LambdaSource>(schema,
                                          bufferManager,
                                          queryManager,
                                          numberOfBuffersToProduce,
                                          gatheringValue,
                                          std::move(generationFunction),
                                          operatorId,
                                          originId,
                                          statisticId,
                                          numSourceLocalBuffers,
                                          gatheringMode,
                                          sourceAffinity,
                                          taskQueueId,
                                          physicalSourceName,
                                          successors);
}

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
                                 const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors) {
    return std::make_shared<MemorySource>(schema,
                                          memoryArea,
                                          memoryAreaSize,
                                          bufferManager,
                                          queryManager,
                                          numBuffersToProcess,
                                          gatheringValue,
                                          operatorId,
                                          originId,
                                          statisticId,
                                          numSourceLocalBuffers,
                                          gatheringMode,
                                          sourceAffinity,
                                          taskQueueId,
                                          physicalSourceName,
                                          successors);
}

DataSourcePtr
Experimental::createStaticDataSource(const SchemaPtr& schema,
                                     const std::string pathTableFile,
                                     const bool lateStart,
                                     const Runtime::BufferManagerPtr& bufferManager,
                                     const Runtime::QueryManagerPtr& queryManager,
                                     OperatorId operatorId,
                                     OriginId originId,
                                     StatisticId statisticId,
                                     size_t numSourceLocalBuffers,
                                     const std::string& physicalSourceName,
                                     const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors) {
    return std::make_shared<Experimental::StaticDataSource>(schema,
                                                            pathTableFile,
                                                            lateStart,
                                                            bufferManager,
                                                            queryManager,
                                                            operatorId,
                                                            originId,
                                                            statisticId,
                                                            numSourceLocalBuffers,
                                                            physicalSourceName,
                                                            successors);
}

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
                                    const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors) {
    return std::make_shared<BenchmarkSource>(schema,
                                             memoryArea,
                                             memoryAreaSize,
                                             bufferManager,
                                             queryManager,
                                             numberOfBuffersToProcess,
                                             gatheringValue,
                                             operatorId,
                                             originId,
                                             statisticId,
                                             numSourceLocalBuffers,
                                             gatheringMode,
                                             sourceMode,
                                             sourceAffinity,
                                             taskQueueId,
                                             physicalSourceName,
                                             successors);
}

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
                              const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors) {
    return std::make_shared<ZmqSource>(schema,
                                       bufferManager,
                                       queryManager,
                                       host,
                                       port,
                                       operatorId,
                                       originId,
                                       statisticId,
                                       numSourceLocalBuffers,
                                       GatheringMode::INTERVAL_MODE,
                                       physicalSourceName,
                                       successors);
}

DataSourcePtr createBinaryFileSource(const SchemaPtr& schema,
                                     const Runtime::BufferManagerPtr& bufferManager,
                                     const Runtime::QueryManagerPtr& queryManager,
                                     const std::string& pathToFile,
                                     OperatorId operatorId,
                                     OriginId originId,
                                     StatisticId statisticId,
                                     size_t numSourceLocalBuffers,
                                     const std::string& physicalSourceName,
                                     const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors,
                                     bool shouldDelay,
                                     uint64_t numberOfBuffersToProduce) {
    return std::make_shared<BinarySource>(schema,
                                          bufferManager,
                                          queryManager,
                                          pathToFile,
                                          operatorId,
                                          originId,
                                          statisticId,
                                          numSourceLocalBuffers,
                                          GatheringMode::INTERVAL_MODE,
                                          physicalSourceName,
                                          successors,
                                          shouldDelay,
                                          numberOfBuffersToProduce);
}

DataSourcePtr createSenseSource(const SchemaPtr& schema,
                                const Runtime::BufferManagerPtr& bufferManager,
                                const Runtime::QueryManagerPtr& queryManager,
                                const std::string& udfs,
                                OperatorId operatorId,
                                OriginId originId,
                                StatisticId statisticId,
                                size_t numSourceLocalBuffers,
                                const std::string& physicalSourceName,
                                const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors) {
    return std::make_shared<SenseSource>(schema,
                                         bufferManager,
                                         queryManager,
                                         udfs,
                                         operatorId,
                                         originId,
                                         statisticId,
                                         numSourceLocalBuffers,
                                         physicalSourceName,
                                         successors);
}

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
                                  bool shouldDelay) {
    return std::make_shared<CSVSource>(schema,
                                       bufferManager,
                                       queryManager,
                                       csvSourceType,
                                       operatorId,
                                       originId,
                                       statisticId,
                                       numSourceLocalBuffers,
                                       GatheringMode::INTERVAL_MODE,
                                       physicalSourceName,
                                       successors,
                                       shouldDelay);
}

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
                                  OperatorId uniqueNetworkSourceId) {
    return std::make_shared<Network::NetworkSource>(schema,
                                                    bufferManager,
                                                    queryManager,
                                                    networkManager,
                                                    nesPartition,
                                                    sinkLocation,
                                                    numSourceLocalBuffers,
                                                    waitTime,
                                                    retryTimes,
                                                    successors,
                                                    version,
                                                    uniqueNetworkSourceId,
                                                    physicalSourceName);
}

DataSourcePtr createMonitoringSource(Monitoring::MetricCollectorPtr metricCollector,
                                     std::chrono::milliseconds waitTime,
                                     Runtime::BufferManagerPtr bufferManager,
                                     Runtime::QueryManagerPtr queryManager,
                                     OperatorId operatorId,
                                     OriginId originId,
                                     StatisticId statisticId,
                                     size_t numSourceLocalBuffers,
                                     const std::string& physicalSourceName,
                                     std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors) {
    return std::make_shared<MonitoringSource>(metricCollector,
                                              waitTime,
                                              bufferManager,
                                              queryManager,
                                              operatorId,
                                              originId,
                                              statisticId,
                                              numSourceLocalBuffers,
                                              physicalSourceName,
                                              successors);
}

#ifdef ENABLE_KAFKA_BUILD
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
                                      uint64_t batchSize,
                                      const std::string& physicalSourceName,
                                      const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors) {
    return std::make_shared<KafkaSource>(schema,
                                         bufferManager,
                                         queryManager,
                                         numberOfBuffersToProduce,
                                         brokers,
                                         topic,
                                         groupId,
                                         autoCommit,
                                         kafkaConsumerTimeout,
                                         offsetMode,
                                         kafkaSourceType,
                                         originId,
                                         statisticId,
                                         operatorId,
                                         numSourceLocalBuffers,
                                         batchSize,
                                         physicalSourceName,
                                         successors);
}
#endif
#ifdef ENABLE_OPC_BUILD
const DataSourcePtr createOPCSource(SchemaPtr schema,
                                    Runtime::BufferManagerPtr bufferManager,
                                    Runtime::QueryManagerPtr queryManager,
                                    std::string url,
                                    UA_NodeId nodeId,
                                    std::string user,
                                    std::string password,
                                    OperatorId operatorId,
                                    OriginId originId,
                                    StatisticId statisticId,
                                    size_t numSourceLocalBuffers,
                                    const std::string& physicalSourceName,
                                    std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors) {
    return std::make_shared<OPCSource>(schema,
                                       bufferManager,
                                       queryManager,
                                       url,
                                       nodeId,
                                       password,
                                       user,
                                       operatorId,
                                       originId,
                                       statisticId,
                                       numSourceLocalBuffers,
                                       GatheringMode::INTERVAL_MODE,
                                       physicalSourceName,
                                       successors);
}
#endif
#ifdef ENABLE_MQTT_BUILD
DataSourcePtr createMQTTSource(const SchemaPtr& schema,
                               const Runtime::BufferManagerPtr& bufferManager,
                               const Runtime::QueryManagerPtr& queryManager,
                               const MQTTSourceTypePtr& mqttSourceType,
                               OperatorId operatorId,
                               OriginId originId,
                               StatisticId statisticId,
                               size_t numSourceLocalBuffers,
                               const std::string& physicalSourceName,
                               const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors) {
    return std::make_shared<MQTTSource>(schema,
                                        bufferManager,
                                        queryManager,
                                        mqttSourceType,
                                        operatorId,
                                        originId,
                                        statisticId,
                                        numSourceLocalBuffers,
                                        GatheringMode::INTERVAL_MODE,
                                        physicalSourceName,
                                        successors);
}
#endif

DataSourcePtr createTCPSource(const SchemaPtr& schema,
                              const Runtime::BufferManagerPtr& bufferManager,
                              const Runtime::QueryManagerPtr& queryManager,
                              const TCPSourceTypePtr& tcpSourceType,
                              OperatorId operatorId,
                              OriginId originId,
                              StatisticId statisticId,
                              size_t numSourceLocalBuffers,
                              const std::string& physicalSourceName,
                              const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors) {
    return std::make_shared<TCPSource>(schema,
                                       bufferManager,
                                       queryManager,
                                       tcpSourceType,
                                       operatorId,
                                       originId,
                                       statisticId,
                                       numSourceLocalBuffers,
                                       GatheringMode::INTERVAL_MODE,
                                       physicalSourceName,
                                       successors);
}
}// namespace NES
