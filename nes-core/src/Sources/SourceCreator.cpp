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

#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Network/NetworkSource.hpp>
#include <Runtime/QueryManager.hpp>
#include <Sources/AdaptiveKFSource.hpp>
#include <Sources/BinarySource.hpp>
#include <Sources/CSVSource.hpp>
#include <Sources/DataSource.hpp>
#include <Sources/DefaultSource.hpp>
#include <Sources/GeneratorSource.hpp>
#include <Sources/KafkaSource.hpp>
#include <Sources/LambdaSource.hpp>
#include <Sources/MemorySource.hpp>
#include <Sources/OPCSource.hpp>
#include <Sources/SenseSource.hpp>
#include <Sources/SourceCreator.hpp>
#include <Sources/ZmqSource.hpp>
#include <chrono>

#ifdef ENABLE_OPC_BUILD
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
                                              size_t numSourceLocalBuffers,
                                              const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors) {
    return std::make_shared<DefaultSource>(schema,
                                           bufferManager,
                                           queryManager,
                                           /*bufferCnt*/ 1,
                                           /*frequency*/ 1000,
                                           operatorId,
                                           numSourceLocalBuffers,
                                           successors);
}

DataSourcePtr
createDefaultDataSourceWithSchemaForVarBuffers(const SchemaPtr& schema,
                                               const Runtime::BufferManagerPtr& bufferManager,
                                               const Runtime::QueryManagerPtr& queryManager,
                                               uint64_t numbersOfBufferToProduce,
                                               uint64_t frequency,
                                               OperatorId operatorId,
                                               size_t numSourceLocalBuffers,
                                               const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors) {
    return std::make_shared<DefaultSource>(schema,
                                           bufferManager,
                                           queryManager,
                                           numbersOfBufferToProduce,
                                           frequency,
                                           operatorId,
                                           numSourceLocalBuffers,
                                           successors);
}

DataSourcePtr
createDefaultSourceWithoutSchemaForOneBuffer(const Runtime::BufferManagerPtr& bufferManager,
                                             const Runtime::QueryManagerPtr& queryManager,
                                             OperatorId operatorId,
                                             size_t numSourceLocalBuffers,
                                             const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors) {
    return std::make_shared<DefaultSource>(Schema::create()->addField("id", DataTypeFactory::createUInt64()),
                                           bufferManager,
                                           queryManager,
                                           /**bufferCnt*/ 1,
                                           /*frequency*/ 1000,
                                           operatorId,
                                           numSourceLocalBuffers,
                                           successors);
}

DataSourcePtr
createLambdaSource(const SchemaPtr& schema,
                   const Runtime::BufferManagerPtr& bufferManager,
                   const Runtime::QueryManagerPtr& queryManager,
                   uint64_t numbersOfBufferToProduce,
                   uint64_t gatheringValue,
                   std::function<void(NES::Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce)>&& generationFunction,
                   OperatorId operatorId,
                   size_t numSourceLocalBuffers,
                   DataSource::GatheringMode gatheringMode,
                   const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors) {
    return std::make_shared<LambdaSource>(schema,
                                          bufferManager,
                                          queryManager,
                                          numbersOfBufferToProduce,
                                          gatheringValue,
                                          std::move(generationFunction),
                                          operatorId,
                                          numSourceLocalBuffers,
                                          gatheringMode,
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
                                 size_t numSourceLocalBuffers,
                                 DataSource::GatheringMode gatheringMode,
                                 const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors) {
    return std::make_shared<MemorySource>(schema,
                                          memoryArea,
                                          memoryAreaSize,
                                          bufferManager,
                                          queryManager,
                                          numBuffersToProcess,
                                          gatheringValue,
                                          operatorId,
                                          numSourceLocalBuffers,
                                          gatheringMode,
                                          successors);
}

DataSourcePtr createBenchmarkSource(const SchemaPtr& schema,
                                    const Runtime::BufferManagerPtr& bufferManager,
                                    const Runtime::QueryManagerPtr& queryManager,
                                    const std::shared_ptr<uint8_t>& memoryArea,
                                    size_t memoryAreaSize,
                                    uint64_t numBuffersToProcess,
                                    uint64_t gatheringValue,
                                    OperatorId operatorId,
                                    size_t numSourceLocalBuffers,
                                    DataSource::GatheringMode gatheringMode,
                                    BenchmarkSource::SourceMode sourceMode,
                                    const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors,
                                    uint64_t sourceAffinity) {
    return std::make_shared<BenchmarkSource>(schema,
                                             memoryArea,
                                             memoryAreaSize,
                                             bufferManager,
                                             queryManager,
                                             numBuffersToProcess,
                                             gatheringValue,
                                             operatorId,
                                             numSourceLocalBuffers,
                                             gatheringMode,
                                             sourceMode,
                                             sourceAffinity,
                                             successors);
}

DataSourcePtr createZmqSource(const SchemaPtr& schema,
                              const Runtime::BufferManagerPtr& bufferManager,
                              const Runtime::QueryManagerPtr& queryManager,
                              const std::string& host,
                              uint16_t port,
                              OperatorId operatorId,
                              size_t numSourceLocalBuffers,
                              const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors) {
    return std::make_shared<ZmqSource>(schema,
                                       bufferManager,
                                       queryManager,
                                       host,
                                       port,
                                       operatorId,
                                       numSourceLocalBuffers,
                                       DataSource::FREQUENCY_MODE,
                                       successors);
}

DataSourcePtr createBinaryFileSource(const SchemaPtr& schema,
                                     const Runtime::BufferManagerPtr& bufferManager,
                                     const Runtime::QueryManagerPtr& queryManager,
                                     const std::string& pathToFile,
                                     OperatorId operatorId,
                                     size_t numSourceLocalBuffers,
                                     const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors) {
    return std::make_shared<BinarySource>(schema,
                                          bufferManager,
                                          queryManager,
                                          pathToFile,
                                          operatorId,
                                          numSourceLocalBuffers,
                                          DataSource::FREQUENCY_MODE,
                                          successors);
}

DataSourcePtr createSenseSource(const SchemaPtr& schema,
                                const Runtime::BufferManagerPtr& bufferManager,
                                const Runtime::QueryManagerPtr& queryManager,
                                const std::string& udfs,
                                OperatorId operatorId,
                                size_t numSourceLocalBuffers,
                                const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors) {
    return std::make_shared<SenseSource>(schema,
                                         bufferManager,
                                         queryManager,
                                         udfs,
                                         operatorId,
                                         numSourceLocalBuffers,
                                         successors);
}

DataSourcePtr createCSVFileSource(const SchemaPtr& schema,
                                  const Runtime::BufferManagerPtr& bufferManager,
                                  const Runtime::QueryManagerPtr& queryManager,
                                  const Configurations::CSVSourceConfigPtr& sourceConfigPtr,
                                  OperatorId operatorId,
                                  size_t numSourceLocalBuffers,
                                  const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors) {
    return std::make_shared<CSVSource>(schema,
                                       bufferManager,
                                       queryManager,
                                       sourceConfigPtr,
                                       operatorId,
                                       numSourceLocalBuffers,
                                       DataSource::FREQUENCY_MODE,
                                       successors);
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
                                  const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors) {
    return std::make_shared<Network::NetworkSource>(schema,
                                                    bufferManager,
                                                    queryManager,
                                                    networkManager,
                                                    nesPartition,
                                                    sinkLocation,
                                                    numSourceLocalBuffers,
                                                    waitTime,
                                                    retryTimes,
                                                    successors);
}

const DataSourcePtr createAdaptiveKFSource(SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager,
                                           uint64_t numberOfTuplesToProducePerBuffer, uint64_t numBuffersToProcess,
                                           uint64_t frequency, bool endlessRepeat, OperatorId operatorId) {
    return std::make_shared<AdaptiveKFSource>(schema, bufferManager, queryManager, numBuffersToProcess,
                                              numberOfTuplesToProducePerBuffer, frequency, endlessRepeat, operatorId);
}

#ifdef ENABLE_KAFKA_BUILD
const DataSourcePtr createKafkaSource(SchemaPtr schema,
                                      Runtime::BufferManagerPtr bufferManager,
                                      Runtime::QueryManagerPtr queryManager,
                                      std::string brokers,
                                      std::string topic,
                                      std::string groupId,
                                      bool autoCommit,
                                      uint64_t kafkaConsumerTimeout,
                                      OperatorId operatorId,
                                      size_t numSourceLocalBuffers) {
    return std::make_shared<KafkaSource>(schema,
                                         bufferManager,
                                         queryManager,
                                         brokers,
                                         topic,
                                         groupId,
                                         autoCommit,
                                         kafkaConsumerTimeout,
                                         operatorId,
                                         numSourceLocalBuffers);
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
                                    size_t numSourceLocalBuffers,
                                    std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors) {
    return std::make_shared<OPCSource>(schema,
                                       bufferManager,
                                       queryManager,
                                       url,
                                       nodeId,
                                       user,
                                       password,
                                       operatorId,
                                       numSourceLocalBuffers,
                                       DataSource::FREQUENCY_MODE,
                                       successors);
}
#endif
#ifdef ENABLE_MQTT_BUILD
DataSourcePtr createMQTTSource(const SchemaPtr& schema,
                               const Runtime::BufferManagerPtr& bufferManager,
                               const Runtime::QueryManagerPtr& queryManager,
                               const Configurations::MQTTSourceConfigPtr& sourceConfig,
                               OperatorId operatorId,
                               size_t numSourceLocalBuffers,
                               const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors,
                               SourceDescriptor::InputFormat inputFormat) {
    return std::make_shared<MQTTSource>(schema,
                                        bufferManager,
                                        queryManager,
                                        sourceConfig,
                                        operatorId,
                                        numSourceLocalBuffers,
                                        DataSource::FREQUENCY_MODE,
                                        successors,
                                        inputFormat);
}
#endif
}// namespace NES
