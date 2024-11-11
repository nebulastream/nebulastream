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

#include <Network/NetworkSink.hpp>
#include <Operators/LogicalOperators/Sinks/StatisticSinkDescriptor.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Sinks/Formats/CsvFormat.hpp>
#include <Sinks/Formats/JsonFormat.hpp>
#include <Sinks/Formats/NesFormat.hpp>
#include <Sinks/Formats/StatisticCollection/StatisticFormatFactory.hpp>
#include <Sinks/Mediums/FileSink.hpp>
#include <Sinks/Mediums/KafkaSink.hpp>
#include <Sinks/Mediums/MonitoringSink.hpp>
#include <Sinks/Mediums/NullOutputSink.hpp>
#include <Sinks/Mediums/PrintSink.hpp>
#include <Sinks/Mediums/RawBufferSink.hpp>
#include <Sinks/Mediums/StatisticSink.hpp>
#include <Sinks/Mediums/ZmqSink.hpp>
#include <Sinks/SinkCreator.hpp>

namespace NES {
DataSinkPtr createCSVFileSink(const SchemaPtr& schema,
                              SharedQueryId sharedQueryId,
                              DecomposedQueryId decomposedQueryId,
                              DecomposedQueryPlanVersion decomposedQueryVersion,
                              const Runtime::NodeEnginePtr& nodeEngine,
                              uint32_t activeProducers,
                              const std::string& filePath,
                              bool append,
                              bool addTimestamp,
                              FaultToleranceType faultToleranceType,
                              uint64_t numberOfOrigins) {
    SinkFormatPtr format = std::make_shared<CsvFormat>(schema, nodeEngine->getBufferManager(), addTimestamp);
    return std::make_shared<FileSink>(format,
                                      nodeEngine,
                                      activeProducers,
                                      filePath,
                                      append,
                                      sharedQueryId,
                                      decomposedQueryId,
                                      decomposedQueryVersion,
                                      faultToleranceType,
                                      numberOfOrigins);
}

DataSinkPtr createBinaryNESFileSink(const SchemaPtr& schema,
                                    SharedQueryId sharedQueryId,
                                    DecomposedQueryId decomposedQueryId,
                                    DecomposedQueryPlanVersion decomposedQueryVersion,
                                    const Runtime::NodeEnginePtr& nodeEngine,
                                    uint32_t activeProducers,
                                    const std::string& filePath,
                                    bool append,
                                    FaultToleranceType faultToleranceType,
                                    uint64_t numberOfOrigins) {
    SinkFormatPtr format = std::make_shared<NesFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<FileSink>(format,
                                      nodeEngine,
                                      activeProducers,
                                      filePath,
                                      append,
                                      sharedQueryId,
                                      decomposedQueryId,
                                      decomposedQueryVersion,
                                      faultToleranceType,
                                      numberOfOrigins);
}

DataSinkPtr createJSONFileSink(const SchemaPtr& schema,
                               SharedQueryId sharedQueryId,
                               DecomposedQueryId decomposedQueryId,
                               const Runtime::NodeEnginePtr& nodeEngine,
                               uint32_t activeProducers,
                               const std::string& filePath,
                               bool append,
                               FaultToleranceType faultToleranceType,
                               uint64_t numberOfOrigins) {
    SinkFormatPtr format = std::make_shared<JsonFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<FileSink>(format,
                                      nodeEngine,
                                      activeProducers,
                                      filePath,
                                      append,
                                      sharedQueryId,
                                      decomposedQueryId,
                                      faultToleranceType,
                                      numberOfOrigins);
}

DataSinkPtr createMigrateFileSink(SharedQueryId sharedQueryId,
                                  DecomposedQueryId decomposedQueryId,
                                  DecomposedQueryPlanVersion decomposedQueryVersion,
                                  Runtime::NodeEnginePtr nodeEngine,
                                  uint32_t numOfProducers,
                                  const std::string& filePath,
                                  bool append,
                                  FaultToleranceType faultToleranceType,
                                  uint64_t numberOfOrigins) {
    return std::make_shared<RawBufferSink>(nodeEngine,
                                           numOfProducers,
                                           filePath,
                                           append,
                                           sharedQueryId,
                                           decomposedQueryId,
                                           decomposedQueryVersion,
                                           faultToleranceType,
                                           numberOfOrigins);
}

DataSinkPtr createCsvZmqSink(const SchemaPtr& schema,
                             SharedQueryId sharedQueryId,
                             DecomposedQueryId decomposedQueryId,
                             DecomposedQueryPlanVersion decomposedQueryVersion,
                             const Runtime::NodeEnginePtr& nodeEngine,
                             uint32_t activeProducers,
                             const std::string& host,
                             uint16_t port,
                             FaultToleranceType faultToleranceType,
                             uint64_t numberOfOrigins) {
    SinkFormatPtr format = std::make_shared<CsvFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<ZmqSink>(format,
                                     nodeEngine,
                                     activeProducers,
                                     host,
                                     port,
                                     false,
                                     sharedQueryId,
                                     decomposedQueryId,
                                     decomposedQueryVersion,
                                     faultToleranceType,
                                     numberOfOrigins);
}

DataSinkPtr createCSVZmqSink(const SchemaPtr& schema,
                             SharedQueryId sharedQueryId,
                             DecomposedQueryId decomposedQueryId,
                             DecomposedQueryPlanVersion decomposedQueryVersion,
                             const Runtime::NodeEnginePtr& nodeEngine,
                             uint32_t activeProducers,
                             const std::string& host,
                             uint16_t port,
                             FaultToleranceType faultToleranceType,
                             uint64_t numberOfOrigins) {
    SinkFormatPtr format = std::make_shared<CsvFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<ZmqSink>(format,
                                     nodeEngine,
                                     activeProducers,
                                     host,
                                     port,
                                     false,
                                     sharedQueryId,
                                     decomposedQueryId,
                                     decomposedQueryVersion,
                                     faultToleranceType,
                                     numberOfOrigins);
}

DataSinkPtr createBinaryZmqSink(const SchemaPtr& schema,
                                SharedQueryId sharedQueryId,
                                DecomposedQueryId decomposedQueryId,
                                DecomposedQueryPlanVersion decomposedQueryVersion,
                                const Runtime::NodeEnginePtr& nodeEngine,
                                uint32_t activeProducers,
                                const std::string& host,
                                uint16_t port,
                                bool internal,
                                FaultToleranceType faultToleranceType,
                                uint64_t numberOfOrigins) {
    SinkFormatPtr format = std::make_shared<NesFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<ZmqSink>(format,
                                     nodeEngine,
                                     activeProducers,
                                     host,
                                     port,
                                     internal,
                                     sharedQueryId,
                                     decomposedQueryId,
                                     decomposedQueryVersion,
                                     faultToleranceType,
                                     numberOfOrigins);
}

DataSinkPtr createCsvPrintSink(const SchemaPtr& schema,
                               SharedQueryId sharedQueryId,
                               DecomposedQueryId decomposedQueryId,
                               DecomposedQueryPlanVersion decomposedQueryVersion,
                               const Runtime::NodeEnginePtr& nodeEngine,
                               uint32_t activeProducers,
                               std::ostream& out,
                               FaultToleranceType faultToleranceType,
                               uint64_t numberOfOrigins) {
    SinkFormatPtr format = std::make_shared<CsvFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<PrintSink>(format,
                                       nodeEngine,
                                       activeProducers,
                                       sharedQueryId,
                                       decomposedQueryId,
                                       decomposedQueryVersion,
                                       out,
                                       faultToleranceType,
                                       numberOfOrigins);
}

DataSinkPtr createNullOutputSink(SharedQueryId sharedQueryId,
                                 DecomposedQueryId decomposedQueryId,
                                 DecomposedQueryPlanVersion decomposedQueryVersion,
                                 const Runtime::NodeEnginePtr& nodeEngine,
                                 uint32_t activeProducers,
                                 FaultToleranceType faultToleranceType,
                                 uint64_t numberOfOrigins) {
    return std::make_shared<NullOutputSink>(nodeEngine,
                                            activeProducers,
                                            sharedQueryId,
                                            decomposedQueryId,
                                            decomposedQueryVersion,
                                            faultToleranceType,
                                            numberOfOrigins);
}

DataSinkPtr createCSVPrintSink(const SchemaPtr& schema,
                               SharedQueryId sharedQueryId,
                               DecomposedQueryId decomposedQueryId,
                               DecomposedQueryPlanVersion decomposedQueryVersion,
                               const Runtime::NodeEnginePtr& nodeEngine,
                               uint32_t activeProducers,
                               std::ostream& out,
                               FaultToleranceType faultToleranceType,
                               uint64_t numberOfOrigins) {
    SinkFormatPtr format = std::make_shared<CsvFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<PrintSink>(format,
                                       nodeEngine,
                                       activeProducers,
                                       sharedQueryId,
                                       decomposedQueryId,
                                       decomposedQueryVersion,
                                       out,
                                       faultToleranceType,
                                       numberOfOrigins);
}

DataSinkPtr createNetworkSink(const SchemaPtr& schema,
                              OperatorId uniqueNetworkSinkDescriptorId,
                              SharedQueryId sharedQueryId,
                              DecomposedQueryId decomposedQueryId,
                              DecomposedQueryPlanVersion decomposedQueryVersion,
                              Network::NodeLocation const& nodeLocation,
                              Network::NesPartition nesPartition,
                              Runtime::NodeEnginePtr const& nodeEngine,
                              size_t numOfProducers,
                              std::chrono::milliseconds waitTime,
                              DecomposedQueryPlanVersion version,
                              uint64_t numberOfOrigins,
                              uint8_t retryTimes) {
    return std::make_shared<Network::NetworkSink>(schema,
                                                  uniqueNetworkSinkDescriptorId,
                                                  sharedQueryId,
                                                  decomposedQueryId,
                                                  decomposedQueryVersion,
                                                  nodeLocation,
                                                  nesPartition,
                                                  nodeEngine,
                                                  numOfProducers,
                                                  waitTime,
                                                  retryTimes,
                                                  numberOfOrigins,
                                                  version);
}

DataSinkPtr createStatisticSink(const SchemaPtr& schema,
                                const Runtime::NodeEnginePtr& nodeEngine,
                                size_t numOfProducers,
                                SharedQueryId sharedQueryId,
                                DecomposedQueryId decomposedQueryId,
                                DecomposedQueryPlanVersion decomposedQueryVersion,
                                FaultToleranceType faultToleranceType,
                                uint64_t numberOfOrigins,
                                Statistic::StatisticSynopsisType sinkFormatType,
                                Statistic::StatisticDataCodec sinkDataCodec) {
    // We can not use the existing SinkFormat, as the interface only returns a std::string. Therefore, we create our own
    // As the SinkMedium expects a SinkFormat, we choose here arbitrary one.
    auto sinkFormat = std::make_shared<NesFormat>(schema, nodeEngine->getBufferManager());

    // We create the correct StatisticSinkFormat and then pass everything to the StatisticSink
    auto statisticSinkFormat =
        Statistic::StatisticFormatFactory::createFromSchema(schema,
                                                            nodeEngine->getBufferManager()->getBufferSize(),
                                                            sinkFormatType,
                                                            sinkDataCodec);
    return std::make_shared<Statistic::StatisticSink>(sinkFormat,
                                                      nodeEngine,
                                                      numOfProducers,
                                                      sharedQueryId,
                                                      decomposedQueryId,
                                                      decomposedQueryVersion,
                                                      faultToleranceType,
                                                      numberOfOrigins,
                                                      nodeEngine->getStatisticManager()->getStatisticStore(),
                                                      statisticSinkFormat);
}

DataSinkPtr createMonitoringSink(Monitoring::MetricStorePtr metricStore,
                                 Monitoring::MetricCollectorType type,
                                 const SchemaPtr& schema,
                                 Runtime::NodeEnginePtr nodeEngine,
                                 uint32_t numOfProducers,
                                 SharedQueryId sharedQueryId,
                                 DecomposedQueryId decomposedQueryId,
                                 DecomposedQueryPlanVersion decomposedQueryVersion,
                                 FaultToleranceType faultToleranceType,
                                 uint64_t numberOfOrigins) {
    SinkFormatPtr format = std::make_shared<NesFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<MonitoringSink>(format,
                                            metricStore,
                                            type,
                                            nodeEngine,
                                            numOfProducers,
                                            sharedQueryId,
                                            decomposedQueryId,
                                            decomposedQueryVersion,
                                            faultToleranceType,
                                            numberOfOrigins);
}

#ifdef ENABLE_KAFKA_BUILD
DataSinkPtr createCsvKafkaSink(SchemaPtr schema,
                               SharedQueryId sharedQueryId,
                               DecomposedQueryId decomposedQueryId,
                               DecomposedQueryPlanVersion decomposedQueryVersion,
                               const Runtime::NodeEnginePtr& nodeEngine,
                               uint32_t activeProducers,
                               const std::string& brokers,
                               const std::string& topic,
                               uint64_t kafkaProducerTimeout,
                               uint64_t numberOfOrigins) {
    SinkFormatPtr format = std::make_shared<CsvFormat>(schema, nodeEngine->getBufferManager());

    return std::make_shared<KafkaSink>(format,
                                       nodeEngine,
                                       activeProducers,
                                       brokers,
                                       topic,
                                       sharedQueryId,
                                       decomposedQueryId,
                                       decomposedQueryVersion,
                                       kafkaProducerTimeout,
                                       numberOfOrigins);
}
#endif
#ifdef ENABLE_OPC_BUILD
DataSinkPtr createOPCSink(SchemaPtr schema,
                          SharedQueryId queryId,
                          QuerySubPlanId querySubPlanId,
                          QuerySubPlanVersion querySubPlanVersion,
                          Runtime::NodeEnginePtr nodeEngine,
                          std::string url,
                          UA_NodeId nodeId,
                          std::string user,
                          std::string password) {
    NES_DEBUG("plz fix me {}", querySubPlanId);
    SinkFormatPtr format = std::make_shared<CsvFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<OPCSink>(format,
                                     nodeEngine,
                                     url,
                                     nodeId,
                                     user,
                                     password,
                                     queryId,
                                     querySubPlanId,
                                     querySubPlanVersion);
}
#endif

#ifdef ENABLE_MQTT_BUILD
DataSinkPtr createMQTTSink(const SchemaPtr& schema,
                           SharedQueryId sharedQueryId,
                           DecomposedQueryId decomposedQueryId,
                           DecomposedQueryPlanVersion decomposedQueryVersion,
                           const Runtime::NodeEnginePtr& nodeEngine,
                           uint32_t numOfProducers,
                           const std::string& address,
                           const std::string& clientId,
                           const std::string& topic,
                           const std::string& user,
                           uint64_t maxBufferedMSGs,
                           const MQTTSinkDescriptor::TimeUnits timeUnit,
                           uint64_t msgDelay,
                           MQTTSinkDescriptor::ServiceQualities qualityOfService,
                           bool asynchronousClient,
                           FaultToleranceType faultToleranceType,
                           uint64_t numberOfOrigins) {
    SinkFormatPtr format = std::make_shared<JsonFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<MQTTSink>(format,
                                      nodeEngine,
                                      numOfProducers,
                                      sharedQueryId,
                                      decomposedQueryId,
                                      decomposedQueryVersion,
                                      address,
                                      clientId,
                                      topic,
                                      user,
                                      maxBufferedMSGs,
                                      timeUnit,
                                      msgDelay,
                                      qualityOfService,
                                      asynchronousClient,
                                      faultToleranceType,
                                      numberOfOrigins);
}
#endif
}// namespace NES
