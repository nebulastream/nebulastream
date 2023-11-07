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
#include <Runtime/NodeEngine.hpp>
#include <Sinks/Formats/CsvFormat.hpp>
#include <Sinks/Formats/JsonFormat.hpp>
#include <Sinks/Formats/NesFormat.hpp>
#include <Sinks/Mediums/FileSink.hpp>
#include <Sinks/Mediums/KafkaSink.hpp>
#include <Sinks/Mediums/MonitoringSink.hpp>
#include <Sinks/Mediums/NullOutputSink.hpp>
#include <Sinks/Mediums/OPCSink.hpp>
#include <Sinks/Mediums/PrintSink.hpp>
#include <Sinks/Mediums/ZmqSink.hpp>
#include <Sinks/SinkCreator.hpp>

namespace NES {
DataSinkPtr createCSVFileSink(const SchemaPtr& schema,
                              QueryId queryId,
                              QuerySubPlanId querySubPlanId,
                              const Runtime::NodeEnginePtr& nodeEngine,
                              uint32_t activeProducers,
                              const std::string& filePath,
                              bool append,
                              bool addTimestamp,
                              uint64_t numberOfOrigins) {
    SinkFormatPtr format = std::make_shared<CsvFormat>(schema, nodeEngine->getBufferManager(), addTimestamp);
    return std::make_shared<FileSink>(format,
                                      nodeEngine,
                                      activeProducers,
                                      filePath,
                                      append,
                                      queryId,
                                      querySubPlanId,
                                      numberOfOrigins);
}

DataSinkPtr createBinaryNESFileSink(const SchemaPtr& schema,
                                    QueryId queryId,
                                    QuerySubPlanId querySubPlanId,
                                    const Runtime::NodeEnginePtr& nodeEngine,
                                    uint32_t activeProducers,
                                    const std::string& filePath,
                                    bool append,
                                    uint64_t numberOfOrigins) {
    SinkFormatPtr format = std::make_shared<NesFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<FileSink>(format,
                                      nodeEngine,
                                      activeProducers,
                                      filePath,
                                      append,
                                      queryId,
                                      querySubPlanId,
                                      numberOfOrigins);
}

DataSinkPtr createJSONFileSink(const SchemaPtr& schema,
                               QueryId queryId,
                               QuerySubPlanId querySubPlanId,
                               const Runtime::NodeEnginePtr& nodeEngine,
                               uint32_t activeProducers,
                               const std::string& filePath,
                               bool append,
                               uint64_t numberOfOrigins) {
    SinkFormatPtr format = std::make_shared<JsonFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<FileSink>(format,
                                      nodeEngine,
                                      activeProducers,
                                      filePath,
                                      append,
                                      queryId,
                                      querySubPlanId,
                                      numberOfOrigins);
}

DataSinkPtr createCsvZmqSink(const SchemaPtr& schema,
                             QueryId queryId,
                             QuerySubPlanId querySubPlanId,
                             const Runtime::NodeEnginePtr& nodeEngine,
                             uint32_t activeProducers,
                             const std::string& host,
                             uint16_t port,
                             uint64_t numberOfOrigins) {
    SinkFormatPtr format = std::make_shared<CsvFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<ZmqSink>(format,
                                     nodeEngine,
                                     activeProducers,
                                     host,
                                     port,
                                     false,
                                     queryId,
                                     querySubPlanId,
                                     numberOfOrigins);
}

DataSinkPtr createCSVZmqSink(const SchemaPtr& schema,
                             QueryId queryId,
                             QuerySubPlanId querySubPlanId,
                             const Runtime::NodeEnginePtr& nodeEngine,
                             uint32_t activeProducers,
                             const std::string& host,
                             uint16_t port,
                             uint64_t numberOfOrigins) {
    SinkFormatPtr format = std::make_shared<CsvFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<ZmqSink>(format,
                                     nodeEngine,
                                     activeProducers,
                                     host,
                                     port,
                                     false,
                                     queryId,
                                     querySubPlanId,
                                     numberOfOrigins);
}

DataSinkPtr createBinaryZmqSink(const SchemaPtr& schema,
                                QueryId queryId,
                                QuerySubPlanId querySubPlanId,
                                const Runtime::NodeEnginePtr& nodeEngine,
                                uint32_t activeProducers,
                                const std::string& host,
                                uint16_t port,
                                bool internal,
                                uint64_t numberOfOrigins) {
    SinkFormatPtr format = std::make_shared<NesFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<ZmqSink>(format,
                                     nodeEngine,
                                     activeProducers,
                                     host,
                                     port,
                                     internal,
                                     queryId,
                                     querySubPlanId,
                                     numberOfOrigins);
}

DataSinkPtr createCsvPrintSink(const SchemaPtr& schema,
                               QueryId queryId,
                               QuerySubPlanId querySubPlanId,
                               const Runtime::NodeEnginePtr& nodeEngine,
                               uint32_t activeProducers,
                               std::ostream& out,
                               uint64_t numberOfOrigins) {
    SinkFormatPtr format = std::make_shared<CsvFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<PrintSink>(format, nodeEngine, activeProducers, queryId, querySubPlanId, out, numberOfOrigins);
}

DataSinkPtr createNullOutputSink(QueryId queryId,
                                 QuerySubPlanId querySubPlanId,
                                 const Runtime::NodeEnginePtr& nodeEngine,
                                 uint32_t activeProducers,
                                 uint64_t numberOfOrigins) {
    return std::make_shared<NullOutputSink>(nodeEngine, activeProducers, queryId, querySubPlanId, numberOfOrigins);
}

DataSinkPtr createCSVPrintSink(const SchemaPtr& schema,
                               QueryId queryId,
                               QuerySubPlanId querySubPlanId,
                               const Runtime::NodeEnginePtr& nodeEngine,
                               uint32_t activeProducers,
                               std::ostream& out,
                               uint64_t numberOfOrigins) {
    SinkFormatPtr format = std::make_shared<CsvFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<PrintSink>(format, nodeEngine, activeProducers, queryId, querySubPlanId, out, numberOfOrigins);
}

DataSinkPtr createNetworkSink(const SchemaPtr& schema,
                              uint64_t uniqueNetworkSinkDescriptorId,
                              QueryId queryId,
                              QuerySubPlanId querySubPlanId,
                              Network::NodeLocation const& nodeLocation,
                              Network::NesPartition nesPartition,
                              Runtime::NodeEnginePtr const& nodeEngine,
                              size_t numOfProducers,
                              std::chrono::milliseconds waitTime,
                              uint64_t numberOfOrigins,
                              uint8_t retryTimes,
                              uint16_t numberOfInputSources) {
    return std::make_shared<Network::NetworkSink>(schema,
                                                  uniqueNetworkSinkDescriptorId,
                                                  queryId,
                                                  querySubPlanId,
                                                  nodeLocation,
                                                  nesPartition,
                                                  nodeEngine,
                                                  numOfProducers,
                                                  waitTime,
                                                  retryTimes,
                                                  numberOfOrigins,
                                                  numberOfInputSources);
}

DataSinkPtr createMonitoringSink(Monitoring::MetricStorePtr metricStore,
                                 Monitoring::MetricCollectorType type,
                                 const SchemaPtr& schema,
                                 Runtime::NodeEnginePtr nodeEngine,
                                 uint32_t numOfProducers,
                                 QueryId queryId,
                                 QuerySubPlanId querySubPlanId,
                                 uint64_t numberOfOrigins) {
    SinkFormatPtr format = std::make_shared<NesFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<MonitoringSink>(format,
                                            metricStore,
                                            type,
                                            nodeEngine,
                                            numOfProducers,
                                            queryId,
                                            querySubPlanId,
                                            numberOfOrigins);
}

#ifdef ENABLE_KAFKA_BUILD
DataSinkPtr createCsvKafkaSink(SchemaPtr schema,
                               QueryId queryId,
                               QuerySubPlanId querySubPlanId,
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
                                       queryId,
                                       querySubPlanId,
                                       kafkaProducerTimeout,
                                       numberOfOrigins);
}
#endif
#ifdef ENABLE_OPC_BUILD
DataSinkPtr createOPCSink(SchemaPtr schema,
                          QueryId queryId,
                          QuerySubPlanId querySubPlanId,
                          Runtime::NodeEnginePtr nodeEngine,
                          std::string url,
                          UA_NodeId nodeId,
                          std::string user,
                          std::string password) {
    NES_DEBUG("plz fix me {}", querySubPlanId);
    SinkFormatPtr format = std::make_shared<CsvFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<OPCSink>(format, nodeEngine, url, nodeId, user, password, queryId, querySubPlanId);
}
#endif

#ifdef ENABLE_MQTT_BUILD
DataSinkPtr createMQTTSink(const SchemaPtr& schema,
                           QueryId queryId,
                           QuerySubPlanId querySubPlanId,
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
                           uint64_t numberOfOrigins) {
    SinkFormatPtr format = std::make_shared<JsonFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<MQTTSink>(format,
                                      nodeEngine,
                                      numOfProducers,
                                      queryId,
                                      querySubPlanId,
                                      address,
                                      clientId,
                                      topic,
                                      user,
                                      maxBufferedMSGs,
                                      timeUnit,
                                      msgDelay,
                                      qualityOfService,
                                      asynchronousClient,
                                      numberOfOrigins);
}
#endif
}// namespace NES
