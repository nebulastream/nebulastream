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

#include <Network/NetworkSink.hpp>
#include <Operators/OperatorId.hpp>
#include <Plans/Query/QuerySubPlanId.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Sinks/Formats/CsvFormat.hpp>
#include <Sinks/Formats/JsonFormat.hpp>
#include <Sinks/Formats/NesFormat.hpp>
#include <Sinks/Formats/TextFormat.hpp>
#include <Sinks/Mediums/FileSink.hpp>
#include <Sinks/Mediums/KafkaSink.hpp>
#include <Sinks/Mediums/MQTTSink.hpp>
#include <Sinks/Mediums/NullOutputSink.hpp>
#include <Sinks/Mediums/OPCSink.hpp>
#include <Sinks/Mediums/PrintSink.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Sinks/Mediums/ZmqSink.hpp>
#include <Sinks/SinkCreator.hpp>
#include <Util/Logger.hpp>

namespace NES {

DataSinkPtr createTextFileSink(const SchemaPtr& schema,
                               QuerySubPlanId querySubPlanId,
                               const Runtime::NodeEnginePtr& nodeEngine,
                               const std::string& filePath,
                               bool append) {
    //TODO: this is not nice and should be fixed such that we only provide the paramter once
    SinkFormatPtr format = std::make_shared<TextFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<FileSink>(format, nodeEngine->getQueryManager(), filePath, append, querySubPlanId);
}

DataSinkPtr createCSVFileSink(const SchemaPtr& schema,
                              QuerySubPlanId querySubPlanId,
                              const Runtime::NodeEnginePtr& nodeEngine,
                              const std::string& filePath,
                              bool append) {
    SinkFormatPtr format = std::make_shared<CsvFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<FileSink>(format, nodeEngine->getQueryManager(), filePath, append, querySubPlanId);
}

DataSinkPtr createBinaryNESFileSink(const SchemaPtr& schema,
                                    QuerySubPlanId querySubPlanId,
                                    const Runtime::NodeEnginePtr& nodeEngine,
                                    const std::string& filePath,
                                    bool append) {
    SinkFormatPtr format = std::make_shared<NesFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<FileSink>(format, nodeEngine->getQueryManager(), filePath, append, querySubPlanId);
}

DataSinkPtr createJSONFileSink(const SchemaPtr& schema,
                               QuerySubPlanId querySubPlanId,
                               const Runtime::NodeEnginePtr& nodeEngine,
                               const std::string& filePath,
                               bool append) {
    SinkFormatPtr format = std::make_shared<JsonFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<FileSink>(format, nodeEngine->getQueryManager(), filePath, append, querySubPlanId);
}

DataSinkPtr createTextZmqSink(const SchemaPtr& schema,
                              QuerySubPlanId querySubPlanId,
                              const Runtime::NodeEnginePtr& nodeEngine,
                              const std::string& host,
                              uint16_t port) {
    SinkFormatPtr format = std::make_shared<TextFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<ZmqSink>(format, nodeEngine->getQueryManager(), host, port, false, querySubPlanId);
}

DataSinkPtr createCSVZmqSink(const SchemaPtr& schema,
                             QuerySubPlanId querySubPlanId,
                             const Runtime::NodeEnginePtr& nodeEngine,
                             const std::string& host,
                             uint16_t port) {
    SinkFormatPtr format = std::make_shared<CsvFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<ZmqSink>(format, nodeEngine->getQueryManager(), host, port, false, querySubPlanId);
}

DataSinkPtr createBinaryZmqSink(const SchemaPtr& schema,
                                QuerySubPlanId querySubPlanId,
                                const Runtime::NodeEnginePtr& nodeEngine,
                                const std::string& host,
                                uint16_t port,
                                bool internal) {
    SinkFormatPtr format = std::make_shared<NesFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<ZmqSink>(format, nodeEngine->getQueryManager(), host, port, internal, querySubPlanId);
}

DataSinkPtr createTextPrintSink(const SchemaPtr& schema,
                                QuerySubPlanId querySubPlanId,
                                const Runtime::NodeEnginePtr& nodeEngine,
                                std::ostream& out) {
    SinkFormatPtr format = std::make_shared<TextFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<PrintSink>(format, nodeEngine->getQueryManager(), querySubPlanId, out);
}

DataSinkPtr createNullOutputSink(QuerySubPlanId querySubPlanId, const Runtime::NodeEnginePtr& nodeEngine) {
    return std::make_shared<NullOutputSink>(nodeEngine->getQueryManager(), querySubPlanId);
}

DataSinkPtr createCSVPrintSink(const SchemaPtr& schema,
                               QuerySubPlanId querySubPlanId,
                               const Runtime::NodeEnginePtr& nodeEngine,
                               std::ostream& out) {
    SinkFormatPtr format = std::make_shared<CsvFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<PrintSink>(format, nodeEngine->getQueryManager(), querySubPlanId, out);
}

DataSinkPtr createNetworkSink(const SchemaPtr& schema,
                              OperatorId globalOperatorId,
                              QuerySubPlanId querySubPlanId,
                              Network::NodeLocation const& nodeLocation,
                              Network::NesPartition nesPartition,
                              Runtime::NodeEnginePtr const& nodeEngine,
                              size_t numOfProducers,
                              std::chrono::milliseconds waitTime,
                              uint8_t retryTimes) {
    return std::make_shared<Network::NetworkSink>(schema,
                                                  globalOperatorId,
                                                  querySubPlanId,
                                                  nodeEngine->getNetworkManager(),
                                                  nodeLocation,
                                                  nesPartition,
                                                  nodeEngine->getBufferManager(),
                                                  nodeEngine->getQueryManager(),
                                                  nodeEngine->getBufferStorage(),
                                                  numOfProducers,
                                                  waitTime,
                                                  retryTimes);
}

#ifdef ENABLE_KAFKA_BUILD
DataSinkPtr
createKafkaSinkWithSchema(SchemaPtr schema, const std::string& brokers, const std::string& topic, uint64_t kafkaProducerTimeout) {
    return std::make_shared<KafkaSink>(schema, brokers, topic, kafkaProducerTimeout);
}
#endif

#ifdef ENABLE_OPC_BUILD
DataSinkPtr createOPCSink(SchemaPtr schema,
                          QuerySubPlanId querySubPlanId,
                          Runtime::NodeEnginePtr nodeEngine,
                          std::string url,
                          UA_NodeId nodeId,
                          std::string user,
                          std::string password) {
    NES_DEBUG("plz fix me" << querySubPlanId);
    SinkFormatPtr format = std::make_shared<TextFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<OPCSink>(format, nodeEngine->getQueryManager(), url, nodeId, user, password, querySubPlanId);
}
#endif

#ifdef ENABLE_MQTT_BUILD
DataSinkPtr createMQTTSink(const SchemaPtr& schema,
                           QuerySubPlanId querySubPlanId,
                           const Runtime::NodeEnginePtr& nodeEngine,
                           const std::string& address,
                           const std::string& clientId,
                           const std::string& topic,
                           const std::string& user,
                           uint64_t maxBufferedMSGs,
                           const MQTTSinkDescriptor::TimeUnits timeUnit,
                           uint64_t msgDelay,
                           MQTTSinkDescriptor::ServiceQualities qualityOfService,
                           bool asynchronousClient) {
    SinkFormatPtr format = std::make_shared<JsonFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<MQTTSink>(format,
                                      nodeEngine->getQueryManager(),
                                      querySubPlanId,
                                      address,
                                      clientId,
                                      topic,
                                      user,
                                      maxBufferedMSGs,
                                      timeUnit,
                                      msgDelay,
                                      qualityOfService,
                                      asynchronousClient);
}
#endif
}// namespace NES
