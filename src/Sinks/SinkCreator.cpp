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
#include <NodeEngine/NodeEngine.hpp>
#include <Operators/OperatorId.hpp>
#include <Plans/Query/QuerySubPlanId.hpp>
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

const DataSinkPtr createTextFileSink(OperatorId logicalOperatorId,
                                     SchemaPtr schema,
                                     QuerySubPlanId parentPlanId,
                                     NodeEngine::NodeEnginePtr nodeEngine,
                                     const std::string& filePath,
                                     bool append) {
    //TODO: this is not nice and should be fixed such that we only provide the parameter once
    SinkFormatPtr format = std::make_shared<TextFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<FileSink>(logicalOperatorId, format, filePath, append, parentPlanId);
}

const DataSinkPtr createCSVFileSink(OperatorId logicalOperatorId,
                                    SchemaPtr schema,
                                    QuerySubPlanId parentPlanId,
                                    NodeEngine::NodeEnginePtr nodeEngine,
                                    const std::string& filePath,
                                    bool append) {
    SinkFormatPtr format = std::make_shared<CsvFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<FileSink>(logicalOperatorId, format, filePath, append, parentPlanId);
}

const DataSinkPtr createBinaryNESFileSink(OperatorId logicalOperatorId,
                                          SchemaPtr schema,
                                          QuerySubPlanId parentPlanId,
                                          NodeEngine::NodeEnginePtr nodeEngine,
                                          const std::string& filePath,
                                          bool append) {
    SinkFormatPtr format = std::make_shared<NesFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<FileSink>(logicalOperatorId, format, filePath, append, parentPlanId);
}

const DataSinkPtr createJSONFileSink(OperatorId logicalOperatorId,
                                     SchemaPtr schema,
                                     QuerySubPlanId parentPlanId,
                                     NodeEngine::NodeEnginePtr nodeEngine,
                                     const std::string& filePath,
                                     bool append) {
    SinkFormatPtr format = std::make_shared<JsonFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<FileSink>(logicalOperatorId, format, filePath, append, parentPlanId);
}

const DataSinkPtr createTextZmqSink(OperatorId logicalOperatorId,
                                    SchemaPtr schema,
                                    QuerySubPlanId parentPlanId,
                                    NodeEngine::NodeEnginePtr nodeEngine,
                                    const std::string& host,
                                    uint16_t port) {
    SinkFormatPtr format = std::make_shared<TextFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<ZmqSink>(logicalOperatorId, format, host, port, false, parentPlanId);
}

const DataSinkPtr createCSVZmqSink(OperatorId logicalOperatorId,
                                   SchemaPtr schema,
                                   QuerySubPlanId parentPlanId,
                                   NodeEngine::NodeEnginePtr nodeEngine,
                                   const std::string& host,
                                   uint16_t port) {
    SinkFormatPtr format = std::make_shared<CsvFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<ZmqSink>(logicalOperatorId, format, host, port, false, parentPlanId);
}

const DataSinkPtr createBinaryZmqSink(OperatorId logicalOperatorId,
                                      SchemaPtr schema,
                                      QuerySubPlanId parentPlanId,
                                      NodeEngine::NodeEnginePtr nodeEngine,
                                      const std::string& host,
                                      uint16_t port,
                                      bool internal) {
    SinkFormatPtr format = std::make_shared<NesFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<ZmqSink>(logicalOperatorId, format, host, port, internal, parentPlanId);
}

const DataSinkPtr createTextPrintSink(OperatorId logicalOperatorId,
                                      SchemaPtr schema,
                                      QuerySubPlanId parentPlanId,
                                      NodeEngine::NodeEnginePtr nodeEngine,
                                      std::ostream& out) {
    SinkFormatPtr format = std::make_shared<TextFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<PrintSink>(logicalOperatorId, format, parentPlanId, out);
}

const DataSinkPtr createNullOutputSink(OperatorId logicalOperatorId, QuerySubPlanId parentPlanId) {
    return std::make_shared<NullOutputSink>(logicalOperatorId, parentPlanId);
}

const DataSinkPtr createCSVPrintSink(OperatorId logicalOperatorId,
                                     SchemaPtr schema,
                                     QuerySubPlanId parentPlanId,
                                     NodeEngine::NodeEnginePtr nodeEngine,
                                     std::ostream& out) {
    SinkFormatPtr format = std::make_shared<CsvFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<PrintSink>(logicalOperatorId, format, parentPlanId, out);
}

const DataSinkPtr createNetworkSink(OperatorId logicalOperatorId,
                                    SchemaPtr schema,
                                    QuerySubPlanId parentPlanId,
                                    Network::NodeLocation nodeLocation,
                                    Network::NesPartition nesPartition,
                                    NodeEngine::NodeEnginePtr nodeEngine,
                                    std::chrono::seconds waitTime,
                                    uint8_t retryTimes) {
    return std::make_shared<Network::NetworkSink>(logicalOperatorId,
                                                  schema,
                                                  parentPlanId,
                                                  nodeEngine->getNetworkManager(),
                                                  nodeLocation,
                                                  nesPartition,
                                                  nodeEngine->getBufferManager(),
                                                  nodeEngine->getQueryManager(),
                                                  waitTime,
                                                  retryTimes);
}

#ifdef ENABLE_KAFKA_BUILD
const DataSinkPtr
createKafkaSinkWithSchema(OperatorId logicalOperatorId, SchemaPtr schema, QuerySubPlanId parentPlanId, const std::string& brokers, const std::string& topic, uint64_t kafkaProducerTimeout) {
    SinkFormatPtr format = std::make_shared<TextFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<KafkaSink>(logicalOperatorId, format, parentPlanId, brokers, topic, kafkaProducerTimeout);
}
#endif

#ifdef ENABLE_OPC_BUILD
const DataSinkPtr createOPCSink(OperatorId logicalOperatorId,
                                SchemaPtr schema,
                                QuerySubPlanId parentPlanId,
                                NodeEngine::NodeEnginePtr nodeEngine,
                                std::string url,
                                UA_NodeId nodeId,
                                std::string user,
                                std::string password) {
    NES_DEBUG("plz fix me" << parentPlanId);
    SinkFormatPtr format = std::make_shared<TextFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<OPCSink>(logicalOperatorId, parentPlanId, format, url, nodeId, user, password);
}
#endif

#ifdef ENABLE_MQTT_BUILD
const DataSinkPtr createMQTTSink(OperatorId logicalOperatorId,
                                 SchemaPtr schema,
                                 QuerySubPlanId parentPlanId,
                                 NodeEngine::NodeEnginePtr nodeEngine,
                                 const std::string address,
                                 const std::string clientId,
                                 const std::string topic,
                                 const std::string user,
                                 uint64_t maxBufferedMSGs,
                                 const MQTTSinkDescriptor::TimeUnits timeUnit,
                                 uint64_t msgDelay,
                                 MQTTSinkDescriptor::ServiceQualities qualityOfService,
                                 bool asynchronousClient) {
    SinkFormatPtr format = std::make_shared<JsonFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<MQTTSink>(logicalOperatorId,
                                      format,
                                      parentPlanId,
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
