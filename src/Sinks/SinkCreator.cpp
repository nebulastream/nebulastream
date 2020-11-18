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
#include <Sinks/Mediums/OPCSink.hpp>
#include <Sinks/Mediums/PrintSink.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Sinks/Mediums/ZmqSink.hpp>
#include <Sinks/SinkCreator.hpp>
#include <Util/Logger.hpp>

namespace NES {

const DataSinkPtr createTestSink() {
    NES_ERROR("Called unimplemented Function");
    NES_NOT_IMPLEMENTED();
}

const DataSinkPtr createTextFileSink(SchemaPtr schema, QuerySubPlanId parentPlanId, NodeEnginePtr nodeEngine,
                                     const std::string& filePath, bool append) {
    //TODO: this is not nice and should be fixed such that we only provide the paramter once
    SinkFormatPtr format = std::make_shared<TextFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<FileSink>(format, filePath, append, parentPlanId);
}

const DataSinkPtr createCSVFileSink(SchemaPtr schema, QuerySubPlanId parentPlanId, NodeEnginePtr nodeEngine,
                                    const std::string& filePath, bool append) {
    SinkFormatPtr format = std::make_shared<CsvFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<FileSink>(format, filePath, append, parentPlanId);
}

const DataSinkPtr createBinaryNESFileSink(SchemaPtr schema, QuerySubPlanId parentPlanId, NodeEnginePtr nodeEngine,
                                          const std::string& filePath, bool append) {
    SinkFormatPtr format = std::make_shared<NesFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<FileSink>(format, filePath, append, parentPlanId);
}

const DataSinkPtr createJSONFileSink(SchemaPtr schema, QuerySubPlanId parentPlanId, NodeEnginePtr nodeEngine,
                                     const std::string& filePath, bool append) {
    SinkFormatPtr format = std::make_shared<JsonFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<FileSink>(format, filePath, append, parentPlanId);
}

const DataSinkPtr createTextZmqSink(SchemaPtr schema, QuerySubPlanId parentPlanId, NodeEnginePtr nodeEngine,
                                    const std::string& host, const uint16_t port) {
    SinkFormatPtr format = std::make_shared<TextFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<ZmqSink>(format, host, port, false, parentPlanId);
}

const DataSinkPtr createCSVZmqSink(SchemaPtr schema, QuerySubPlanId parentPlanId, NodeEnginePtr nodeEngine,
                                   const std::string& host, const uint16_t port) {
    SinkFormatPtr format = std::make_shared<CsvFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<ZmqSink>(format, host, port, false, parentPlanId);
}

const DataSinkPtr createBinaryZmqSink(SchemaPtr schema, QuerySubPlanId parentPlanId, NodeEnginePtr nodeEngine,
                                      const std::string& host, const uint16_t port, bool internal) {
    SinkFormatPtr format = std::make_shared<NesFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<ZmqSink>(format, host, port, internal, parentPlanId);
}

const DataSinkPtr createTextPrintSink(SchemaPtr schema, QuerySubPlanId parentPlanId, NodeEnginePtr nodeEngine,
                                      std::ostream& out) {
    SinkFormatPtr format = std::make_shared<TextFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<PrintSink>(format, parentPlanId, out);
}

const DataSinkPtr createCSVPrintSink(SchemaPtr schema, QuerySubPlanId parentPlanId, NodeEnginePtr nodeEngine, std::ostream& out) {
    SinkFormatPtr format = std::make_shared<CsvFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<PrintSink>(format, parentPlanId, out);
}

const DataSinkPtr createNetworkSink(SchemaPtr schema, QuerySubPlanId parentPlanId, Network::NodeLocation nodeLocation,
                                    Network::NesPartition nesPartition, NodeEnginePtr nodeEngine, std::chrono::seconds waitTime,
                                    uint8_t retryTimes) {
    return std::make_shared<Network::NetworkSink>(schema, parentPlanId, nodeEngine->getNetworkManager(), nodeLocation,
                                                  nesPartition, nodeEngine->getBufferManager(), nodeEngine->getQueryManager(),
                                                  waitTime, retryTimes);
}

#ifdef ENABLE_KAFKA_BUILD
const DataSinkPtr createKafkaSinkWithSchema(SchemaPtr schema, const std::string& brokers, const std::string& topic,
                                            const size_t kafkaProducerTimeout) {
    return std::make_shared<KafkaSink>(schema, brokers, topic, kafkaProducerTimeout);
}
#endif

#ifdef ENABLE_OPC_BUILD
const DataSinkPtr createOPCSink(SchemaPtr schema, QuerySubPlanId parentPlanId, NodeEnginePtr nodeEngine, std::string url,
                                UA_NodeId nodeId, std::string user, std::string password) {
    NES_DEBUG("plz fix me" << parentPlanId);
    SinkFormatPtr format = std::make_shared<TextFormat>(schema, nodeEngine->getBufferManager());
    return std::make_shared<OPCSink>(format, url, nodeId, user, password);
}
#endif
}// namespace NES
