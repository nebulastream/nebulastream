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
#include <Operators/LogicalOperators/Network/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/KafkaSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/MQTTSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/MonitoringSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/NullOutputSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sinks/StatisticSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/ZmqSinkDescriptor.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <QueryCompiler/Phases/Translations/ConvertLogicalToPhysicalSink.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Sinks/SinkCreator.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES {

DataSinkPtr ConvertLogicalToPhysicalSink::createDataSink(OperatorId operatorId,
                                                         const SinkDescriptorPtr& sinkDescriptor,
                                                         const SchemaPtr& schema,
                                                         const Runtime::NodeEnginePtr& nodeEngine,
                                                         const QueryCompilation::PipelineQueryPlanPtr& pipelineQueryPlan,
                                                         size_t numOfProducers) {
    NES_DEBUG("Convert sink  {}", operatorId);
    NES_ASSERT(nodeEngine, "Invalid node engine");
    NES_ASSERT(pipelineQueryPlan, "Invalid query sub-plan");
    if (sinkDescriptor->instanceOf<PrintSinkDescriptor>()) {
        NES_DEBUG("ConvertLogicalToPhysicalSink: Creating print sink {}", schema->toString());
        const PrintSinkDescriptorPtr printSinkDescriptor = sinkDescriptor->as<PrintSinkDescriptor>();
        return createCsvPrintSink(schema,
                                  pipelineQueryPlan->getQueryId(),
                                  pipelineQueryPlan->getQuerySubPlanId(),
                                  pipelineQueryPlan->getQuerySubPlanVersion(),
                                  nodeEngine,
                                  numOfProducers,
                                  std::cout,
                                  printSinkDescriptor->getNumberOfOrigins());
    } else if (sinkDescriptor->instanceOf<NullOutputSinkDescriptor>()) {
        const NullOutputSinkDescriptorPtr nullOutputSinkDescriptor = sinkDescriptor->as<NullOutputSinkDescriptor>();
        NES_DEBUG("ConvertLogicalToPhysicalSink: Creating nulloutput sink {}", schema->toString());
        return createNullOutputSink(pipelineQueryPlan->getQueryId(),
                                    pipelineQueryPlan->getQuerySubPlanId(),
                                    pipelineQueryPlan->getQuerySubPlanVersion(),
                                    nodeEngine,
                                    numOfProducers,
                                    nullOutputSinkDescriptor->getNumberOfOrigins());
    } else if (sinkDescriptor->instanceOf<ZmqSinkDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSink: Creating ZMQ sink");
        const ZmqSinkDescriptorPtr zmqSinkDescriptor = sinkDescriptor->as<ZmqSinkDescriptor>();
        return createBinaryZmqSink(schema,
                                   pipelineQueryPlan->getQueryId(),
                                   pipelineQueryPlan->getQuerySubPlanId(),
                                   pipelineQueryPlan->getQuerySubPlanVersion(),
                                   nodeEngine,
                                   numOfProducers,
                                   zmqSinkDescriptor->getHost(),
                                   zmqSinkDescriptor->getPort(),
                                   zmqSinkDescriptor->isInternal(),
                                   zmqSinkDescriptor->getNumberOfOrigins());
    } else if (sinkDescriptor->instanceOf<MonitoringSinkDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSink: Creating Monitoring sink");
        const MonitoringSinkDescriptorPtr monitoringSinkDescriptor = sinkDescriptor->as<MonitoringSinkDescriptor>();
        return createMonitoringSink(nodeEngine->getMetricStore(),
                                    monitoringSinkDescriptor->getCollectorType(),
                                    schema,
                                    nodeEngine,
                                    numOfProducers,
                                    pipelineQueryPlan->getQueryId(),
                                    pipelineQueryPlan->getQuerySubPlanId(),
                                    pipelineQueryPlan->getQuerySubPlanVersion(),
                                    monitoringSinkDescriptor->getNumberOfOrigins());
    }
#ifdef ENABLE_KAFKA_BUILD
    else if (sinkDescriptor->instanceOf<KafkaSinkDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSink: Creating Kafka sink");
        const KafkaSinkDescriptorPtr kafkaSinkDescriptor = sinkDescriptor->as<KafkaSinkDescriptor>();

        if (kafkaSinkDescriptor->getSinkFormatAsString() == "CSV_FORMAT") {
            return createCsvKafkaSink(schema,
                                      pipelineQueryPlan->getQueryId(),
                                      pipelineQueryPlan->getQuerySubPlanId(),
                                      pipelineQueryPlan->getQuerySubPlanVersion(),
                                      nodeEngine,
                                      numOfProducers,
                                      kafkaSinkDescriptor->getBrokers(),
                                      kafkaSinkDescriptor->getTopic(),
                                      kafkaSinkDescriptor->getTimeout(),
                                      kafkaSinkDescriptor->getNumberOfOrigins());
        } else {
            NES_THROW_RUNTIME_ERROR("Sinkformat " << kafkaSinkDescriptor->getSinkFormatAsString()
                                                  << " currently not supported for Kafka");
        }
    }
#endif
#ifdef ENABLE_OPC_BUILD
    else if (sinkDescriptor->instanceOf<OPCSinkDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSink: Creating OPC sink");
        const OPCSinkDescriptorPtr opcSinkDescriptor = sinkDescriptor->as<OPCSinkDescriptor>();
        return createOPCSink(schema,
                             querySubPlan->getQueryId(),
                             querySubPlan->getQuerySubPlanId(),
                             querySubPlan->getQuerySubPlanVersion(),
                             nodeEngine,
                             opcSinkDescriptor->getUrl(),
                             opcSinkDescriptor->getNodeId(),
                             opcSinkDescriptor->getUser(),
                             opcSinkDescriptor->getPassword());
    }
#endif
#ifdef ENABLE_MQTT_BUILD
    else if (sinkDescriptor->instanceOf<MQTTSinkDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSink: Creating MQTT sink");
        const MQTTSinkDescriptorPtr mqttSinkDescriptor = sinkDescriptor->as<MQTTSinkDescriptor>();
        // Two MQTT clients with the same client-id can not communicate with the same broker. Therefore, client-ids should generally be unique.
        // If the user does not pass a client-id explicitly, we utilize the operatorId to generate a client-id that is guaranteed to be unique.
        std::string clientId = mqttSinkDescriptor->getClientId() + "-" + operatorId.toString();
        return createMQTTSink(schema,
                              pipelineQueryPlan->getQueryId(),
                              pipelineQueryPlan->getQuerySubPlanId(),
                              pipelineQueryPlan->getQuerySubPlanVersion(),
                              nodeEngine,
                              numOfProducers,
                              mqttSinkDescriptor->getAddress(),
                              clientId,
                              mqttSinkDescriptor->getTopic(),
                              mqttSinkDescriptor->getUser(),
                              mqttSinkDescriptor->getMaxBufferedMSGs(),
                              mqttSinkDescriptor->getTimeUnit(),
                              mqttSinkDescriptor->getMsgDelay(),
                              mqttSinkDescriptor->getQualityOfService(),
                              mqttSinkDescriptor->getAsynchronousClient(),
                              mqttSinkDescriptor->getNumberOfOrigins());
    }
#endif
    else if (sinkDescriptor->instanceOf<FileSinkDescriptor>()) {
        auto fileSinkDescriptor = sinkDescriptor->as<FileSinkDescriptor>();
        NES_INFO("ConvertLogicalToPhysicalSink: Creating file sink for format={}", fileSinkDescriptor->getSinkFormatAsString());
        if (fileSinkDescriptor->getSinkFormatAsString() == "CSV_FORMAT") {
            return createCSVFileSink(schema,
                                     pipelineQueryPlan->getQueryId(),
                                     pipelineQueryPlan->getQuerySubPlanId(),
                                     pipelineQueryPlan->getQuerySubPlanVersion(),
                                     nodeEngine,
                                     numOfProducers,
                                     fileSinkDescriptor->getFileName(),
                                     fileSinkDescriptor->getAppend(),
                                     fileSinkDescriptor->getAddTimestamp(),
                                     fileSinkDescriptor->getNumberOfOrigins());
        } else if (fileSinkDescriptor->getSinkFormatAsString() == "NES_FORMAT") {
            return createBinaryNESFileSink(schema,
                                           pipelineQueryPlan->getQueryId(),
                                           pipelineQueryPlan->getQuerySubPlanId(),
                                           pipelineQueryPlan->getQuerySubPlanVersion(),
                                           nodeEngine,
                                           numOfProducers,
                                           fileSinkDescriptor->getFileName(),
                                           fileSinkDescriptor->getAppend(),
                                           fileSinkDescriptor->getNumberOfOrigins());
        } else if (fileSinkDescriptor->getSinkFormatAsString() == "MIGRATION_FORMAT") {
            return createMigrateFileSink(pipelineQueryPlan->getQueryId(),
                                         pipelineQueryPlan->getQuerySubPlanId(),
                                         pipelineQueryPlan->getQuerySubPlanVersion(),
                                         nodeEngine,
                                         numOfProducers,
                                         fileSinkDescriptor->getFileName(),
                                         fileSinkDescriptor->getAppend(),
                                         fileSinkDescriptor->getNumberOfOrigins());
        } else {
            NES_ERROR("createDataSink: unsupported format");
            throw std::invalid_argument("Unknown File format");
        }
    } else if (sinkDescriptor->instanceOf<Network::NetworkSinkDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSink: Creating network sink");
        auto networkSinkDescriptor = sinkDescriptor->as<Network::NetworkSinkDescriptor>();
        return createNetworkSink(schema,
                                 networkSinkDescriptor->getUniqueId(),
                                 pipelineQueryPlan->getQueryId(),
                                 pipelineQueryPlan->getQuerySubPlanId(),
                                 pipelineQueryPlan->getQuerySubPlanVersion(),
                                 networkSinkDescriptor->getNodeLocation(),
                                 networkSinkDescriptor->getNesPartition(),
                                 nodeEngine,
                                 numOfProducers,
                                 networkSinkDescriptor->getWaitTime(),
                                 networkSinkDescriptor->getVersion(),
                                 nodeEngine->getNumberOfBuffersToProduce(),
                                 networkSinkDescriptor->getNumberOfOrigins(),
                                 networkSinkDescriptor->getRetryTimes());
    } else if (sinkDescriptor->instanceOf<Statistic::StatisticSinkDescriptor>()) {
        const auto statisticSinkDescriptor = sinkDescriptor->as<Statistic::StatisticSinkDescriptor>();
        return createStatisticSink(schema,
                                   nodeEngine,
                                   numOfProducers,
                                   pipelineQueryPlan->getQueryId(),
                                   pipelineQueryPlan->getQuerySubPlanId(),
                                   pipelineQueryPlan->getQuerySubPlanVersion(),
                                   statisticSinkDescriptor->getNumberOfOrigins(),
                                   statisticSinkDescriptor->getSinkFormatType(),
                                   statisticSinkDescriptor->getSinkDataCodec());
    } else {
        NES_ERROR("ConvertLogicalToPhysicalSink: Unknown Sink Descriptor Type");
        throw std::invalid_argument("Unknown Sink Descriptor Type");
    }
}

}// namespace NES
