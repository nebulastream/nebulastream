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
#include <Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/KafkaSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/MQTTSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/NullOutputSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/OPCSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/ZmqSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/MaterializedViewSinkDescriptor.hpp>
#include <Phases/ConvertLogicalToPhysicalSink.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Sinks/SinkCreator.hpp>
#include <Util/Logger.hpp>

namespace NES {

DataSinkPtr ConvertLogicalToPhysicalSink::createDataSink(OperatorId operatorId,
                                                         const SinkDescriptorPtr& sinkDescriptor,
                                                         const SchemaPtr& schema,
                                                         const Runtime::NodeEnginePtr& nodeEngine,
                                                         const QueryCompilation::PipelineQueryPlanPtr& querySubPlan,
                                                         size_t numOfProducers) {
    NES_DEBUG("Convert sink " << operatorId);
    NES_ASSERT(nodeEngine, "Invalid node engine");
    NES_ASSERT(querySubPlan, "Invalid query sub-plan");
    if (sinkDescriptor->instanceOf<PrintSinkDescriptor>()) {
        NES_DEBUG("ConvertLogicalToPhysicalSink: Creating print sink" << schema->toString());
        return createTextPrintSink(schema, querySubPlan->getQuerySubPlanId(), nodeEngine, std::cout);
    }
    if (sinkDescriptor->instanceOf<NullOutputSinkDescriptor>()) {
        NES_DEBUG("ConvertLogicalToPhysicalSink: Creating nulloutput sink" << schema->toString());
        return createNullOutputSink(querySubPlan->getQuerySubPlanId(), nodeEngine);
    } else if (sinkDescriptor->instanceOf<ZmqSinkDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSink: Creating ZMQ sink");
        const ZmqSinkDescriptorPtr zmqSinkDescriptor = sinkDescriptor->as<ZmqSinkDescriptor>();
        return createBinaryZmqSink(schema,
                                   querySubPlan->getQuerySubPlanId(),
                                   nodeEngine,
                                   zmqSinkDescriptor->getHost(),
                                   zmqSinkDescriptor->getPort(),
                                   zmqSinkDescriptor->isInternal());
    }
#ifdef ENABLE_KAFKA_BUILD
    else if (sinkDescriptor->instanceOf<KafkaSinkDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSink: Creating Kafka sink");
        const KafkaSinkDescriptorPtr kafkaSinkDescriptor = sinkDescriptor->as<KafkaSinkDescriptor>();
        return createKafkaSinkWithSchema(schema,
                                         querySubPlan->getQuerySubPlanId(),
                                         kafkaSinkDescriptor->getBrokers(),
                                         kafkaSinkDescriptor->getTopic(),
                                         kafkaSinkDescriptor->getTimeout());
    }
#endif
#ifdef ENABLE_OPC_BUILD
    else if (sinkDescriptor->instanceOf<OPCSinkDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSink: Creating OPC sink");
        const OPCSinkDescriptorPtr opcSinkDescriptor = sinkDescriptor->as<OPCSinkDescriptor>();
        return createOPCSink(schema,
                             querySubPlan->getQuerySubPlanId(),
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
        std::string clientId =
            (mqttSinkDescriptor->getClientId() != "") ? mqttSinkDescriptor->getClientId() : std::to_string(operatorId);
        return createMQTTSink(schema,
                              querySubPlan->getQuerySubPlanId(),
                              nodeEngine,
                              mqttSinkDescriptor->getAddress(),
                              clientId,
                              mqttSinkDescriptor->getTopic(),
                              mqttSinkDescriptor->getUser(),
                              mqttSinkDescriptor->getMaxBufferedMSGs(),
                              mqttSinkDescriptor->getTimeUnit(),
                              mqttSinkDescriptor->getMsgDelay(),
                              mqttSinkDescriptor->getQualityOfService(),
                              mqttSinkDescriptor->getAsynchronousClient());
    }
#endif
    else if (sinkDescriptor->instanceOf<FileSinkDescriptor>()) {
        auto fileSinkDescriptor = sinkDescriptor->as<FileSinkDescriptor>();
        NES_INFO(
            "ConvertLogicalToPhysicalSink: Creating Binary file sink for format=" << fileSinkDescriptor->getSinkFormatAsString());
        if (fileSinkDescriptor->getSinkFormatAsString() == "CSV_FORMAT") {
            return createCSVFileSink(schema,
                                     querySubPlan->getQuerySubPlanId(),
                                     nodeEngine,
                                     fileSinkDescriptor->getFileName(),
                                     fileSinkDescriptor->getAppend());
        } else if (fileSinkDescriptor->getSinkFormatAsString() == "NES_FORMAT") {
            return createBinaryNESFileSink(schema,
                                           querySubPlan->getQuerySubPlanId(),
                                           nodeEngine,
                                           fileSinkDescriptor->getFileName(),
                                           fileSinkDescriptor->getAppend());
        } else if (fileSinkDescriptor->getSinkFormatAsString() == "TEXT_FORMAT") {
            return createTextFileSink(schema,
                                      querySubPlan->getQuerySubPlanId(),
                                      nodeEngine,
                                      fileSinkDescriptor->getFileName(),
                                      fileSinkDescriptor->getAppend());
        } else {
            NES_ERROR("createDataSink: unsupported format");
            throw std::invalid_argument("Unknown File format");
        }
    } else if (sinkDescriptor->instanceOf<Network::NetworkSinkDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSink: Creating network sink");
        auto networkSinkDescriptor = sinkDescriptor->as<Network::NetworkSinkDescriptor>();
        return createNetworkSink(schema,
                                 networkSinkDescriptor->getUniqueNetworkSinkDescriptorId(),
                                 querySubPlan->getQuerySubPlanId(),
                                 networkSinkDescriptor->getNodeLocation(),
                                 networkSinkDescriptor->getNesPartition(),
                                 nodeEngine,
                                 numOfProducers,
                                 networkSinkDescriptor->getWaitTime(),
                                 networkSinkDescriptor->getRetryTimes());
    } else if (sinkDescriptor->instanceOf<Experimental::MaterializedView::MaterializedViewSinkDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSink: Creating materialized view sink");
        auto materializedViewSinkDescriptor = sinkDescriptor->as<Experimental::MaterializedView::MaterializedViewSinkDescriptor>();
        return Experimental::MaterializedView::createMaterializedViewSink(schema,
                                          nodeEngine,
                                          querySubPlan->getQuerySubPlanId(),
                                          materializedViewSinkDescriptor->getViewId());
    } else {
        NES_ERROR("ConvertLogicalToPhysicalSink: Unknown Sink Descriptor Type");
        throw std::invalid_argument("Unknown Sink Descriptor Type");
    }
}

}// namespace NES
