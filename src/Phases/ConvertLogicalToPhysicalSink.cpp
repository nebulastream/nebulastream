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

#include <Network/NetworkManager.hpp>
#include <Network/NetworkSink.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/KafkaSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/OPCSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/ZmqSinkDescriptor.hpp>
#include <Phases/ConvertLogicalToPhysicalSink.hpp>
#include <Sinks/SinkCreator.hpp>
#include <Util/Logger.hpp>

namespace NES {

DataSinkPtr ConvertLogicalToPhysicalSink::createDataSink(SchemaPtr schema, SinkDescriptorPtr sinkDescriptor, NodeEnginePtr nodeEngine, QuerySubPlanId querySubPlanId) {
    NES_ASSERT(nodeEngine, "Invalid node engine");
    if (sinkDescriptor->instanceOf<PrintSinkDescriptor>()) {
        NES_DEBUG("ConvertLogicalToPhysicalSink: Creating print sink" << schema->toString());
        const PrintSinkDescriptorPtr printSinkDescriptor = sinkDescriptor->as<PrintSinkDescriptor>();
        return createTextPrintSink(schema, querySubPlanId, nodeEngine, std::cout);
    } else if (sinkDescriptor->instanceOf<ZmqSinkDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSink: Creating ZMQ sink");
        const ZmqSinkDescriptorPtr zmqSinkDescriptor = sinkDescriptor->as<ZmqSinkDescriptor>();
        return createBinaryZmqSink(schema, querySubPlanId, nodeEngine, zmqSinkDescriptor->getHost(), zmqSinkDescriptor->getPort(), zmqSinkDescriptor->isInternal());
    }
#ifdef ENABLE_KAFKA_BUILD
    else if (sinkDescriptor->instanceOf<KafkaSinkDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSink: Creating Kafka sink");
        const KafkaSinkDescriptorPtr kafkaSinkDescriptor = sinkDescriptor->as<KafkaSinkDescriptor>();
        return createKafkaSinkWithSchema(schema, querySubPlanId, kafkaSinkDescriptor->getBrokers(),
                                         kafkaSinkDescriptor->getTopic(),
                                         kafkaSinkDescriptor->getTimeout());
    }
#endif
#ifdef ENABLE_OPC_BUILD
    else if (sinkDescriptor->instanceOf<OPCSinkDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSink: Creating OPC sink");
        const OPCSinkDescriptorPtr opcSinkDescriptor = sinkDescriptor->as<OPCSinkDescriptor>();
        return createOPCSink(schema, querySubPlanId, nodeEngine, opcSinkDescriptor->getUrl(),
                             opcSinkDescriptor->getNodeId(),
                             opcSinkDescriptor->getUser(),
                             opcSinkDescriptor->getPassword());
    }
#endif
    else if (sinkDescriptor->instanceOf<FileSinkDescriptor>()) {
        auto fileSinkDescriptor = sinkDescriptor->as<FileSinkDescriptor>();
        NES_INFO("ConvertLogicalToPhysicalSink: Creating Binary file sink for format=" << fileSinkDescriptor->getSinkFormatAsString());
        if (fileSinkDescriptor->getSinkFormatAsString() == "CSV_FORMAT") {
            return createCSVFileSink(schema, querySubPlanId, nodeEngine, fileSinkDescriptor->getFileName(), fileSinkDescriptor->getAppend());
        } else if (fileSinkDescriptor->getSinkFormatAsString() == "NES_FORMAT") {
            return createBinaryNESFileSink(schema, querySubPlanId, nodeEngine, fileSinkDescriptor->getFileName(), fileSinkDescriptor->getAppend());
        } else if (fileSinkDescriptor->getSinkFormatAsString() == "TEXT_FORMAT") {
            return createTextFileSink(schema, querySubPlanId, nodeEngine, fileSinkDescriptor->getFileName(), fileSinkDescriptor->getAppend());
        } else {
            NES_ERROR("createDataSink: unsupported format");
            throw std::invalid_argument("Unknown File format");
        }
    } else if (sinkDescriptor->instanceOf<Network::NetworkSinkDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSink: Creating network sink");
        auto networkSinkDescriptor = sinkDescriptor->as<Network::NetworkSinkDescriptor>();
        return createNetworkSink(schema, querySubPlanId, networkSinkDescriptor->getNodeLocation(),
                                 networkSinkDescriptor->getNesPartition(), nodeEngine, networkSinkDescriptor->getWaitTime(),
                                 networkSinkDescriptor->getRetryTimes());
    } else {
        NES_ERROR("ConvertLogicalToPhysicalSink: Unknown Sink Descriptor Type");
        throw std::invalid_argument("Unknown Sink Descriptor Type");
    }
}

}// namespace NES
