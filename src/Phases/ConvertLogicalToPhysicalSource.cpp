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

#include <Operators/LogicalOperators/Sources/BinarySourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/CsvSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/DefaultSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/KafkaSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/OPCSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SenseSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/ZmqSourceDescriptor.hpp>

#include <Network/NetworkManager.hpp>
#include <Phases/ConvertLogicalToPhysicalSource.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/Logger.hpp>

#include <NodeEngine/NodeEngine.hpp>
#include <Operators/LogicalOperators/Sources/YSBSourceDescriptor.hpp>

namespace NES {

DataSourcePtr ConvertLogicalToPhysicalSource::createDataSource(SourceDescriptorPtr sourceDescriptor, NodeEngine::NodeEnginePtr nodeEngine) {

    NES_ASSERT(nodeEngine, "invalid engine");
    BufferManagerPtr bufferManager = nodeEngine->getBufferManager();
    NodeEngine::QueryManagerPtr queryManager = nodeEngine->getQueryManager();
    Network::NetworkManagerPtr networkManager = nodeEngine->getNetworkManager();

    if (sourceDescriptor->instanceOf<ZmqSourceDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSource: Creating ZMQ source");
        const ZmqSourceDescriptorPtr zmqSourceDescriptor = sourceDescriptor->as<ZmqSourceDescriptor>();
        return createZmqSource(zmqSourceDescriptor->getSchema(), bufferManager, queryManager, zmqSourceDescriptor->getHost(),
                               zmqSourceDescriptor->getPort(), sourceDescriptor->getOperatorId());
    } else if (sourceDescriptor->instanceOf<DefaultSourceDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSource: Creating Default source");
        const DefaultSourceDescriptorPtr defaultSourceDescriptor = sourceDescriptor->as<DefaultSourceDescriptor>();
        return createDefaultDataSourceWithSchemaForVarBuffers(defaultSourceDescriptor->getSchema(), bufferManager, queryManager,
                                                              defaultSourceDescriptor->getNumbersOfBufferToProduce(),
                                                              defaultSourceDescriptor->getFrequency(),
                                                              sourceDescriptor->getOperatorId());
    } else if (sourceDescriptor->instanceOf<BinarySourceDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSource: Creating Binary File source");
        const BinarySourceDescriptorPtr binarySourceDescriptor = sourceDescriptor->as<BinarySourceDescriptor>();
        return createBinaryFileSource(binarySourceDescriptor->getSchema(), bufferManager, queryManager,
                                      binarySourceDescriptor->getFilePath(), sourceDescriptor->getOperatorId());
    } else if (sourceDescriptor->instanceOf<CsvSourceDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSource: Creating CSV file source");
        const CsvSourceDescriptorPtr csvSourceDescriptor = sourceDescriptor->as<CsvSourceDescriptor>();
        return createCSVFileSource(csvSourceDescriptor->getSchema(), bufferManager, queryManager,
                                   csvSourceDescriptor->getFilePath(), csvSourceDescriptor->getDelimiter(),
                                   csvSourceDescriptor->getNumberOfTuplesToProducePerBuffer(),
                                   csvSourceDescriptor->getNumBuffersToProcess(), csvSourceDescriptor->getFrequency(),
                                   csvSourceDescriptor->getSkipHeader(), sourceDescriptor->getOperatorId());
#ifdef ENABLE_KAFKA_BUILD
    } else if (sourceDescriptor->instanceOf<KafkaSourceDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSource: Creating Kafka source");
        const KafkaSourceDescriptorPtr kafkaSourceDescriptor = sourceDescriptor->as<KafkaSourceDescriptor>();
        return createKafkaSource(kafkaSourceDescriptor->getSchema(), bufferManager, queryManager,
                                 kafkaSourceDescriptor->getBrokers(), kafkaSourceDescriptor->getTopic(),
                                 kafkaSourceDescriptor->getGroupId(), kafkaSourceDescriptor->isAutoCommit(),
                                 kafkaSourceDescriptor->getKafkaConnectTimeout());
#endif
#ifdef ENABLE_OPC_BUILD
    } else if (sourceDescriptor->instanceOf<OPCSourceDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSource: Creating OPC source");
        const OPCSourceDescriptorPtr opcSourceDescriptor = sourceDescriptor->as<OPCSourceDescriptor>();
        return createOPCSource(opcSourceDescriptor->getSchema(), bufferManager, queryManager, opcSourceDescriptor->getUrl(),
                               opcSourceDescriptor->getNodeId(), opcSourceDescriptor->getUser(),
                               opcSourceDescriptor->getPassword(), sourceDescriptor->getOperatorId());
#endif
    } else if (sourceDescriptor->instanceOf<SenseSourceDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSource: Creating sense source");
        const SenseSourceDescriptorPtr senseSourceDescriptor = sourceDescriptor->as<SenseSourceDescriptor>();
        return createSenseSource(senseSourceDescriptor->getSchema(), bufferManager, queryManager,
                                 senseSourceDescriptor->getUdfs(), sourceDescriptor->getOperatorId());
    } else if (sourceDescriptor->instanceOf<Network::NetworkSourceDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSource: Creating network source");
        const Network::networkSourceDescriptorPtr networkSourceDescriptor =
            sourceDescriptor->as<Network::NetworkSourceDescriptor>();
        return createNetworkSource(networkSourceDescriptor->getSchema(), bufferManager, queryManager, networkManager,
                                   networkSourceDescriptor->getNesPartition());
    } else if (sourceDescriptor->instanceOf<YSBSourceDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSource: Creating ysb source");
        const YSBSourceDescriptorPtr ysbSourceDescriptor = sourceDescriptor->as<YSBSourceDescriptor>();
        return createYSBSource(bufferManager, queryManager, ysbSourceDescriptor->getNumberOfTuplesToProducePerBuffer(),
                               ysbSourceDescriptor->getNumBuffersToProcess(), ysbSourceDescriptor->getFrequency(),
                               ysbSourceDescriptor->getOperatorId());
    } else {
        NES_ERROR("ConvertLogicalToPhysicalSource: Unknown Source Descriptor Type " << sourceDescriptor->getSchema()->toString());
        throw std::invalid_argument("Unknown Source Descriptor Type");
    }
}

}// namespace NES
