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
#include <Operators/LogicalOperators/Sources/LambdaSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/MQTTSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/MemorySourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/OPCSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SenseSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/ZmqSourceDescriptor.hpp>

#include <Network/NetworkManager.hpp>
#include <Phases/ConvertLogicalToPhysicalSource.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/Logger.hpp>

#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineForwaredRefs.hpp>

#ifdef NES_ENABLE_NUMA_SUPPORT
#if defined(__linux__)
#include <numa.h>
#endif
#endif
namespace NES {

DataSourcePtr
ConvertLogicalToPhysicalSource::createDataSource(OperatorId operatorId,
                                                 const SourceDescriptorPtr& sourceDescriptor,
                                                 const Runtime::NodeEnginePtr& nodeEngine,
                                                 size_t numSourceLocalBuffers,
                                                 const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors) {
    NES_ASSERT(nodeEngine, "invalid engine");
    auto numaNodeIndex = 0;
#ifdef NES_ENABLE_NUMA_SUPPORT
#if defined(__linux__)
    if (sourceDescriptor->instanceOf<MemorySourceDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSource: Creating memory source");
        auto memorySourceDescriptor = sourceDescriptor->as<MemorySourceDescriptor>();
        auto nodeOfCpu = numa_node_of_cpu(memorySourceDescriptor->getSourceAffinity());
        numaNodeIndex = nodeOfCpu;
    }
#endif
#endif
    auto bufferManager = nodeEngine->getBufferManager(numaNodeIndex);
    auto queryManager = nodeEngine->getQueryManager();
    auto networkManager = nodeEngine->getNetworkManager();

    if (sourceDescriptor->instanceOf<ZmqSourceDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSource: Creating ZMQ source");
        const ZmqSourceDescriptorPtr zmqSourceDescriptor = sourceDescriptor->as<ZmqSourceDescriptor>();
        return createZmqSource(zmqSourceDescriptor->getSchema(),
                               bufferManager,
                               queryManager,
                               zmqSourceDescriptor->getHost(),
                               zmqSourceDescriptor->getPort(),
                               operatorId,
                               numSourceLocalBuffers,
                               successors);
    }
    if (sourceDescriptor->instanceOf<DefaultSourceDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSource: Creating Default source");
        const DefaultSourceDescriptorPtr defaultSourceDescriptor = sourceDescriptor->as<DefaultSourceDescriptor>();
        return createDefaultDataSourceWithSchemaForVarBuffers(defaultSourceDescriptor->getSchema(),
                                                              bufferManager,
                                                              queryManager,
                                                              defaultSourceDescriptor->getNumbersOfBufferToProduce(),
                                                              defaultSourceDescriptor->getFrequencyCount(),
                                                              operatorId,
                                                              numSourceLocalBuffers,
                                                              successors);
    } else if (sourceDescriptor->instanceOf<BinarySourceDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSource: Creating Binary File source");
        const BinarySourceDescriptorPtr binarySourceDescriptor = sourceDescriptor->as<BinarySourceDescriptor>();
        return createBinaryFileSource(binarySourceDescriptor->getSchema(),
                                      bufferManager,
                                      queryManager,
                                      binarySourceDescriptor->getFilePath(),
                                      operatorId,
                                      numSourceLocalBuffers,
                                      successors);
    } else if (sourceDescriptor->instanceOf<CsvSourceDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSource: Creating CSV file source");
        const CsvSourceDescriptorPtr csvSourceDescriptor = sourceDescriptor->as<CsvSourceDescriptor>();
        return createCSVFileSource(csvSourceDescriptor->getSchema(),
                                   bufferManager,
                                   queryManager,
                                   csvSourceDescriptor->getFilePath(),
                                   csvSourceDescriptor->getDelimiter(),
                                   csvSourceDescriptor->getNumberOfTuplesToProducePerBuffer(),
                                   csvSourceDescriptor->getNumBuffersToProcess(),
                                   csvSourceDescriptor->getFrequencyCount(),
                                   csvSourceDescriptor->getSkipHeader(),
                                   operatorId,
                                   numSourceLocalBuffers,
                                   successors);
#ifdef ENABLE_KAFKA_BUILD
    } else if (sourceDescriptor->instanceOf<KafkaSourceDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSource: Creating Kafka source");
        const KafkaSourceDescriptorPtr kafkaSourceDescriptor = sourceDescriptor->as<KafkaSourceDescriptor>();
        return createKafkaSource(kafkaSourceDescriptor->getSchema(),
                                 bufferManager,
                                 queryManager,
                                 kafkaSourceDescriptor->getBrokers(),
                                 kafkaSourceDescriptor->getTopic(),
                                 kafkaSourceDescriptor->getGroupId(),
                                 kafkaSourceDescriptor->isAutoCommit(),
                                 kafkaSourceDescriptor->getKafkaConnectTimeout());
#endif
#ifdef ENABLE_MQTT_BUILD
    } else if (sourceDescriptor->instanceOf<MQTTSourceDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSource: Creating OPC source");
        const MQTTSourceDescriptorPtr mqttSourceDescriptor = sourceDescriptor->as<MQTTSourceDescriptor>();
        return createMQTTSource(mqttSourceDescriptor->getSchema(),
                                bufferManager,
                                queryManager,
                                mqttSourceDescriptor->getServerAddress(),
                                mqttSourceDescriptor->getClientId(),
                                mqttSourceDescriptor->getUser(),
                                mqttSourceDescriptor->getTopic(),
                                operatorId,
                                numSourceLocalBuffers,
                                successors);
#endif
#ifdef ENABLE_OPC_BUILD
    } else if (sourceDescriptor->instanceOf<OPCSourceDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSource: Creating OPC source");
        const OPCSourceDescriptorPtr opcSourceDescriptor = sourceDescriptor->as<OPCSourceDescriptor>();
        return createOPCSource(opcSourceDescriptor->getSchema(),
                               bufferManager,
                               queryManager,
                               opcSourceDescriptor->getUrl(),
                               opcSourceDescriptor->getNodeId(),
                               opcSourceDescriptor->getUser(),
                               opcSourceDescriptor->getPassword(),
                               operatorId,
                               numSourceLocalBuffers,
                               successors);
#endif
    } else if (sourceDescriptor->instanceOf<SenseSourceDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSource: Creating sense source");
        const SenseSourceDescriptorPtr senseSourceDescriptor = sourceDescriptor->as<SenseSourceDescriptor>();
        return createSenseSource(senseSourceDescriptor->getSchema(),
                                 bufferManager,
                                 queryManager,
                                 senseSourceDescriptor->getUdfs(),
                                 operatorId,
                                 numSourceLocalBuffers,
                                 successors);
    } else if (sourceDescriptor->instanceOf<Network::NetworkSourceDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSource: Creating network source");
        const Network::networkSourceDescriptorPtr networkSourceDescriptor =
            sourceDescriptor->as<Network::NetworkSourceDescriptor>();
        return createNetworkSource(networkSourceDescriptor->getSchema(),
                                   bufferManager,
                                   queryManager,
                                   networkManager,
                                   networkSourceDescriptor->getNesPartition(),
                                   numSourceLocalBuffers,
                                   successors);
    } else if (sourceDescriptor->instanceOf<MemorySourceDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSource: Creating memory source");
        auto memorySourceDescriptor = sourceDescriptor->as<MemorySourceDescriptor>();
        return createMemorySource(memorySourceDescriptor->getSchema(),
                                  bufferManager,
                                  queryManager,
                                  memorySourceDescriptor->getMemoryArea(),
                                  memorySourceDescriptor->getMemoryAreaSize(),
                                  memorySourceDescriptor->getNumBuffersToProcess(),
                                  memorySourceDescriptor->getGatheringValue(),
                                  operatorId,
                                  numSourceLocalBuffers,
                                  memorySourceDescriptor->getGatheringMode(),
                                  memorySourceDescriptor->getSourceMode(),
                                  successors,
                                  memorySourceDescriptor->getSourceAffinity());
    } else if (sourceDescriptor->instanceOf<LambdaSourceDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSource: Creating lambda source");
        auto lambdaSourceDescriptor = sourceDescriptor->as<LambdaSourceDescriptor>();
        return createLambdaSource(lambdaSourceDescriptor->getSchema(),
                                  bufferManager,
                                  queryManager,
                                  lambdaSourceDescriptor->getNumBuffersToProcess(),
                                  lambdaSourceDescriptor->getGatheringValue(),
                                  std::move(lambdaSourceDescriptor->getGeneratorFunction()),
                                  operatorId,
                                  numSourceLocalBuffers,
                                  lambdaSourceDescriptor->getGatheringMode(),
                                  successors);
    } else {
        NES_ERROR("ConvertLogicalToPhysicalSource: Unknown Source Descriptor Type " << sourceDescriptor->getSchema()->toString());
        throw std::invalid_argument("Unknown Source Descriptor Type");
    }
}

}// namespace NES
