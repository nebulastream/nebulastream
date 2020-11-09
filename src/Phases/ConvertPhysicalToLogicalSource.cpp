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

#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/BinarySourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/CsvSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/DefaultSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/KafkaSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/OPCSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SenseSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/ZmqSourceDescriptor.hpp>
#include <Phases/ConvertPhysicalToLogicalSource.hpp>
#include <Sources/BinarySource.hpp>
#include <Sources/CSVSource.hpp>
#include <Sources/DataSource.hpp>
#include <Sources/DefaultSource.hpp>
#include <Sources/KafkaSource.hpp>
#include <Sources/OPCSource.hpp>
#include <Sources/SenseSource.hpp>
#include <Sources/ZmqSource.hpp>
#include <Util/Logger.hpp>

namespace NES {

SourceDescriptorPtr ConvertPhysicalToLogicalSource::createSourceDescriptor(DataSourcePtr dataSource) {
    SourceType srcType = dataSource->getType();
    switch (srcType) {

        case ZMQ_SOURCE: {

            NES_INFO("ConvertPhysicalToLogicalSource: Creating ZMQ source");
            const ZmqSourcePtr zmqSourcePtr = std::dynamic_pointer_cast<ZmqSource>(dataSource);
            SourceDescriptorPtr zmqSourceDescriptor =
                ZmqSourceDescriptor::create(dataSource->getSchema(), zmqSourcePtr->getHost(), zmqSourcePtr->getPort(), dataSource->getSourceId());
            return zmqSourceDescriptor;
        }
        case DEFAULT_SOURCE: {
            NES_INFO("ConvertPhysicalToLogicalSource: Creating Default source");
            const DefaultSourcePtr defaultSourcePtr = std::dynamic_pointer_cast<DefaultSource>(dataSource);
            const SourceDescriptorPtr defaultSourceDescriptor =
                DefaultSourceDescriptor::create(defaultSourcePtr->getSchema(),
                                                defaultSourcePtr->getNumBuffersToProcess(),
                                                defaultSourcePtr->getGatheringInterval(), dataSource->getSourceId());
            return defaultSourceDescriptor;
        }
        case BINARY_SOURCE: {
            NES_INFO("ConvertPhysicalToLogicalSource: Creating Binary File source");
            const BinarySourcePtr binarySourcePtr = std::dynamic_pointer_cast<BinarySource>(dataSource);
            const SourceDescriptorPtr binarySourceDescriptor =
                BinarySourceDescriptor::create(binarySourcePtr->getSchema(), binarySourcePtr->getFilePath(), dataSource->getSourceId());
            return binarySourceDescriptor;
        }
        case CSV_SOURCE: {
            NES_INFO("ConvertPhysicalToLogicalSource: Creating CSV File source");
            const CSVSourcePtr csvSourcePtr = std::dynamic_pointer_cast<CSVSource>(dataSource);
            const SourceDescriptorPtr csvSourceDescriptor =
                CsvSourceDescriptor::create(csvSourcePtr->getSchema(),
                                            csvSourcePtr->getFilePath(),
                                            csvSourcePtr->getDelimiter(),
                                            csvSourcePtr->getNumberOfTuplesToProducePerBuffer(),
                                            csvSourcePtr->getNumBuffersToProcess(),
                                            csvSourcePtr->getGatheringInterval(),
                                            csvSourcePtr->isEndlessRepeat(),
                                            csvSourcePtr->getSkipHeader(), dataSource->getSourceId());
            return csvSourceDescriptor;
        }
#ifdef ENABLE_KAFKA_BUILD
        case KAFKA_SOURCE: {
            NES_INFO("ConvertPhysicalToLogicalSource: Creating Kafka source");
            const KafkaSourcePtr kafkaSourcePtr = std::dynamic_pointer_cast<KafkaSource>(dataSource);
            const SourceDescriptorPtr kafkaSourceDescriptor =
                KafkaSourceDescriptor::create(kafkaSourcePtr->getSchema(),
                                              kafkaSourcePtr->getBrokers(),
                                              kafkaSourcePtr->getTopic(),
                                              kafkaSourcePtr->getGroupId(),
                                              kafkaSourcePtr->isAutoCommit(),
                                              kafkaSourcePtr->getKafkaConsumerTimeout().count());
            return kafkaSourceDescriptor;
        }
#endif
#ifdef ENABLE_OPC_BUILD
        case OPC_SOURCE: {
            NES_INFO("ConvertPhysicalToLogicalSource: Creating OPC source");
            const OPCSourcePtr opcSourcePtr = std::dynamic_pointer_cast<OPCSource>(dataSource);
            const SourceDescriptorPtr opcSourceDescriptor = OPCSourceDescriptor::create(opcSourcePtr->getSchema(), opcSourcePtr->getUrl(),
                                                                                        opcSourcePtr->getNodeId(), opcSourcePtr->getUser(),
                                                                                        opcSourcePtr->getPassword());
            return opcSourceDescriptor;
        }
#endif
        case SENSE_SOURCE: {
            NES_INFO("ConvertPhysicalToLogicalSource: Creating sense source");
            const SenseSourcePtr senseSourcePtr = std::dynamic_pointer_cast<SenseSource>(dataSource);
            const SourceDescriptorPtr senseSourceDescriptor =
                SenseSourceDescriptor::create(senseSourcePtr->getSchema(), senseSourcePtr->getUdsf(), dataSource->getSourceId());
            return senseSourceDescriptor;
        }
        default: {
            NES_ERROR("ConvertPhysicalToLogicalSource: Unknown Data Source Type " << srcType);
            throw std::invalid_argument("Unknown Source Descriptor Type ");
        }
    }
}

}// namespace NES
