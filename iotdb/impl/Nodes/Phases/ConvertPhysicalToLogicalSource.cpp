#include <Nodes/Operators/LogicalOperators/Sources/BinarySourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/CsvSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/DefaultSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/KafkaSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SenseSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/ZmqSourceDescriptor.hpp>
#include <Nodes/Phases/ConvertPhysicalToLogicalSource.hpp>
#include <Nodes/Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <SourceSink/ZmqSource.hpp>
#include <SourceSink/DefaultSource.hpp>
#include <SourceSink/CSVSource.hpp>
#include <SourceSink/BinarySource.hpp>
#include <SourceSink/KafkaSource.hpp>
#include <SourceSink/SenseSource.hpp>
#include <SourceSink/DataSource.hpp>
#include <Util/Logger.hpp>

namespace NES {

LogicalOperatorNodePtr ConvertPhysicalToLogicalSource::createDataSource(DataSourcePtr dataSource) {

    SourceType srcType = dataSource->getType();
    BufferManagerPtr bufferManager;
    QueryManagerPtr queryManager;
    switch (srcType) {

        case ZMQ_SOURCE: {

            NES_INFO("ConvertPhysicalToLogicalSource: Creating ZMQ source");
            const ZmqSourcePtr zmqSourcePtr = std::dynamic_pointer_cast<ZmqSource>(dataSource);
            SourceDescriptorPtr zmqSourceDescriptor =
                ZmqSourceDescriptor::create(dataSource->getSchema(), zmqSourcePtr->getHost(), zmqSourcePtr->getPort());
            return createSourceLogicalOperatorNode(zmqSourceDescriptor);
        }
        case DEFAULT_SOURCE: {
            NES_INFO("ConvertPhysicalToLogicalSource: Creating Default source");
            const DefaultSourcePtr defaultSourcePtr = std::dynamic_pointer_cast<DefaultSource>(dataSource);
            const SourceDescriptorPtr defaultSourceDescriptor =
                DefaultSourceDescriptor::create(defaultSourcePtr->getSchema(),
                                                defaultSourcePtr->getNumBuffersToProcess(),
                                                defaultSourcePtr->getGatheringInterval());
            return createSourceLogicalOperatorNode(defaultSourceDescriptor);
        }
        case BINARY_SOURCE: {
            NES_INFO("ConvertPhysicalToLogicalSource: Creating Binary File source");
            const BinarySourcePtr binarySourcePtr = std::dynamic_pointer_cast<BinarySource>(dataSource);
            const SourceDescriptorPtr binarySourceDescriptor =
                BinarySourceDescriptor::create(binarySourcePtr->getSchema(), binarySourcePtr->getFilePath());
            return createSourceLogicalOperatorNode(binarySourceDescriptor);
        }
        case CSV_SOURCE: {
            NES_INFO("ConvertPhysicalToLogicalSource: Creating CSV File source");
            const CSVSourcePtr csvSourcePtr = std::dynamic_pointer_cast<CSVSource>(dataSource);
            const SourceDescriptorPtr csvSourceDescriptor =
                CsvSourceDescriptor::create(csvSourcePtr->getSchema(),
                                            csvSourcePtr->getFilePath(),
                                            csvSourcePtr->getDelimiter(),
                                            csvSourcePtr->getNumBuffersToProcess(),
                                            csvSourcePtr->getGatheringInterval());
            return createSourceLogicalOperatorNode(csvSourceDescriptor);
        }
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
            return createSourceLogicalOperatorNode(kafkaSourceDescriptor);
        }
        case SENSE_SOURCE: {
            NES_INFO("ConvertPhysicalToLogicalSource: Creating sense source");
            const SenseSourcePtr senseSourcePtr = std::dynamic_pointer_cast<SenseSource>(dataSource);
            const SourceDescriptorPtr senseSourceDescriptor =
                SenseSourceDescriptor::create(senseSourcePtr->getSchema(), senseSourcePtr->getUdsf());
            return createSourceLogicalOperatorNode(senseSourceDescriptor);
        }
        default: {
            NES_ERROR("ConvertPhysicalToLogicalSource: Unknown Data Source Type " + srcType);
            throw std::invalid_argument("Unknown Source Descriptor Type " + srcType);
        }
    }
}

} // namespace NES
