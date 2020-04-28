#include <SourceSink/BinarySource.hpp>
#include <SourceSink/CSVSource.hpp>
#include <SourceSink/GeneratorSource.hpp>
#include <SourceSink/ZmqSource.hpp>
#include <SourceSink/DataSource.hpp>
#include <NodeEngine/Dispatcher.hpp>
#include <NodeEngine/MemoryLayout/MemoryLayout.hpp>
#include <NodeEngine/MemoryLayout/PhysicalSchema.hpp>
#include <NodeEngine/MemoryLayout/PhysicalField.hpp>
#include <SourceSink/SourceCreator.hpp>
#include <SourceSink/DefaultSource.hpp>
#include <SourceSink/SenseSource.hpp>
#include <SourceSink/KafkaSource.hpp>

namespace NES {

const DataSourcePtr createDefaultDataSourceWithSchemaForOneBuffer(
    SchemaPtr schema, BufferManagerPtr bufferManager, DispatcherPtr dispatcher) {
    return std::make_shared<DefaultSource>(schema, bufferManager, dispatcher, /*bufferCnt*/ 1, /*frequency*/ 1);
}

const DataSourcePtr createDefaultDataSourceWithSchemaForVarBuffers(
    SchemaPtr schema, BufferManagerPtr bufferManager, DispatcherPtr dispatcher, size_t numbersOfBufferToProduce, size_t frequency) {
    return std::make_shared<DefaultSource>(schema, bufferManager, dispatcher, numbersOfBufferToProduce, frequency);
}

const DataSourcePtr createDefaultSourceWithoutSchemaForOneBufferForOneBuffer(BufferManagerPtr bufferManager, DispatcherPtr dispatcher) {
    return std::make_shared<DefaultSource>(
        Schema::create()->addField(createField("id", UINT64)), bufferManager, dispatcher, /**bufferCnt*/ 1, /*frequency*/ 1);
}

const DataSourcePtr createZmqSource(SchemaPtr schema, BufferManagerPtr bufferManager, DispatcherPtr dispatcher,
                                    const std::string& host,
                                    const uint16_t port) {
    return std::make_shared<ZmqSource>(schema, bufferManager, dispatcher, host, port);
}

const DataSourcePtr createBinaryFileSource(SchemaPtr schema, BufferManagerPtr bufferManager, DispatcherPtr dispatcher,
                                           const std::string& pathToFile) {
    return std::make_shared<BinarySource>(schema, bufferManager, dispatcher, pathToFile);
}

const DataSourcePtr createSenseSource(SchemaPtr schema, BufferManagerPtr bufferManager, DispatcherPtr dispatcher,
                                      const std::string& udfs) {
    return std::make_shared<SenseSource>(schema, bufferManager, dispatcher, udfs);
}

const DataSourcePtr createCSVFileSource(SchemaPtr schema, BufferManagerPtr bufferManager, DispatcherPtr dispatcher,
                                        const std::string& pathToFile,
                                        const std::string& delimiter,
                                        size_t numbersOfBufferToProduce,
                                        size_t frequency) {
    return std::make_shared<CSVSource>(schema, bufferManager, dispatcher, pathToFile, delimiter,
                                       numbersOfBufferToProduce, frequency);
}

const DataSourcePtr createKafkaSource(SchemaPtr schema, BufferManagerPtr bufferManager, DispatcherPtr dispatcher, std::string brokers, std::string topic, std::string groupId,
                                      bool autoCommit, uint64_t kafkaConsumerTimeout) {
    return std::make_shared<KafkaSource>(schema, bufferManager, dispatcher, brokers, topic, groupId, autoCommit, kafkaConsumerTimeout);
}

}

