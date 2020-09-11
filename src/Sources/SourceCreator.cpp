#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Network/NetworkSource.hpp>
#include <NodeEngine/MemoryLayout/MemoryLayout.hpp>
#include <NodeEngine/MemoryLayout/PhysicalField.hpp>
#include <NodeEngine/MemoryLayout/PhysicalSchema.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <Sources/BinarySource.hpp>
#include <Sources/CSVSource.hpp>
#include <Sources/DataSource.hpp>
#include <Sources/DefaultSource.hpp>
#include <Sources/GeneratorSource.hpp>
#include <Sources/KafkaSource.hpp>
#include <Sources/OPCSource.hpp>
#include <Sources/SenseSource.hpp>
#include <Sources/SourceCreator.hpp>
#include <Sources/ZmqSource.hpp>

#ifdef ENABLE_OPC_BUILD
#include <open62541/client_config_default.h>
#include <open62541/client_highlevel.h>
#include <open62541/plugin/log_stdout.h>

#endif

namespace NES {

const DataSourcePtr createDefaultDataSourceWithSchemaForOneBuffer(
    SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager) {
    return std::make_shared<DefaultSource>(schema, bufferManager, queryManager, /*bufferCnt*/ 1, /*frequency*/ 1);
}

const DataSourcePtr createDefaultDataSourceWithSchemaForVarBuffers(
    SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager, size_t numbersOfBufferToProduce, size_t frequency) {
    return std::make_shared<DefaultSource>(schema, bufferManager, queryManager, numbersOfBufferToProduce, frequency);
}

const DataSourcePtr createDefaultSourceWithoutSchemaForOneBufferForOneBuffer(BufferManagerPtr bufferManager, QueryManagerPtr queryManager) {
    return std::make_shared<DefaultSource>(
        Schema::create()->addField("id", DataTypeFactory::createUInt64()), bufferManager, queryManager, /**bufferCnt*/ 1, /*frequency*/ 1);
}

const DataSourcePtr createZmqSource(SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager,
                                    const std::string& host,
                                    const uint16_t port) {
    return std::make_shared<ZmqSource>(schema, bufferManager, queryManager, host, port);
}

const DataSourcePtr createBinaryFileSource(SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager,
                                           const std::string& pathToFile) {
    return std::make_shared<BinarySource>(schema, bufferManager, queryManager, pathToFile);
}

const DataSourcePtr createSenseSource(SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager,
                                      const std::string& udfs) {
    return std::make_shared<SenseSource>(schema, bufferManager, queryManager, udfs);
}

const DataSourcePtr createCSVFileSource(SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager,
                                        const std::string& pathToFile,
                                        const std::string& delimiter,
                                        size_t numberOfTuplesToProducePerBuffer,
                                        size_t numbersOfBufferToProduce,
                                        size_t frequency) {
    return std::make_shared<CSVSource>(schema, bufferManager, queryManager, pathToFile, delimiter,
                                       numberOfTuplesToProducePerBuffer, numbersOfBufferToProduce, frequency);
}

const DataSourcePtr createNetworkSource(SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager,
                                        Network::NetworkManagerPtr networkManager, Network::NesPartition nesPartition) {
    return std::make_shared<Network::NetworkSource>(schema, bufferManager, queryManager, networkManager, nesPartition);
}
#ifdef ENABLE_KAFKA_BUILD
const DataSourcePtr createKafkaSource(SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager, std::string brokers, std::string topic, std::string groupId,
                                      bool autoCommit, uint64_t kafkaConsumerTimeout) {
    return std::make_shared<KafkaSource>(schema, bufferManager, queryManager, brokers, topic, groupId, autoCommit, kafkaConsumerTimeout);
}
#endif

#ifdef ENABLE_OPC_BUILD

const DataSourcePtr createOPCSource(SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager, const std::string& url,
                                    UA_NodeId* nodeId, const std::string& user, const std::string& password) {
    return std::make_shared<OPCSource>(schema, bufferManager, queryManager, url, nodeId, user, password);
}
#endif
}// namespace NES
