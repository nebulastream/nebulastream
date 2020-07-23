#ifndef INCLUDE_SOURCESINK_SINKCREATOR_HPP_
#define INCLUDE_SOURCESINK_SINKCREATOR_HPP_
#include <Network/NetworkManager.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include "Network/NetworkSink.hpp"

#ifdef ENABLE_KAFKA_BUILD
#include <cppkafka/configuration.h>
#endif// KAFKASINK_HPP

namespace NES {
/**
 * @brief create test sink
 * @Note this method is currently not implemented
 */
const DataSinkPtr createTestSink();

/**
 * @brief create a print test sink with a schema
 * @param schema of sink
 * @param output stream
 * @return a data sink pointer
 */
const DataSinkPtr createPrintSinkWithSchema(SchemaPtr schema, std::ostream& out, BufferManagerPtr bufferManager);

/**
 * @brief create a csv test sink without a schema and append to existing file
 * @param schema of sink
 * @param path to file
 * @param bool indicating if data is appended (true) or overwritten (false)
 * @return a data sink pointer
 */
const DataSinkPtr createCSVFileSinkWithSchema(SchemaPtr schema, const std::string& filePath, BufferManagerPtr bufferManager, bool append);

/**
 * @brief create a binary test sink with a schema
 * @param schema of sink
 * @param path to file
 * @param bool indicating if data is appended (true) or overwritten (false)
 * @return a data sink pointer
 */
const DataSinkPtr createTextFileSinkWithSchema(SchemaPtr schema,
                                               const std::string& filePath, BufferManagerPtr bufferManager, bool append);

/**
 * @brief create a binary test sink with a schema into the nes
 * @param schema of sink
 * @param path to file
 * @param bool indicating if data is appended (true) or overwritten (false)
 * @return a data sink pointer
 */
const DataSinkPtr createBinaryNESFileSinkWithSchema(SchemaPtr schema,
                                                    const std::string& filePath, BufferManagerPtr bufferManager, bool append);

/**
 * @brief create a JSON test sink with a schema int
 * @param schema of sink
 * @param path to file
 * @param bool indicating if data is appended (true) or overwritten (false)
 * @return a data sink pointer
 */
const DataSinkPtr createJSONFileSinkWithSchema(SchemaPtr schema,
                                               const std::string& filePath, BufferManagerPtr bufferManager, bool append);

/**
 * @brief create a ZMQ test sink with a schema
 * @param schema of sink
 * @param hostname as sting
 * @param port at uint16
 * @return a data sink pointer
 */
const DataSinkPtr createZmqSink(SchemaPtr schema, const std::string& host,
                                const uint16_t port, BufferManagerPtr bufferManager);

/**
 * @brief create a network data sink
 * @param schema
 * @param networkManager
 * @param nodeLocation
 * @param nesPartition
 * @param waitTime
 * @param retryTimes
 * @return a data sink pointer
 */
const DataSinkPtr createNetworkSink(SchemaPtr schema, Network::NetworkManagerPtr networkManager, Network::NodeLocation nodeLocation,
                                    Network::NesPartition nesPartition, BufferManagerPtr bufferManager, std::chrono::seconds waitTime = std::chrono::seconds(2), uint8_t retryTimes = 5);

/**
 * @brief create test sink of YSB benchmark
 */
const DataSinkPtr createYSBPrintSink();
#ifdef ENABLE_KAFKA_BUILD
/**
 * @brief create kafka sink
 * @param schema: schema of the data
 * @param brokers: broker list
 * @param topic: kafka topic to write to
 * @param kafkaProducerTimeout: kafka producer timeout
 * @return a data sink pointer
 */
const DataSinkPtr createKafkaSinkWithSchema(SchemaPtr schema, const std::string& brokers, const std::string& topic,
                                            const size_t kafkaProducerTimeout);
#endif
}// namespace NES
#endif /* INCLUDE_SOURCESINK_SINKCREATOR_HPP_ */
