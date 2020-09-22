#ifndef INCLUDE_SOURCESINK_SINKCREATOR_HPP_
#define INCLUDE_SOURCESINK_SINKCREATOR_HPP_
#include <Network/NetworkManager.hpp>
#include <Network/NetworkSink.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <NodeEngine/NodeEngine.hpp>

#ifdef ENABLE_KAFKA_BUILD
#include <cppkafka/configuration.h>
#endif// KAFKASINK_HPP
#ifdef ENABLE_OPC_BUILD
#include <open62541/client_config_default.h>
#include <open62541/client_highlevel.h>
#include <open62541/plugin/log_stdout.h>
#endif

namespace NES {
/**
 * @brief create test sink
 * @Note this method is currently not implemented
 */
const DataSinkPtr createTestSink();

/**
 * @brief create a csv test sink without a schema and append to existing file
 * @param schema of sink
 * @param bufferManager
 * @param path to file
 * @param bool indicating if data is appended (true) or overwritten (false)
 * @return a data sink pointer
 */

const DataSinkPtr createCSVFileSink(SchemaPtr schema, QuerySubPlanId parentPlanId, NodeEnginePtr nodeEngine, const std::string& filePath, bool append);

/**
 * @brief create a binary test sink with a schema
 * @param schema of sink
 * @param bufferManager
 * @param path to file
 * @param bool indicating if data is appended (true) or overwritten (false)
 * @return a data sink pointer
 */
const DataSinkPtr createTextFileSink(SchemaPtr schema, QuerySubPlanId parentPlanId, NodeEnginePtr nodeEngine,
                                     const std::string& filePath, bool append);

/**
 * @brief create a binary test sink with a schema into the nes
 * @param schema of sink
 * @param bufferManager
 * @param path to file
 * @param bool indicating if data is appended (true) or overwritten (false)
 * @return a data sink pointer
 */
const DataSinkPtr createBinaryNESFileSink(SchemaPtr schema, QuerySubPlanId parentPlanId, NodeEnginePtr nodeEngine,
                                          const std::string& filePath, bool append);

/**
 * @brief create a JSON test sink with a schema int
 * @param schema of sink
 * @param bufferManager
 * @param path to file
 * @param bool indicating if data is appended (true) or overwritten (false)
 * @return a data sink pointer
 */
const DataSinkPtr createJSONFileSink(SchemaPtr schema, QuerySubPlanId parentPlanId, NodeEnginePtr nodeEngine,
                                     const std::string& filePath, bool append);

/**
 * @brief create a ZMQ test sink with a schema and Text format output
 * @param schema of sink
 * @param bufferManager
 * @param hostname as sting
 * @param port at uint16
 * @param internal refers to the usage of this zmq sink as a fwd operator such that we dont have to send the schema, only the data
 * @return a data sink pointer
 */
const DataSinkPtr createTextZmqSink(SchemaPtr schema, QuerySubPlanId parentPlanId, NodeEnginePtr nodeEngine, const std::string& host,
                                    const uint16_t port);
#ifdef ENABLE_OPC_BUILD
/**
 * @brief create a OPC test sink with a schema
 * @param schema of sink
 * @param bufferManager
 * @param url to OPC server as string
 * @param nodeId to save data in
 * @param user name as string to log in to the OPC server
 * @param password as string to log in to the OPC server
 * @return a data sink pointer
 */
const DataSinkPtr createOPCSink(SchemaPtr schema, QuerySubPlanId parentPlanId, NodeEnginePtr nodeEngine, const std::string url, UA_NodeId* nodeId, const std::string user, const std::string password);
#endif
/**
 * @brief create a ZMQ test sink with a schema and CSV format output
 * @param schema of sink
 * @param bufferManager
 * @param hostname as sting
 * @param port at uint16
 * @return a data sink pointer
 */
const DataSinkPtr createCSVZmqSink(SchemaPtr schema, QuerySubPlanId parentPlanId, NodeEnginePtr nodeEngine, const std::string& host,
                                   const uint16_t port);

/**
 * @brief create a ZMQ test sink with a schema and NES_FORMAT format output
 * @param schema of sink
 * @param bufferManager
 * @param hostname as sting
 * @param port at uint16
 * @return a data sink pointer
 */
const DataSinkPtr createBinaryZmqSink(SchemaPtr schema, QuerySubPlanId parentPlanId, NodeEnginePtr nodeEngine, const std::string& host,
                                      const uint16_t port, bool internal);

/**
 * @brief create a print test sink with a schema
 * @param schema of sink
 * @param bufferManager
 * @param output stream
 * @return a data sink pointer
 */
const DataSinkPtr createTextPrintSink(SchemaPtr schema, QuerySubPlanId parentPlanId, NodeEnginePtr nodeEngine, std::ostream& out);

/**
 * @brief create a print test sink with a schema
 * @param schema of sink
 * @param parentPlanId id of the parent qep
 * @param bufferManager
 * @param output stream
 * @return a data sink pointer
 */
const DataSinkPtr createCSVPrintSink(SchemaPtr schema, QuerySubPlanId parentPlanId, NodeEnginePtr nodeEngine, std::ostream& out);

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
const DataSinkPtr createNetworkSink(SchemaPtr schema, QuerySubPlanId parentPlanId, Network::NodeLocation nodeLocation,
                                    Network::NesPartition nesPartition, NodeEnginePtr nodeEngine,
                                    std::chrono::seconds waitTime = std::chrono::seconds(2), uint8_t retryTimes = 5);

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
