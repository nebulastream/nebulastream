#ifndef INCLUDE_SOURCESINK_SOURCECREATOR_HPP_
#define INCLUDE_SOURCESINK_SOURCECREATOR_HPP_

#include <SourceSink/DataSource.hpp>
#include <SourceSink/GeneratorSource.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#ifdef ENABLE_KAFKA_BUILD
#include <cppkafka/configuration.h>
#endif// KAFKASINK_HPP
namespace NES {

/**
 * @brief function to create a test source which produces 10 tuples within one buffer with value one based on a schema
 * @param schema of the data source
 * @return a const data source pointer
 */
const DataSourcePtr createDefaultDataSourceWithSchemaForOneBuffer(SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager);

/**
 * @brief function to create a test source which produces 10 tuples with value one in N buffers of based on a schema
 * @param schema of the data source
 * @param number of buffers that should be produced
 * @param frequency when to gather the next buffer
 * @return a const data source pointer
 */
const DataSourcePtr createDefaultDataSourceWithSchemaForVarBuffers(SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager,
                                                                   size_t numbersOfBufferToProduce,
                                                                   size_t frequency);

/**
 * @brief function to create a test source which produces 10 tuples with value one without a schema
 * @return a const data source pointer
 */
const DataSourcePtr createDefaultSourceWithoutSchemaForOneBufferForOneBuffer(BufferManagerPtr bufferManager, QueryManagerPtr queryManager);

/**
 * @brief function to create an empty zmq source
 * @param schema of data source
 * @return a const data source pointer
 */
const DataSourcePtr createZmqSource(SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager, const std::string& host, const uint16_t port);

/**
 * @brief function to create a binary file source
 * @param schema of data source
 * @param path to the file to reading
 * @return a const data source pointer
 */
const DataSourcePtr createBinaryFileSource(SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager, const std::string& pathToFile);

/**
 * @brief function to create a sense source
 * @param schema of data source
 * @param udfs of the file
 * @return a const data source pointer
 */
const DataSourcePtr createSenseSource(SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager, const std::string& udfs);

/**
 * @brief function to create a csvfile source
 * @param schema of data source
 * @return a const data source pointer
 */
const DataSourcePtr createCSVFileSource(SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager,
                                        const std::string& path_to_file,
                                        const std::string& delimiter,
                                        size_t numBuffersToProcess,
                                        size_t frequency);
#ifdef ENABLE_KAFKA_BUILD
/**
 * @brief Create kafka source
 * @param schema schema of the elements
 * @param brokers list of brokers
 * @param topic kafka topic
 * @param groupId group id
 * @param autoCommit bool indicating if offset has to be committed automatically or not
 * @param kafkaConsumerTimeout  kafka consumer timeout
 * @return
 */
const DataSourcePtr createKafkaSource(SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager,
                                      std::string brokers,
                                      std::string topic,
                                      std::string groupId,
                                      bool autoCommit,
                                      uint64_t kafkaConsumerTimeout);
#endif
}// namespace NES
#endif /* INCLUDE_SOURCESINK_SOURCECREATOR_HPP_ */
