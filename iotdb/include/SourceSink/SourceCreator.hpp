#ifndef INCLUDE_SOURCESINK_SOURCECREATOR_HPP_
#define INCLUDE_SOURCESINK_SOURCECREATOR_HPP_

#include <SourceSink/DataSource.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <SourceSink/GeneratorSource.hpp>
#include <cppkafka/configuration.h>

namespace NES {

/**
 * @brief function to create a test source which produces 10 tuples within one buffer with value one based on a schema
 * @param schema of the data source
 * @return a const data source pointer
 */
const DataSourcePtr createDefaultDataSourceWithSchemaForOneBuffer(SchemaPtr schema);

/**
 * @brief function to create a test source which produces 10 tuples with value one in N buffers of based on a schema
 * @param schema of the data source
 * @param number of buffers that should be produced
 * @param frequency when to gather the next buffer
 * @return a const data source pointer
 */
const DataSourcePtr createDefaultDataSourceWithSchemaForVarBuffers(SchemaPtr schema,
                                                                   size_t numbersOfBufferToProduce,
                                                                   size_t frequency);

/**
 * @brief function to create a test source which produces 10 tuples with value one without a schema
 * @return a const data source pointer
 */
const DataSourcePtr createDefaultSourceWithoutSchemaForOneBufferForOneBuffer();

/**
 * @brief function to create a test source which produces 10 tuples with value one in N buffers without a schema
 * @param number of buffers to produce
 * @return a const data source pointer
 */
const DataSourcePtr createDefaultSourceWithoutSchemaForOneBufferForVarBuffers(size_t numbersOfBufferToProduce,
                                                                              double frequency);

/**
 * @brief function to create an empty zmq source
 * @param schema of data source
 * @return a const data source pointer
 */
const DataSourcePtr createZmqSource(SchemaPtr schema, const std::string& host, const uint16_t port);

/**
 * @brief function to create a binary file source
 * @param schema of data source
 * @param path to the file to readin
 * @return a const data source pointer
 */
const DataSourcePtr createBinaryFileSource(SchemaPtr schema, const std::string& path_to_file);

/**
 * @brief function to create a sense source
 * @param schema of data source
 * @param udfs of the file
 * @return a const data source pointer
 */
const DataSourcePtr createSenseSource(SchemaPtr schema, const std::string& udfs);

/**
 * @brief function to create a csvfile source
 * @param schema of data source
 * @return a const data source pointer
 */
const DataSourcePtr createCSVFileSource(SchemaPtr schema,
                                        const std::string& path_to_file,
                                        const std::string& delimiter,
                                        size_t numBuffersToProcess,
                                        size_t frequency);

const DataSourcePtr createKafkaSource(SchemaPtr schema, std::string brokers, std::string topic, bool autoCommit,
                                      cppkafka::Configuration config, uint64_t kafkaConsumerTimeout);

}
#endif /* INCLUDE_SOURCESINK_SOURCECREATOR_HPP_ */
