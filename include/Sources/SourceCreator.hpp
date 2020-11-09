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

#ifndef INCLUDE_SOURCESINK_SOURCECREATOR_HPP_
#define INCLUDE_SOURCESINK_SOURCECREATOR_HPP_

#include <Network/NetworkManager.hpp>
#include <Sources/DataSource.hpp>
#include <Sources/GeneratorSource.hpp>
#ifdef ENABLE_KAFKA_BUILD
#include <cppkafka/configuration.h>
#endif// KAFKASINK_HPP
#ifdef ENABLE_OPC_BUILD
#include <open62541/client_config_default.h>
#include <open62541/client_highlevel.h>
#include <open62541/client_subscriptions.h>
#include <open62541/plugin/log_stdout.h>

#endif
namespace NES {

/**
 * @brief function to create a test source which produces 10 tuples within one buffer with value one based on a schema
 * @param schema of the data source
 * @return a const data source pointer
 */
const DataSourcePtr createDefaultDataSourceWithSchemaForOneBuffer(SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager, size_t sourceId);

/**
 * @brief function to create a test source which produces 10 tuples with value one in N buffers of based on a schema
 * @param schema of the data source
 * @param number of buffers that should be produced
 * @param frequency when to gather the next buffer
 * @return a const data source pointer
 */
const DataSourcePtr createDefaultDataSourceWithSchemaForVarBuffers(SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager,
                                                                   size_t numbersOfBufferToProduce,
                                                                   size_t frequency, size_t sourceId);

/**
 * @brief function to create a test source which produces 10 tuples with value one without a schema
 * @return a const data source pointer
 */
const DataSourcePtr createDefaultSourceWithoutSchemaForOneBufferForOneBuffer(BufferManagerPtr bufferManager, QueryManagerPtr queryManager, size_t sourceId);

/**
 * @brief function to create an empty zmq source
 * @param schema of data source
 * @return a const data source pointer
 */
const DataSourcePtr createZmqSource(SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager, const std::string& host, const uint16_t port, size_t sourceId);

/**
 * @brief function to create a binary file source
 * @param schema of data source
 * @param path to the file to reading
 * @return a const data source pointer
 */
const DataSourcePtr createBinaryFileSource(SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager, const std::string& pathToFile, size_t sourceId);

/**
 * @brief function to create a sense source
 * @param schema of data source
 * @param udfs of the file
 * @return a const data source pointer
 */
const DataSourcePtr createSenseSource(SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager, const std::string& udfs, size_t sourceId);

/**
 * @brief function to create a csvfile source
 * @param schema of data source
 * @return a const data source pointer
 */
const DataSourcePtr createCSVFileSource(SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager,
                                        const std::string& path_to_file, const std::string& delimiter,
                                        size_t numberOfTuplesToProducePerBuffer, size_t numBuffersToProcess, size_t frequency, bool endlessRepeat, bool skipHeader, size_t sourceId);

/**
 * @brief function to create a network source
 * @param schema
 * @param bufferManager
 * @param queryManager
 * @param networkManager
 * @param nesPartition
 * @return a const data source pointer
 */
const DataSourcePtr createNetworkSource(SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager,
                                        Network::NetworkManagerPtr networkManager, Network::NesPartition nesPartition);

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

#ifdef ENABLE_OPC_BUILD

/**
 * @brief Create OPC source
 * @param schema schema of the elements
 * @param url the url of the OPC server
 * @param nodeId the node id of the desired node
 * @param user name if connecting with a server with authentication
 * @param password for authentication if needed
 * @return
 */
const DataSourcePtr createOPCSource(SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager, std::string url,
                                    UA_NodeId nodeId, std::string user, std::string password);
#endif

}// namespace NES
#endif /* INCLUDE_SOURCESINK_SOURCECREATOR_HPP_ */
