/*
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

#ifndef NES_RUNTIME_INCLUDE_SINKS_SINKCREATOR_HPP_
#define NES_RUNTIME_INCLUDE_SINKS_SINKCREATOR_HPP_
#include <Monitoring/MonitoringForwardRefs.hpp>
#include <Operators/LogicalOperators/Sinks/StatisticSinkDescriptor.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#ifdef ENABLE_OPC_BUILD
#include <open62541/client_config_default.h>
#include <open62541/client_highlevel.h>
#include <open62541/plugin/log_stdout.h>
#endif
#ifdef ENABLE_MQTT_BUILD
#include <Sinks/Mediums/MQTTSink.hpp>
#endif

namespace NES {
/**
 * @brief create a csv test sink without a schema and append to existing file
 * @param schema of sink
 * @param sharedQueryId shared query id
 * @param decomposedQueryId decomposed query id
 * @param nodeEngine node engine
 * @param activeProducers producers
 * @param filePath path to file
 * @param append bool indicating if data is appended (true) or overwritten (false)
 * @param faultToleranceType fault tolerance type
 * @param addTimestamp bool indicating if timestamp is appended
 * @param numberOfOrigins: number of origins of a given query
 * @return a data sink pointer
 */

DataSinkPtr createCSVFileSink(const SchemaPtr& schema,
                              SharedQueryId sharedQueryId,
                              DecomposedQueryId decomposedQueryId,
                              DecomposedQueryPlanVersion decomposedQueryVersion,
                              const Runtime::NodeEnginePtr& nodeEngine,
                              uint32_t activeProducers,
                              const std::string& filePath,
                              bool append,
                              FaultToleranceType faultToleranceType,
                              bool addTimestamp = false,
                              uint64_t numberOfOrigins = 1);

/**
 * @brief create a migrate test sink
 * @param sharedQueryId shared query id
 * @param decomposedQueryId decomposed query id
 * @param nodeEngine node engine
 * @param numOfProducers number of producers
 * @param filePath path to file
 * @param append bool indicating if data is appended (true) or overwritten (false)
 * @param faultToleranceType fault tolerance type
 * @param numberOfOrigins: number of origins of a given query
 * @return a data sink pointer
 */
DataSinkPtr createMigrateFileSink(SharedQueryId sharedQueryId,
                                  DecomposedQueryId decomposedQueryId,
                                  DecomposedQueryPlanVersion decomposedQueryVersion,
                                  Runtime::NodeEnginePtr nodeEngine,
                                  uint32_t numOfProducers,
                                  const std::string& filePath,
                                  bool append,
                                  FaultToleranceType faultToleranceType,
                                  uint64_t numberOfOrigins = 1);

/**
 * @brief create a binary test sink with a schema into the nes
 * @param schema of sink
 * @param sharedQueryId shared query id
 * @param decomposedQueryId decomposed query id
 * @param nodeEngine node engine
 * @param numOfProducers number of producers
 * @param filePath path to file
 * @param append bool indicating if data is appended (true) or overwritten (false)
 * @param faultToleranceType fault tolerance type
 * @param numberOfOrigins: number of origins of a given query
 * @return a data sink pointer
 */
DataSinkPtr createBinaryNESFileSink(const SchemaPtr& schema,
                                    SharedQueryId sharedQueryId,
                                    DecomposedQueryId decomposedQueryId,
                                    DecomposedQueryPlanVersion decomposedQueryVersion,
                                    const Runtime::NodeEnginePtr& nodeEngine,
                                    uint32_t numOfProducers,
                                    const std::string& filePath,
                                    bool append,
                                    FaultToleranceType faultToleranceType,
                                    uint64_t numberOfOrigins = 1);

/**
 * @brief create a JSON test sink with a schema int
* @param schema of sink
 * @param sharedQueryId shared query id
 * @param decomposedQueryId decomposed query id
 * @param nodeEngine node engine
 * @param numOfProducers number of producers
 * @param filePath path to file
 * @param append bool indicating if data is appended (true) or overwritten (false)
 * @param faultToleranceType fault tolerance type
 * @param numberOfOrigins: number of origins of a given query
 * @return a data sink pointer
 */
DataSinkPtr createJSONFileSink(const SchemaPtr& schema,
                               SharedQueryId sharedQueryId,
                               DecomposedQueryId decomposedQueryId,
                               DecomposedQueryPlanVersion decomposedQueryVersion,
                               const Runtime::NodeEnginePtr& nodeEngine,
                               uint32_t numOfProducers,
                               const std::string& filePath,
                               bool append,
                               FaultToleranceType faultToleranceType,
                               uint64_t numberOfOrigins = 1);

/**
 * @brief create a ZMQ test sink with a schema and Csv format output
* @param schema of sink
 * @param sharedQueryId shared query id
 * @param decomposedQueryId decomposed query id
 * @param nodeEngine node engine
 * @param numOfProducers number of producers
 * @param host host
 * @param port port
 * @param faultToleranceType fault tolerance type
 * @param numberOfOrigins: number of origins of a given query
 * @return a data sink pointer
 */
DataSinkPtr createCsvZmqSink(const SchemaPtr& schema,
                             SharedQueryId sharedQueryId,
                             DecomposedQueryId decomposedQueryId,
                             DecomposedQueryPlanVersion decomposedQueryVersion,
                             const Runtime::NodeEnginePtr& nodeEngine,
                             uint32_t numOfProducers,
                             const std::string& host,
                             uint16_t port,
                             FaultToleranceType faultToleranceType,
                             uint64_t numberOfOrigins = 1);
#ifdef ENABLE_OPC_BUILD
/**
 * @brief create a OPC test sink with a schema
 * @param schema of sink
 * @param queryId
 * @param bufferManager
 * @param url to OPC server as string
 * @param nodeId to save data in
 * @param user name as string to log in to the OPC server
 * @param password as string to log in to the OPC server
 * @return a data sink pointer
 */
DataSinkPtr createOPCSink(SchemaPtr schema,
                          SharedQueryId queryId,
                          QuerySubPlanId querySubPlanId,
                          QuerySubPlanVersion querySubPlanVersion,
                          Runtime::NodeEnginePtr nodeEngine,
                          std::string url,
                          UA_NodeId nodeId,
                          std::string user,
                          std::string password);
#endif
/**
 * @brief create a ZMQ test sink with a schema and CSV format output
* @param schema of sink
 * @param sharedQueryId shared query id
 * @param decomposedQueryId decomposed query id
 * @param nodeEngine node engine
 * @param numOfProducers number of producers
 * @param host host
 * @param port port
 * @param faultToleranceType fault tolerance type
 * @param numberOfOrigins: number of origins of a given query
 * @return a data sink pointer
 */
DataSinkPtr createCSVZmqSink(const SchemaPtr& schema,
                             SharedQueryId sharedQueryId,
                             DecomposedQueryId decomposedQueryId,
                             DecomposedQueryPlanVersion decomposedQueryVersion,
                             const Runtime::NodeEnginePtr& nodeEngine,
                             uint32_t numOfProducers,
                             const std::string& host,
                             uint16_t port,
                             FaultToleranceType faultToleranceType,
                             uint64_t numberOfOrigins = 1);

/**
 * @brief create a ZMQ test sink with a schema and NES_FORMAT format output
* @param schema of sink
 * @param sharedQueryId shared query id
 * @param decomposedQueryId decomposed query id
 * @param nodeEngine node engine
 * @param activeProducers number of producers
 * @param host host
 * @param port port
 * @param internal
 * @param faultToleranceType fault tolerance type
 * @param numberOfOrigins: number of origins of a given query
 * @return a data sink pointer
 */
DataSinkPtr createBinaryZmqSink(const SchemaPtr& schema,
                                SharedQueryId sharedQueryId,
                                DecomposedQueryId decomposedQueryId,
                                DecomposedQueryPlanVersion decomposedQueryVersion,
                                const Runtime::NodeEnginePtr& nodeEngine,
                                uint32_t activeProducers,
                                const std::string& host,
                                uint16_t port,
                                bool internal,
                                FaultToleranceType faultToleranceType,
                                uint64_t numberOfOrigins = 1);

/**
 * @brief create a print test sink with a schema
* @param schema of sink
 * @param sharedQueryId shared query id
 * @param decomposedQueryId decomposed query id
 * @param nodeEngine node engine
 * @param activeProducers number of producers
 * @param out out stream
 * @param faultToleranceType fault tolerance type
 * @param numberOfOrigins: number of origins of a given query
 * @return a data sink pointer
 */
DataSinkPtr createCsvPrintSink(const SchemaPtr& schema,
                               SharedQueryId sharedQueryId,
                               DecomposedQueryId decomposedQueryId,
                               DecomposedQueryPlanVersion decomposedQueryVersion,
                               const Runtime::NodeEnginePtr& nodeEngine,
                               uint32_t activeProducers,
                               std::ostream& out,
                               FaultToleranceType faultToleranceType,
                               uint64_t numberOfOrigins = 1);

/**
 * @brief create a print that does not output something
 * @param sharedQueryId shared query id
 * @param decomposedQueryId decomposed query id
 * @param nodeEngine node engine
 * @param activeProducers number of producers
 * @param faultToleranceType fault tolerance type
 * @param numberOfOrigins: number of origins of a given query
 * @return a data sink pointer
 */
DataSinkPtr createNullOutputSink(SharedQueryId sharedQueryId,
                                 DecomposedQueryId decomposedQueryId,
                                 DecomposedQueryPlanVersion decomposedQueryVersion,
                                 const Runtime::NodeEnginePtr& nodeEngine,
                                 uint32_t activeProducers,
                                 FaultToleranceType faultToleranceType,
                                 uint64_t numberOfOrigins = 1);

/**
 * @brief create a print test sink with a schema
* @param schema of sink
 * @param sharedQueryId shared query id
 * @param decomposedQueryId decomposed query id
 * @param nodeEngine node engine
 * @param activeProducers number of producers
 * @param out out stream
 * @param faultToleranceType fault tolerance type
 * @param numberOfOrigins: number of origins of a given query
 * @return a data sink pointer
 */
DataSinkPtr createCSVPrintSink(const SchemaPtr& schema,
                               SharedQueryId sharedQueryId,
                               DecomposedQueryId decomposedQueryId,
                               DecomposedQueryPlanVersion decomposedQueryVersion,
                               const Runtime::NodeEnginePtr& nodeEngine,
                               uint32_t activeProducers,
                               std::ostream& out,
                               FaultToleranceType faultToleranceType,
                               uint64_t numberOfOrigins = 1);

/**
 * @brief create a network data sink
* @param schema of sink
 * @param uniqueNetworkSinkDescriptorId
 * @param sharedQueryId shared query id
 * @param decomposedQueryId decomposed query id
 * @param nodeLocation
 * @param nesPartition
 * @param nodeEngine node engine
 * @param numOfProducers
 * @param waitTime
 * @param version
 * @param faultToleranceType fault tolerance type
 * @param numberOfOrigins: number of origins of a given query
 * @param retryTimes
 * @return a data sink pointer
 */
DataSinkPtr createNetworkSink(const SchemaPtr& schema,
                              OperatorId uniqueNetworkSinkDescriptorId,
                              SharedQueryId sharedQueryId,
                              DecomposedQueryId decomposedQueryId,
                              DecomposedQueryPlanVersion decomposedQueryVersion,
                              const Network::NodeLocation& nodeLocation,
                              Network::NesPartition nesPartition,
                              const Runtime::NodeEnginePtr& nodeEngine,
                              size_t numOfProducers,
                              std::chrono::milliseconds waitTime,
                              DecomposedQueryPlanVersion version,
                              FaultToleranceType faultToleranceType,
                              uint64_t numberOfOrigins = 1,
                              uint8_t retryTimes = 5);

/**
 * @brief create a monitoring data sink
 * @param metricStore
 * @param nodeEngine
 * @param numOfProducers
 * @param sharedQueryId
 * @param decomposedQueryId
 * @param numberOfOrigins
 * @return a data sink pointer
 */
DataSinkPtr createMonitoringSink(Monitoring::MetricStorePtr metricStore,
                                 Monitoring::MetricCollectorType collectorType,
                                 const SchemaPtr& schema,
                                 Runtime::NodeEnginePtr nodeEngine,
                                 uint32_t numOfProducers,
                                 SharedQueryId sharedQueryId,
                                 DecomposedQueryId decomposedQueryId,
                                 DecomposedQueryPlanVersion decomposedQueryVersion,
                                 FaultToleranceType faultToleranceType,
                                 uint64_t numberOfOrigins = 1);

/**
 * @brief Creates a StatisticSink
 * @param schema
 * @param nodeEngine
 * @param numOfProducers
 * @param sharedQueryId
 * @param decomposedQueryId
 * @param numberOfOrigins
 * @param sinkFormatType
 * @param sinkDataCodec
 * @return DataSinkPtr
 */
DataSinkPtr createStatisticSink(const SchemaPtr& schema,
                                const Runtime::NodeEnginePtr& nodeEngine,
                                size_t numOfProducers,
                                SharedQueryId sharedQueryId,
                                DecomposedQueryId decomposedQueryId,
                                DecomposedQueryPlanVersion decomposedQueryIdVersion,
                                FaultToleranceType faultToleranceType,
                                uint64_t numberOfOrigins,
                                Statistic::StatisticSynopsisType sinkFormatType,
                                Statistic::StatisticDataCodec sinkDataCodec);

#ifdef ENABLE_KAFKA_BUILD
/**
 * @brief create kafka sink
 * @param schema: schema of the data
 * @param sharedQueryId of the query that writes to the sink
 * @param decomposedQueryId of the query that writes to the sink
 * @param nodeEngine
 * @param activeProducers how many different queries write to this sink which is needed for sink shutdown
 * @param brokers: broker list
 * @param topic: kafka topic to write to
 * @param kafkaProducerTimeout: kafka producer timeout
 * @param numberOfOrigins
 * @return a data sink pointer
 */
DataSinkPtr createCsvKafkaSink(SchemaPtr schema,
                               SharedQueryId sharedQueryId,
                               DecomposedQueryId decomposedQueryId,
                               DecomposedQueryPlanVersion decomposedQueryVersion,
                               const Runtime::NodeEnginePtr& nodeEngine,
                               uint32_t activeProducers,
                               const std::string& brokers,
                               const std::string& topic,
                               uint64_t kafkaProducerTimeout,
                               FaultToleranceType faultToleranceType,
                               uint64_t numberOfOrigins);
#endif
#ifdef ENABLE_MQTT_BUILD
/**
 * @brief create MQTT sink
 * @param schema: schema of the data
 * @param sharedQueryId
 * @param decomposedQueryId
 * @param nodeEngine: a node engine pointer, e.g. for access to the buffer manager
 * @param numOfProducers
 * @param address: address of a MQTT broker
 * @param clientID
 * @param topic: broker topic/path to which the MQTT messages are published
 * @param user: used to identify client at broker
 * @param maxBufferedMSGs: maximal number for how many messages can be buffered at client
 * @param timeUnit: unit used to interpret the msgDelay value (seconds vs milliseconds vs nanoseconds)
 * @param msgDelay: how long to wait before sending the next message from client
 * @param qualityOfService: either 'at most once' or 'at least once'. QOS > 0 required for a non-clean (persistent) session.
 * @param asynchronousClient: 1 if client should be asynchronous, else 0
 * @param faultToleranceType
 * @param numberOfOrigins: number of origins of a given query
 * @return a data sink pointer
 */
DataSinkPtr createMQTTSink(const SchemaPtr& schema,
                           SharedQueryId sharedQueryId,
                           DecomposedQueryId decomposedQueryId,
                           DecomposedQueryPlanVersion decomposedQueryVersion,
                           const Runtime::NodeEnginePtr& nodeEngine,
                           uint32_t numOfProducers,
                           std::string const& address,
                           std::string const& clientID,
                           std::string const& topic,
                           std::string const& user,
                           uint64_t maxBufferedMSGs,
                           MQTTSinkDescriptor::TimeUnits timeUnit,
                           uint64_t msgDelay,
                           MQTTSinkDescriptor::ServiceQualities qualityOfService,
                           bool asynchronousClient,
                           FaultToleranceType faultToleranceType,
                           uint64_t numberOfOrigins = 1);
#endif

}// namespace NES
#endif// NES_RUNTIME_INCLUDE_SINKS_SINKCREATOR_HPP_
