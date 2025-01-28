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

#ifndef NES_CONFIGURATIONS_INCLUDE_CONFIGURATIONS_WORKER_WORKERCONFIGURATION_HPP_
#define NES_CONFIGURATIONS_INCLUDE_CONFIGURATIONS_WORKER_WORKERCONFIGURATION_HPP_

#include "Configurations/BaseConfiguration.hpp"
#include "Configurations/ConfigurationOption.hpp"
#include "Configurations/Enums/EnumOptionDetails.hpp"
#include "Configurations/Enums/QueryExecutionMode.hpp"
#include "Configurations/Validation/NonZeroValidation.hpp"
#include "Configurations/Worker/GeoLocationFactory.hpp"
#include "Configurations/Worker/PhysicalSourceTypeFactory.hpp"
#include "Configurations/Worker/QueryCompilerConfiguration.hpp"
#include "Configurations/Worker/WorkerMobilityConfiguration.hpp"
#include "Identifiers/Identifiers.hpp"
#include "Identifiers/NESStrongTypeYaml.hpp"
#include "Util/Mobility/GeoLocation.hpp"
#include "Util/Mobility/SpatialType.hpp"
#include <string>

namespace NES {

class PhysicalSource;
using PhysicalSourcePtr = std::shared_ptr<PhysicalSource>;

namespace Configurations {

class WorkerConfiguration;
using WorkerConfigurationPtr = std::shared_ptr<WorkerConfiguration>;

/**
 * @brief object for storing worker configuration
 */
class WorkerConfiguration : public BaseConfiguration {
  public:
    WorkerConfiguration() : BaseConfiguration(){};
    WorkerConfiguration(std::string name, std::string description) : BaseConfiguration(name, description){};

    /**
     * @brief Factory function for a worker config
     */
    static WorkerConfigurationPtr create() { return std::make_shared<WorkerConfiguration>(); }

    /**
     * @brief Id of the Worker.
     * This is used to uniquely identify workers within the cluster.
     */
    ScalarOption<WorkerId> workerId = {WORKER_ID,
                                       INVALID_WORKER_NODE_ID.toString(),
                                       "Worker id.",
                                       {std::make_shared<NumberValidation>()}};

    /**
     * @brief IP of the Worker.
     */
    StringOption localWorkerHost = {LOCAL_WORKER_HOST_CONFIG, "127.0.0.1", "Worker IP or hostname."};

    /**
     * @brief log path.
     */
    StringOption logPath = {LOG_PATH_CONFIG, "nesWorkerStarter.log", "Path to the log file."};

    /**
     * @brief Port for the RPC server of the Worker.
     * This is used to receive control messages from the coordinator or other workers .
     */
    UIntOption rpcPort = {RPC_PORT_CONFIG, "0", "RPC server port of the NES Worker.", {std::make_shared<NumberValidation>()}};

    /**
     * @brief Port of the Data server of this worker.
     * This is used to receive data.
     */
    UIntOption dataPort = {DATA_PORT_CONFIG, "0", "Data port of the NES Worker.", {std::make_shared<NumberValidation>()}};

    /**
     * @brief Server IP of the NES Coordinator to which the NES Worker should connect.
     */
    StringOption coordinatorHost = {COORDINATOR_HOST_CONFIG,
                                    "127.0.0.1",
                                    "Server IP or hostname of the NES Coordinator to which the NES Worker should connect."};
    /**
     * @brief RPC server Port of the NES Coordinator to which the NES Worker should connect. Needs to be set and needs
     * to be the same as rpcPort in Coordinator.
     */
    UIntOption coordinatorPort = {
        COORDINATOR_PORT_CONFIG,
        "4000",
        "RPC server Port of the NES Coordinator to which the NES Worker should connect. Needs to be set and needs "
        "to be the same as rpcPort in Coordinator.",
        {std::make_shared<NumberValidation>()}};

    /**
     * @brief Parent ID of this node.
     */
    ScalarOption<WorkerId> parentId = {PARENT_ID_CONFIG,
                                       INVALID_WORKER_NODE_ID.toString(),
                                       "Parent ID of this node.",
                                       {std::make_shared<NumberValidation>()}};

    /**
     * @brief The current log level. Controls the detail of log messages.
     */
    EnumOption<LogLevel> logLevel = {LOG_LEVEL_CONFIG,
                                     LogLevel::LOG_INFO,
                                     "The log level (LOG_NONE, LOG_WARNING, LOG_DEBUG, LOG_INFO, LOG_TRACE)"};

    /**
     * @brief Number of Slots define the amount of computing resources that are usable at the coordinator.
     * This enables the restriction of the amount of concurrently deployed queryIdAndCatalogEntryMapping and operators.
     */
    UIntOption numberOfSlots = {NUMBER_OF_SLOTS_CONFIG,
                                std::to_string(UINT16_MAX),
                                "Number of computing slots for the NES Worker.",
                                {std::make_shared<NumberValidation>()}};

    /**
     * @brief The link bandwidth of the link in Mbps.
     */
    UIntOption bandwidth = {BANDWIDTH_IN_MBPS, "0", "The link bandwidth in Mbps.", {std::make_shared<NumberValidation>()}};

    /**
     * @brief The link latency in milliseconds.
     */
    UIntOption latency = {LATENCY_IN_MS, "0", "The link latency in milliseconds.", {std::make_shared<NumberValidation>()}};

    /**
     * @brief Configures the number of worker threads.
     */
    UIntOption numWorkerThreads = {"numWorkerThreads",
                                   "1",
                                   "Number of worker threads.",
                                   {std::make_shared<NonZeroValidation>(), std::make_shared<NumberValidation>()}};

    /**
     * @brief The number of buffers in the global buffer manager.
     * Controls how much memory is consumed by the system.
     */
    UIntOption numberOfBuffersInGlobalBufferManager = {NUMBER_OF_BUFFERS_IN_GLOBAL_BUFFER_MANAGER_CONFIG,
                                                       "1024",
                                                       "Number buffers in global buffer pool.",
                                                       {std::make_shared<NumberValidation>()}};
    /**
     * @brief Indicates how many buffers a single worker thread can allocate.
     */
    UIntOption numberOfBuffersPerWorker = {NUMBER_OF_BUFFERS_PER_WORKER_CONFIG,
                                           "128",
                                           "Number buffers in task local buffer pool.",
                                           {std::make_shared<NumberValidation>()}};

    /**
     * @brief Indicates how many buffers a single data source can allocate.
     * This property controls the backpressure mechanism as a data source that can't allocate new records can't ingest more data.
     */
    UIntOption numberOfBuffersInSourceLocalBufferPool = {NUMBER_OF_BUFFERS_IN_SOURCE_LOCAL_BUFFER_POOL_CONFIG,
                                                         "64",
                                                         "Number buffers in source local buffer pool.",
                                                         {std::make_shared<NumberValidation>()}};

    /**
     * @brief Configures the wait time for collecting metrics in the monitoring streams.
     * Monitoring has to be enabled for it to work.
     */
    UIntOption monitoringWaitTime = {MONITORING_WAIT_TIME,
                                     "1000",
                                     "Sampling period of metrics (ms).",
                                     {std::make_shared<NumberValidation>()}};

    /**
     * @brief Configures the buffer size of individual TupleBuffers in bytes.
     * This property has to be the same over a whole deployment.
     */
    UIntOption bufferSizeInBytes = {BUFFERS_SIZE_IN_BYTES_CONFIG,
                                    "4096",
                                    "BufferSizeInBytes.",
                                    {std::make_shared<NumberValidation>()}};

    /**
     * @brief Indicates a list of cpu cores, which are used to pin data sources to specific cores.
     * @deprecated this value is deprecated and will be removed.
     */
    StringOption sourcePinList = {SOURCE_PIN_LIST_CONFIG, "", "comma separated list of where to map the sources"};

    /**
     * @brief Indicates a list of cpu cores, which are used  to pin worker threads to specific cores.
     * @deprecated this value is deprecated and will be removed.
     */
    StringOption workerPinList = {WORKER_PIN_LIST_CONFIG, "", "comma separated list of where to map the worker"};

    /**
     * @brief Pins specific worker threads to specific queues.
     * @deprecated this value is deprecated and will be removed.
     */
    StringOption queuePinList = {QUEUE_PIN_LIST_CONFIG, "", "comma separated list of where to map the worker on the queue"};

    /**
     * @brief Enables support for Non-Uniform Memory Access (NUMA) systems.
     */
    BoolOption numaAwareness = {NUMA_AWARENESS_CONFIG,
                                "false",
                                "Enable Numa-Aware execution",
                                {std::make_shared<BooleanValidation>()}};

    /**
     * @brief Enables the monitoring stack
     */
    BoolOption enableMonitoring = {ENABLE_MONITORING_CONFIG,
                                   "false",
                                   "Enable monitoring",
                                   {std::make_shared<BooleanValidation>()}};

    /**
     * @brief Enables source sharing
     * */
    BoolOption enableSourceSharing = {ENABLE_SOURCE_SHARING_CONFIG,
                                      "false",
                                      "Enable source sharing",
                                      {std::make_shared<BooleanValidation>()}};

    /**
     * @brief Enables the statistic output
     */
    BoolOption enableStatisticOuput = {ENABLE_STATISTIC_OUTPUT_CONFIG,
                                       "false",
                                       "Enable statistic output",
                                       {std::make_shared<BooleanValidation>()}};

    /**
     * @brief Sets configuration properties for the query compiler.
     */
    QueryCompilerConfiguration queryCompiler = {QUERY_COMPILER_CONFIG, "Configuration for the query compiler"};

    /**
     * @brief Indicates a sequence of physical sources.
     */
    SequenceOption<WrapOption<PhysicalSourceTypePtr, PhysicalSourceTypeFactory>> physicalSourceTypes = {PHYSICAL_SOURCES,
                                                                                                        "Physical sources"};

    /**
     * @brief location coordinate of the node if any
     */
    WrapOption<NES::Spatial::DataTypes::Experimental::GeoLocation,
               Configurations::Spatial::Index::Experimental::GeoLocationFactory>
        locationCoordinates = {LOCATION_COORDINATES_CONFIG, "the physical location of the worker"};

    /**
     * @brief specify if the worker is running on a mobile device, if it is a node with a known fixed loction, or if it
     * does not have a known location.
     */
    EnumOption<NES::Spatial::Experimental::SpatialType> nodeSpatialType = {
        SPATIAL_TYPE_CONFIG,
        NES::Spatial::Experimental::SpatialType::NO_LOCATION,
        "specifies if the worker has no known location or if it is a fixed location node or mobile node"};

    /**
     * @brief specifies the path to a yaml file containing a mobility configuration
     */
    Spatial::Mobility::Experimental::WorkerMobilityConfiguration mobilityConfiguration = {
        MOBILITY_CONFIG_CONFIG,
        "the configuration data for the location provider class"};

    /**
     * @brief Configuration yaml path.
     * @warning this is just a placeholder configuration
     */
    StringOption configPath = {CONFIG_PATH, "", "Path to configuration file."};

    UIntOption numberOfBuffersToProduce = {BUFFERS_TO_PRODUCE, "100000", "buff"};

#ifdef TFDEF
    BoolOption isTensorflowSupported = {TENSORFLOW_SUPPORTED_CONFIG, false, "Tensorflow model execution supported by the worker"};
#endif// TFDEF

    /**
     * @brief Configuration numberOfQueues.
     * Set the number of processing queues in the system
     */
    UIntOption numberOfQueues = {NUMBER_OF_QUEUES, "1", "Number of processing queues.", {std::make_shared<NumberValidation>()}};

    /**
     * @brief Configuration numberOfThreadsPerQueue.
     * Set the number of threads per processing queue in the system
     */
    UIntOption numberOfThreadsPerQueue = {NUMBER_OF_THREAD_PER_QUEUE,
                                          "0",
                                          "Number of threads per processing queue.",
                                          {std::make_shared<NumberValidation>()}};

    /**
     * @brief Number of buffers per epoch
     * Set trimming frequency for upstream backup
     */
    UIntOption numberOfBuffersPerEpoch = {NUMBER_OF_BUFFERS_PER_EPOCH,
                                          "100",
                                          "Number of tuple buffers allowed in one epoch.",
                                          {std::make_shared<NumberValidation>()}};

    /**
     * @brief Configuration queryManagerMode
     * The modus in which the query manager is running
     *      - Dynamic: only one queue overall
     *      - Static: use queue per query and a specified number of threads per queue
     */
    EnumOption<Runtime::QueryExecutionMode> queryManagerMode = {
        QUERY_MANAGER_MODE,
        Runtime::QueryExecutionMode::Dynamic,
        "Which mode the query manager is running in. (Dynamic, Static, NumaAware, Invalid)"};

    /**
     * @brief Configuration of waiting time of the worker health check.
     * Set the number of seconds waiting to perform health checks
     */
    UIntOption workerHealthCheckWaitTime = {HEALTH_CHECK_WAIT_TIME,
                                            "1",
                                            "Number of seconds to wait between health checks",
                                            {std::make_shared<NumberValidation>()}};

    /* Network specific settings */

    UIntOption senderHighwatermark = {SENDER_HIGH_WATERMARK,
                                      "8",
                                      "Number of tuple buffers allowed in one network channel before blocking transfer.",
                                      {std::make_shared<NumberValidation>()}};

    BoolOption isJavaUDFSupported = {TENSORFLOW_SUPPORTED_CONFIG,
                                     "false",
                                     "Java UDF execution supported by the worker",
                                     {std::make_shared<BooleanValidation>()}};

    /**
     * @brief Let network sinks use a separate thread to establish a connection
     */
    BoolOption connectSinksAsync = {CONNECT_SINKS_ASYNC,
                                    "false",
                                    "Let network sinks use a separate thread to establish a connection",
                                    {std::make_shared<BooleanValidation>()}};

    /**
     * @brief Let network sources use a separate thread to establish an event channel to their upstream sink
     */
    BoolOption connectSourceEventChannelsAsync = {
        CONNECT_SOURCE_ASYNC,
        "false",
        "Let network sources use a separate thread to establish a the upstream event channel",
        {std::make_shared<BooleanValidation>()}};

  private:
    std::vector<Configurations::BaseOption*> getOptions() override {
        return {&workerId,
                &localWorkerHost,
                &coordinatorHost,
                &rpcPort,
                &dataPort,
                &coordinatorPort,
                &numberOfSlots,
                &bandwidth,
                &latency,
                &numWorkerThreads,
                &numberOfBuffersInGlobalBufferManager,
                &numberOfBuffersPerWorker,
                &numberOfBuffersInSourceLocalBufferPool,
                &bufferSizeInBytes,
                &numberOfBuffersToProduce,
                &parentId,
                &logLevel,
                &sourcePinList,
                &workerPinList,
                &queuePinList,
                &numaAwareness,
                &enableMonitoring,
                &monitoringWaitTime,
                &queryCompiler,
                &physicalSourceTypes,
                &locationCoordinates,
                &nodeSpatialType,
                &mobilityConfiguration,
                &numberOfQueues,
                &numberOfThreadsPerQueue,
                &numberOfBuffersPerEpoch,
                &queryManagerMode,
                &enableSourceSharing,
                &workerHealthCheckWaitTime,
                &configPath,
                &connectSinksAsync,
                &connectSourceEventChannelsAsync,
#ifdef TFDEF
                &isTensorflowSupported
#endif
        };
    }
};
}// namespace Configurations
}// namespace NES

#endif// NES_CONFIGURATIONS_INCLUDE_CONFIGURATIONS_WORKER_WORKERCONFIGURATION_HPP_
