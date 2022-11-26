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

#ifndef NES_INCLUDE_CONFIGURATIONS_WORKER_WORKERCONFIGURATION_HPP_
#define NES_INCLUDE_CONFIGURATIONS_WORKER_WORKERCONFIGURATION_HPP_

#include <Common/Location.hpp>
#include <Configurations/BaseConfiguration.hpp>
#include <Configurations/ConfigurationOption.hpp>
#include <Configurations/Worker/LocationFactory.hpp>
#include <Configurations/Worker/PhysicalSourceFactory.hpp>
#include <Configurations/Worker/QueryCompilerConfiguration.hpp>
#include <Runtime/QueryExecutionMode.hpp>
#include <Spatial/LocationProviderCSV.hpp>
#include <Util/Experimental/LocationProviderType.hpp>
#include <map>
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
    /**
     * @brief IP of the Worker.
     */
    StringOption localWorkerIp = {LOCAL_WORKER_IP_CONFIG, "127.0.0.1", "Worker IP."};

    /**
     * @brief Port for the RPC server of the Worker.
     * This is used to receive control messages from the coordinator or other workers .
     */
    UIntOption rpcPort = {RPC_PORT_CONFIG, 0, "RPC server port of the NES Worker."};

    /**
     * @brief Port of the Data server of this worker.
     * This is used to receive data.
     */
    UIntOption dataPort = {DATA_PORT_CONFIG, 0, "Data port of the NES Worker."};

    /**
     * @brief Server IP of the NES Coordinator to which the NES Worker should connect.
     */
    StringOption coordinatorIp = {COORDINATOR_IP_CONFIG,
                                  "127.0.0.1",
                                  "Server IP of the NES Coordinator to which the NES Worker should connect."};
    /**
     * @brief RPC server Port of the NES Coordinator to which the NES Worker should connect. Needs to be set and needs
     * to be the same as rpcPort in Coordinator.
     */
    UIntOption coordinatorPort = {
        COORDINATOR_PORT_CONFIG,
        4000,
        "RPC server Port of the NES Coordinator to which the NES Worker should connect. Needs to be set and needs "
        "to be the same as rpcPort in Coordinator."};

    /**
     * @brief Parent ID of this node.
     */
    UIntOption parentId = {PARENT_ID_CONFIG, 0, "Parent ID of this node."};

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
    UIntOption numberOfSlots = {NUMBER_OF_SLOTS_CONFIG, UINT16_MAX, "Number of computing slots for the NES Worker."};

    /**
     * @brief Configures the number of worker threads.
     */
    UIntOption numWorkerThreads = {"numWorkerThreads", 1, "Number of worker threads."};

    /**
     * @brief The number of buffers in the global buffer manager.
     * Controls how much memory is consumed by the system.
     */
    UIntOption numberOfBuffersInGlobalBufferManager = {NUMBER_OF_BUFFERS_IN_GLOBAL_BUFFER_MANAGER_CONFIG,
                                                       1024,
                                                       "Number buffers in global buffer pool."};
    /**
     * @brief Indicates how many buffers a single worker thread can allocate.
     */
    UIntOption numberOfBuffersPerWorker = {NUMBER_OF_BUFFERS_PER_WORKER_CONFIG, 128, "Number buffers in task local buffer pool."};

    /**
     * @brief Indicates how many buffers a single data source can allocate.
     * This property controls the backpressure mechanism as a data source that can't allocate new records can't ingest more data.
     */
    UIntOption numberOfBuffersInSourceLocalBufferPool = {NUMBER_OF_BUFFERS_IN_SOURCE_LOCAL_BUFFER_POOL_CONFIG,
                                                         64,
                                                         "Number buffers in source local buffer pool."};
    /**
     * @brief Configures the buffer size of individual TupleBuffers in bytes.
     * This property has to be the same over a whole deployment.
     */
    UIntOption bufferSizeInBytes = {BUFFERS_SIZE_IN_BYTES_CONFIG, 4096, "BufferSizeInBytes."};

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
    BoolOption numaAwareness = {NUMA_AWARENESS_CONFIG, false, "Enable Numa-Aware execution"};

    /**
     * @brief Enables the monitoring stack
     */
    BoolOption enableMonitoring = {ENABLE_MONITORING_CONFIG, false, "Enable monitoring"};

    /**
     * @brief Enables source sharing
     * */
    BoolOption enableSourceSharing = {ENABLE_SOURCE_SHARING_CONFIG, false, "Enable source sharing"};

    /**
     * @brief Enables the statistic output
     */
    BoolOption enableStatisticOutput = {ENABLE_STATISTIC_OUTPUT_CONFIG, false, "Enable statistic output"};
    /**
     * @brief Sets configuration properties for the query compiler.
     */
    QueryCompilerConfiguration queryCompiler = {QUERY_COMPILER_CONFIG, "Configuration for the query compiler"};

    /**
     * @brief Indicates a sequence of physical sources.
     */
    SequenceOption<WrapOption<PhysicalSourcePtr, PhysicalSourceFactory>> physicalSources = {PHYSICAL_SOURCES, "Physical sources"};

    /**
     * @brief location coordinate of the node if any
     */
    WrapOption<NES::Spatial::Index::Experimental::Location, Configurations::Spatial::Index::Experimental::LocationFactory>
        locationCoordinates = {LOCATION_COORDINATES_CONFIG, "the physical location of the worker"};

    /**
     * @brief specify if the worker is running on a mobile device. If this option is set, setting the fixedLocationCoordinates
     * option as well will have no effect and the worker will be considered mobile and not a field node
     */
    BoolOption isMobile = {IS_MOBILE_CONFIG, false, "define if this worker is running on a mobile device"};

    /**
     * @brief specify from which kind of interface a mobile worker can obtain its current location. This can for example be a GPS device or
     * a simulation
     */
    EnumOption<NES::Spatial::Mobility::Experimental::LocationProviderType> locationSourceType = {
        LOCATION_SOURCE_TYPE_CONFIG,
        NES::Spatial::Mobility::Experimental::LocationProviderType::CSV,
        "the kind of interface which the  mobile worker gets its geolocation info from"};

    /**
     * @brief specify the config data specific to the source of location data which was specified in the locationSourceType option
     */
    StringOption locationSourceConfig = {LOCATION_SOURCE_CONFIG, "", "the configuration data for the location interface"};

    /**
     * @brief Configuration yaml path.
     * @warning this is just a placeholder configuration
     */
    StringOption configPath = {CONFIG_PATH, "", "Path to configuration file."};

    /**
     * @brief Factory function for a worker config
     */
    static std::shared_ptr<WorkerConfiguration> create() { return std::make_shared<WorkerConfiguration>(); }

    /**
     * @brief Configuration numberOfQueues.
     * Set the number of processing queues in the system
     */
    UIntOption numberOfQueues = {NUMBER_OF_QUEUES, 1, "Number of processing queues."};

    /**
     * @brief Configuration numberOfThreadsPerQueue.
     * Set the number of threads per processing queue in the system
     */
    UIntOption numberOfThreadsPerQueue = {NUMBER_OF_THREAD_PER_QUEUE, 0, "Number of threads per processing queue."};

    /**
     * @brief Number of buffers per epoch
     * Set trimming frequency for upstream backup
     */
    UIntOption numberOfBuffersPerEpoch = {NUMBER_OF_BUFFERS_PER_EPOCH, 100, "Number of tuple buffers allowed in one epoch."};

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
    UIntOption workerHealthCheckWaitTime = {HEALTH_CHECK_WAIT_TIME, 1, "Number of seconds to wait between health checks"};

  private:
    std::vector<Configurations::BaseOption*> getOptions() override {
        return {&localWorkerIp,
                &coordinatorIp,
                &rpcPort,
                &dataPort,
                &coordinatorPort,
                &numberOfSlots,
                &numWorkerThreads,
                &numberOfBuffersInGlobalBufferManager,
                &numberOfBuffersPerWorker,
                &numberOfBuffersInSourceLocalBufferPool,
                &bufferSizeInBytes,
                &parentId,
                &logLevel,
                &sourcePinList,
                &workerPinList,
                &queuePinList,
                &numaAwareness,
                &enableMonitoring,
                &queryCompiler,
                &physicalSources,
                &locationCoordinates,
                &isMobile,
                &locationSourceType,
                &locationSourceConfig,
                &numberOfQueues,
                &numberOfThreadsPerQueue,
                &numberOfBuffersPerEpoch,
                &queryManagerMode,
                &enableSourceSharing,
                &workerHealthCheckWaitTime,
                &configPath};
    }
};
}// namespace Configurations
// namespace Configurations
}// namespace NES

#endif// NES_INCLUDE_CONFIGURATIONS_WORKER_WORKERCONFIGURATION_HPP_
