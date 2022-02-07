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

#include <Configurations/ConfigOptions/BaseConfiguration.hpp>
#include <Configurations/ConfigurationOption.hpp>
#include <Configurations/Worker/PhysicalSourceFactory.hpp>
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
    StringOption localWorkerIp = {LOCAL_WORKER_IP_CONFIG, "127.0.0.1", "Worker IP."};
    StringOption coordinatorIp = {COORDINATOR_IP_CONFIG,
                                  "127.0.0.1",
                                  "Server IP of the NES Coordinator to which the NES Worker should connect."};
    UIntOption coordinatorPort = {
        COORDINATOR_PORT_CONFIG,
        4000,
        "RPC server Port of the NES Coordinator to which the NES Worker should connect. Needs to be set and needs "
        "to be the same as rpcPort in Coordinator."};
    UIntOption rpcPort = {RPC_PORT_CONFIG, 4000, "RPC server port of the NES Worker."};
    UIntOption dataPort = {DATA_PORT_CONFIG, 4001, "Data port of the NES Worker."};
    UIntOption numberOfSlots = {NUMBER_OF_SLOTS_CONFIG, UINT16_MAX, "Number of computing slots for the NES Worker."};
    UIntOption numWorkerThreads = {"numWorkerThreads", 1, "Number of worker threads."};

    UIntOption numberOfBuffersInGlobalBufferManager = {NUMBER_OF_BUFFERS_IN_GLOBAL_BUFFER_MANAGER_CONFIG,
                                                       1024,
                                                       "Number buffers in global buffer pool."};
    UIntOption numberOfBuffersPerWorker = {NUMBER_OF_BUFFERS_PER_WORKER_CONFIG, 128, "Number buffers in task local buffer pool."};
    UIntOption numberOfBuffersInSourceLocalBufferPool = {NUMBER_OF_BUFFERS_IN_SOURCE_LOCAL_BUFFER_POOL_CONFIG,
                                                         64,
                                                         "Number buffers in source local buffer pool."};
    UIntOption bufferSizeInBytes = {BUFFERS_SIZE_IN_BYTES_CONFIG, 4096, "BufferSizeInBytes."};
    UIntOption parentId = {PARENT_ID_CONFIG, 0, "Parent ID of this node."};
    StringOption logLevel = {LOG_LEVEL_CONFIG, "LOG_DEBUG", "Log level (LOG_NONE, LOG_WARNING, LOG_DEBUG, LOG_INFO, LOG_TRACE) "};

    StringOption queryCompilerCompilationStrategy = {
        QUERY_COMPILER_COMPILATION_STRATEGY_CONFIG,
        "OPTIMIZE",
        "Indicates the optimization strategy for the query compiler [FAST|DEBUG|OPTIMIZE]."};

    StringOption queryCompilerPipeliningStrategy = {
        QUERY_COMPILER_PIPELINING_STRATEGY_CONFIG,
        "OPERATOR_FUSION",
        "Indicates the pipelining strategy for the query compiler [OPERATOR_FUSION|OPERATOR_AT_A_TIME]."};

    StringOption queryCompilerOutputBufferOptimizationLevel = {
        QUERY_COMPILER_OUTPUT_BUFFER_OPTIMIZATION_CONFIG,
        "ALL",
        "Indicates the OutputBufferAllocationStrategy "
        "[ALL|NO|ONLY_INPLACE_OPERATIONS_NO_FALLBACK,"
        "|REUSE_INPUT_BUFFER_AND_OMIT_OVERFLOW_CHECK_NO_FALLBACK,|"
        "REUSE_INPUT_BUFFER_NO_FALLBACK|OMIT_OVERFLOW_CHECK_NO_FALLBACK]. "};
    StringOption sourcePinList = {SOURCE_PIN_LIST_CONFIG, "", "comma separated list of where to map the sources"};
    StringOption workerPinList = {WORKER_PIN_LIST_CONFIG, "", "comma separated list of where to map the worker"};
    StringOption queuePinList = {QUEUE_PIN_LIST_CONFIG, "", "comma separated list of where to map the worker on the queue"};
    BoolOption numaAwareness = {NUMA_AWARENESS_CONFIG, false, "Enable Numa-Aware execution"};
    BoolOption enableMonitoring = {ENABLE_MONITORING_CONFIG, false, "Enable monitoring"};
    SequenceOption<WrapOption<PhysicalSourcePtr, PhysicalSourceFactory>> physicalSources = {"physicalSources",
                                                                                            "Physical sources"};

    static std::shared_ptr<WorkerConfiguration> create() { return std::make_shared<WorkerConfiguration>(); }

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
                &queryCompilerCompilationStrategy,
                &queryCompilerPipeliningStrategy,
                &queryCompilerOutputBufferOptimizationLevel,
                &sourcePinList,
                &workerPinList,
                &queuePinList,
                &numaAwareness,
                &enableMonitoring};
    }
};
}// namespace Configurations
// namespace Configurations
}// namespace NES

#endif// NES_INCLUDE_CONFIGURATIONS_WORKER_WORKERCONFIGURATION_HPP_
