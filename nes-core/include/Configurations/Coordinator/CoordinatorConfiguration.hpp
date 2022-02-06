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

#ifndef NES_INCLUDE_CONFIGURATIONS_COORDINATOR_COORDINATORCONFIGURATION_HPP_
#define NES_INCLUDE_CONFIGURATIONS_COORDINATOR_COORDINATORCONFIGURATION_HPP_

#include <Configurations/ConfigOptions/BaseConfiguration.hpp>
#include <Configurations/ConfigurationOption.hpp>
#include <iostream>
#include <map>
#include <string>
#include <thread>

namespace NES {

class LogicalSource;
using LogicalSourcePtr = std::shared_ptr<LogicalSource>;

namespace Configurations {

enum LogLevel { LOG_NONE, LOG_WARNING, LOG_DEBUG, LOG_INFO, LOG_TRACE };

class CoordinatorConfiguration;
using CoordinatorConfigurationPtr = std::shared_ptr<CoordinatorConfiguration>;

/**
 * @brief ConfigOptions for Coordinator
 */
class CoordinatorConfiguration : public BaseConfiguration {
  public:
    StringOption restIp = {REST_IP_CONFIG, "127.0.0.1", "NES ip of the REST server."};
    StringOption coordinatorIp = {COORDINATOR_IP_CONFIG, "127.0.0.1", "RPC IP address of NES Coordinator."};
    UIntOption rpcPort = {RPC_PORT_CONFIG, 4000, "RPC server port of the NES Coordinator"};
    UIntOption restPort = {REST_PORT_CONFIG, 8081, "Port exposed for rest endpoints"};
    UIntOption dataPort = {DATA_PORT_CONFIG, 3001, "NES data server port"};
    UIntOption numberOfSlots = {NUMBER_OF_SLOTS_CONFIG, UINT16_MAX, "Number of computing slots for NES Coordinator"};
    EnumOption<LogLevel> logLevel = {LOG_LEVEL_CONFIG,
                                     LOG_DEBUG,
                                     "The log level (LOG_NONE, LOG_WARNING, LOG_DEBUG, LOG_INFO, LOG_TRACE)"};
    UIntOption numberOfBuffersInGlobalBufferManager = {NUMBER_OF_BUFFERS_IN_GLOBAL_BUFFER_MANAGER_CONFIG,
                                                       1024,
                                                       "Number buffers in global buffer pool."};
    UIntOption numberOfBuffersPerWorker = {NUMBER_OF_BUFFERS_PER_WORKER_CONFIG, 128, "Number buffers in task local buffer pool."};
    UIntOption numberOfBuffersInSourceLocalBufferPool = {NUMBER_OF_BUFFERS_IN_SOURCE_LOCAL_BUFFER_POOL_CONFIG,
                                                         64,
                                                         "Number buffers in source local buffer pool."};
    UIntOption bufferSizeInBytes = {BUFFERS_SIZE_IN_BYTES_CONFIG, 4096, "BufferSizeInBytes."};
    UIntOption numWorkerThreads = {NUM_WORKER_THREADS_CONFIG, 1, "Number of worker threads."};
    UIntOption queryBatchSize = {QUERY_BATCH_SIZE_CONFIG, 1, "The number of queries to be processed together"};
    StringOption queryMergerRule = {QUERY_MERGER_RULE_CONFIG,
                                    "DefaultQueryMergerRule",
                                    "The rule to be used for performing query merging"};
    BoolOption enableSemanticQueryValidation = {ENABLE_SEMANTIC_QUERY_VALIDATION_CONFIG,
                                                false,
                                                "Enable semantic query validation feature"};
    BoolOption enableMonitoring = {ENABLE_MONITORING_CONFIG, false, "Enable monitoring"};

    StringOption memoryLayoutPolicy = {
        MEMORY_LAYOUT_POLICY_CONFIG,
        "FORCE_ROW_LAYOUT",
        "selects the memory layout selection policy can be [FORCE_ROW_LAYOUT|FORCE_COLUMN_LAYOUT]"};

    BoolOption performOnlySourceOperatorExpansion = {
        PERFORM_ONLY_SOURCE_OPERATOR_EXPANSION,
        false,
        "Perform only source operator duplication when applying Logical Source Expansion Rewrite Rule. (Default: false)"};
    BoolOption enableQueryReconfiguration = {
        PERFORM_ONLY_SOURCE_OPERATOR_EXPANSION,
        false,
        "Perform only source operator duplication when applying Logical Source Expansion Rewrite Rule. (Default: false)"};

    std::vector<LogicalSourcePtr> logicalSources;

  private:
    std::vector<Configurations::BaseOption*> getOptions() override {
        return {
            &restIp,
            &coordinatorIp,
            &rpcPort,
            &restPort,
            &dataPort,
            &numberOfSlots,
            &logLevel,
            &numberOfBuffersInGlobalBufferManager,
            &numberOfBuffersPerWorker,
            &numberOfBuffersInSourceLocalBufferPool,
            &bufferSizeInBytes,
            &numWorkerThreads,
            &queryBatchSize,
            &queryMergerRule,
            &enableSemanticQueryValidation,
            &enableMonitoring,
            &memoryLayoutPolicy,
            &performOnlySourceOperatorExpansion,
        };
    }
};

}// namespace Configurations
}// namespace NES

#endif// NES_INCLUDE_CONFIGURATIONS_COORDINATOR_COORDINATORCONFIGURATION_HPP_
