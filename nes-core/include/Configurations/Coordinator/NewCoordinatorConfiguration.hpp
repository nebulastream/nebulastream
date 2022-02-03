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

#ifndef NES_INCLUDE_CONFIGURATIONS_COORDINATOR_NEWCOORDINATORCONFIGURATION_HPP_
#define NES_INCLUDE_CONFIGURATIONS_COORDINATOR_NEWCOORDINATORCONFIGURATION_HPP_

#include <Configurations/ConfigOptions/BaseConfiguration.hpp>
#include <Configurations/Coordinator/OptimizerConfiguration.hpp>
#include <iostream>
#include <map>
#include <string>
#include <thread>

namespace NES {

namespace Configurations {

enum LogLevel { LOG_NONE, LOG_WARNING, LOG_DEBUG, LOG_INFO, LOG_TRACE };

class LogicalSourceFactory {
  public:
    static LogicalSourcePtr createFromString(std::string string) {
        NES_DEBUG("Hallo" << string);
        return LogicalSourcePtr();
    }
    static LogicalSourcePtr createFromYaml(Yaml::Node ) {
        NES_DEBUG("Hallo");
        return LogicalSourcePtr();
    }
};

/**
 * @brief ConfigOptions for Coordinator
 */
class NEWCoordinatorConfiguration : public BaseConfiguration {
  public:
    StringOption restIp = {"restIp", "127.0.0.1", "NES ip of the REST server."};
    StringOption coordinatorIp = {"coordinatorIp", "127.0.0.1", "RPC IP address of NES Coordinator."};
    IntOption rpcPort = {"rpcPort", 4000, "RPC server port of the NES Coordinator"};
    IntOption restPort = {"restPort", 8081, "Port exposed for rest endpoints"};
    IntOption dataPort = {"dataPort", 3001, "NES data server port"};
    IntOption numberOfSlots = {"numberOfSlots", UINT16_MAX, "Number of computing slots for NES Coordinator"};
    EnumOption<LogLevel> logLevel = {"logLevel",
                                     LOG_DEBUG,
                                     "The log level (LOG_NONE, LOG_WARNING, LOG_DEBUG, LOG_INFO, LOG_TRACE)"};
    IntOption numberOfBuffersInGlobalBufferManager = {"numberOfBuffersInGlobalBufferManager",
                                                      1024,
                                                      "Number buffers in global buffer pool."};
    IntOption numberOfBuffersPerWorker = {"numberOfBuffersPerWorker", 128, "Number buffers in task local buffer pool."};
    IntOption numberOfBuffersInSourceLocalBufferPool = {"numberOfBuffersInSourceLocalBufferPool",
                                                        64,
                                                        "Number buffers in source local buffer pool."};
    IntOption bufferSizeInBytes = {"bufferSizeInBytes", 4096, "BufferSizeInBytes."};
    IntOption numWorkerThreads = {"numWorkerThreads", 1, "Number of worker threads."};
    BoolOption enableMonitoring = {"enableMonitoring", false, "Enable monitoring"};
    OptimizerConfiguration optimizerConfig = {"optimizerConfig", "Defines optimizer configuration"};
    SequenceOption<WrapOption<LogicalSourcePtr, LogicalSourceFactory>> sequence = {"sequence", "Defines optimizer configuration"};

  private:
    std::vector<Configurations::BaseOption*> getOptions() override {
        return {&restIp,
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
                &enableMonitoring,
                &optimizerConfig,
                &sequence};
    }
};

}// namespace Configurations
}// namespace NES

#endif// NES_INCLUDE_CONFIGURATIONS_COORDINATOR_NEWCOORDINATORCONFIGURATION_HPP_
